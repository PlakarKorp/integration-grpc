package exporter

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	gconn "github.com/PlakarKorp/integration-grpc"
	"github.com/PlakarKorp/kloset/connectors"
	"github.com/PlakarKorp/kloset/connectors/exporter"
	"github.com/PlakarKorp/kloset/location"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Exporter struct {
	client ExporterClient

	cookie string
	root   string
	typ    string
	origin string
	flags  location.Flags

	// maxConcurrency caps how many payloads we upload at once.
	maxConcurrency int

	// hasPutData tells whether the server understands entry packets and
	// the PutData RPC.  When it does not, we fall back to sending the
	// records and their payloads over the Export stream, one at a time.
	hasPutData bool
}

// payloadJob is the payload for records sent thru PutData.  buf holds
// the bytes already read off rd while sizing the record; if err is
// set, the payload is to be failed.
type payloadJob struct {
	id  uint64
	buf []byte
	rd  io.ReadCloser
	err error
}

const (
	// entries whose size is up to inlineThreshold can travel
	// inline with the record instead of using a PutData stream.
	// Need to stay below the server' MaxRecvsgSize.
	inlineThreshold = 64 * 1024

	// putDataChunkSize is the size for the PutData message.
	putDataChunkSize = 256 * 1024
)

func ignoreEOF(err error) error {
	if errors.Is(err, io.EOF) {
		return nil
	}
	return err
}

func drain(records <-chan *connectors.Record) {
	for record := range records {
		if record.Reader != nil {
			record.Reader.Close()
		}
	}
}

func unwrap(err error) error {
	if err == nil {
		return nil
	}

	status, ok := status.FromError(err)
	if !ok {
		return err
	}

	switch status.Code() {
	case codes.Canceled:
		return context.Canceled
	case codes.Unavailable:
		return fmt.Errorf("I/O error communicating with the integration (%s)", status.Message())
	default:
		return fmt.Errorf("%s", status.Message())
	}
}

func NewExporter(ctx context.Context, client grpc.ClientConnInterface, opts *connectors.Options, proto string, config map[string]string) (exporter.Exporter, error) {
	maxConcurrency := opts.MaxConcurrency
	if maxConcurrency < 1 {
		maxConcurrency = 1
	}

	exporter := &Exporter{
		client:         NewExporterClient(client),
		maxConcurrency: maxConcurrency,
	}

	res, err := exporter.client.Init(ctx, &InitRequest{
		Options: &Options{
			Maxconcurrency: int64(opts.MaxConcurrency),
			Hostname:       opts.Hostname,
			Os:             opts.OperatingSystem,
			Arch:           opts.Architecture,
			Cwd:            opts.CWD,
			Excludes:       opts.Excludes,
			Noxattr:        opts.NoXattr,
		},
		Proto:  proto,
		Config: config,
	})
	if err != nil {
		return nil, unwrap(err)
	}

	exporter.root = res.Root
	exporter.typ = res.Type
	exporter.origin = res.Origin
	exporter.flags = location.Flags(res.Flags)
	exporter.hasPutData = res.HasPutData

	return exporter, nil
}

func (g *Exporter) Origin() string        { return g.origin }
func (g *Exporter) Type() string          { return g.typ }
func (g *Exporter) Root() string          { return g.root }
func (g *Exporter) Flags() location.Flags { return g.flags }

func (g *Exporter) Ping(ctx context.Context) error {
	_, err := g.client.Ping(ctx, &PingRequest{})
	return unwrap(err)
}

func (g *Exporter) Close(ctx context.Context) error {
	_, err := g.client.Close(ctx, &CloseRequest{})
	return unwrap(err)
}

func (g *Exporter) transmitRecords(stream grpc.BidiStreamingClient[ExportRequest, ExportResponse], records <-chan *connectors.Record) error {
	defer drain(records)
	defer stream.CloseSend()

	for record := range records {
		hdr := ExportRequest{
			Packet: &ExportRequest_Record{
				Record: gconn.RecordToProto(record),
			},
		}
		if err := stream.Send(&hdr); err != nil {
			if errors.Is(err, io.EOF) {
				err = nil
			}
			if record.Reader != nil {
				record.Reader.Close()
			}
			return err
		}

		if !hdr.GetRecord().HasReader {
			continue
		}

		sendData := func(buf []byte) error {
			return stream.Send(&ExportRequest{
				Packet: &ExportRequest_Chunk{
					Chunk: buf,
				},
			})
		}

		buf := make([]byte, 1024*1024)
		for {
			n, err := record.Reader.Read(buf)
			if n != 0 {
				if err := sendData(buf[:n]); err != nil {
					if errors.Is(err, io.EOF) {
						err = nil
					}
					record.Reader.Close()
					return err
				}
			}
			if errors.Is(err, io.EOF) {
				record.Reader.Close()
				break
			}
			if err != nil {
				record.Reader.Close()
				return err
			}
		}

		if err := sendData(nil); err != nil {
			if errors.Is(err, io.EOF) {
				err = nil
			}
			return err
		}
	}
	return nil
}

// transmitEntries sends the records in order over the Export stream,
// uploading the payloads that don't fit inline over maxConcurrency
// PutData streams.
func (g *Exporter) transmitEntries(ctx context.Context, stream grpc.BidiStreamingClient[ExportRequest, ExportResponse], records <-chan *connectors.Record) error {
	defer drain(records)

	// whoever fails first cancels the rest and is the one
	// reported; later failures are only fallout.
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	jobs := make(chan payloadJob, g.maxConcurrency)

	var wg sync.WaitGroup
	for range g.maxConcurrency {
		wg.Go(func() {
			// keep draining after a failure: sendEntries is
			// still feeding us and the readers need closing.
			for job := range jobs {
				if err := g.putData(ctx, job); err != nil {
					cancel(err)
				}
			}
		})
	}

	err := g.sendEntries(ctx, stream, records, jobs)
	close(jobs)

	if err != nil {
		// the session is going down; don't wait on payloads that
		// nobody is going to read any more.
		cancel(err)
	}

	// let the exporter know the records are over while we finish to
	// upload the data.
	if err := ignoreEOF(stream.CloseSend()); err != nil {
		cancel(err)
	}

	wg.Wait()
	return context.Cause(ctx)
}

func (g *Exporter) sendEntries(ctx context.Context, stream grpc.BidiStreamingClient[ExportRequest, ExportResponse], records <-chan *connectors.Record, jobs chan<- payloadJob) error {
	send := func(entry *ExportEntry) error {
		return unwrap(ignoreEOF(stream.Send(&ExportRequest{
			Packet: &ExportRequest_Entry{Entry: entry},
		})))
	}

	var id uint64
	for record := range records {
		if err := ctx.Err(); err != nil {
			record.Close()
			return err
		}

		entry := &ExportEntry{Record: gconn.RecordToProto(record)}
		if !entry.Record.HasReader {
			if err := send(entry); err != nil {
				return err
			}
			continue
		}

		buf, inline, err := probe(record)
		if inline {
			entry.Chunk = buf
			record.Reader.Close()
			if err := send(entry); err != nil {
				return err
			}
			continue
		}

		id++
		job := payloadJob{id: id, buf: buf, rd: record.Reader, err: err}

		entry.Id = id
		if err := send(entry); err != nil {
			record.Reader.Close()
			return err
		}

		jobs <- job
	}

	return nil
}

// probe reads the start of a record payload to decide whether it can
// travel inline.  inline is true when the bytes it returns are the
// whole payload; err is a read failure.
//
// We have to try to read because for xattrs the size is not exposed.
func probe(record *connectors.Record) (buf []byte, inline bool, err error) {
	if record.FileInfo.Lsize > inlineThreshold {
		// no point in reading: we know it won't fit.
		return nil, false, nil
	}

	buf = make([]byte, inlineThreshold)
	n, err := io.ReadFull(record.Reader, buf)
	switch {
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		// short read: that was all of it.  Keep the slice
		// non-nil so that an empty payload stays distinct from
		// no payload at all.
		return buf[:n], true, nil
	case err != nil:
		return buf[:n], false, err
	default:
		return buf[:n], false, nil
	}
}

// putData sends one record payload over a new PutData stream.  Any
// reading error on our side are transmitted and will be handled by
// the other side.  Any unexpected failure (i.e. transport level), so
// failures not acknowledged by the other side, are returned.
func (g *Exporter) putData(ctx context.Context, job payloadJob) error {
	defer job.rd.Close()

	stream, err := g.client.PutData(ctx)
	if err != nil {
		return unwrap(err)
	}

	send := func(packet isPutDataRequest_Packet) error {
		return stream.Send(&PutDataRequest{Packet: packet})
	}

	finish := func(reason error) error {
		res, err := stream.CloseAndRecv()
		if err != nil {
			return unwrap(err)
		}
		// was it deliberately halted by the other side?
		if reason != nil && !res.Aborted {
			return fmt.Errorf("integration stopped reading payload %d", job.id)
		}
		return nil
	}

	if err := send(&PutDataRequest_Id{Id: job.id}); err != nil {
		return finish(err)
	}

	if job.err != nil {
		return finish(send(&PutDataRequest_Error{Error: job.err.Error()}))
	}

	if len(job.buf) != 0 {
		if err := send(&PutDataRequest_Chunk{Chunk: job.buf}); err != nil {
			return finish(err)
		}
	}

	buf := make([]byte, putDataChunkSize)
	for {
		n, err := job.rd.Read(buf)
		if n != 0 {
			if err := send(&PutDataRequest_Chunk{Chunk: buf[:n]}); err != nil {
				return finish(err)
			}
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			if err := send(&PutDataRequest_Error{Error: err.Error()}); err != nil {
				return finish(err)
			}
			break
		}
	}

	return finish(nil)
}

func (g *Exporter) receiveResults(stream grpc.BidiStreamingClient[ExportRequest, ExportResponse], results chan<- *connectors.Result) error {
	defer close(results)

	for {
		res, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = nil
			}
			return unwrap(err)
		}

		if res.Result == nil {
			return fmt.Errorf("expected a result")
		}

		result, err := gconn.ResultFromProto(res.Result)
		if err != nil {
			return err
		}

		results <- result
	}
}

func (g *Exporter) Export(ctx context.Context, records <-chan *connectors.Record, results chan<- *connectors.Result) error {
	stream, err := g.client.Export(ctx)
	if err != nil {
		return err
	}

	var wg errgroup.Group

	wg.Go(func() error {
		if g.hasPutData {
			return g.transmitEntries(ctx, stream, records)
		}
		return g.transmitRecords(stream, records)
	})

	wg.Go(func() error {
		return g.receiveResults(stream, results)
	})

	return wg.Wait()
}

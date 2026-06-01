package importer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	gconn "github.com/PlakarKorp/integration-grpc"
	"github.com/PlakarKorp/kloset/connectors"
	"github.com/PlakarKorp/kloset/connectors/importer"
	"github.com/PlakarKorp/kloset/location"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Importer struct {
	client ImporterClient

	cookie string
	typ    string
	origin string
	root   string
	flags  location.Flags
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

func NewImporter(ctx context.Context, client grpc.ClientConnInterface, opts *connectors.Options, proto string, config map[string]string) (importer.Importer, error) {
	importer := &Importer{
		client: NewImporterClient(client),
	}

	initReq := InitRequest{
		Options: &Options{
			Hostname:       opts.Hostname,
			Os:             opts.OperatingSystem,
			Arch:           opts.Architecture,
			Cwd:            opts.CWD,
			Maxconcurrency: int64(opts.MaxConcurrency),
			Excludes:       opts.Excludes,
			Noxattr:        opts.NoXattr,
		},
		Proto:  proto,
		Config: config,
	}

	res, err := importer.client.Init(ctx, &initReq)
	if err != nil {
		return nil, unwrap(err)
	}

	importer.typ = res.Type
	importer.origin = res.Origin
	importer.root = res.Root

	// We always need acknowledgments and forward them to the
	// other side.  It'll be go-kloset-sdk job to not propagate
	// them eventually.
	importer.flags = location.Flags(res.Flags) | location.FLAG_NEEDACK

	return importer, nil
}

func (g *Importer) Origin() string        { return g.origin }
func (g *Importer) Type() string          { return g.typ }
func (g *Importer) Root() string          { return g.root }
func (g *Importer) Flags() location.Flags { return g.flags }

func (g *Importer) Ping(ctx context.Context) error {
	_, err := g.client.Ping(ctx, &PingRequest{})
	return unwrap(err)
}

func (g *Importer) Close(ctx context.Context) error {
	_, err := g.client.Close(ctx, &CloseRequest{})
	return unwrap(err)
}

type streamReader struct {
	ctx    context.Context
	cancel func()
	stream grpc.ServerStreamingClient[OpenResponse]
	buf    bytes.Buffer
}

func (g *Importer) open(parentctx context.Context, record *connectors.Record) (io.ReadCloser, error) {
	ctx, cancel := context.WithCancel(parentctx)

	stream, err := g.client.Open(ctx, &OpenRequest{
		Record: &gconn.Record{
			Pathname:  record.Pathname,
			IsXattr:   record.IsXattr,
			XattrName: record.XattrName,
			XattrType: gconn.ExtendedAttributeType(record.XattrType),
			Target:    record.Target,
		},
	})
	if err != nil {
		cancel() // possibly not needed, but makes LSP happy
		return nil, err
	}

	return &streamReader{
		ctx:    ctx,
		cancel: cancel,
		stream: stream,
	}, nil
}

func (s *streamReader) Read(p []byte) (n int, err error) {
	// A zero-length read must not consume a frame; just confirm we're
	// not at EOF.  io.Reader's contract allows returning (0, nil) here.
	if len(p) == 0 {
		return 0, nil
	}

	if s.buf.Len() != 0 {
		// bytes.Buffer.Read returns (0, io.EOF) only when empty, which
		// we ruled out above.  Any read that yields data is enough.
		n, _ = s.buf.Read(p)
		if n > 0 {
			return n, nil
		}
	}

	fileResponse, err := s.stream.Recv()
	if err != nil {
		if err == io.EOF {
			return 0, io.EOF
		}
		return 0, fmt.Errorf("failed to receive file data: %w", unwrap(err))
	}
	if chunk := fileResponse.GetChunk(); len(chunk) > 0 {
		s.buf.Write(chunk)
		n, _ = s.buf.Read(p)
		if n > 0 {
			return n, nil
		}
	}
	return 0, fmt.Errorf("unexpected response: %v", fileResponse)
}

// Closing of the actual reader is left to the caller, close is not to be
// forwarded over grpc.
// We cancel the per-stream context so the server stops sending, then
// drain so we don't leak the underlying stream goroutine.  Cancellation
// turns the drain into an O(1) operation: the next Recv returns
// context.Canceled (which we treat as a clean shutdown) instead of us
// having to receive the whole remaining payload.
func (s *streamReader) Close() error {
	s.cancel()

	for {
		_, err := s.stream.Recv()
		if err == nil {
			continue
		}
		if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
			return nil
		}
		// gRPC reports a cancelled stream with a Canceled status code
		// rather than the sentinel; unwrap maps it back.
		if errors.Is(unwrap(err), context.Canceled) {
			return nil
		}
		return err
	}
}

func (g *Importer) receiveRecords(ctx context.Context, stream grpc.BidiStreamingClient[ImportRequest, ImportResponse], records chan<- *connectors.Record) error {
	defer close(records)

	for {
		res, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = nil
			}
			return err
		}

		if res != nil && res.Finished {
			return nil
		}

		if res == nil || res.Record == nil {
			return fmt.Errorf("expected a record")
		}

		record, err := gconn.RecordFromProto(res.Record)
		if err != nil {
			return err
		}

		record.Reader = connectors.NewLazyReader(func() (io.ReadCloser, error) {
			return g.open(ctx, record)
		})

		records <- record
	}
}

func (g *Importer) sendResults(stream grpc.BidiStreamingClient[ImportRequest, ImportResponse], results <-chan *connectors.Result) error {
	for result := range results {
		hdr := ImportRequest{
			Result: gconn.ResultToProto(result),
		}
		if err := stream.Send(&hdr); err != nil {
			for range results {
				// The underlying transport died we still need to drain the
				// channel or we will deadlock
			}

			return err
		}
	}

	return stream.CloseSend()
}

func (g *Importer) Import(ctx context.Context, records chan<- *connectors.Record, results <-chan *connectors.Result) error {
	stream, err := g.client.Import(ctx)
	if err != nil {
		return err
	}

	var wg errgroup.Group

	wg.Go(func() error {
		return g.receiveRecords(ctx, stream, records)
	})

	wg.Go(func() error {
		return g.sendResults(stream, results)
	})

	return wg.Wait()
}

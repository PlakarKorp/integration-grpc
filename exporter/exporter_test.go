package exporter

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	gconn "github.com/PlakarKorp/integration-grpc"
	"github.com/PlakarKorp/kloset/connectors"
	"github.com/PlakarKorp/kloset/objects"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// ---------------------------------------------------------------------------
// Fake server
// ---------------------------------------------------------------------------

type fakeServer struct {
	UnimplementedExporterServer

	mu       sync.Mutex
	received []receivedFile

	respond []*connectors.Result

	// earlyExportErr, when set, makes the Export handler abort after its first
	// Recv() and return this error, which grpc turns into a non-OK status
	// in the stream trailer.
	earlyExportErr error

	// sendNilResult makes the Export handler emit an ExportResponse with no
	// Result at all before anything else.
	// Takes precedence over earlyExportErr
	sendNilResult bool
}

type receivedFile struct {
	record  *gconn.Record
	payload []byte
}

func (f *fakeServer) Init(ctx context.Context, req *InitRequest) (*InitResponse, error) {
	return &InitResponse{Origin: "o", Type: "t", Root: "/", Flags: 0}, nil
}
func (f *fakeServer) Ping(ctx context.Context, _ *PingRequest) (*PingResponse, error) {
	return &PingResponse{}, nil
}
func (f *fakeServer) Close(ctx context.Context, _ *CloseRequest) (*CloseResponse, error) {
	return &CloseResponse{}, nil
}

func (f *fakeServer) Export(stream grpc.BidiStreamingServer[ExportRequest, ExportResponse]) error {
	if f.sendNilResult {
		if _, err := stream.Recv(); err != nil {
			return err
		}
		return stream.Send(&ExportResponse{})
	}

	if f.earlyExportErr != nil {
		// Mirrors a connector whose Export() fails: the handler returns
		// while the client may still be sending records.
		if _, err := stream.Recv(); err != nil {
			return err
		}
		return f.earlyExportErr
	}

	var current *gconn.Record
	var buf bytes.Buffer

	// Pre-queue any responses to send after we see at least one record.
	pending := append([]*connectors.Result(nil), f.respond...)

	flush := func() {
		f.mu.Lock()
		f.received = append(f.received, receivedFile{
			record:  current,
			payload: append([]byte(nil), buf.Bytes()...),
		})
		f.mu.Unlock()
		current = nil
		buf.Reset()
	}

	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}

		switch p := req.Packet.(type) {
		case *ExportRequest_Record:
			if current != nil {
				flush()
			}
			current = p.Record
			if !p.Record.HasReader {
				flush()
			}
		case *ExportRequest_Chunk:
			if len(p.Chunk) == 0 {
				// terminator chunk (client sends a zero-length chunk
				// after each file; protobuf may unmarshal nil as []byte{})
				flush()
			} else {
				buf.Write(p.Chunk)
			}
		}

		// echo back a result for each completed file, draining the
		// `pending` list (if any was supplied).
		f.mu.Lock()
		for len(pending) > 0 && len(f.received) > 0 {
			r := pending[0]
			pending = pending[1:]
			if err := stream.Send(&ExportResponse{Result: gconn.ResultToProto(r)}); err != nil {
				f.mu.Unlock()
				return err
			}
		}
		f.mu.Unlock()
	}
	if current != nil {
		flush()
	}
	return nil
}

// ---------------------------------------------------------------------------
// kloset mocking
// ---------------------------------------------------------------------------

// runExport is basically a no-op kloset restore: it feeds n records into the
// channel from a producer goroutine and consumes the results, returning the
// error Export() reported. producerDone reports whether the producer managed
// to finish; a false means it was left blocked on an undrained channel.
func runExport(t *testing.T, exp *Exporter, n int) (err error, producerFinished bool) {
	t.Helper()

	records := make(chan *connectors.Record, 1)
	results := make(chan *connectors.Result, 64)

	producer := make(chan struct{})
	go func() {
		defer close(producer)
		for range n {
			records <- exportableRecord()
		}
		close(records)
	}()

	consumer := make(chan struct{})
	go func() {
		defer close(consumer)
		for range results {
		}
	}()

	errCh := make(chan error, 1)
	go func() { errCh <- exp.Export(t.Context(), records, results) }()

	select {
	case err = <-errCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for Export to return")
	}

	select {
	case <-producer:
		producerFinished = true
	case <-time.After(2 * time.Second):
		// Not fatal: the caller asserts on it, and a blocked producer is
		// exactly the regression this reports.
	}

	select {
	case <-consumer:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for the result consumer to exit: results was never closed")
	}

	return err, producerFinished
}

// ---------------------------------------------------------------------------
// scaffolding
// ---------------------------------------------------------------------------

func dialTestServer(t *testing.T, srv *fakeServer) (*grpc.ClientConn, func()) {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	s := grpc.NewServer()
	RegisterExporterServer(s, srv)
	go func() { _ = s.Serve(lis) }()

	conn, err := grpc.NewClient("passthrough://bufconn",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return conn, func() {
		_ = conn.Close()
		s.Stop()
	}
}

func exportableRecord() *connectors.Record {
	rec := &connectors.Record{
		Pathname: "/f.txt",
		FileInfo: objects.FileInfo{Lname: "f.txt", Lsize: 4, Lmode: 0o644},
	}
	rec.Reader = io.NopCloser(bytes.NewReader([]byte("data")))
	return rec
}

func dialFailing(t *testing.T, srv *fakeServer) *Exporter {
	t.Helper()
	conn, cleanup := dialTestServer(t, srv)
	t.Cleanup(cleanup)

	exp, err := NewExporter(t.Context(), conn, &connectors.Options{}, "p", nil)
	if err != nil {
		t.Fatalf("NewExporter: %v", err)
	}
	return exp.(*Exporter)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestExporter_InitAndGetters(t *testing.T) {
	conn, cleanup := dialTestServer(t, &fakeServer{})
	defer cleanup()

	exp, err := NewExporter(context.Background(), conn, &connectors.Options{}, "p", nil)
	if err != nil {
		t.Fatalf("NewExporter: %v", err)
	}
	if exp.Origin() != "o" || exp.Type() != "t" || exp.Root() != "/" {
		t.Errorf("getters: %q %q %q", exp.Origin(), exp.Type(), exp.Root())
	}
}

func TestExporter_SendsRecordAndChunks(t *testing.T) {
	srv := &fakeServer{
		respond: []*connectors.Result{
			{Record: connectors.Record{Pathname: "/a.txt"}},
		},
	}
	conn, cleanup := dialTestServer(t, srv)
	defer cleanup()

	exp, err := NewExporter(context.Background(), conn, &connectors.Options{}, "p", nil)
	if err != nil {
		t.Fatalf("NewExporter: %v", err)
	}

	payload := bytes.Repeat([]byte("ab"), 100)
	rec := &connectors.Record{
		Pathname: "/a.txt",
		FileInfo: objects.FileInfo{
			Lname: "a.txt",
			Lsize: int64(len(payload)),
			Lmode: 0o644,
		},
		Reader: io.NopCloser(bytes.NewReader(payload)),
	}

	records := make(chan *connectors.Record, 1)
	results := make(chan *connectors.Result, 1)
	records <- rec
	close(records)

	done := make(chan error, 1)
	go func() { done <- exp.Export(context.Background(), records, results) }()

	// Drain results.
	gotResult := false
	for r := range results {
		if r == nil {
			t.Fatalf("nil result")
		}
		gotResult = true
	}
	if !gotResult {
		t.Errorf("expected at least one result from server")
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Export: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for Export")
	}

	srv.mu.Lock()
	defer srv.mu.Unlock()
	if len(srv.received) != 1 {
		t.Fatalf("server saw %d files, want 1", len(srv.received))
	}
	if !bytes.Equal(srv.received[0].payload, payload) {
		t.Errorf("payload mismatch: got %d bytes, want %d", len(srv.received[0].payload), len(payload))
	}
}

func TestExporter_DirectoryRecordSkipsChunks(t *testing.T) {
	srv := &fakeServer{}
	conn, cleanup := dialTestServer(t, srv)
	defer cleanup()

	exp, err := NewExporter(context.Background(), conn, &connectors.Options{}, "p", nil)
	if err != nil {
		t.Fatalf("NewExporter: %v", err)
	}

	rec := &connectors.Record{
		Pathname: "/d",
		FileInfo: objects.FileInfo{
			Lname: "d",
			Lmode: 0o755 | 0x80000000, // dir bit
		},
	}

	records := make(chan *connectors.Record, 1)
	results := make(chan *connectors.Result, 1)
	records <- rec
	close(records)

	done := make(chan error, 1)
	go func() { done <- exp.Export(context.Background(), records, results) }()

	for range results {
	}
	<-done

	srv.mu.Lock()
	defer srv.mu.Unlock()
	if len(srv.received) != 1 || len(srv.received[0].payload) != 0 {
		t.Errorf("expected one empty-payload record, got %+v", srv.received)
	}
}

func TestExporter_ExportErrorIsReported(t *testing.T) {
	exp := dialFailing(t, &fakeServer{
		earlyExportErr: errors.New("simulated export tool failure"),
	})

	err, _ := runExport(t, exp, 50)

	if err == nil {
		t.Fatal("expected the error returned by the plugin's Export(), got nil")
	}
	if errors.Is(err, io.EOF) {
		t.Fatalf("got a bare io.EOF from the send path, want the plugin's error: %v", err)
	}
	if !strings.Contains(err.Error(), "simulated export tool failure") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestExporter_ExportErrorDrainsRecords(t *testing.T) {
	exp := dialFailing(t, &fakeServer{
		earlyExportErr: errors.New("simulated export tool failure"),
	})

	_, producerFinished := runExport(t, exp, 50)

	if !producerFinished {
		t.Fatal("the producer is still blocked writing to records: the channel was not drained")
	}
}

func TestExporter_ExportCanceledIsUnwrapped(t *testing.T) {
	exp := dialFailing(t, &fakeServer{
		earlyExportErr: status.Error(codes.Canceled, "ctx"),
	})

	err, _ := runExport(t, exp, 50)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestExporter_ExportUnavailableIsUnwrapped(t *testing.T) {
	exp := dialFailing(t, &fakeServer{
		earlyExportErr: status.Error(codes.Unavailable, "down"),
	})

	err, _ := runExport(t, exp, 50)

	if err == nil || !strings.Contains(err.Error(), "I/O error") {
		t.Fatalf("expected Unavailable to be wrapped as I/O error, got %v", err)
	}
}

func TestExporter_NilResultIsRejected(t *testing.T) {
	exp := dialFailing(t, &fakeServer{sendNilResult: true})

	err, producerFinished := runExport(t, exp, 5)

	if err == nil {
		t.Fatal("expected an error for a Result-less response, got nil")
	}
	if !strings.Contains(err.Error(), "expected a result") {
		t.Errorf("unexpected error: %v", err)
	}
	if !producerFinished {
		t.Error("the producer is still blocked writing to records")
	}
}

func TestExporter_ExportSucceeds(t *testing.T) {
	exp := dialFailing(t, &fakeServer{})

	err, producerFinished := runExport(t, exp, 5)

	if err != nil {
		t.Fatalf("Export returned: %v", err)
	}
	if !producerFinished {
		t.Error("the producer is still blocked writing to records")
	}
}

// TestExporter_TransmitRecordsCollapsesEOF tests transmitRecords directly,
// because the collapse cannot be observed through Export(): receiveResults is
// already parked in Recv() when the handler dies, so it always reports the
// real status before transmitRecords notices its Send failed. The collapse is
// what keeps that guarantee from resting on scheduling -- errgroup surfaces
// whichever goroutine reports first, and a bare io.EOF would win any race it
// did happen to win.
func TestExporter_TransmitRecordsCollapsesEOF(t *testing.T) {
	srv := &fakeServer{earlyExportErr: errors.New("simulated export tool failure")}
	conn, cleanup := dialTestServer(t, srv)
	defer cleanup()

	exp, err := NewExporter(t.Context(), conn, &connectors.Options{}, "p", nil)
	if err != nil {
		t.Fatalf("NewExporter: %v", err)
	}
	g := exp.(*Exporter)

	stream, err := g.client.Export(t.Context())
	if err != nil {
		t.Fatalf("Export: %v", err)
	}

	records := make(chan *connectors.Record, 1)
	producer := make(chan struct{})
	go func() {
		defer close(producer)
		for i := 0; i < 50; i++ {
			records <- exportableRecord()
		}
		close(records)
	}()

	// The first Send unblocks the handler's single Recv, after which it
	// returns its error and every later Send fails with io.EOF.
	if err := g.transmitRecords(stream, records); err != nil {
		t.Fatalf("transmitRecords leaked a transport error, want nil: %v (isEOF=%v)",
			err, errors.Is(err, io.EOF))
	}

	select {
	case <-producer:
	case <-time.After(2 * time.Second):
		t.Fatal("the producer is still blocked writing to records")
	}
}

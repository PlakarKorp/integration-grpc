package exporter

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
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

	// hasPutData advertises the entry protocol in Init, putting the
	// client on the PutData path.
	hasPutData bool

	// putDataErr makes every PutData handler fail with it.
	putDataErr error

	// failFirstPutData fails the first PutData stream and parks the
	// others until they are cancelled, so that the failure has
	// in-flight payloads to take down with it.
	failFirstPutData bool

	// abortPayloads makes the PutData handlers stop reading after the
	// first chunk and report the payload as deliberately dropped.
	abortPayloads bool

	// putDataBarrier, when set, blocks every PutData handler until it
	// is closed, after signalling putDataArrived.  Used to observe how
	// many payloads are in flight at once.
	putDataBarrier chan struct{}
	putDataArrived chan struct{}

	putDataCalls int

	// entries and payloads are joined into received as the two halves
	// of a record arrive, in either order.
	entries  map[uint64]*gconn.Record
	payloads map[uint64][]byte
	// payloadErrs holds what the client reported it could not read.
	payloadErrs map[uint64]string
}

// join moves a record into received once both its entry and its
// payload have arrived.  Called with mu held.
func (f *fakeServer) join(id uint64) {
	record, ok := f.entries[id]
	if !ok {
		return
	}
	payload, ok := f.payloads[id]
	if !ok {
		return
	}
	delete(f.entries, id)
	delete(f.payloads, id)
	f.received = append(f.received, receivedFile{record: record, payload: payload})
}

func (f *fakeServer) PutData(stream grpc.ClientStreamingServer[PutDataRequest, PutDataResponse]) error {
	if f.putDataErr != nil {
		return f.putDataErr
	}

	f.mu.Lock()
	f.putDataCalls++
	first := f.putDataCalls == 1
	f.mu.Unlock()

	if f.failFirstPutData {
		if first {
			return status.Error(codes.Internal, "nope")
		}
		<-stream.Context().Done()
		return stream.Context().Err()
	}

	if f.putDataBarrier != nil {
		f.putDataArrived <- struct{}{}
		<-f.putDataBarrier
	}

	var (
		id     uint64
		seen   bool
		failed bool
		buf    bytes.Buffer
	)

	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}

		switch p := req.Packet.(type) {
		case *PutDataRequest_Id:
			id, seen = p.Id, true
		case *PutDataRequest_Chunk:
			if !seen {
				return status.Error(codes.InvalidArgument, "chunk before id")
			}
			buf.Write(p.Chunk)
			if f.abortPayloads {
				f.mu.Lock()
				f.payloadErrs[id] = "aborted"
				f.mu.Unlock()
				return stream.SendAndClose(&PutDataResponse{Aborted: true})
			}
		case *PutDataRequest_Error:
			if !seen {
				return status.Error(codes.InvalidArgument, "error before id")
			}
			failed = true
			f.mu.Lock()
			f.payloadErrs[id] = p.Error
			f.mu.Unlock()
		}
	}

	if !seen {
		return status.Error(codes.InvalidArgument, "no id")
	}

	if !failed {
		f.mu.Lock()
		f.payloads[id] = buf.Bytes()
		f.join(id)
		f.mu.Unlock()
	}

	return stream.SendAndClose(&PutDataResponse{})
}

type receivedFile struct {
	record  *gconn.Record
	payload []byte
}

func (f *fakeServer) Init(ctx context.Context, req *InitRequest) (*InitResponse, error) {
	return &InitResponse{Origin: "o", Type: "t", Root: "/", Flags: 0, HasPutData: f.hasPutData}, nil
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
		case *ExportRequest_Entry:
			f.mu.Lock()
			switch {
			case p.Entry.Id != 0:
				f.entries[p.Entry.Id] = p.Entry.Record
				f.join(p.Entry.Id)
			default:
				f.received = append(f.received, receivedFile{
					record:  p.Entry.Record,
					payload: p.Entry.Chunk,
				})
			}
			f.mu.Unlock()
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

// ---------------------------------------------------------------------------
// Entry protocol (PutData)
// ---------------------------------------------------------------------------

func newEntryServer() *fakeServer {
	return &fakeServer{
		hasPutData:  true,
		entries:     make(map[uint64]*gconn.Record),
		payloads:    make(map[uint64][]byte),
		payloadErrs: make(map[uint64]string),
	}
}

func dialEntryServer(t *testing.T, srv *fakeServer, maxConcurrency int) *Exporter {
	t.Helper()
	conn, cleanup := dialTestServer(t, srv)
	t.Cleanup(cleanup)

	opts := &connectors.Options{MaxConcurrency: maxConcurrency}
	exp, err := NewExporter(t.Context(), conn, opts, "p", nil)
	if err != nil {
		t.Fatalf("NewExporter: %v", err)
	}
	if !exp.(*Exporter).hasPutData {
		t.Fatal("client did not pick up has_put_data")
	}
	return exp.(*Exporter)
}

// recordOf builds a record whose reader serves payload, optionally
// failing with readErr once payload is exhausted.
func recordOf(name string, payload []byte, readErr error) *connectors.Record {
	var rd io.Reader = bytes.NewReader(payload)
	if readErr != nil {
		rd = io.MultiReader(rd, errReader{readErr})
	}
	return &connectors.Record{
		Pathname: "/" + name,
		FileInfo: objects.FileInfo{Lname: name, Lsize: int64(len(payload)), Lmode: 0o644},
		Reader:   io.NopCloser(rd),
	}
}

type errReader struct{ err error }

func (e errReader) Read([]byte) (int, error) { return 0, e.err }

// exportRecords runs a whole Export over recs and returns its error.
func exportRecords(t *testing.T, exp *Exporter, recs ...*connectors.Record) error {
	t.Helper()

	records := make(chan *connectors.Record, len(recs))
	for _, rec := range recs {
		records <- rec
	}
	close(records)

	results := make(chan *connectors.Result, len(recs)+1)
	done := make(chan error, 1)
	go func() { done <- exp.Export(t.Context(), records, results) }()

	for range results {
	}

	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for Export to return")
		return nil
	}
}

func TestExporter_EntryInlinesSmallPayloads(t *testing.T) {
	big := bytes.Repeat([]byte("x"), inlineThreshold*3)

	for _, tc := range []struct {
		name     string
		payload  []byte
		putDatas int
	}{
		{"empty", nil, 0},
		{"small", []byte("data"), 0},
		{"threshold", bytes.Repeat([]byte("y"), inlineThreshold-1), 0},
		{"large", big, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := newEntryServer()
			exp := dialEntryServer(t, srv, 4)

			if err := exportRecords(t, exp, recordOf(tc.name, tc.payload, nil)); err != nil {
				t.Fatalf("Export: %v", err)
			}

			srv.mu.Lock()
			defer srv.mu.Unlock()

			if len(srv.received) != 1 {
				t.Fatalf("server saw %d records, want 1", len(srv.received))
			}
			if got := srv.received[0].payload; !bytes.Equal(got, tc.payload) {
				t.Errorf("payload: got %d bytes, want %d", len(got), len(tc.payload))
			}
			if srv.putDataCalls != tc.putDatas {
				t.Errorf("PutData calls: got %d, want %d", srv.putDataCalls, tc.putDatas)
			}
		})
	}
}

func TestExporter_EntrySendsDirectoriesWithoutPayload(t *testing.T) {
	srv := newEntryServer()
	exp := dialEntryServer(t, srv, 4)

	dir := &connectors.Record{
		Pathname: "/d",
		FileInfo: objects.FileInfo{Lname: "d", Lmode: 0o755 | os.ModeDir},
	}
	if err := exportRecords(t, exp, dir); err != nil {
		t.Fatalf("Export: %v", err)
	}

	srv.mu.Lock()
	defer srv.mu.Unlock()

	if len(srv.received) != 1 {
		t.Fatalf("server saw %d records, want 1", len(srv.received))
	}
	if got := srv.received[0].payload; got != nil {
		t.Errorf("directory carried a %d byte payload", len(got))
	}
	if srv.putDataCalls != 0 {
		t.Errorf("directory used %d PutData streams", srv.putDataCalls)
	}
}

// A record we cannot read is the exporter's to report: the session
// carries on.
func TestExporter_EntryReadFailureIsPerRecord(t *testing.T) {
	boom := errors.New("boom")

	for _, tc := range []struct {
		name    string
		payload []byte
	}{
		// fails while probing for the inline threshold
		{"probing", []byte("short")},
		// fails once the payload is already on its way
		{"uploading", bytes.Repeat([]byte("x"), inlineThreshold*3)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := newEntryServer()
			exp := dialEntryServer(t, srv, 4)

			rec := recordOf(tc.name, tc.payload, boom)
			if err := exportRecords(t, exp, rec, recordOf("ok", []byte("fine"), nil)); err != nil {
				t.Fatalf("Export: %v", err)
			}

			srv.mu.Lock()
			defer srv.mu.Unlock()

			if len(srv.payloadErrs) != 1 {
				t.Fatalf("server saw %d payload errors, want 1", len(srv.payloadErrs))
			}
			for _, msg := range srv.payloadErrs {
				if !strings.Contains(msg, boom.Error()) {
					t.Errorf("payload error %q does not mention %q", msg, boom)
				}
			}
			if len(srv.received) != 1 {
				t.Errorf("server completed %d records, want 1 (the good one)", len(srv.received))
			}
		})
	}
}

// An exporter that stops reading a payload says so in the response,
// which is not a failure of the session.
func TestExporter_EntryAbortedPayloadIsNotAnError(t *testing.T) {
	srv := newEntryServer()
	srv.abortPayloads = true
	exp := dialEntryServer(t, srv, 4)

	big := bytes.Repeat([]byte("x"), inlineThreshold*3)
	if err := exportRecords(t, exp, recordOf("big", big, nil)); err != nil {
		t.Fatalf("Export: %v", err)
	}
}

// Anything else going wrong on a PutData stream is transport, and
// takes the session down rather than losing a record quietly.
func TestExporter_PutDataFailureFailsExport(t *testing.T) {
	srv := newEntryServer()
	srv.putDataErr = status.Error(codes.Internal, "nope")
	exp := dialEntryServer(t, srv, 4)

	big := bytes.Repeat([]byte("x"), inlineThreshold*3)
	err := exportRecords(t, exp, recordOf("big", big, nil), recordOf("big2", big, nil))
	if err == nil {
		t.Fatal("Export succeeded despite a failing PutData")
	}
	if !strings.Contains(err.Error(), "nope") {
		t.Errorf("Export: %v, want it to mention the PutData failure", err)
	}
}

// The point of the whole exercise: payloads upload in parallel.
func TestExporter_EntryPayloadsUploadConcurrently(t *testing.T) {
	const concurrency = 4

	srv := newEntryServer()
	srv.putDataBarrier = make(chan struct{})
	srv.putDataArrived = make(chan struct{})
	exp := dialEntryServer(t, srv, concurrency)

	big := bytes.Repeat([]byte("x"), inlineThreshold*3)
	recs := make([]*connectors.Record, concurrency)
	for i := range recs {
		recs[i] = recordOf("big", big, nil)
	}

	done := make(chan error, 1)
	go func() { done <- exportRecords(t, exp, recs...) }()

	// Every handler blocks on the barrier, so seeing all of them
	// arrive means they are all open at the same time.
	for range concurrency {
		select {
		case <-srv.putDataArrived:
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for concurrent PutData streams")
		}
	}
	close(srv.putDataBarrier)

	if err := <-done; err != nil {
		t.Fatalf("Export: %v", err)
	}

	srv.mu.Lock()
	defer srv.mu.Unlock()
	if len(srv.received) != concurrency {
		t.Errorf("server completed %d records, want %d", len(srv.received), concurrency)
	}
}

// A payload failure cancels the payloads still in flight; what comes
// back must be the failure, not the context.Canceled it caused.
func TestExporter_PutDataFailureBeatsTheCancellation(t *testing.T) {
	const concurrency = 4

	srv := newEntryServer()
	srv.failFirstPutData = true
	exp := dialEntryServer(t, srv, concurrency)

	big := bytes.Repeat([]byte("x"), inlineThreshold*3)
	recs := make([]*connectors.Record, concurrency)
	for i := range recs {
		recs[i] = recordOf("big", big, nil)
	}

	err := exportRecords(t, exp, recs...)
	if err == nil {
		t.Fatal("Export succeeded despite a failing PutData")
	}
	if !strings.Contains(err.Error(), "nope") {
		t.Errorf("Export: %v, want the PutData failure rather than its fallout", err)
	}
}

type trackingReader struct {
	io.Reader
	closed atomic.Bool
}

func (r *trackingReader) Close() error {
	r.closed.Store(true)
	return nil
}

// Whatever ends an export, the records we took over and never sent
// still own an open reader until we close it.
func TestExporter_AbandonedRecordsAreClosed(t *testing.T) {
	const count = 50

	for _, tc := range []struct {
		name    string
		srv     *fakeServer
		payload []byte
	}{{
		// the Export stream dies under the sender
		name:    "legacy",
		srv:     &fakeServer{earlyExportErr: errors.New("simulated export tool failure")},
		payload: []byte("data"),
	}, {
		// a payload fails, which cancels the sender
		name:    "entry",
		srv:     newEntryServer(),
		payload: bytes.Repeat([]byte("x"), inlineThreshold*3),
	}} {
		t.Run(tc.name, func(t *testing.T) {
			tc.srv.putDataErr = status.Error(codes.Internal, "nope")

			conn, cleanup := dialTestServer(t, tc.srv)
			t.Cleanup(cleanup)

			exp, err := NewExporter(t.Context(), conn, &connectors.Options{MaxConcurrency: 1}, "p", nil)
			if err != nil {
				t.Fatalf("NewExporter: %v", err)
			}

			readers := make([]*trackingReader, count)
			records := make(chan *connectors.Record)
			go func() {
				defer close(records)
				for i := range readers {
					readers[i] = &trackingReader{Reader: bytes.NewReader(tc.payload)}
					rec := recordOf("f", tc.payload, nil)
					rec.Reader = readers[i]
					records <- rec
				}
			}()

			results := make(chan *connectors.Result, count+1)
			done := make(chan error, 1)
			go func() { done <- exp.Export(t.Context(), records, results) }()

			for range results {
			}

			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("timeout waiting for Export to return")
			}

			for i, rd := range readers {
				if rd == nil {
					t.Fatalf("the producer never got to record %d", i)
				}
				if !rd.closed.Load() {
					t.Fatalf("record %d was abandoned with its reader open", i)
				}
			}
		})
	}
}

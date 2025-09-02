package importer

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/snapshot/importer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	grpc_importer_pkg "github.com/PlakarKorp/integration-grpc/importer/pkg"
)

type GrpcImporter struct {
	GrpcClientScan   grpc_importer_pkg.ImporterClient
	GrpcClientReader grpc_importer_pkg.ImporterClient
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
	default:
		return fmt.Errorf("%s", status.Message())
	}
}

func NewImporter(ctx context.Context, client grpc.ClientConnInterface, opts *importer.Options, proto string, config map[string]string) (importer.Importer, error) {
	importer := &GrpcImporter{
		GrpcClientScan:   grpc_importer_pkg.NewImporterClient(client),
		GrpcClientReader: grpc_importer_pkg.NewImporterClient(client),
	}

	initReq := grpc_importer_pkg.InitRequest{
		Options: &grpc_importer_pkg.Options{
			Hostname:       opts.Hostname,
			Os:             opts.OperatingSystem,
			Arch:           opts.Architecture,
			Cwd:            opts.CWD,
			Maxconcurrency: int64(opts.MaxConcurrency),
		},
		Proto:  proto,
		Config: config,
	}

	res, err := importer.GrpcClientScan.Init(ctx, &initReq)
	if err != nil {
		return nil, unwrap(err)
	}

	if res.Error != nil {
		return nil, fmt.Errorf("%s", *res.Error)
	}

	return importer, nil
}

func (g *GrpcImporter) Origin(ctx context.Context) (string, error) {
	info, err := g.GrpcClientScan.Info(ctx, &grpc_importer_pkg.InfoRequest{})
	if err != nil {
		return "", unwrap(err)
	}
	return info.GetOrigin(), nil
}

func (g *GrpcImporter) Type(ctx context.Context) (string, error) {
	info, err := g.GrpcClientScan.Info(ctx, &grpc_importer_pkg.InfoRequest{})
	if err != nil {
		return "", unwrap(err)
	}
	return info.GetType(), nil
}

func (g *GrpcImporter) Root(ctx context.Context) (string, error) {
	info, err := g.GrpcClientScan.Info(context.Background(), &grpc_importer_pkg.InfoRequest{})
	if err != nil {
		return "", unwrap(err)
	}
	return info.GetRoot(), nil
}

func (g *GrpcImporter) Close(ctx context.Context) error {
	_, err := g.GrpcClientScan.Close(ctx, &grpc_importer_pkg.CloseRequest{})
	if err != nil {
		return fmt.Errorf("failed to close importer: %w", unwrap(err))
	}
	return nil
}

type GrpcReader struct {
	client grpc_importer_pkg.ImporterClient
	stream grpc_importer_pkg.Importer_OpenReaderClient
	path   string
	buf    *bytes.Buffer
	ctx    context.Context
}

func NewGrpcReader(ctx context.Context, client grpc_importer_pkg.ImporterClient, path string) *GrpcReader {
	return &GrpcReader{
		client: client,
		buf:    bytes.NewBuffer(nil),
		path:   path,
		ctx:    ctx,
	}
}

func (g *GrpcReader) Read(p []byte) (n int, err error) {
	if g.buf.Len() != 0 {
		n, err = g.buf.Read(p)
		if n > 0 || err != nil {
			return n, err
		}
	}

	if g.stream == nil {
		g.stream, err = g.client.OpenReader(g.ctx, &grpc_importer_pkg.OpenReaderRequest{
			Pathname: g.path,
		})
		if err != nil {
			return 0, fmt.Errorf("failed to open file %s: %w", g.path, unwrap(err))
		}
	}

	fileResponse, err := g.stream.Recv()
	if err != nil {
		if err == io.EOF {
			return 0, io.EOF
		}
		return 0, fmt.Errorf("failed to receive file data: %w", unwrap(err))
	}
	if fileResponse.GetChunk() != nil {
		g.buf.Write(fileResponse.GetChunk())
		n, err = g.buf.Read(p)
		if n > 0 || err != nil {
			return n, err
		}
	}
	return 0, fmt.Errorf("unexpected response: %v", fileResponse)
}

func (g *GrpcReader) Close() error {
	_, err := g.client.CloseReader(g.ctx, &grpc_importer_pkg.CloseReaderRequest{
		Pathname: g.path,
	})
	if err != nil {
		return fmt.Errorf("failed to close record %s: %w", g.path, unwrap(err))
	}
	return nil
}

func (g *GrpcImporter) Scan(ctx context.Context) (<-chan *importer.ScanResult, error) {
	stream, err := g.GrpcClientScan.Scan(ctx, &grpc_importer_pkg.ScanRequest{})
	if err != nil {
		return nil, fmt.Errorf("failed to start scan: %w", unwrap(err))
	}

	results := make(chan *importer.ScanResult, 1000)
	go func() {
		defer close(results)

		for {
			response, err := stream.Recv()
			if err != nil {
				if err == io.EOF {
					break
				}
				results <- importer.NewScanError("", fmt.Errorf("failed to receive scan response: %w", unwrap(err)))
			}
			isXattr := false
			if response.GetRecord().GetXattr() != nil {
				isXattr = true
			}

			if response.GetRecord() != nil {
				results <- &importer.ScanResult{
					Record: &importer.ScanRecord{
						Pathname: response.GetPathname(),
						Reader: importer.NewLazyReader(func() (io.ReadCloser, error) {
							return NewGrpcReader(ctx, g.GrpcClientReader, response.GetPathname()), nil
						}),
						FileInfo: objects.FileInfo{
							Lname:      response.GetRecord().GetFileinfo().GetName(),
							Lsize:      response.GetRecord().GetFileinfo().GetSize(),
							Lmode:      fs.FileMode(response.GetRecord().GetFileinfo().GetMode()),
							LmodTime:   response.GetRecord().GetFileinfo().GetModTime().AsTime(),
							Ldev:       response.GetRecord().GetFileinfo().GetDev(),
							Lino:       response.GetRecord().GetFileinfo().GetIno(),
							Luid:       response.GetRecord().GetFileinfo().GetUid(),
							Lgid:       response.GetRecord().GetFileinfo().GetGid(),
							Lnlink:     uint16(response.GetRecord().GetFileinfo().GetNlink()),
							Lusername:  response.GetRecord().GetFileinfo().GetUsername(),
							Lgroupname: response.GetRecord().GetFileinfo().GetGroupname(),
						},
						Target:         response.GetRecord().Target,
						FileAttributes: response.GetRecord().GetFileAttributes(),
						IsXattr:        isXattr,
						XattrName:      response.GetRecord().GetXattr().GetName(),
						XattrType:      objects.Attribute(response.GetRecord().GetXattr().GetType()),
					},
					Error: nil,
				}
			} else if response.GetError() != nil {
				results <- importer.NewScanError(response.GetPathname(), fmt.Errorf("scan error: %s", response.GetError().GetMessage()))
			} else {
				results <- importer.NewScanError("", fmt.Errorf("unexpected response: %v", response))
			}
		}
	}()
	return results, nil
}

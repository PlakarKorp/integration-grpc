package exporter

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"sort"
	"strings"
	"sync"

	grpc_exporter "github.com/PlakarKorp/integration-grpc/exporter/pkg"
	"github.com/PlakarKorp/kloset/connectors"
	"github.com/PlakarKorp/kloset/connectors/exporter"
	"github.com/PlakarKorp/kloset/location"
	"github.com/PlakarKorp/kloset/objects"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	// google being google I guess.  No idea why this is actually
	// required, but otherwise it breaks the workspace setup
	// c.f. https://github.com/googleapis/go-genproto/issues/1015
	_ "google.golang.org/genproto/protobuf/ptype"

	"google.golang.org/protobuf/types/known/timestamppb"
)

type LinkType int

const (
	HARDLINK LinkType = iota
	SYMLINK
)

type GrpcExporter struct {
	GrpcClient grpc_exporter.ExporterClient

	cookie string
	root   string
	typ    string
	origin string

	hardlinks      map[string]*hardlinkRecord
	hardlinksMutex sync.Mutex
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

func NewExporter(ctx context.Context, client grpc.ClientConnInterface, opts *connectors.Options, proto string, config map[string]string) (exporter.Exporter, error) {
	exporter := &GrpcExporter{
		GrpcClient: grpc_exporter.NewExporterClient(client),
	}

	res, err := exporter.GrpcClient.Init(ctx, &grpc_exporter.InitRequest{
		Options: &grpc_exporter.Options{
			Maxconcurrency: int64(opts.MaxConcurrency),
		},
		Proto:  proto,
		Config: config,
	})
	if err != nil {
		return nil, unwrap(err)
	}

	root, err := exporter.GrpcClient.Root(ctx, &grpc_exporter.RootRequest{Cookie: res.Cookie})
	if err != nil {
		exporter.Close(ctx)
		return nil, err
	}

	// ugly, but we don't have other ways of getting this info.
	_, orig, ok := strings.Cut(config["location"], "://")
	if ok && !strings.HasPrefix(orig, "/") {
		orig, _, _ = strings.Cut(orig, "/")
	} else {
		orig = "localhost"
	}

	exporter.cookie = res.Cookie
	exporter.root = root.RootPath
	exporter.typ = proto
	exporter.origin = orig
	return exporter, nil
}

func (g *GrpcExporter) Origin() string        { return g.origin }
func (g *GrpcExporter) Type() string          { return g.typ }
func (g *GrpcExporter) Root() string          { return g.root }
func (g *GrpcExporter) Flags() location.Flags { return 0 }

func (g *GrpcExporter) Ping(ctx context.Context) error {
	return errors.ErrUnsupported
}

func (g *GrpcExporter) Close(ctx context.Context) error {
	_, err := g.GrpcClient.Close(ctx, &grpc_exporter.CloseRequest{Cookie: g.cookie})
	return unwrap(err)
}

func (g *GrpcExporter) CreateDirectory(ctx context.Context, pathname string) error {
	_, err := g.GrpcClient.CreateDirectory(ctx, &grpc_exporter.CreateDirectoryRequest{
		Cookie:   g.cookie,
		Pathname: pathname,
	})
	return unwrap(err)
}

func (g *GrpcExporter) SetPermissions(ctx context.Context, pathname string, fileinfo *objects.FileInfo) error {
	_, err := g.GrpcClient.SetPermissions(ctx, &grpc_exporter.SetPermissionsRequest{
		Cookie:   g.cookie,
		Pathname: pathname,
		FileInfo: &grpc_exporter.FileInfo{
			Name:      fileinfo.Lname,
			Mode:      uint32(fileinfo.Lmode),
			ModTime:   timestamppb.New(fileinfo.LmodTime),
			Dev:       fileinfo.Ldev,
			Ino:       fileinfo.Lino,
			Uid:       fileinfo.Luid,
			Gid:       fileinfo.Lgid,
			Nlink:     uint32(fileinfo.Lnlink),
			Username:  fileinfo.Lusername,
			Groupname: fileinfo.Lgroupname,
			Flags:     fileinfo.Flags,
		},
	})
	return unwrap(err)
}

func (g *GrpcExporter) CreateLink(ctx context.Context, oldname string, newname string, ltype LinkType) error {
	_, err := g.GrpcClient.CreateLink(ctx, &grpc_exporter.CreateLinkRequest{
		Cookie:  g.cookie,
		Oldname: oldname,
		Newname: newname,
		Ltype:   uint32(ltype),
	})

	return unwrap(err)
}

func (g *GrpcExporter) StoreFile(ctx context.Context, pathname string, fp io.Reader, size int64) error {
	stream, err := g.GrpcClient.StoreFile(ctx)
	if err != nil {
		return unwrap(err)
	}

	if err := stream.Send(&grpc_exporter.StoreFileRequest{
		Cookie: g.cookie,
		Type: &grpc_exporter.StoreFileRequest_Header{
			Header: &grpc_exporter.Header{
				Pathname: pathname,
				Size:     uint64(size),
			},
		},
	}); err != nil {
		if err == io.EOF {
			_, err = stream.CloseAndRecv()
		}
		return unwrap(err)
	}

	buf := make([]byte, 1024*1024)
	for {
		n, err := fp.Read(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			_, _ = stream.CloseAndRecv()
			return err
		}

		if err := stream.Send(&grpc_exporter.StoreFileRequest{
			Cookie: g.cookie,
			Type: &grpc_exporter.StoreFileRequest_Data{
				Data: &grpc_exporter.Data{
					Chunk: buf[:n],
				},
			},
		}); err != nil {
			if err == io.EOF {
				_, err = stream.CloseAndRecv()
			}
			return unwrap(err)
		}
	}

	_, err = stream.CloseAndRecv()
	return unwrap(err)
}

type dirRec struct {
	path string
	info *objects.FileInfo
}

type hardlinkRecord struct {
	dest string
	done chan struct{}
	err  error
}

func (g *GrpcExporter) restoreFile(ctx context.Context, record *connectors.Record) error {
	var (
		nlink = record.FileInfo.Lnlink
		dev   = record.FileInfo.Ldev
		ino   = record.FileInfo.Lino
		size  = record.FileInfo.Lsize
	)

	var hardlinkKey string
	var rec *hardlinkRecord
	var ok bool
	var isLeader bool
	var leaderErr error
	var leaderDest string

	// Hardlink coordination: exactly one leader writes the file, others wait and link.
	if nlink > 1 {
		hardlinkKey = fmt.Sprintf("%d:%d", dev, ino)

		g.hardlinksMutex.Lock()
		rec, ok = g.hardlinks[hardlinkKey]
		if !ok {
			rec = &hardlinkRecord{done: make(chan struct{})}
			g.hardlinks[hardlinkKey] = rec
			isLeader = true
		}
		g.hardlinksMutex.Unlock()

		// Follower: wait for leader to finish, then create hardlink or propagate error.
		if !isLeader {
			<-rec.done

			if rec.err != nil {
				return rec.err
			}

			if err := g.CreateLink(ctx, rec.dest, record.Pathname, HARDLINK); err != nil {
				return err
			}
			return nil
		}
	}

	// Leader: publish result to followers when done.
	if isLeader {
		defer func() {
			rec.dest = leaderDest
			rec.err = leaderErr
			close(rec.done)
		}()
	}

	if err := g.StoreFile(ctx, record.Pathname, record.Reader, size); err != nil {
		return err
	}

	if err := g.SetPermissions(ctx, record.Pathname, &record.FileInfo); err != nil {
		return err
	}

	if isLeader {
		leaderDest = record.Pathname
	}

	return nil
}

func (g *GrpcExporter) Export(ctx context.Context, records <-chan *connectors.Record, results chan<- *connectors.Result) error {
	defer close(results)

	var (
		directories []dirRec
	)

	for record := range records {
		switch {
		case record.Err != nil:
			// we don't restore errors
			results <- record.Ok()

		case record.IsXattr:
			// we didn't restore xattrs either
			results <- record.Ok()

		default:
			if record.FileInfo.IsDir() {
				if err := g.CreateDirectory(ctx, record.Pathname); err != nil {
					results <- record.Error(err)
					continue
				}

				directories = append(directories, dirRec{
					path: record.Pathname,
					info: &record.FileInfo,
				})

				results <- record.Ok()
				continue
			}

			if record.FileInfo.Mode()&fs.ModeSymlink != 0 {
				if err := g.CreateLink(ctx, record.Target, record.Pathname, SYMLINK); err != nil {
					results <- record.Error(err)
					continue
				}

				if err := g.SetPermissions(ctx, record.Pathname, &record.FileInfo); err != nil {
					results <- record.Error(err)
					continue
				}

				results <- record.Ok()
				continue
			}

			if !record.FileInfo.Mode().IsRegular() {
				// we didn't restore non-regular files
				results <- record.Ok()
				continue
			}

			if err := g.restoreFile(ctx, record); err != nil {
				results <- record.Error(err)
			} else {
				results <- record.Ok()
			}
		}
	}

	sort.Slice(directories, func(i, j int) bool {
		di := strings.Count(directories[i].path, "/")
		dj := strings.Count(directories[j].path, "/")
		return di > dj
	})

	for _, d := range directories {
		// what about failures?
		g.SetPermissions(ctx, d.path, d.info)
	}

	return nil
}

package sdk

import (
	"context"

	grpc_importer "github.com/PlakarKorp/integration-grpc/importer"
	"github.com/PlakarKorp/kloset/connectors"
	"github.com/PlakarKorp/kloset/connectors/importer"
	"github.com/PlakarKorp/kloset/kcontext"
	"google.golang.org/grpc"
)

type importerPluginConn struct {
	importer.Importer

	conn *grpc.ClientConn
}

func (c *importerPluginConn) Close(ctx context.Context) error {
	err := c.Importer.Close(ctx)
	errConn := c.conn.Close()
	if err != nil {
		return err
	}
	return errConn
}

func ExecImporter(ktx *kcontext.KContext, proto string, params map[string]string, opts *connectors.Options, exe string, args []string) (importer.Importer, error) {
	client, err := spawn(ktx.Context, exe, args)
	if err != nil {
		return nil, err
	}

	imp, err := grpc_importer.NewImporter(ktx, client, opts, proto, params)
	if err != nil {
		client.Close()
		return nil, err
	}

	return &importerPluginConn{imp, client}, nil
}

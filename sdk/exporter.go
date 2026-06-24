package sdk

import (
	"context"

	grpc_exporter "github.com/PlakarKorp/integration-grpc/exporter"
	"github.com/PlakarKorp/kloset/connectors"
	"github.com/PlakarKorp/kloset/connectors/exporter"
	"github.com/PlakarKorp/kloset/kcontext"
	"google.golang.org/grpc"
)

type exporterPluginConn struct {
	exporter.Exporter

	conn *grpc.ClientConn
}

func (c *exporterPluginConn) Close(ctx context.Context) error {
	err := c.Exporter.Close(ctx)
	errConn := c.conn.Close()
	if err != nil {
		return err
	}
	return errConn
}

func ExecExporter(ktx *kcontext.KContext, proto string, params map[string]string, opts *connectors.Options, exe string, args []string) (exporter.Exporter, error) {
	client, err := spawn(ktx.Context, exe, args)
	if err != nil {
		return nil, err
	}

	exp, err := grpc_exporter.NewExporter(ktx, client, opts, proto, params)
	if err != nil {
		client.Close()
		return nil, err
	}

	return &exporterPluginConn{exp, client}, nil
}

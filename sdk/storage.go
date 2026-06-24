package sdk

import (
	"context"

	grpc_storage "github.com/PlakarKorp/integration-grpc/storage"
	"github.com/PlakarKorp/kloset/connectors/storage"
	"google.golang.org/grpc"
)

type storagePluginConn struct {
	storage.Store

	conn *grpc.ClientConn
}

func (c *storagePluginConn) Close(ctx context.Context) error {
	err := c.Store.Close(ctx)
	errConn := c.conn.Close()
	if err != nil {
		return err
	}
	return errConn
}

func ExecStorage(ctx context.Context, proto string, params map[string]string, exe string, args []string) (storage.Store, error) {
	client, err := spawn(ctx, exe, args)
	if err != nil {
		return nil, err
	}

	store, err := grpc_storage.NewStorage(ctx, client, proto, params)
	if err != nil {
		client.Close()
		return nil, err
	}

	return &storagePluginConn{store, client}, nil
}

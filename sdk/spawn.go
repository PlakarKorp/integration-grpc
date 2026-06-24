package sdk

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func spawn(ctx context.Context, exe string, args []string) (*grpc.ClientConn, error) {
	cmd := exec.CommandContext(ctx, exe, args...)
	cmd.Stderr = os.Stderr // let child's stderr pass through for logging

	wr, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	rd, err := cmd.StdoutPipe()
	if err != nil {
		wr.Close()
		return nil, err
	}

	stdin, ok := rd.(*os.File)
	if !ok {
		wr.Close()
		rd.Close()
		reason := "stdin is not a file"
		return nil, fmt.Errorf("failed to spawn plugin: %s", reason)
	}

	stdout, ok := wr.(*os.File)
	if !ok {
		wr.Close()
		rd.Close()
		reason := "stdout is not a file"
		return nil, fmt.Errorf("failed to spawn plugin: %s", reason)
	}

	if err := cmd.Start(); err != nil {
		wr.Close()
		rd.Close()
		return nil, err
	}

	conn := newStdioConn(stdin, stdout, cmd, nil)

	client, err := grpc.NewClient("127.0.0.1:0",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return conn, nil
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithIdleTimeout(0),
	)
	if err != nil {
		conn.Close()
		return nil, err
	}
	return client, nil
}

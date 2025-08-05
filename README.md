# gRPC Integration

## What is gRPC?

**gRPC** is a high-performance, open-source universal RPC framework that enables client and server applications to communicate transparently, and makes it easier to build connected systems. It uses HTTP/2 for transport, Protocol Buffers as the interface description language, and provides features such as authentication, load balancing, and more.

This integration allows:

- Seamless backup of files and data using gRPC services into a Kloset repository
- Direct restoration of snapshots to remote destinations via gRPC
- Compatibility with modern systems and tools that use gRPC for communication

## Installation

If a pre-built package exists for your system and architecture,
you can simply install it using:

```sh
$ plakar pkg add grpc
```

Otherwise,
you can first build it:

```sh
$ plakar pkg build grpc
```

This should produce `grpc-vX.Y.Z.ptar` that can be installed with:

```bash
$ plakar pkg add ./grpc-v0.1.0.ptar
```

## Configuration

The configuration parameters are as follows:

- `endpoint` (required): The address of the gRPC server in the form `<host>:<port>`
- `use_tls` (optional): Whether to use TLS for the connection (defaults to false)
- `credentials` (optional): Path to credentials or authentication tokens if required

## Example Usage

```bash
# configure a gRPC source
$ plakar source add myGRPCsrc grpc://grpc.example.org:50051

# backup the source
$ plakar backup @myGRPCsrc

# configure a gRPC destination
$ plakar destination add myGRPCdst grpc://grpc.example.org:50051

# restore the snapshot to the destination
$ plakar restore -to @myGRPCdst <snapid>
```

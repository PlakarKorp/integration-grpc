# gRPC Integration

## Overview

**gRPC** is a high-performance, open-source universal RPC framework that enables client and server applications to communicate transparently, and makes it easier to build connected systems. It uses HTTP/2 for transport, Protocol Buffers as the interface description language, and provides features such as authentication, load balancing, and more.

This integration allows:

- Seamless backup of files and data using gRPC services into a Kloset repository
- Direct restoration of snapshots to remote destinations via gRPC
- Compatibility with modern systems and tools that use gRPC for communication

## Configuration

> **Note:** gRPC integration does not require any specific configuration parameters. It exist as a connector to all the plugins made with the sdk of Plakar.
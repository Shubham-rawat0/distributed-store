# Distributed Store

A peer-to-peer distributed file storage system built in Go. Nodes store and retrieve files across a TCP mesh network with AES-256-CTR encryption and content-addressable storage.

## Architecture

```
┌─────────────────────────────────────────────┐
│              FileServer (Core)               │
│  ┌──────────┐  ┌──────────────────────────┐ │
│  │  Store    │  │  AES-CTR Encryption      │ │
│  │ (disk)   │  │  (copyEncrypt/Decrypt)   │ │
│  └──────────┘  └──────────────────────────┘ │
├─────────────────────────────────────────────┤
│         p2p Package (Network Layer)          │
│  ┌─────────────┐ ┌──────────┐ ┌───────────┐ │
│  │ Transport    │ │ Peer     │ │ Encoding  │ │
│  │ (interface)  │ │ (TCP)    │ │ (GOB/raw) │ │
│  └─────────────┘ └──────────┘ └───────────┘ │
└─────────────────────────────────────────────┘
```

## How It Works

1. **Store** - Data is written to local disk, then broadcast to all peers with an encrypted stream.
2. **Get** - Checks local disk first. If missing, requests the file from peers and decrypts on receipt.
3. **Peers** - Nodes connect via TCP on startup using bootstrap addresses, forming a full mesh.

File paths use a **Content-Addressable Storage (CAS)** layout: keys are SHA-1 hashed and split into nested directory segments for efficient disk organization.

## Project Structure

```
├── main.go            # Entry point; wires up demo nodes
├── server.go          # FileServer: store/get/broadcast/peer management
├── store.go           # On-disk storage with CAS path transform
├── crypto.go          # AES-256-CTR encryption/decryption utilities
├── Makefile           # Build, run, and test targets
└── p2p/
    ├── transport.go       # Peer and Transport interfaces
    ├── tcp_transport.go   # TCP implementation
    ├── encoding.go        # Decoder interface and implementations
    ├── handshake.go       # Handshake function type
    └── message.go         # RPC message type and protocol constants
```

## Getting Started

### Prerequisites

- Go 1.26.4+

### Build & Run

```bash
make build   # builds binary to bin/fs
make run     # builds and runs the demo
make test    # runs all tests
```

### Manual

```bash
go build -o bin/fs .
./bin/fs
```

## Usage

```go
server := NewFileServer(FileServerOpts{
    ID:               ":3000",
    EncKey:           newEncryptionKey(),
    StorageRoot:      "network",
    PathTransformFunc: CASPathTransformFunc,
    Transport:        NewTCPTransport(opts),
    BootStrapNodes:   []string{":4000", ":5000"},
})

server.Start()
server.Store("my-file.txt", fileReader)
data, _ := server.Get("my-file.txt")
server.Stop()
```

## Configuration

| Option | Type | Description |
|--------|------|-------------|
| `ID` | `string` | Server listen address (e.g. `:3000`) |
| `EncKey` | `[]byte` | 32-byte AES-256 encryption key |
| `StorageRoot` | `string` | Root directory for file storage |
| `PathTransformFunc` | `func` | Disk layout strategy (CAS or default) |
| `Transport` | `p2p.Transport` | Network transport implementation |
| `BootStrapNodes` | `[]string` | Peer addresses to connect on startup |

## License

MIT

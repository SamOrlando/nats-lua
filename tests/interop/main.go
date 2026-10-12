package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func main() {
	url, bucket, mode := os.Args[1], os.Args[2], os.Args[3]
	nc, err := nats.Connect(url)
	if err != nil { panic(err) }
	defer nc.Close()
	js, _ := jetstream.New(nc)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	obs, err := js.ObjectStore(ctx, bucket)
	if err != nil { panic(err) }
	switch mode {
	case "read":
		name := os.Args[4]
		b, err := obs.GetBytes(ctx, name)
		if err != nil { fmt.Println("GO_READ_ERR", err); os.Exit(1) }
		info, _ := obs.GetInfo(ctx, name)
		s := sha256.Sum256(b)
		fmt.Printf("GO_READ_OK name=%s size=%d chunks=%d digest=%s sha=%s\n", name, len(b), info.Chunks, info.Digest, hex.EncodeToString(s[:]))
		l, _ := obs.List(ctx)
		fmt.Printf("GO_LIST %d\n", len(l))
	case "write":
		data := make([]byte, 333333)
		for i := range data { data[i] = byte(i*13 + i/7) }
		meta := jetstream.ObjectMeta{Name: "from-go/data.bin", Description: "written by nats.go",
			Headers: nats.Header{"X-Go": []string{"yes"}}, Opts: &jetstream.ObjectMetaOptions{ChunkSize: 40000}}
		info, err := obs.Put(ctx, meta, bytesReader(data))
		if err != nil { panic(err) }
		s := sha256.Sum256(data)
		fmt.Printf("GO_WRITE_OK size=%d chunks=%d digest=%s sha=%s\n", info.Size, info.Chunks, info.Digest, hex.EncodeToString(s[:]))
		gi, _ := obs.GetInfo(ctx, "from-go/data.bin")
		if _, err := obs.AddLink(ctx, "go-link", gi); err != nil { panic(err) }
	case "readlink":
		b, err := obs.GetBytes(ctx, os.Args[4])
		if err != nil { fmt.Println("GO_READ_ERR", err); os.Exit(1) }
		fmt.Printf("GO_LINK_OK size=%d\n", len(b))
	}
}

type br struct{ b []byte; p int }
func (r *br) Read(x []byte) (int, error) { if r.p >= len(r.b) { return 0, ioEOF }; n := copy(x, r.b[r.p:]); r.p += n; return n, nil }
func bytesReader(b []byte) *br { return &br{b: b} }
var ioEOF = io.EOF

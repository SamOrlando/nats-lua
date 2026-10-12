// Copyright (c) 2023-2026 Sam Orlando
// SPDX-License-Identifier: MIT (see LICENSE)
// KV / ordered consumer / service API interop helper for nats-lua (nats.go).
//   kv-read <url> <bucket> <key>          -> KV_READ value=<v> rev=<n> keys=<n>
//   kv-write <url> <bucket> <key> <value> -> KV_WRITE rev=<n>
//   kv-watch <url> <bucket>               -> KV_WATCH initial=<n> (end of initial data reached)
//   ordered <url> <stream> <subject> <n>  -> ORDERED n=<n> last=<seq>
//   micro-serve <url> <name> <seconds>    -> serves endpoint "<name>.echo" (uppercases)
//   micro-discover <url> <name>           -> MICRO ping=<n> endpoints=<n> requests=<n>
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nats.go/micro"
)

func main() {
	mode, url := os.Args[1], os.Args[2]
	nc, err := nats.Connect(url)
	if err != nil { panic(err) }
	defer nc.Close()
	js, _ := jetstream.New(nc)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	switch mode {
	case "kv-read":
		kv, err := js.KeyValue(ctx, os.Args[3])
		if err != nil { fmt.Println("KV_ERR", err); os.Exit(1) }
		e, err := kv.Get(ctx, os.Args[4])
		if err != nil { fmt.Println("KV_ERR", err); os.Exit(1) }
		keys, _ := kv.Keys(ctx)
		h, _ := kv.History(ctx, os.Args[4])
		fmt.Printf("KV_READ value=%s rev=%d keys=%d history=%d\n", string(e.Value()), e.Revision(), len(keys), len(h))
	case "kv-write":
		kv, err := js.KeyValue(ctx, os.Args[3])
		if err != nil { fmt.Println("KV_ERR", err); os.Exit(1) }
		rev, err := kv.Put(ctx, os.Args[4], []byte(os.Args[5]))
		if err != nil { fmt.Println("KV_ERR", err); os.Exit(1) }
		if err := kv.Delete(ctx, "go-deleted"); err != nil { fmt.Println("KV_ERR", err); os.Exit(1) }
		fmt.Printf("KV_WRITE rev=%d\n", rev)
	case "kv-watch":
		kv, err := js.KeyValue(ctx, os.Args[3])
		if err != nil { fmt.Println("KV_ERR", err); os.Exit(1) }
		w, err := kv.WatchAll(ctx)
		if err != nil { fmt.Println("KV_ERR", err); os.Exit(1) }
		n := 0
		for e := range w.Updates() {
			if e == nil { break }
			n++
		}
		fmt.Printf("KV_WATCH initial=%d\n", n)
	case "ordered":
		n, _ := strconv.Atoi(os.Args[5])
		c, err := js.OrderedConsumer(ctx, os.Args[3], jetstream.OrderedConsumerConfig{FilterSubjects: []string{os.Args[4]}})
		if err != nil { fmt.Println("ORDERED_ERR", err); os.Exit(1) }
		got, last := 0, uint64(0)
		for got < n {
			m, err := c.Next(jetstream.FetchMaxWait(5 * time.Second))
			if err != nil { fmt.Println("ORDERED_ERR", err); os.Exit(1) }
			md, _ := m.Metadata()
			last = md.Sequence.Stream
			got++
		}
		fmt.Printf("ORDERED n=%d last=%d\n", got, last)
	case "micro-serve":
		secs, _ := strconv.Atoi(os.Args[4])
		svc, err := micro.AddService(nc, micro.Config{Name: os.Args[3], Version: "1.2.3", Description: "go service"})
		if err != nil { panic(err) }
		svc.AddEndpoint("echo", micro.HandlerFunc(func(r micro.Request) {
			if string(r.Data()) == "fail" { r.Error("400", "bad input", nil); return }
			r.Respond([]byte(strings.ToUpper(string(r.Data()))))
		}), micro.WithEndpointSubject(os.Args[3]+".echo"))
		fmt.Println("MICRO_SERVING")
		time.Sleep(time.Duration(secs) * time.Second)
		svc.Stop()
	case "micro-discover":
		name := os.Args[3]
		pings := collect(nc, "$SRV.PING."+name)
		infos := collect(nc, "$SRV.INFO."+name)
		stats := collect(nc, "$SRV.STATS."+name)
		eps, reqs, errs := 0, 0, 0
		for _, b := range infos {
			var i micro.Info
			if err := json.Unmarshal(b, &i); err != nil { fmt.Println("MICRO_ERR info", err, string(b)); os.Exit(1) }
			eps += len(i.Endpoints)
		}
		for _, b := range stats {
			var s micro.Stats
			if err := json.Unmarshal(b, &s); err != nil { fmt.Println("MICRO_ERR stats", err, string(b)); os.Exit(1) }
			for _, e := range s.Endpoints { reqs += e.NumRequests; errs += e.NumErrors }
		}
		for _, b := range pings {
			var p micro.Ping
			if err := json.Unmarshal(b, &p); err != nil || p.Type != micro.PingResponseType { fmt.Println("MICRO_ERR ping", err, string(b)); os.Exit(1) }
		}
		fmt.Printf("MICRO ping=%d endpoints=%d requests=%d errors=%d\n", len(pings), eps, reqs, errs)
	}
}

func collect(nc *nats.Conn, subj string) [][]byte {
	inbox := nats.NewInbox()
	sub, _ := nc.SubscribeSync(inbox)
	defer sub.Unsubscribe()
	nc.PublishRequest(subj, inbox, nil)
	var out [][]byte
	for {
		m, err := sub.NextMsg(700 * time.Millisecond)
		if err != nil { break }
		out = append(out, m.Data)
	}
	return out
}

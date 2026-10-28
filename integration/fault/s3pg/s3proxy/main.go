// Copyright 2026 The Tessera authors. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// s3proxy is a fault-injecting HTTP proxy intended to sit between Tessera s3pg replicas and an
// S3-compatible server (path-style addressing). Each request independently suffers at most one
// fault, chosen with a seeded RNG (request sequence number + -seed):
//
//   - err:     reply 500 InternalError / 503 SlowDown / 503 ServiceUnavailable without forwarding;
//   - drop:    close (RST) the client connection without forwarding;
//   - ambig:   AMBIGUOUS WRITE - forward the request (the object IS written) and then either reset
//     the client connection or reply 500, so the client believes the write failed;
//   - stall:   hold the request for -stall (beyond client timeouts), then reset without forwarding;
//   - delayed: hold the client for -stall and reset it, but forward the request upstream after a
//     random delay of up to -delay_max (a DELAYED WRITE that lands after the client gave up, as a
//     buffering load balancer or a slow server can produce).
//
// Write (PUT/POST/DELETE) and read (GET/HEAD) requests have separate rates. -latency/-jitter
// delay requests BEFORE they are forwarded (so a delayed read returns the state at the time it is
// forwarded): every request by default, or only reads/writes (-latency_ops) and/or only keys with
// -latency_prefix. The global partition switch makes every request hang (no response) until the
// partition is lifted, at which point those connections are reset.
//
// /stats counts, besides the faults, every request by operation and key class, e.g. "LIST_entries",
// "LIST_entries_partials" (GC), "GET_checkpoint", "PUT_tile", "PUT_checkpoint_ifmatch", "DELETEOBJECTS", and upstream 412 answers
// per class ("412_PUT_checkpoint"), so that S3 requests per integration can be measured.
//
// Control API (on -ctl): GET /ctl?faults=on|off&partition=on|off&<rate>=<float>&latency=<dur>
// &latency_ops=all|reads|writes&latency_prefix=<p>..., GET /stats.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	listen        = flag.String("listen", "127.0.0.1:7010", "listen address")
	upstream      = flag.String("upstream", "http://127.0.0.1:9000", "upstream S3 endpoint")
	ctlAddr       = flag.String("ctl", "127.0.0.1:7011", "control API listen address")
	seed          = flag.Uint64("seed", 1, "RNG seed")
	logFile       = flag.String("log", "", "fault log file (default stderr)")
	enabled       = flag.Bool("faults", true, "inject faults initially")
	onlyKeys      = flag.String("only_prefix", "", "if set, only inject faults for object keys (after the bucket) with this prefix, e.g. 'checkpoint'")
	latencyFilter = flag.Bool("latency_filtered", false, "apply -latency/-jitter only to writes matching -only_prefix (the request is forwarded even if the client gives up meanwhile); same as -latency_ops=writes -latency_prefix=<-only_prefix>")
)

type rates struct {
	WErr, WDrop, WAmbig, WStall, WDelayed      float64
	RErr, RDrop, RStall                        float64
	Latency, Jitter, Stall, DelayMin, DelayMax time.Duration
	// LatencyOps ("all", "reads", "writes") and LatencyPrefix restrict which requests are delayed.
	LatencyOps, LatencyPrefix string
	// WriteLatency is added to every write (on top of Latency), before it is forwarded.
	WriteLatency time.Duration
}

var (
	mu        sync.Mutex
	on        bool
	partition bool
	partCh    = make(chan struct{})
	cur       rates
	reqID     atomic.Uint64
	counts    sync.Map // fault name -> *atomic.Uint64
	flog      *log.Logger
	upClient  = &http.Client{Transport: &http.Transport{Proxy: nil, MaxIdleConnsPerHost: 64}, Timeout: 60 * time.Second}
)

func count(k string) {
	v, _ := counts.LoadOrStore(k, &atomic.Uint64{})
	v.(*atomic.Uint64).Add(1)
}

func init() {
	flag.Float64Var(&cur.WErr, "w_err", 0, "write: probability of a 5xx without forwarding")
	flag.Float64Var(&cur.WDrop, "w_drop", 0, "write: probability of dropping the connection without forwarding")
	flag.Float64Var(&cur.WAmbig, "w_ambig", 0, "write: probability of forwarding and then failing the response (ambiguous write)")
	flag.Float64Var(&cur.WStall, "w_stall", 0, "write: probability of stalling beyond client timeouts, then dropping")
	flag.Float64Var(&cur.WDelayed, "w_delayed", 0, "write: probability of a delayed write (client reset, request forwarded later)")
	flag.Float64Var(&cur.RErr, "r_err", 0, "read: probability of a 5xx")
	flag.Float64Var(&cur.RDrop, "r_drop", 0, "read: probability of dropping the connection")
	flag.Float64Var(&cur.RStall, "r_stall", 0, "read: probability of stalling beyond client timeouts")
	flag.DurationVar(&cur.Latency, "latency", 0, "fixed latency per request")
	flag.DurationVar(&cur.Jitter, "jitter", 0, "random extra latency per request")
	flag.DurationVar(&cur.Stall, "stall", 15*time.Second, "stall duration")
	flag.DurationVar(&cur.DelayMin, "delay_min", 0, "minimum extra delay (after the client gave up) before a delayed write is forwarded")
	flag.DurationVar(&cur.DelayMax, "delay_max", 12*time.Second, "maximum extra delay (after the client gave up) before a delayed write is forwarded")
	flag.StringVar(&cur.LatencyOps, "latency_ops", "all", "requests delayed by -latency/-jitter: all, reads (GET/HEAD) or writes (PUT/POST/DELETE)")
	flag.StringVar(&cur.LatencyPrefix, "latency_prefix", "", "if set, -latency/-jitter only delay requests for keys with this prefix")
	flag.DurationVar(&cur.WriteLatency, "write_latency", 0, "extra latency for every write (PUT/POST/DELETE), before forwarding (independent of -latency_ops/-latency_prefix)")
}

// classify returns the operation (LIST, GET, HEAD, PUT, DELETE, DELETEOBJECTS, ...) and the key class
// (checkpoint, entries, tile, other) of a path-style S3 request.
func classify(r *http.Request, key string) (string, string) {
	op := r.Method
	q := r.URL.Query()
	switch {
	case r.Method == http.MethodGet && key == "" && (q.Has("list-type") || q.Has("prefix")):
		op, key = "LIST", q.Get("prefix")
	case r.Method == http.MethodPost && q.Has("delete"):
		op = "DELETEOBJECTS"
	}
	class := "other"
	switch {
	case key == "checkpoint" || strings.HasSuffix(key, "/checkpoint"):
		class = "checkpoint"
	case strings.Contains(key, "tile/entries/") || strings.Contains(key, "tile/data/"):
		class = "entries"
	case strings.Contains(key, "tile/"):
		class = "tile"
	}
	if op == "LIST" && strings.HasSuffix(key, ".p/") {
		class += "_partials" // garbage collection lists the partial versions of a tile/bundle
	}
	return op, class
}

func reset(w http.ResponseWriter) {
	hj, ok := w.(http.Hijacker)
	if !ok {
		panic("not hijackable")
	}
	c, _, err := hj.Hijack()
	if err != nil {
		return
	}
	if tc, ok := c.(*net.TCPConn); ok {
		_ = tc.SetLinger(0)
	}
	_ = c.Close()
}

func s3err(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_, _ = fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>%s</Code><Message>injected fault</Message><RequestId>fault</RequestId></Error>`, code)
}

// doUpstream forwards the request (with the already-read body) and returns the response with its body read.
func doUpstream(r *http.Request, body []byte) (*http.Response, []byte, error) {
	u := *upstream + r.URL.RequestURI()
	req, err := http.NewRequest(r.Method, u, bytes.NewReader(body))
	if err != nil {
		return nil, nil, err
	}
	for k, vs := range r.Header {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	req.Header.Del("Transfer-Encoding")
	req.ContentLength = int64(len(body))
	req.Host = r.Host
	resp, err := upClient.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	rb, err := io.ReadAll(resp.Body)
	return resp, rb, err
}

func objKey(r *http.Request) string {
	// Path-style: /bucket/key...
	p := r.URL.Path
	if len(p) > 0 && p[0] == '/' {
		p = p[1:]
	}
	for i := 0; i < len(p); i++ {
		if p[i] == '/' {
			return p[i+1:]
		}
	}
	return ""
}

func handler(w http.ResponseWriter, r *http.Request) {
	id := reqID.Add(1)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		reset(w)
		return
	}
	mu.Lock()
	part, pc, enabledNow, rt := partition, partCh, on, cur
	mu.Unlock()
	key := objKey(r)
	op, class := classify(r, key)
	desc := fmt.Sprintf("req=%d %s %s", id, r.Method, r.URL.Path)
	if op == "LIST" {
		desc = fmt.Sprintf("req=%d LIST prefix=%s", id, r.URL.Query().Get("prefix"))
	}
	opName := op + "_" + class
	if op == "DELETEOBJECTS" {
		opName = op
	}
	if r.Header.Get("If-None-Match") != "" {
		desc += " [If-None-Match]"
		opName += "_ifnonematch"
	}
	if v := r.Header.Get("If-Match"); v != "" {
		desc += " [If-Match " + v + "]"
		opName += "_ifmatch"
	}
	count("op_" + opName)
	if part {
		count("partitioned")
		flog.Printf("%s PARTITIONED (held)", desc)
		<-pc
		reset(w)
		return
	}
	rng := rand.New(rand.NewPCG(*seed, id))
	isWrite := r.Method == http.MethodPut || r.Method == http.MethodPost || r.Method == http.MethodDelete
	matches := *onlyKeys == "" || strings.HasPrefix(key, *onlyKeys)
	delayOp := rt.LatencyOps == "" || rt.LatencyOps == "all" || (rt.LatencyOps == "writes") == isWrite
	if (rt.Latency > 0 || rt.Jitter > 0) && delayOp && (rt.LatencyPrefix == "" || strings.HasPrefix(key, rt.LatencyPrefix)) {
		time.Sleep(rt.Latency + time.Duration(rng.Float64()*float64(rt.Jitter)))
	}
	if isWrite && rt.WriteLatency > 0 {
		time.Sleep(rt.WriteLatency)
	}
	fault := ""
	if enabledNow && matches {
		x := rng.Float64()
		write := isWrite
		type opt struct {
			name string
			p    float64
		}
		var opts []opt
		if write {
			opts = []opt{{"err", rt.WErr}, {"drop", rt.WDrop}, {"ambig", rt.WAmbig}, {"stall", rt.WStall}, {"delayed", rt.WDelayed}}
		} else {
			opts = []opt{{"err", rt.RErr}, {"drop", rt.RDrop}, {"stall", rt.RStall}}
		}
		acc := 0.0
		for _, o := range opts {
			acc += o.p
			if x < acc {
				fault = o.name
				break
			}
		}
	}
	switch fault {
	case "err":
		codes := []struct {
			s int
			c string
		}{{500, "InternalError"}, {503, "SlowDown"}, {503, "ServiceUnavailable"}}
		c := codes[rng.IntN(len(codes))]
		count("err")
		flog.Printf("%s ERR %d %s", desc, c.s, c.c)
		s3err(w, c.s, c.c)
		return
	case "drop":
		count("drop")
		flog.Printf("%s DROP (not forwarded)", desc)
		reset(w)
		return
	case "stall":
		count("stall")
		flog.Printf("%s STALL %v then reset (not forwarded)", desc, rt.Stall)
		select {
		case <-time.After(rt.Stall):
		case <-r.Context().Done():
			// Client gave up; keep the server side "stuck" a little longer anyway.
		}
		reset(w)
		return
	case "delayed":
		count("delayed")
		d := rt.DelayMin + time.Duration(rng.Float64()*float64(max(0, rt.DelayMax-rt.DelayMin)))
		flog.Printf("%s DELAYED: client held then reset, forwarding after client gives up + %v", desc, d)
		rc := r.Clone(context.Background())
		go func() {
			<-r.Context().Done()
			time.Sleep(d)
			resp, _, err := doUpstream(rc, body)
			if err != nil {
				flog.Printf("%s DELAYED write forwarded: error %v", desc, err)
				return
			}
			flog.Printf("%s DELAYED write forwarded now: upstream status %d", desc, resp.StatusCode)
		}()
		select {
		case <-time.After(rt.Stall):
			reset(w)
		case <-r.Context().Done():
		}
		return
	}
	resp, rb, err := doUpstream(r, body)
	if err != nil {
		count("upstream_error")
		flog.Printf("%s upstream error: %v", desc, err)
		reset(w)
		return
	}
	if fault == "ambig" {
		count("ambig")
		if rng.IntN(2) == 0 {
			flog.Printf("%s AMBIGUOUS: forwarded (upstream %d), then reset", desc, resp.StatusCode)
			reset(w)
		} else {
			flog.Printf("%s AMBIGUOUS: forwarded (upstream %d), then replied 500", desc, resp.StatusCode)
			s3err(w, 500, "InternalError")
		}
		return
	}
	count("forwarded")
	if resp.StatusCode == http.StatusPreconditionFailed {
		count("upstream_412")
		count("412_" + op + "_" + class)
		flog.Printf("%s upstream 412", desc)
	}
	for k, vs := range resp.Header {
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(rb)))
	w.WriteHeader(resp.StatusCode)
	_, _ = w.Write(rb)
}

func main() {
	flag.Parse()
	out := io.Writer(os.Stderr)
	if *logFile != "" {
		f, err := os.OpenFile(*logFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			log.Fatal(err)
		}
		out = f
	}
	flog = log.New(out, "", log.LstdFlags|log.Lmicroseconds|log.LUTC)
	on = *enabled
	if *latencyFilter {
		cur.LatencyOps, cur.LatencyPrefix = "writes", *onlyKeys
	}
	flog.Printf("s3proxy %s -> %s seed=%d faults=%v rates=%+v", *listen, *upstream, *seed, on, cur)

	ctl := http.NewServeMux()
	ctl.HandleFunc("/ctl", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		mu.Lock()
		if v := q.Get("faults"); v != "" {
			on = v == "on"
		}
		if v := q.Get("partition"); v != "" {
			np := v == "on"
			if partition && !np {
				close(partCh)
				partCh = make(chan struct{})
			}
			partition = np
		}
		for k, d := range map[string]*float64{"w_err": &cur.WErr, "w_drop": &cur.WDrop, "w_ambig": &cur.WAmbig, "w_stall": &cur.WStall,
			"w_delayed": &cur.WDelayed, "r_err": &cur.RErr, "r_drop": &cur.RDrop, "r_stall": &cur.RStall} {
			if v := q.Get(k); v != "" {
				if f, err := strconv.ParseFloat(v, 64); err == nil {
					*d = f
				}
			}
		}
		if q.Has("latency_ops") {
			cur.LatencyOps = q.Get("latency_ops")
		}
		if q.Has("latency_prefix") {
			cur.LatencyPrefix = q.Get("latency_prefix")
		}
		for k, d := range map[string]*time.Duration{"latency": &cur.Latency, "write_latency": &cur.WriteLatency, "jitter": &cur.Jitter, "stall": &cur.Stall, "delay_min": &cur.DelayMin, "delay_max": &cur.DelayMax} {
			if v := q.Get(k); v != "" {
				if f, err := time.ParseDuration(v); err == nil {
					*d = f
				}
			}
		}
		res := map[string]any{"faults": on, "partition": partition, "rates": cur}
		mu.Unlock()
		flog.Printf("ctl %s -> %+v", r.URL.RawQuery, res)
		_ = json.NewEncoder(w).Encode(res)
	})
	ctl.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		m := map[string]uint64{"requests": reqID.Load()}
		counts.Range(func(k, v any) bool { m[k.(string)] = v.(*atomic.Uint64).Load(); return true })
		_ = json.NewEncoder(w).Encode(m)
	})
	go func() { log.Fatal(http.ListenAndServe(*ctlAddr, ctl)) }()
	srv := &http.Server{Addr: *listen, Handler: http.HandlerFunc(handler)}
	log.Fatal(srv.ListenAndServe())
}

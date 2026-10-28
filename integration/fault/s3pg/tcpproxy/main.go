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

// tcpproxy is a fault-injecting TCP proxy intended to sit between Tessera s3pg replicas and
// PostgreSQL. It understands just enough of the PostgreSQL wire protocol to recognise COMMIT
// statements, so that it can produce AMBIGUOUS COMMITS: the COMMIT is forwarded to the server,
// which commits, but the connection is reset before the client sees the result.
//
// Faults (all seedable; each connection gets its own RNG derived from -seed and its sequence number):
//   - reset: with probability -reset_prob per connection, the connection is reset (RST on both
//     sides) once an exponentially distributed number of bytes (mean -reset_mean_bytes) has been
//     forwarded in a randomly chosen direction (mid-transaction faults);
//   - ambiguous commit: each COMMIT is, with probability -commit_ambig_prob, forwarded and the
//     connection reset -commit_ambig_delay later without forwarding the server's reply;
//   - blackhole: with probability -blackhole_prob per connection, forwarding stops in both
//     directions (without closing anything, so the server does not notice a client that gives up)
//     for a random duration up to -blackhole_max once a random offset is reached; afterwards the
//     connection either resumes or is reset;
//   - latency: every forwarded chunk is delayed by -latency plus up to -jitter;
//   - partition: a global switch (control API) that stops all forwarding, holds closes and new
//     connections until lifted.
//
// Control API (on -ctl): GET /ctl?faults=on|off&partition=on|off&reset_prob=..&commit_ambig_prob=..
// &blackhole_prob=..&latency=..&jitter=..&killall=1, GET /stats.
package main

import (
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
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
	listen   = flag.String("listen", "127.0.0.1:7000", "listen address")
	upstream = flag.String("upstream", "127.0.0.1:5530", "upstream (PostgreSQL) address")
	ctlAddr  = flag.String("ctl", "127.0.0.1:7001", "control API listen address")
	seed     = flag.Uint64("seed", 1, "RNG seed")
	logFile  = flag.String("log", "", "fault log file (default stderr)")
	enabled  = flag.Bool("faults", true, "inject faults initially")

	resetProb      = flag.Float64("reset_prob", 0, "per-connection probability of a reset at a random byte offset")
	resetMean      = flag.Float64("reset_mean_bytes", 20000, "mean byte offset (exponential) of a reset")
	commitAmbig    = flag.Float64("commit_ambig_prob", 0, "per-COMMIT probability of an ambiguous commit")
	commitAmbigDly = flag.Duration("commit_ambig_delay", 100*time.Millisecond, "delay between forwarding COMMIT and resetting")
	blackholeProb  = flag.Float64("blackhole_prob", 0.0, "per-connection probability of a blackhole")
	blackholeMax   = flag.Duration("blackhole_max", 15*time.Second, "maximum blackhole duration")
	latency        = flag.Duration("latency", 0, "fixed latency per forwarded chunk")
	jitter         = flag.Duration("jitter", 0, "random extra latency per forwarded chunk")
)

type settings struct {
	Enabled       bool
	Partition     bool
	ResetProb     float64
	ResetMean     float64
	CommitAmbig   float64
	BlackholeProb float64
	BlackholeMax  time.Duration
	Latency       time.Duration
	Jitter        time.Duration
}

var (
	mu       sync.Mutex
	cur      settings
	partCond = sync.NewCond(&mu)
	conns    = map[uint64]*pconn{}

	stats struct {
		Conns, Resets, AmbigCommits, Commits, Blackholes, Partitioned, BytesC2S, BytesS2C atomic.Uint64
	}
	flog *log.Logger
)

func get() settings { mu.Lock(); defer mu.Unlock(); return cur }

// waitPartition blocks while the global partition is active.
func waitPartition() {
	mu.Lock()
	for cur.Partition {
		partCond.Wait()
	}
	mu.Unlock()
}

type pconn struct {
	id      uint64
	c, s    *net.TCPConn
	rngMu   sync.Mutex
	rng     *rand.Rand
	plan    plan
	once    sync.Once
	dead    atomic.Bool
	mute    atomic.Bool // s2c forwarding suppressed (ambiguous commit in progress)
	bhMu    sync.Mutex
	bhUntil time.Time
	bhDone  bool
	sent    [2]uint64 // bytes forwarded per direction (0 = c2s, 1 = s2c)
}

type plan struct {
	resetDir    int
	resetAt     uint64
	hasReset    bool
	bhDir       int
	bhAt        uint64
	hasBH       bool
	bhDur       time.Duration
	bhThenReset bool
}

func (p *pconn) rand() float64 { p.rngMu.Lock(); defer p.rngMu.Unlock(); return p.rng.Float64() }

func dirName(d int) string { return [2]string{"c2s", "s2c"}[d] }

// kill resets both sides of the connection.
func (p *pconn) kill(why string) {
	p.once.Do(func() {
		p.dead.Store(true)
		if why != "" {
			flog.Printf("conn=%d %s", p.id, why)
		}
		for _, c := range []*net.TCPConn{p.c, p.s} {
			if c != nil {
				_ = c.SetLinger(0)
				_ = c.Close()
			}
		}
		mu.Lock()
		delete(conns, p.id)
		mu.Unlock()
	})
}

// closeGently closes both sides without RST (propagates a normal close).
func (p *pconn) closeGently() {
	p.once.Do(func() {
		p.dead.Store(true)
		_ = p.c.Close()
		if p.s != nil {
			_ = p.s.Close()
		}
		mu.Lock()
		delete(conns, p.id)
		mu.Unlock()
	})
}

// waitBlackhole blocks while this connection is blackholed.
func (p *pconn) waitBlackhole() {
	for {
		p.bhMu.Lock()
		until := p.bhUntil
		p.bhMu.Unlock()
		d := time.Until(until)
		if d <= 0 {
			return
		}
		time.Sleep(min(d, 100*time.Millisecond))
	}
}

func (p *pconn) gate() {
	waitPartition()
	p.waitBlackhole()
}

// pgParser tracks client->server message boundaries and detects COMMIT statements.
type pgParser struct {
	startup     bool // still expecting an untyped startup-phase message
	hdr         []byte
	need        int // remaining payload bytes of the current message
	typ         byte
	body        []byte // captured payload prefix for Q / P messages
	pendingSync bool   // a commit was sent with the extended protocol; trigger on Sync
}

// feed processes b and returns the offsets (exclusive end positions within b) at which a COMMIT
// has been completely sent (i.e. the server will execute it).
func (pp *pgParser) feed(b []byte) []int {
	var out []int
	i := 0
	for i < len(b) {
		if pp.need == 0 {
			// Reading a header.
			hlen := 5
			if pp.startup {
				hlen = 4
			}
			take := min(hlen-len(pp.hdr), len(b)-i)
			pp.hdr = append(pp.hdr, b[i:i+take]...)
			i += take
			if len(pp.hdr) < hlen {
				continue
			}
			var l uint32
			if pp.startup {
				pp.typ = 0
				l = binary.BigEndian.Uint32(pp.hdr[0:4])
			} else {
				pp.typ = pp.hdr[0]
				l = binary.BigEndian.Uint32(pp.hdr[1:5])
			}
			pp.hdr = pp.hdr[:0]
			pp.need = int(l) - 4
			pp.body = pp.body[:0]
			if pp.need <= 0 {
				pp.need = 0
				if end := pp.msgDone(); end {
					out = append(out, i)
				}
			}
			continue
		}
		take := min(pp.need, len(b)-i)
		if (pp.typ == 'Q' || pp.typ == 'P' || pp.typ == 0) && len(pp.body) < 64 {
			pp.body = append(pp.body, b[i:i+min(take, 64-len(pp.body))]...)
		}
		pp.need -= take
		i += take
		if pp.need == 0 {
			if end := pp.msgDone(); end {
				out = append(out, i)
			}
		}
	}
	return out
}

func isCommit(s []byte) bool {
	t := strings.ToLower(strings.TrimSpace(strings.TrimRight(string(s), "\x00")))
	t = strings.TrimSuffix(t, ";")
	return t == "commit" || t == "commit transaction" || t == "end"
}

// msgDone is called at the end of each message, returns true if a COMMIT has just been sent.
func (pp *pgParser) msgDone() bool {
	if pp.startup {
		if len(pp.body) >= 4 {
			code := binary.BigEndian.Uint32(pp.body[0:4])
			// SSLRequest / GSSENCRequest: the client sends another startup message next.
			if code == 80877103 || code == 80877104 {
				return false
			}
		}
		pp.startup = false
		return false
	}
	switch pp.typ {
	case 'Q':
		return isCommit(pp.body)
	case 'P':
		// Parse: statement name (cstring) then query (cstring).
		parts := strings.SplitN(string(pp.body), "\x00", 3)
		if len(parts) >= 2 && isCommit([]byte(parts[1])) {
			pp.pendingSync = true
		}
	case 'S':
		if pp.pendingSync {
			pp.pendingSync = false
			return true
		}
	}
	return false
}

func (p *pconn) forward(dir int, src, dst *net.TCPConn) {
	buf := make([]byte, 32*1024)
	var parser *pgParser
	if dir == 0 {
		parser = &pgParser{startup: true}
	}
	for {
		n, err := src.Read(buf)
		if n > 0 {
			chunk := buf[:n]
			p.gate()
			if p.dead.Load() {
				return
			}
			s := get()
			if s.Latency > 0 || s.Jitter > 0 {
				d := s.Latency
				if s.Jitter > 0 {
					d += time.Duration(p.rand() * float64(s.Jitter))
				}
				time.Sleep(d)
			}
			if dir == 1 && p.mute.Load() {
				// Drop server replies while an ambiguous commit is being produced.
				continue
			}
			// Blackhole trigger.
			if s.Enabled && p.plan.hasBH && p.plan.bhDir == dir && !p.bhDone && p.sent[dir]+uint64(n) > p.plan.bhAt {
				p.bhMu.Lock()
				p.bhDone = true
				p.bhUntil = time.Now().Add(p.plan.bhDur)
				p.bhMu.Unlock()
				stats.Blackholes.Add(1)
				flog.Printf("conn=%d BLACKHOLE dir=%s offset=%d dur=%v then_reset=%v", p.id, dirName(dir), p.plan.bhAt, p.plan.bhDur, p.plan.bhThenReset)
				p.gate()
				if p.plan.bhThenReset {
					stats.Resets.Add(1)
					p.kill("RESET after blackhole")
					return
				}
				flog.Printf("conn=%d BLACKHOLE end, resuming", p.id)
			}
			// Random-offset reset.
			if s.Enabled && p.plan.hasReset && p.plan.resetDir == dir && p.sent[dir]+uint64(n) > p.plan.resetAt {
				k := int(p.plan.resetAt - p.sent[dir])
				if k > 0 {
					_, _ = dst.Write(chunk[:k])
				}
				stats.Resets.Add(1)
				p.kill(fmt.Sprintf("RESET dir=%s offset=%d", dirName(dir), p.plan.resetAt))
				return
			}
			// Ambiguous commit.
			if parser != nil {
				ends := parser.feed(chunk)
				ambigAt := -1
				for _, e := range ends {
					stats.Commits.Add(1)
					if s.Enabled && ambigAt < 0 && p.rand() < s.CommitAmbig {
						ambigAt = e
					}
				}
				if ambigAt >= 0 {
					p.mute.Store(true)
					_, _ = dst.Write(chunk[:ambigAt])
					stats.AmbigCommits.Add(1)
					time.Sleep(*commitAmbigDly)
					p.kill(fmt.Sprintf("AMBIGUOUS_COMMIT reset after forwarding COMMIT (c2s offset %d)", p.sent[0]+uint64(ambigAt)))
					return
				}
			}
			if _, werr := dst.Write(chunk); werr != nil {
				p.kill("")
				return
			}
			p.sent[dir] += uint64(n)
			if dir == 0 {
				stats.BytesC2S.Add(uint64(n))
			} else {
				stats.BytesS2C.Add(uint64(n))
			}
		}
		if err != nil {
			// Hold the close while partitioned/blackholed: the peer must not learn about it.
			p.gate()
			if err == io.EOF {
				p.closeGently()
			} else {
				p.kill("")
			}
			return
		}
	}
}

func expo(r *rand.Rand, mean float64) uint64 {
	return uint64(-math.Log(1-r.Float64()) * mean)
}

func handle(id uint64, c *net.TCPConn) {
	s := get()
	r := rand.New(rand.NewPCG(*seed, id))
	p := &pconn{id: id, c: c, rng: r}
	if s.Enabled {
		if r.Float64() < s.ResetProb {
			p.plan.hasReset = true
			p.plan.resetDir = r.IntN(2)
			p.plan.resetAt = expo(r, s.ResetMean)
		}
		if r.Float64() < s.BlackholeProb {
			p.plan.hasBH = true
			p.plan.bhDir = r.IntN(2)
			p.plan.bhAt = expo(r, s.ResetMean)
			p.plan.bhDur = time.Duration(r.Float64()*float64(s.BlackholeMax)) + 500*time.Millisecond
			p.plan.bhThenReset = r.IntN(2) == 0
		}
	}
	mu.Lock()
	conns[id] = p
	part := cur.Partition
	mu.Unlock()
	if part {
		stats.Partitioned.Add(1)
	}
	// A partition also holds new connections (as if SYNs were dropped).
	waitPartition()
	up, err := net.DialTimeout("tcp", *upstream, 5*time.Second)
	if err != nil {
		flog.Printf("conn=%d upstream dial failed: %v", id, err)
		p.kill("")
		return
	}
	p.s = up.(*net.TCPConn)
	if p.dead.Load() {
		_ = p.s.Close()
		return
	}
	go p.forward(0, c, p.s)
	go p.forward(1, p.s, c)
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
	cur = settings{
		Enabled: *enabled, ResetProb: *resetProb, ResetMean: *resetMean, CommitAmbig: *commitAmbig,
		BlackholeProb: *blackholeProb, BlackholeMax: *blackholeMax, Latency: *latency, Jitter: *jitter,
	}
	flog.Printf("tcpproxy %s -> %s seed=%d settings=%+v", *listen, *upstream, *seed, cur)

	http.HandleFunc("/ctl", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		mu.Lock()
		if v := q.Get("faults"); v != "" {
			cur.Enabled = v == "on"
		}
		if v := q.Get("partition"); v != "" {
			cur.Partition = v == "on"
			if !cur.Partition {
				partCond.Broadcast()
			}
		}
		pf := func(k string, d *float64) {
			if v := q.Get(k); v != "" {
				if f, err := strconv.ParseFloat(v, 64); err == nil {
					*d = f
				}
			}
		}
		pd := func(k string, d *time.Duration) {
			if v := q.Get(k); v != "" {
				if f, err := time.ParseDuration(v); err == nil {
					*d = f
				}
			}
		}
		pf("reset_prob", &cur.ResetProb)
		pf("reset_mean_bytes", &cur.ResetMean)
		pf("commit_ambig_prob", &cur.CommitAmbig)
		pf("blackhole_prob", &cur.BlackholeProb)
		pd("blackhole_max", &cur.BlackholeMax)
		pd("latency", &cur.Latency)
		pd("jitter", &cur.Jitter)
		s := cur
		var kill []*pconn
		if q.Get("killall") != "" {
			for _, p := range conns {
				kill = append(kill, p)
			}
		}
		mu.Unlock()
		for _, p := range kill {
			stats.Resets.Add(1)
			p.kill("RESET (killall)")
		}
		flog.Printf("ctl %s -> %+v", r.URL.RawQuery, s)
		_ = json.NewEncoder(w).Encode(s)
	})
	http.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		open := len(conns)
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{
			"conns": stats.Conns.Load(), "open": open, "resets": stats.Resets.Load(), "ambiguous_commits": stats.AmbigCommits.Load(),
			"commits": stats.Commits.Load(), "blackholes": stats.Blackholes.Load(), "partitioned_conns": stats.Partitioned.Load(),
			"bytes_c2s": stats.BytesC2S.Load(), "bytes_s2c": stats.BytesS2C.Load(),
		})
	})
	go func() { log.Fatal(http.ListenAndServe(*ctlAddr, nil)) }()

	l, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatal(err)
	}
	var id uint64
	for {
		c, err := l.Accept()
		if err != nil {
			log.Fatal(err)
		}
		id++
		stats.Conns.Add(1)
		go handle(id, c.(*net.TCPConn))
	}
}

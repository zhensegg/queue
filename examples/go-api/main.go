package main

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

var (
	brokerAddr = envOr("BROKER_ADDR", "127.0.0.1:9090")
	listenAddr = envOr("LISTEN_ADDR", ":8080")
	topicName  = envOr("TOPIC", "api")
	poolSize   = envInt("POOL", 8)
	numWorkers = envInt("CONSUMERS", 2)
	topic      = []byte(topicName)
	payload    = fixedPayload(256)
	recLen     = uint64(8 + len(topic) + len(payload))
)

var (
	pubPool  []*conn
	consPool []*conn
	next     uint64
	published uint64
	consumed uint64
	pFailed  uint64
	cFailed  uint64
	cursor   uint64
	fetchMu  sync.Mutex
	latMu    sync.Mutex
	lat      []uint64
	latCnt   uint64
)

type conn struct {
	mu sync.Mutex
	c  net.Conn
	r  *bufio.Reader
}

func fixedPayload(n int) []byte {
	p := make([]byte, n)
	for i := range p {
		p[i] = 'x'
	}
	copy(p, "zhensegg-api:")
	return p
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func dial() (*conn, error) {
	c, err := net.Dial("tcp", brokerAddr)
	if err != nil {
		return nil, err
	}
	if tcp, ok := c.(*net.TCPConn); ok {
		tcp.SetNoDelay(true)
	}
	return &conn{c: c, r: bufio.NewReaderSize(c, 64*1024)}, nil
}

func frame(op byte, topic []byte, payload []byte) []byte {
	total := 1 + 4 + 4 + len(topic) + len(payload)
	buf := make([]byte, 4+total)
	binary.BigEndian.PutUint32(buf[0:4], uint32(total))
	buf[4] = op
	binary.BigEndian.PutUint32(buf[5:9], uint32(len(topic)))
	binary.BigEndian.PutUint32(buf[9:13], uint32(len(payload)))
	copy(buf[13:], topic)
	copy(buf[13+len(topic):], payload)
	return buf
}

func (cn *conn) roundTrip(op byte, topic, payload []byte) (byte, []byte, error) {
	cn.mu.Lock()
	defer cn.mu.Unlock()

	cn.c.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if _, err := cn.c.Write(frame(op, topic, payload)); err != nil {
		return 0, nil, err
	}
	cn.c.SetReadDeadline(time.Now().Add(5 * time.Second))
	var hdr [4]byte
	if _, err := io.ReadFull(cn.r, hdr[:]); err != nil {
		return 0, nil, err
	}
	body := make([]byte, binary.BigEndian.Uint32(hdr[:]))
	if _, err := io.ReadFull(cn.r, body); err != nil {
		return 0, nil, err
	}
	rop := body[0]
	tl := binary.BigEndian.Uint32(body[1:5])
	pl := binary.BigEndian.Uint32(body[5:9])
	if int(9+tl+pl) != len(body) {
		return 0, nil, errors.New("bad frame length")
	}
	return rop, body[9+tl : 9+tl+pl], nil
}

func (cn *conn) publish() (uint64, error) {
	ts := uint64(time.Now().UnixNano())
	p := make([]byte, len(payload))
	copy(p, payload)
	binary.BigEndian.PutUint64(p[0:8], ts)
	op, pl, err := cn.roundTrip(0x01, topic, p)
	if err != nil {
		return 0, err
	}
	switch op {
	case 0x05:
		return binary.BigEndian.Uint64(pl[0:8]), nil
	case 0x09:
		return 0, errors.New(string(pl))
	default:
		return 0, fmt.Errorf("unexpected op 0x%02x", op)
	}
}

var errNotFound = errors.New("not_found")

func (cn *conn) fetch(off uint64, length uint32) ([]byte, error) {
	p := make([]byte, 12)
	binary.BigEndian.PutUint64(p[0:8], off)
	binary.BigEndian.PutUint32(p[8:12], length)
	cn.mu.Lock()
	defer cn.mu.Unlock()
	cn.c.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if _, err := cn.c.Write(frame(0x03, topic, p)); err != nil {
		return nil, err
	}
	cn.c.SetReadDeadline(time.Now().Add(1 * time.Second))
	var hdr [4]byte
	if _, err := io.ReadFull(cn.r, hdr[:]); err != nil {
		return nil, err
	}
	body := make([]byte, binary.BigEndian.Uint32(hdr[:]))
	if _, err := io.ReadFull(cn.r, body); err != nil {
		return nil, err
	}
	op := body[0]
	tl := binary.BigEndian.Uint32(body[1:5])
	pl := binary.BigEndian.Uint32(body[5:9])
	if int(9+tl+pl) != len(body) {
		return nil, errors.New("bad frame length")
	}
	data := body[9+tl : 9+tl+pl]
	if op == 0x07 {
		return data, nil
	}
	if op == 0x09 {
		return nil, errNotFound
	}
	return nil, fmt.Errorf("unexpected op 0x%02x", op)
}

func acquire() *conn {
	return pubPool[atomic.AddUint64(&next, 1)%uint64(len(pubPool))]
}

func publishHandler(w http.ResponseWriter, _ *http.Request) {
	_, err := acquire().publish()
	if err != nil {
		atomic.AddUint64(&pFailed, 1)
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	atomic.AddUint64(&published, 1)
	w.Write([]byte("ok\n"))
}

func consumerLoop(id int, cn *conn) {
	for {
		fetchMu.Lock()
		cur := atomic.LoadUint64(&cursor)
		data, err := cn.fetch(cur, uint32(recLen))
		if err != nil {
			fetchMu.Unlock()
			if errors.Is(err, errNotFound) {
				time.Sleep(200 * time.Microsecond)
				continue
			}
			atomic.AddUint64(&cFailed, 1)
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if len(data) != len(payload) {
			fetchMu.Unlock()
			time.Sleep(100 * time.Microsecond)
			continue
		}
		atomic.StoreUint64(&cursor, cur+recLen)
		fetchMu.Unlock()

		ts := int64(binary.BigEndian.Uint64(data[0:8]))
		d := uint64(time.Now().UnixNano() - ts)
		latMu.Lock()
		if len(lat) < 100000 {
			lat = append(lat, d)
		} else {
			lat[(latCnt%100000)] = d
		}
		latCnt++
		latMu.Unlock()
		atomic.AddUint64(&consumed, 1)
		_ = id
	}
}

func statsHandler(w http.ResponseWriter, _ *http.Request) {
	latMu.Lock()
	s := make([]uint64, len(lat))
	copy(s, lat)
	latMu.Unlock()
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	pct := func(p float64) uint64 {
		if len(s) == 0 {
			return 0
		}
		return s[int(float64(len(s)-1)*p)]
	}
	pub := atomic.LoadUint64(&published)
	con := atomic.LoadUint64(&consumed)
	json.NewEncoder(w).Encode(map[string]any{
		"published":  pub,
		"consumed":   con,
		"lag":        pub - con,
		"pub_errors": atomic.LoadUint64(&pFailed),
		"con_errors": atomic.LoadUint64(&cFailed),
		"e2e_p50_us": pct(0.50) / 1000,
		"e2e_p99_us": pct(0.99) / 1000,
	})
}

func main() {
	total := poolSize + numWorkers
	for len(pubPool)+len(consPool) < total {
		c, err := dial()
		if err != nil {
			log.Printf("broker not ready (%v), retrying", err)
			time.Sleep(300 * time.Millisecond)
			continue
		}
		if len(pubPool) < poolSize {
			pubPool = append(pubPool, c)
		} else {
			consPool = append(consPool, c)
		}
	}
	log.Printf("connected: %d publish conns + %d consumer conns to %s", len(pubPool), len(consPool), brokerAddr)

	for i := 0; i < numWorkers; i++ {
		go consumerLoop(i, consPool[i])
	}

	http.HandleFunc("/publish", publishHandler)
	http.HandleFunc("/stats", statsHandler)
	http.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("zhensegg-api\n")) })

	log.Printf("listening on %s", listenAddr)
	log.Fatal(http.ListenAndServe(listenAddr, nil))
}

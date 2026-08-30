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
	topic      = []byte(topicName)
	payload    = fixedPayload(256)
)

var (
	pool   []*conn
	next   uint64
	acked  uint64
	failed uint64
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

func (cn *conn) publish() (uint64, error) {
	cn.mu.Lock()
	defer cn.mu.Unlock()

	total := 1 + 4 + 4 + len(topic) + len(payload)
	buf := make([]byte, 4+total)
	binary.BigEndian.PutUint32(buf[0:4], uint32(total))
	buf[4] = 0x01
	binary.BigEndian.PutUint32(buf[5:9], uint32(len(topic)))
	binary.BigEndian.PutUint32(buf[9:13], uint32(len(payload)))
	copy(buf[13:], topic)
	copy(buf[13+len(topic):], payload)

	cn.c.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if _, err := cn.c.Write(buf); err != nil {
		return 0, err
	}

	cn.c.SetReadDeadline(time.Now().Add(5 * time.Second))
	var hdr [4]byte
	if _, err := io.ReadFull(cn.r, hdr[:]); err != nil {
		return 0, err
	}
	body := make([]byte, binary.BigEndian.Uint32(hdr[:]))
	if _, err := io.ReadFull(cn.r, body); err != nil {
		return 0, err
	}
	op := body[0]
	tl := binary.BigEndian.Uint32(body[1:5])
	pl := binary.BigEndian.Uint32(body[5:9])
	if int(9+tl+pl) != len(body) {
		return 0, errors.New("bad frame length")
	}
	switch op {
	case 0x05:
		return binary.BigEndian.Uint64(body[9+tl : 9+tl+8]), nil
	case 0x09:
		return 0, errors.New(string(body[9+tl : 9+tl+pl]))
	default:
		return 0, fmt.Errorf("unexpected op 0x%02x", op)
	}
}

func acquire() *conn {
	return pool[atomic.AddUint64(&next, 1)%uint64(len(pool))]
}

func publishHandler(w http.ResponseWriter, _ *http.Request) {
	off, err := acquire().publish()
	if err != nil {
		atomic.AddUint64(&failed, 1)
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	atomic.AddUint64(&acked, 1)
	w.Write([]byte("ok " + strconv.FormatUint(off, 10) + "\n"))
}

func statsHandler(w http.ResponseWriter, _ *http.Request) {
	json.NewEncoder(w).Encode(map[string]uint64{"acked": atomic.LoadUint64(&acked), "errors": atomic.LoadUint64(&failed)})
}

func main() {
	for len(pool) < poolSize {
		c, err := dial()
		if err != nil {
			log.Printf("broker not ready (%v), retrying", err)
			time.Sleep(300 * time.Millisecond)
			continue
		}
		pool = append(pool, c)
	}
	log.Printf("connected %d conns to broker %s", len(pool), brokerAddr)

	http.HandleFunc("/publish", publishHandler)
	http.HandleFunc("/stats", statsHandler)
	http.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("zhensegg-api\n")) })

	log.Printf("listening on %s", listenAddr)
	log.Fatal(http.ListenAndServe(listenAddr, nil))
}

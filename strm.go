// Package strm provide high level nats jetstream functions.
package strm

import (
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/siuyin/dflt"
)

// Server encapsulates an embedded nats jetstream server.
type Server struct {
	svr *server.Server
	nc  *nats.Conn
	js  nats.JetStreamContext
}

// DB is a pricing key-value database.
type DB struct {
	kv nats.KeyValue
}

var s *Server

func svrInit() *Server {
	host := dflt.EnvString("NATS_HOST", "localhost")
	s = &Server{}
	s.svr = newEmbeddedNATSServer(host)
	s.nc = newNATSConn(host)
	s.js = newJetStream(s.nc)
	return s
}

// DBInit sets up a pricing database.
func DBInit(name string) *DB {
	if s == nil {
		s = svrInit()
	}

	db := &DB{}
	db.kv = newKeyValueStore(s.js, name)
	return db
}

// Close closes the pricing database.
func (db *DB) Close() {
	s.nc.Close()
}

func newEmbeddedNATSServer(host string) *server.Server {
	svr, err := server.NewServer(&server.Options{
		ServerName: "Pricing",
		Host:       host,
		JetStream:  true,
		StoreDir:   "/tmp/pricing",
	})
	if err != nil {
		log.Fatal(err)
	}

	svr.Start()
	for {
		if svr.ReadyForConnections(100 * time.Millisecond) {
			break
		}
	}
	return svr
}

func newNATSConn(host string) *nats.Conn {
	nc, err := nats.Connect(fmt.Sprintf("nats://%s:4222", host))
	if err != nil {
		log.Fatal(err)
	}
	return nc
}

func newJetStream(nc *nats.Conn) nats.JetStreamContext {
	js, err := nc.JetStream()
	if err != nil {
		log.Fatal(err)
	}
	return js
}

func newKeyValueStore(js nats.JetStreamContext, name string) nats.KeyValue {
	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: name})
	if err != nil {
		log.Fatal(err)
	}
	return kv
}

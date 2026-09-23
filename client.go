package dataflow

import (
	"context"
	"crypto/tls"
	"encoding/hex"
	"math/rand/v2"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/huginnlabs-dev/dataflow-go/encoder"
	pb "github.com/huginnlabs-dev/dataflow-go/gen/dataflowpb"
)

// pipeline wires the event path from Span.End to the SaaS ingestion API:
// events land in the replay buffer and a background goroutine streams them
// over gRPC, trimming the buffer as the server acknowledges durability.
type pipeline struct {
	buf     *eventBuffer
	acked   atomic.Int64
	wake    chan struct{}
	cfg     *settings
	encKey  []byte
	encSalt string
}

var globalPipeline atomic.Pointer[pipeline]

// startSender initialises the pipeline and spawns the streaming loop. It
// runs exactly once per process (guarded by startOnce in Configure).
func startSender(s *settings) {
	if s.endpoint == "" {
		s.cfg.Logger.Printf("dataflow: no endpoint configured; SDK stays passive (set DATAFLOW_ENDPOINT or Config.Endpoint)")
		return
	}
	if s.cfg.APIKey == "" {
		s.cfg.Logger.Printf("dataflow: no API key configured; SDK stays passive (set DATAFLOW_API_KEY or Config.APIKey)")
		return
	}
	p := &pipeline{
		buf:  newEventBuffer(orDefault(s.cfg.BufferSize, 10000)),
		wake: make(chan struct{}, 1),
		cfg:  s,
	}
	if s.encrypted {
		salt, err := encoder.SaltFromHex("")
		if err == nil {
			var key []byte
			key, err = encoder.DeriveKey(s.cfg.EncryptionKey, salt)
			if err == nil {
				p.encKey, p.encSalt = key, hex.EncodeToString(salt)
			}
		}
		if err != nil {
			s.cfg.Logger.Printf("dataflow: encryption disabled, key setup failed: %v", err)
		}
	} else {
		s.cfg.Logger.Printf("dataflow: warning: no encryption key set; captured payloads are sent as plaintext")
	}
	globalPipeline.Store(p)
	go p.run()
}

// enqueue is the single entry point from Span.End into the delivery path.
func enqueue(ev *pb.TraceEvent) {
	p := globalPipeline.Load()
	if p == nil {
		return
	}
	p.buf.Add(ev)
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

func encryptionSalt() string {
	if p := globalPipeline.Load(); p != nil {
		return p.encSalt
	}
	return ""
}

func encryptPayload(raw []byte) ([]byte, []byte, error) {
	p := globalPipeline.Load()
	if p == nil {
		return nil, nil, context.Canceled
	}
	return encoder.Encrypt(p.encKey, raw)
}

// run keeps a streaming connection alive forever, backing off exponentially
// (capped at 30s) across failures.
func (p *pipeline) run() {
	backoff := time.Second
	for {
		if err := p.streamOnce(); err != nil {
			p.cfg.logger().Printf("dataflow: ingest stream error: %v; retrying in %s", err, backoff)
			time.Sleep(backoff + time.Duration(rand.IntN(500))*time.Millisecond)
			backoff = min(backoff*2, 30*time.Second)
			continue
		}
		backoff = time.Second
	}
}

// streamOnce dials the ingestion endpoint, replays everything above the
// last acked sequence and then follows live events. Each sent event is
// acknowledged before the next is sent; the ack watermark lives on the
// pipeline so it survives reconnects.
func (p *pipeline) streamOnce() error {
	creds := grpc.DialOption(grpc.WithTransportCredentials(insecure.NewCredentials()))
	if p.cfg.useTLS {
		creds = grpc.DialOption(grpc.WithTransportCredentials(
			credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12})))
	}
	conn, err := grpc.NewClient(p.cfg.endpoint, creds)
	if err != nil {
		return err
	}
	defer conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	md := metadata.Pairs("x-api-key", p.cfg.cfg.APIKey)
	stream, err := pb.NewDataflowServiceClient(conn).
		StreamEvents(metadata.NewOutgoingContext(ctx, md))
	if err != nil {
		return err
	}

	ackCh := make(chan int64, 32)
	errCh := make(chan error, 2)
	go func() {
		for {
			ack, err := stream.Recv()
			if err != nil {
				errCh <- err
				return
			}
			select {
			case ackCh <- ack.LastSeq:
			case <-ctx.Done():
				return
			}
		}
	}()

	for {
		pending := p.buf.After(p.acked.Load())
		if len(pending) == 0 {
			select {
			case <-p.wake:
			case <-ctx.Done():
				return ctx.Err()
			}
			continue
		}
		// Send a bounded window, then wait for the server watermark to
		// cover it; unacked events stay in the buffer for replay.
		pending = pending[:min(len(pending), sendWindow)]
		last := pending[len(pending)-1].Seq
		for _, ev := range pending {
			if err := stream.Send(ev); err != nil {
				return err
			}
		}
		for p.acked.Load() < last {
			select {
			case a := <-ackCh:
				if a > p.acked.Load() {
					p.acked.Store(a)
					p.buf.Acked(a)
				}
			case err := <-errCh:
				return err
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

// sendWindow is how many events are pushed before waiting on acks.
const sendWindow = 64

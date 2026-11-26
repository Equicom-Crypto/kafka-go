package kafka

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"strconv"
	"testing"
	"time"

	ktesting "github.com/segmentio/kafka-go/testing"
)

func TestClientInitProducerId(t *testing.T) {
	if !ktesting.KafkaIsAtLeast("0.11.0") {
		return
	}

	// TODO: look into why this test fails on Kafka 3.0.0 and higher when transactional support
	// work is revisited.
	if ktesting.KafkaIsAtLeast("3.0.0") {
		t.Skip("Skipping test because it fails on Kafka version 3.0.0 or higher.")
	}

	client, shutdown := newLocalClient()
	defer shutdown()

	tid := makeTransactionalID()
	// Wait for kafka setup and Coordinator to be available.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	respc, err := waitForCoordinatorIndefinitely(ctx, client, &FindCoordinatorRequest{
		Addr:    client.Addr,
		Key:     tid,
		KeyType: CoordinatorKeyTypeTransaction,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Now establish a connection with the transaction coordinator
	transactionCoordinator := TCP(net.JoinHostPort(respc.Coordinator.Host, strconv.Itoa(int(respc.Coordinator.Port))))
	client, shutdown = newClient(transactionCoordinator)
	defer shutdown()

	// Check if producer epoch increases and PID remains the same when producer is
	// initialized again with the same transactionalID
	resp, err := client.InitProducerID(context.Background(), &InitProducerIDRequest{
		Addr:                 transactionCoordinator,
		TransactionalID:      tid,
		TransactionTimeoutMs: 30000,
	})
	if err != nil {
		t.Fatal(err)
	}

	if resp.Error != nil {
		t.Fatal(resp.Error)
	}

	epoch1 := resp.Producer.ProducerEpoch
	pid1 := resp.Producer.ProducerID

	resp, err = client.InitProducerID(context.Background(), &InitProducerIDRequest{
		Addr:                 transactionCoordinator,
		TransactionalID:      tid,
		TransactionTimeoutMs: 30000,
		ProducerID:           pid1,
		ProducerEpoch:        epoch1,
	})
	if err != nil {
		t.Fatal(err)
	}

	if resp.Error != nil {
		t.Fatal(resp.Error)
	}

	epoch2 := resp.Producer.ProducerEpoch
	pid2 := resp.Producer.ProducerID

	if pid1 != pid2 {
		t.Fatalf("PID should stay the same across producer sessions; expected: %v got: %v", pid1, pid2)
	}

	if epoch2-epoch1 <= 0 {
		t.Fatal("Epoch should increase when producer is initialized again with the same transactionID")
	}

	// Checks if transaction timeout is too high
	// Transaction timeout should never be higher than broker config `transaction.max.timeout.ms`
	resp, _ = client.InitProducerID(context.Background(), &InitProducerIDRequest{
		Addr:                 client.Addr,
		TransactionalID:      tid,
		TransactionTimeoutMs: 30000000,
	})
	if !errors.Is(resp.Error, InvalidTransactionTimeout) {
		t.Fatal("Should have errored with: Transaction timeout specified is higher than `transaction.max.timeout.ms`")
	}
}

// apiKeyConn records the api key of every request frame written to it.
type apiKeyConn struct {
	net.Conn
	keys chan<- int16
	buf  []byte
}

func (c *apiKeyConn) Write(b []byte) (int, error) {
	c.buf = append(c.buf, b...)
	for len(c.buf) >= 6 {
		size := int(int32(binary.BigEndian.Uint32(c.buf[:4])))
		if len(c.buf) < 4+size {
			break
		}
		c.keys <- int16(binary.BigEndian.Uint16(c.buf[4:6]))
		c.buf = c.buf[4+size:]
	}
	return c.Conn.Write(b)
}

func TestClientInitProducerIDWithoutTransactionalID(t *testing.T) {
	if !ktesting.KafkaIsAtLeast("0.11.0") {
		return
	}
	keys := make(chan int16, 1024)
	dialer := &net.Dialer{}
	client := &Client{
		Addr:    TCP("localhost"),
		Timeout: 5 * time.Second,
		Transport: &Transport{
			Dial: func(ctx context.Context, network, addr string) (net.Conn, error) {
				c, err := dialer.DialContext(ctx, network, addr)
				if err != nil {
					return nil, err
				}
				return &apiKeyConn{Conn: c, keys: keys}, nil
			},
		},
	}
	defer client.Transport.(*Transport).CloseIdleConnections()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var pids []int
	for i := 0; i < 2; i++ {
		resp, err := client.InitProducerID(ctx, &InitProducerIDRequest{ProducerID: -1, ProducerEpoch: -1})
		if err != nil {
			t.Fatal(err)
		}
		if resp.Error != nil {
			t.Fatal(resp.Error)
		}
		if resp.Producer.ProducerID < 0 || resp.Producer.ProducerEpoch != 0 {
			t.Fatalf("unexpected producer identity %+v", *resp.Producer)
		}
		pids = append(pids, resp.Producer.ProducerID)
	}
	if pids[0] == pids[1] {
		t.Fatalf("expected a new producer id per initialization, got %d twice", pids[0])
	}
	close(keys)
	for key := range keys {
		if key == int16(10) { // FindCoordinator
			t.Fatal("InitProducerId without a transactional id looked up a transaction coordinator")
		}
	}
}

package eslgo

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/asseco-voice/eslgo/command"
	"github.com/rs/zerolog"
)

// A connection whose socket has died must be torn down rather than left looking
// alive. Silently keeping it means no event is ever delivered again and every
// sender waits forever for a reply nobody is left to read - with no disconnect
// reported, so the owner never reconnects.

func TestReceiveLoopClosesConnectionOnReadError(t *testing.T) {
	server, client := net.Pipe()
	defer client.Close()

	disconnected := make(chan string, 1)
	connection := NewConnection(client, false, zerolog.Nop(), "test-read-error", func(id string) {
		disconnected <- id
	})
	defer connection.Close()

	// Killing the far end makes the next read fail.
	if err := server.Close(); err != nil {
		t.Fatalf("failed closing the server end of the pipe: %v", err)
	}

	select {
	case id := <-disconnected:
		if id != "test-read-error" {
			t.Fatalf("disconnect reported for the wrong connection: %s", id)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a dead socket must tear the connection down and report the disconnect")
	}

	if connection.runningContext.Err() == nil {
		t.Fatal("the running context must be cancelled once the socket is dead")
	}
}

// syncBuffer collects a connection's log output. A Conn logs from all of its
// goroutines - receive loop, event loop, context loop, auth loop - so the plain
// bytes.Buffer this replaced was itself a data race, reported under -race even
// though the library was clean.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

// A per-call connection ends when FreeSWITCH closes the socket after the call
// hangs up - twice per call, on every call. Logging that as an error buries the
// real ones: at production call volumes it is hundreds of ERROR lines an hour on
// a perfectly healthy system, which is enough to trip error-rate alerting. The
// teardown itself must still happen; only the severity differs.
func TestReceiveLoopLogsCleanPeerCloseBelowError(t *testing.T) {
	logged := &syncBuffer{}
	logger := zerolog.New(logged)

	server, client := net.Pipe()
	defer client.Close()

	disconnected := make(chan string, 1)
	connection := NewConnection(client, false, logger, "test-clean-close", func(id string) {
		disconnected <- id
	})
	defer connection.Close()

	// A plain Close on the far end is an ordinary FIN: the reader sees io.EOF.
	if err := server.Close(); err != nil {
		t.Fatalf("failed closing the server end of the pipe: %v", err)
	}

	select {
	case <-disconnected:
	case <-time.After(5 * time.Second):
		t.Fatal("a closed socket must still tear the connection down")
	}

	out := logged.String()
	if strings.Contains(out, `"level":"error"`) {
		t.Fatalf("a clean peer close must not be logged at error level:\n%s", out)
	}
	if !strings.Contains(out, "peer closed the connection") {
		t.Fatalf("expected the clean-close teardown to be logged, got:\n%s", out)
	}
}

func TestSendCommandReturnsWhenConnectionDies(t *testing.T) {
	server, client := net.Pipe()
	defer client.Close()

	connection := NewConnection(client, false, zerolog.Nop(), "test-sender-release", nil)
	defer connection.Close()

	// net.Pipe is unbuffered, so the command's write needs a reader on the far end;
	// the point of the test is the wait for a reply that never comes.
	go func() { _, _ = io.Copy(io.Discard, server) }()

	result := make(chan error, 1)
	go func() {
		_, err := connection.SendCommand(context.Background(), command.API{Command: "status"})
		result <- err
	}()

	// Give the sender time to reach the wait before the socket dies under it.
	time.Sleep(100 * time.Millisecond)
	if err := server.Close(); err != nil {
		t.Fatalf("failed closing the server end of the pipe: %v", err)
	}

	select {
	case err := <-result:
		if err == nil {
			t.Fatal("a sender waiting on a dead connection must fail, not succeed")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a dead connection must release senders waiting for a reply")
	}
}

// serveReplies answers every command read off the socket with a bare +OK, standing
// in for FreeSWITCH. Stops when the socket dies or count replies have been sent;
// count <= 0 serves until then.
func serveReplies(conn net.Conn, count int) {
	reader := bufio.NewReader(conn)
	for sent := 0; count <= 0 || sent < count; sent++ {
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				return
			}
			if line == "\r\n" || line == "\n" {
				break // end of this command
			}
		}
		if _, err := conn.Write([]byte("Content-Type: command/reply\r\nReply-Text: +OK\r\n\r\n")); err != nil {
			return
		}
	}
}

// A write deadline belongs to the socket, not to the command that set it, and it is
// absolute. A bounded command must therefore not leave its expired deadline behind
// for the next caller: on a long-lived connection the two mix freely - probes and
// registration lookups are bounded, while leg commands are not - and a stale
// deadline fails every later deadline-free write instantly with i/o timeout.
func TestSendCommandClearsDeadlineForUnboundedCallers(t *testing.T) {
	server, client := net.Pipe()
	defer client.Close()
	defer server.Close()

	connection := NewConnection(client, false, zerolog.Nop(), "test-deadline-residue", nil)
	defer connection.Close()

	go serveReplies(server, 2)

	bounded, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := connection.SendCommand(bounded, command.API{Command: "status"}); err != nil {
		t.Fatalf("the bounded command should have been answered well inside its deadline: %v", err)
	}

	// Let the deadline the first command installed fall into the past.
	time.Sleep(200 * time.Millisecond)

	if _, err := connection.SendCommand(context.Background(), command.API{Command: "status"}); err != nil {
		t.Fatalf("a command with no deadline must not inherit the previous command's expired one: %v", err)
	}
}

// Concurrent senders must still be serialised one command at a time, and every one
// of them must get a reply. Exercises the write lock under load - run with -race.
func TestSendCommandSerialisesConcurrentSenders(t *testing.T) {
	server, client := net.Pipe()
	defer client.Close()
	defer server.Close()

	connection := NewConnection(client, false, zerolog.Nop(), "test-concurrent-senders", nil)
	defer connection.Close()

	const senders = 25

	// Stand-in for FreeSWITCH: one reply per command read off the socket.
	go func() {
		reader := bufio.NewReader(server)
		for i := 0; i < senders; i++ {
			for {
				line, err := reader.ReadString('\n')
				if err != nil {
					return
				}
				if line == "\r\n" || line == "\n" {
					break // end of this command
				}
			}
			if _, err := server.Write([]byte("Content-Type: command/reply\r\nReply-Text: +OK\r\n\r\n")); err != nil {
				return
			}
		}
	}()

	var wait sync.WaitGroup
	failures := make(chan error, senders)
	for i := 0; i < senders; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			response, err := connection.SendCommand(ctx, command.API{Command: "status"})
			if err != nil {
				failures <- err
				return
			}
			if !response.IsOk() {
				failures <- fmt.Errorf("unexpected reply: %s", response.GetReply())
			}
		}()
	}
	wait.Wait()
	close(failures)

	for err := range failures {
		t.Fatalf("a concurrent sender did not get its reply: %v", err)
	}
}

// One command at a time per connection is required for correctness: the reply
// channels carry no correlation id. Waiting for that turn must still respect the
// caller's deadline, so a stuck command cannot wedge every other sender.
func TestSendCommandHonoursDeadlineWhileQueuedBehindAnotherSender(t *testing.T) {
	server, client := net.Pipe()
	defer client.Close()
	defer server.Close()

	connection := NewConnection(client, false, zerolog.Nop(), "test-queued-deadline", nil)
	defer connection.Close()

	go func() { _, _ = io.Copy(io.Discard, server) }()

	// Occupy the connection with a sender that will never get its reply.
	go func() {
		_, _ = connection.SendCommand(context.Background(), command.API{Command: "status"})
	}()
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := connection.SendCommand(ctx, command.API{Command: "status"})
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("a queued sender whose deadline expires must fail")
	}
	if elapsed > 2*time.Second {
		t.Fatalf("a queued sender must give up at its own deadline, waited %s", elapsed)
	}
}

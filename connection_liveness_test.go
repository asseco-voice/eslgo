package eslgo

import (
	"context"
	"io"
	"net"
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

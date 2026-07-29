/*
 * Copyright (c) 2020 Percipia
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Contributor(s):
 * Andrew Querol <aquerol@percipia.com>
 */
package eslgo

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"github.com/rs/zerolog"
	"io"
	"net"
	"net/textproto"
	"runtime/debug"
	"sync"
	"time"

	"github.com/asseco-voice/eslgo/command"
	"github.com/google/uuid"
)

type Channels struct {
	replyChannel       chan *RawResponse
	apiResponseChannel chan *RawResponse
	eventPlainChannel  chan *RawResponse
	eventXmlChannel    chan *RawResponse
	eventJsonChannel   chan *RawResponse
	authRequestChannel chan *RawResponse
	disconnectChannel  chan *RawResponse
}

func (c *Channels) Close() {
	close(c.replyChannel)
	close(c.apiResponseChannel)
	close(c.eventPlainChannel)
	close(c.eventJsonChannel)
	close(c.authRequestChannel)
	close(c.disconnectChannel)
}

func (c *Channels) ReplyChannel() chan *RawResponse {
	return c.replyChannel
}

func (c *Channels) ApiResponseChannel() chan *RawResponse {
	return c.apiResponseChannel
}

func (c *Channels) EventPlainChannel() chan *RawResponse {
	return c.eventPlainChannel
}

func (c *Channels) EventXmlChannel() chan *RawResponse {
	return c.eventXmlChannel
}

func (c *Channels) EventJsonChannel() chan *RawResponse {
	return c.eventJsonChannel
}

func (c *Channels) AuthRequestChannel() chan *RawResponse {
	return c.authRequestChannel
}

func (c *Channels) DisconnectChannel() chan *RawResponse {
	return c.disconnectChannel
}

func (c *Channels) IsClosed(channel <-chan *RawResponse) bool {
	select {
	case _, ok := <-channel:
		return !ok
	default:
		return false // Channel is not closed
	}
}

/*Conn ...*/
type Conn struct {
	conn   net.Conn
	reader *bufio.Reader
	header *textproto.Reader
	// writeLock serialises command/reply pairs on this connection. It is a channel
	// rather than a sync.Mutex so that waiting for it can honour the caller's
	// context: a sender must never be stuck behind another sender for longer than
	// its own deadline allows.
	writeLock      chan struct{}
	runningContext context.Context
	stopFunc       func()

	channels Channels

	responseChanMutex sync.RWMutex
	eventListenerLock sync.RWMutex
	eventListeners    map[string]map[string]EventListener
	outbound          bool
	closeOnce         sync.Once
	finishedChannel   chan bool
	logger            zerolog.Logger
	connectionId      string
	onDisconnect      func(string)
	disconnected      bool
}

func (c *Conn) ReplyChannel() chan *RawResponse {
	return c.channels.replyChannel
}

func (c *Conn) ApiResponseChannel() chan *RawResponse {
	return c.channels.apiResponseChannel
}

func (c *Conn) EventPlainChannel() chan *RawResponse {
	return c.channels.eventPlainChannel
}

func (c *Conn) EventXmlChannel() chan *RawResponse {
	return c.channels.eventXmlChannel
}

func (c *Conn) EventJsonChannel() chan *RawResponse {
	return c.channels.eventJsonChannel
}

func (c *Conn) AuthRequestChannel() chan *RawResponse {
	return c.channels.authRequestChannel
}

func (c *Conn) DisconnectChannel() chan *RawResponse {
	return c.channels.disconnectChannel
}

func (c *Channels) ByType(channelType string) chan *RawResponse {
	switch channelType {
	case TypeReply:
		return c.replyChannel
	case TypeAPIResponse:
		return c.apiResponseChannel
	case TypeEventPlain:
		return c.eventPlainChannel
	case TypeEventXML:
		return c.eventXmlChannel
	case TypeEventJSON:
		return c.eventJsonChannel
	case TypeAuthRequest:
		return c.authRequestChannel
	case TypeDisconnect:
		return c.disconnectChannel
	default:
		return nil
	}
}

func (c *Conn) Outbound() bool {
	return c.outbound
}

func (c *Conn) ConnectionId() string {
	return c.connectionId
}

func (c *Conn) FinishedChannel() chan bool {
	return c.finishedChannel
}

func (c *Conn) SetFinishedChannel(finishedChannel chan bool) {
	c.finishedChannel = finishedChannel
}

func (c *Conn) RunningContext() context.Context {
	return c.runningContext
}

const EndOfMessage = "\r\n\r\n"

// keepAlivePeriod is how often the OS probes an otherwise idle ESL socket. Short
// enough that a dead peer is noticed in well under a minute, long enough to be
// invisible on a healthy connection.
const keepAlivePeriod = 15 * time.Second

func NewConnection(c net.Conn, outbound bool, logger zerolog.Logger, connectionId string, onDisconnect func(string)) *Conn {
	reader := bufio.NewReader(c)
	header := textproto.NewReader(reader)

	// An ESL connection can sit idle for a long time, so nothing in the protocol
	// reveals a peer that vanished without a FIN/RST - a wedged FreeSWITCH, a
	// killed container, a firewall dropping an idle flow. Keepalive is what turns
	// that half-open socket into the read error that tears the connection down.
	if tcpConn, ok := c.(*net.TCPConn); ok {
		if err := tcpConn.SetKeepAlive(true); err != nil {
			logger.Warn().Err(err).Msgf("[ID: %s] failed enabling TCP keepalive", connectionId)
		} else if err := tcpConn.SetKeepAlivePeriod(keepAlivePeriod); err != nil {
			logger.Warn().Err(err).Msgf("[ID: %s] failed setting TCP keepalive period", connectionId)
		}
	}

	runningContext, stop := context.WithCancel(context.Background())

	instance := &Conn{
		conn:   c,
		reader: reader,
		header: header,
		channels: Channels{
			replyChannel:       make(chan *RawResponse),
			apiResponseChannel: make(chan *RawResponse),
			eventPlainChannel:  make(chan *RawResponse),
			eventXmlChannel:    make(chan *RawResponse),
			eventJsonChannel:   make(chan *RawResponse),
			authRequestChannel: make(chan *RawResponse, 1), // Buffered to ensure we do not lose the initial auth request before we are setup to respond
			disconnectChannel:  make(chan *RawResponse),
		},
		runningContext:    runningContext,
		stopFunc:          stop,
		eventListeners:    make(map[string]map[string]EventListener),
		outbound:          outbound,
		logger:            logger,
		connectionId:      connectionId,
		onDisconnect:      onDisconnect,
		writeLock:         make(chan struct{}, 1),
		responseChanMutex: sync.RWMutex{},
		eventListenerLock: sync.RWMutex{},
	}
	go instance.receiveLoop()
	go instance.eventLoop()
	go instance.contextLoop()
	return instance
}

/*RegisterEventListener ... */
func (c *Conn) RegisterEventListener(channelUUID string, listener EventListener) string {
	c.eventListenerLock.Lock()
	defer c.eventListenerLock.Unlock()

	id := uuid.New().String()
	c.logger.Debug().Msgf("registering listener for channelUUID (%s) with id (%s)", channelUUID, id)
	if _, ok := c.eventListeners[channelUUID]; ok {
		c.eventListeners[channelUUID][id] = listener
	} else {
		c.eventListeners[channelUUID] = map[string]EventListener{id: listener}
	}
	return id
}

/*RemoveEventListener .. */
func (c *Conn) RemoveEventListener(channelUUID string, id string) {
	c.logger.Debug().Msgf("removing listener for %s", channelUUID)
	c.eventListenerLock.Lock()
	defer c.eventListenerLock.Unlock()

	if listeners, ok := c.eventListeners[channelUUID]; ok {
		delete(listeners, id)
	}
}

func (c *Conn) SendCommand(ctx context.Context, command command.Command) (*RawResponse, error) {
	commandId := uuid.New().String()
	if c == nil {
		return nil, fmt.Errorf("connection is closed - skip sending command (%s)", command.BuildMessage())
	}
	c.logger.Debug().Msgf("[ID: %s][action_id: %s] sending command %s", c.connectionId, commandId, command.BuildMessage())
	if c.disconnected {
		c.logger.Debug().Msgf("[ID: %s][action_id: %s] connection disconnected, skipping command %s", c.connectionId, commandId, command.BuildMessage())
		return nil, fmt.Errorf("connection closed")
	}
	// Only one command may be in flight per connection: the reply channels carry no
	// correlation id, so releasing this before the reply arrives would hand one
	// sender's reply to another. Waiting for it must still be interruptible,
	// otherwise a single slow or stuck command silently wedges every other sender
	// on the connection regardless of the deadlines they were given.
	select {
	case c.writeLock <- struct{}{}:
		defer func() { <-c.writeLock }()
	case <-c.runningContext.Done():
		c.logger.Debug().Msgf("[ID: %s][action_id: %s] connection context is done while waiting to send", c.connectionId, commandId)
		return nil, c.runningContext.Err()
	case <-ctx.Done():
		c.logger.Error().Err(ctx.Err()).Msgf("[ID: %s][action_id: %s] context done while waiting to send", c.connectionId, commandId)
		return nil, ctx.Err()
	}

	// The write deadline is a property of the shared socket, not of this call, and it
	// is absolute: left in place it outlives the command that set it and, once past,
	// makes the next deadline-free write fail instantly with i/o timeout. Long-lived
	// connections mix bounded and unbounded callers, so each command must state the
	// deadline in full - including clearing it when it has none.
	if deadline, ok := ctx.Deadline(); ok {
		_ = c.conn.SetWriteDeadline(deadline)
	} else {
		_ = c.conn.SetWriteDeadline(time.Time{})
	}
	c.logger.Debug().Msgf("[ID: %s][action_id: %s] writing to socket", c.connectionId, commandId)
	_, err := c.conn.Write([]byte(command.BuildMessage() + EndOfMessage))
	if err != nil {
		c.logger.Error().Err(err).Msgf("[ID: %s][action_id: %s] error writing to socket", c.connectionId, commandId)
		return nil, err
	}

	replyChannel := c.ReplyChannel()
	responseChannel := c.ApiResponseChannel()

	// Get response
	c.logger.Debug().Msgf("[ID: %s][action_id: %s] locking mutex and waiting for response", c.connectionId, commandId)
	c.responseChanMutex.RLock()
	defer c.responseChanMutex.RUnlock()
	select {
	case response := <-replyChannel:
		c.logger.Debug().Msgf("[ID: %s][action_id: %s] command/reply", c.connectionId, commandId)
		if response == nil {
			c.logger.Error().Msgf("[ID: %s][action_id: %s] connection closed", c.connectionId, commandId)
			// We only get nil here if the channel is closed
			return nil, errors.New("connection closed")
		}
		return response, nil
	case response := <-responseChannel:
		c.logger.Debug().Msgf("[ID: %s][action_id: %s] api/response", c.connectionId, commandId)
		if response == nil {
			c.logger.Error().Msgf("[ID: %s][action_id: %s] connection closed", c.connectionId, commandId)
			// We only get nil here if the channel is closed
			return nil, errors.New("connection closed")
		}
		return response, nil
	case <-c.runningContext.Done():
		c.logger.Debug().Msgf("[ID: %s][action_id: %s] connection context is done", c.connectionId, commandId)
		return nil, c.runningContext.Err()
	case <-ctx.Done():
		c.logger.Error().Err(ctx.Err()).Msgf("[ID: %s][action_id: %s] context done", c.connectionId, commandId)
		return nil, ctx.Err()
	}
}

func (c *Conn) ExitAndClose() {
	defer func() {
		if r := recover(); r != nil {
			c.logger.Printf("Unhandled panic: %v\nStack trace:\n%s", r, debug.Stack())
		}
	}()
	c.logger.Debug().Msgf("[ID: %s] ExitAndClose", c.connectionId)
	c.closeOnce.Do(func() {
		// Attempt a graceful closing of the connection with FreeSWITCH
		ctx, cancel := context.WithTimeout(c.runningContext, time.Second)
		c.logger.Debug().Msgf("[ID: %s] sending exit command", c.connectionId)
		_, _ = c.SendCommand(ctx, command.Exit{})
		cancel()
		c.close()
	})
}

func (c *Conn) Close() {
	c.closeOnce.Do(c.close)
}

func (c *Conn) close() {
	c.logger.Debug().Msgf("[ID: %s] close", c.connectionId)
	// Allow users to do anything they need to do before we tear everything down
	c.logger.Debug().Msgf("[ID: %s] stopFunc", c.connectionId)
	c.stopFunc()
	c.channels.Close()
	// Close the connection only after we have the response channel lock and we have deleted all response channels to ensure we don't receive on a closed channel
	c.logger.Debug().Msgf("[ID: %s] closing underling connection", c.connectionId)
	err := c.conn.Close()
	if err != nil {
		c.logger.Error().Err(err).Msgf("[ID: %s] failed closing underling connection", c.connectionId)
	}
}

func (c *Conn) callEventListener(event *Event) {
	c.eventListenerLock.RLock()
	defer c.eventListenerLock.RUnlock()

	// First check if there are any general event listener
	if listeners, ok := c.eventListeners[EventListenAll]; ok {
		for _, listener := range listeners {
			go listener(event)
		}
	}

	// Next call any listeners for a particular channel
	if event.HasHeader("Unique-Id") {
		channelUUID := event.GetHeader("Unique-Id")
		if listeners, ok := c.eventListeners[channelUUID]; ok {
			for _, listener := range listeners {
				go listener(event)
			}
		}
	}

	// Next call any listeners for a particular application
	if event.HasHeader("Application-UUID") {
		appUUID := event.GetHeader("Application-UUID")
		if listeners, ok := c.eventListeners[appUUID]; ok {
			for _, listener := range listeners {
				go listener(event)
			}
		}
	}

	// Next call any listeners for a particular job
	if event.HasHeader("Job-UUID") {
		jobUUID := event.GetHeader("Job-UUID")
		if listeners, ok := c.eventListeners[jobUUID]; ok {
			for _, listener := range listeners {
				go listener(event)
			}
		}
	}
}

func (c *Conn) eventLoop() {
	defer func(log zerolog.Logger) {
		if r := recover(); r != nil {
			log.Error().Msgf("######### RECOVERED PANICKED GOROUTINE ######### \n %v \n %s", r, string(debug.Stack()))
		}
	}(c.logger)

	c.logger.Debug().Msgf("[ID: %s][action_id: event_loop] starting event loop", c.connectionId)
	plainEventChannel := c.EventPlainChannel()
	xmlEventChannel := c.EventXmlChannel()
	jsonEventChannel := c.EventJsonChannel()
	disconnectChannel := c.DisconnectChannel()

	for {
		var event *Event
		var err error
		c.responseChanMutex.RLock()
		select {
		case raw := <-plainEventChannel:
			c.logger.Debug().Msgf("[ID: %s][action_id: event_loop] event %s", c.connectionId, TypeEventPlain)
			if raw == nil {
				c.Close()
				// We only get nil here if the channel is closed
				c.responseChanMutex.RUnlock()
				return
			}
			event, err = readPlainEvent(raw.Body)
		case raw := <-xmlEventChannel:
			c.logger.Debug().Msgf("[ID: %s][action_id: event_loop] event %s", c.connectionId, TypeEventXML)
			if raw == nil {
				c.Close()
				// We only get nil here if the channel is closed
				c.responseChanMutex.RUnlock()
				return
			}
			event, err = readXMLEvent(raw.Body)
		case raw := <-jsonEventChannel:
			c.logger.Debug().Msgf("[ID: %s][action_id: event_loop] event %s", c.connectionId, TypeEventJSON)
			if raw == nil {
				c.Close()
				// We only get nil here if the channel is closed
				c.responseChanMutex.RUnlock()
				return
			}
			event, err = readJSONEvent(raw.Body)
		case <-disconnectChannel:
			c.logger.Warn().Msgf("[ID: %s][action_id: event_loop] connection disconnected", c.connectionId)
			c.disconnected = true
			c.responseChanMutex.RUnlock()
			c.Close()
			return
		case <-c.runningContext.Done():
			c.logger.Debug().Msgf("[ID: %s][action_id: event_loop] running context done", c.connectionId)
			c.responseChanMutex.RUnlock()
			return
		}

		if err != nil {
			c.logger.Error().Err(err).Msgf("[ID: %s][action_id: event_loop] Error parsing event", c.connectionId)
			continue
		}

		c.callEventListener(event)
	}
}

func (c *Conn) contextLoop() {
	<-c.runningContext.Done()
	c.logger.Debug().Msgf("[ID: %s][action_id: context_loop] context is done", c.connectionId)
	if c.FinishedChannel() != nil {
		c.logger.Debug().Msgf("[ID: %s][action_id: context_loop] finished channel is not null, sending event", c.connectionId)
		c.FinishedChannel() <- true
	} else {
		c.logger.Debug().Msgf("[ID: %s][action_id: context_loop] finished channel is null  - not sending event", c.connectionId)
	}
	if c.onDisconnect != nil {
		c.logger.Debug().Msgf("[ID: %s][action_id: context_loop] on disconnect is not null, calling function onDisconnect", c.connectionId)
		c.onDisconnect(c.connectionId)
	} else {
		c.logger.Debug().Msgf("[ID: %s][action_id: context_loop] on disconnect is null, not calling function onDisconnect", c.connectionId)
	}
	c.logger.Debug().Msgf("[ID: %s][action_id: context_loop] context loop finished", c.connectionId)
}

func (c *Conn) receiveLoop() {
	loopId := uuid.New().String()
	c.logger.Debug().Msgf("[ID: %s][action_id: %s] starting receive loop", c.connectionId, loopId)

	defer func(log zerolog.Logger) {
		if r := recover(); r != nil {
			log.Error().Msgf("######### RECOVERED PANICKED GOROUTINE ######### \n %v \n %s", r, string(debug.Stack()))
		}
	}(c.logger)

	for c.runningContext.Err() == nil {
		response, err := c.readResponse()
		if err != nil {
			// Nothing will ever be read from this socket again, so the connection is
			// finished whether or not FreeSWITCH told us so. Returning without tearing
			// it down leaves it looking alive while it silently swallows every event
			// and every command reply - senders then block forever waiting for a
			// response that no one is left to deliver, and no disconnect is reported.
			// Close cancels the running context, which releases those senders and lets
			// contextLoop fire the disconnect callback so the owner can reconnect.
			//
			// Severity distinguishes the two ways a socket ends. An EOF is FreeSWITCH
			// closing it cleanly, which for a per-call connection is simply the call
			// hanging up - ordinary, and twice per call, so logging it as an error
			// buries the real ones. Anything else (a reset, a timeout) is unexpected
			// and worth an error: that is the shape of the incident this teardown was
			// added for.
			if c.runningContext.Err() == nil {
				if errors.Is(err, io.EOF) {
					c.logger.Debug().Msgf("[ID: %s][action_id: %s] peer closed the connection, closing", c.connectionId, loopId)
				} else {
					c.logger.Error().Err(err).Msgf("[ID: %s][action_id: %s] receive loop failed to read, closing connection", c.connectionId, loopId)
				}
			}
			c.Close()
			return
		}
		responseChan := c.channels.ByType(response.GetHeader("Content-Type"))
		if responseChan == nil {
			c.logger.Warn().Msgf("[ID: %s][action_id: %s] no channel has been found for %s", c.connectionId, loopId, response.GetHeader("Content-Type"))
			continue
		}

		// We have a handler
		// Only allow 5 seconds to allow the handler to receive hte message on the channel
		ctx, cancel := context.WithTimeout(c.runningContext, 2*time.Second)
		defer cancel()

		// Check if the channel is closed
		if c.channels.IsClosed(responseChan) {
			c.logger.Warn().Msgf("[ID: %s][action_id: %s] channel is closed for %s", c.connectionId, loopId, response.GetHeader("Content-Type"))
			continue
		}

		select {
		case responseChan <- response:
		case <-c.runningContext.Done():
			c.logger.Warn().Msgf("[ID: %s][action_id: %s] running context done", c.connectionId, loopId)
			// Parent connection context has stopped we most likely shutdown in the middle of waiting for a handler to handle the message
			return
		case <-ctx.Done():
			// Do not return an error since this is not fatal but log since it could be a indication of problems
			c.logger.Warn().Msgf("[ID: %s][action_id: %s] no one to handle response. Is the connection overloaded or stopping?\n%v", c.connectionId, loopId, response)
		}
	}
}

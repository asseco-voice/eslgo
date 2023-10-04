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
	"github.com/rs/zerolog"
	"net"
	"net/textproto"
	"sync"
	"time"

	"github.com/AkronimBlack/eslgo/command"
	"github.com/google/uuid"
)

/*Conn ...*/
type Conn struct {
	conn              net.Conn
	reader            *bufio.Reader
	header            *textproto.Reader
	writeLock         sync.Mutex
	runningContext    context.Context
	stopFunc          func()
	responseChannels  map[string]chan *RawResponse
	responseChanMutex sync.RWMutex
	eventListenerLock sync.RWMutex
	eventListeners    map[string]map[string]EventListener
	outbound          bool
	closeOnce         sync.Once
	finishedChannel   chan bool
	logger            zerolog.Logger
	connectionId      string
	onDisconnect      func(string)
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

// NewConnection exported constructor for alterative builds
func NewConnection(c net.Conn, outbound bool, logger zerolog.Logger, connectionId string, onDisconnect func(string)) *Conn {
	reader := bufio.NewReader(c)
	header := textproto.NewReader(reader)

	runningContext, stop := context.WithCancel(context.Background())

	instance := &Conn{
		conn:   c,
		reader: reader,
		header: header,
		responseChannels: map[string]chan *RawResponse{
			TypeReply:       make(chan *RawResponse),
			TypeAPIResponse: make(chan *RawResponse),
			TypeEventPlain:  make(chan *RawResponse),
			TypeEventXML:    make(chan *RawResponse),
			TypeEventJSON:   make(chan *RawResponse),
			TypeAuthRequest: make(chan *RawResponse, 1), // Buffered to ensure we do not lose the initial auth request before we are setup to respond
			TypeDisconnect:  make(chan *RawResponse),
		},
		runningContext: runningContext,
		stopFunc:       stop,
		eventListeners: make(map[string]map[string]EventListener),
		outbound:       outbound,
		logger:         logger,
		connectionId:   connectionId,
		onDisconnect:   onDisconnect,
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
	c.logger.Debug().Msgf("registering listener for %s with id ", channelUUID, id)
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
	c.logger.Debug().Msgf("[ID: %s][action_id: %s] sending command %s", c.connectionId, commandId, command.BuildMessage())
	c.writeLock.Lock()
	defer c.writeLock.Unlock()

	if deadline, ok := ctx.Deadline(); ok {
		_ = c.conn.SetWriteDeadline(deadline)
	}
	c.logger.Debug().Msgf("[ID: %s][action_id: %s] writing to socket", c.connectionId, commandId)
	_, err := c.conn.Write([]byte(command.BuildMessage() + EndOfMessage))
	if err != nil {
		c.logger.Error().Err(err).Msgf("[ID: %s][action_id: %s] error writing to socket", c.connectionId, commandId)
		return nil, err
	}

	// Get response
	c.logger.Debug().Msgf("[ID: %s][action_id: %s] locking mutex and waiting for response", c.connectionId, commandId)
	c.responseChanMutex.RLock()
	defer c.responseChanMutex.RUnlock()
	select {
	case response := <-c.responseChannels[TypeReply]:
		c.logger.Debug().Msgf("[ID: %s][action_id: %s] command/reply", c.connectionId, commandId)
		if response == nil {
			c.logger.Error().Msgf("[ID: %s][action_id: %s] connection closed", c.connectionId, commandId)
			// We only get nil here if the channel is closed
			return nil, errors.New("connection closed")
		}
		return response, nil
	case response := <-c.responseChannels[TypeAPIResponse]:
		c.logger.Debug().Msgf("[ID: %s][action_id: %s] api/response", c.connectionId, commandId)
		if response == nil {
			c.logger.Error().Msgf("[ID: %s][action_id: %s] connection closed", c.connectionId, commandId)
			// We only get nil here if the channel is closed
			return nil, errors.New("connection closed")
		}
		return response, nil
	case response := <-c.responseChannels[TypeDisconnect]:
		c.logger.Debug().Msgf("[ID: %s][action_id: %s] text/disconnect-notice", c.connectionId, commandId)
		if response == nil {
			c.logger.Error().Msgf("[ID: %s][action_id: %s] freeswitch sent disconnect-notice", c.connectionId, commandId)
			// We only get nil here if the channel is closed
			return nil, errors.New("connection closed")
		}
		return response, nil
	case <-ctx.Done():
		c.logger.Error().Err(ctx.Err()).Msgf("[ID: %s][action_id: %s] context done", c.connectionId, commandId)
		return nil, ctx.Err()
	}
}

func (c *Conn) ExitAndClose() {
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
	//c.responseChanMutex.Lock()
	//defer c.responseChanMutex.Unlock()
	for key, responseChan := range c.responseChannels {
		c.logger.Debug().Msgf("[ID: %s] removing channel %s", c.connectionId, key)
		close(responseChan)
		delete(c.responseChannels, key)
	}

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
	eventLoopId := uuid.New().String()
	c.logger.Debug().Msgf("[ID: %s][action_id: %s] starting event loop", c.connectionId, eventLoopId)
	for {
		var event *Event
		var err error
		c.responseChanMutex.RLock()
		select {
		case raw := <-c.responseChannels[TypeEventPlain]:
			c.logger.Debug().Msgf("[ID: %s][action_id: %s] event %s", c.connectionId, eventLoopId, TypeEventPlain)
			if raw == nil {
				c.Close()
				// We only get nil here if the channel is closed
				c.responseChanMutex.RUnlock()
				return
			}
			event, err = readPlainEvent(raw.Body)
		case raw := <-c.responseChannels[TypeEventXML]:
			c.logger.Debug().Msgf("[ID: %s][action_id: %s] event %s", c.connectionId, eventLoopId, TypeEventXML)
			if raw == nil {
				c.Close()
				// We only get nil here if the channel is closed
				c.responseChanMutex.RUnlock()
				return
			}
			event, err = readXMLEvent(raw.Body)
		case raw := <-c.responseChannels[TypeEventJSON]:
			c.logger.Debug().Msgf("[ID: %s][action_id: %s] event %s", c.connectionId, eventLoopId, TypeEventJSON)
			if raw == nil {
				c.Close()
				// We only get nil here if the channel is closed
				c.responseChanMutex.RUnlock()
				return
			}
			event, err = readJSONEvent(raw.Body)
		case <-c.responseChannels[TypeDisconnect]:
			c.logger.Warn().Msgf("[ID: %s][action_id: %s] connection disconnected", c.connectionId, eventLoopId)
			c.responseChanMutex.RUnlock()
			c.Close()
			return
		case <-c.runningContext.Done():
			c.logger.Debug().Msgf("[ID: %s][action_id: %s] running context done", c.connectionId, eventLoopId)
			c.responseChanMutex.RUnlock()
			return
		}

		if err != nil {
			c.logger.Error().Err(err).Msgf("[ID: %s][action_id: %s] Error parsing event", c.connectionId, eventLoopId)
			continue
		}

		c.callEventListener(event)
	}
}

func (c *Conn) contextLoop() {
	<-c.runningContext.Done()
	c.logger.Debug().Msgf("[ID: %s][action_id: context_loop] context is done", c.connectionId)
	if c.FinishedChannel() != nil {
		c.FinishedChannel() <- true
	}
	if c.onDisconnect != nil {
		c.onDisconnect(c.connectionId)
	}
	return
}

func (c *Conn) receiveLoop() {
	loopId := uuid.New().String()
	c.logger.Debug().Msgf("[ID: %s][action_id: %s] starting receive loop", c.connectionId, loopId)
	for c.runningContext.Err() == nil {
		response, err := c.readResponse()
		if err != nil {
			return
		}
		c.responseChanMutex.RLock()
		responseChan, ok := c.responseChannels[response.GetHeader("Content-Type")]
		if !ok && len(c.responseChannels) <= 0 {
			return
		}

		// We have a handler
		if ok {
			// Only allow 5 seconds to allow the handler to receive hte message on the channel
			ctx, cancel := context.WithTimeout(c.runningContext, 2*time.Second)
			defer cancel()

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
		} else {
			return
		}
		c.responseChanMutex.RUnlock()
	}
}

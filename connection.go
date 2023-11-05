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
	"net"
	"net/textproto"
	"runtime/debug"
	"sync"
	"time"

	"github.com/AkronimBlack/eslgo/command"
	"github.com/google/uuid"
)

/*Conn ...*/
type Conn struct {
	conn                      net.Conn
	reader                    *bufio.Reader
	header                    *textproto.Reader
	writeLock                 sync.Mutex
	runningContext            context.Context
	stopFunc                  func()
	responseChannels          map[string]chan *RawResponse
	responseChannelsReadMutex sync.RWMutex
	responseChanMutex         sync.RWMutex
	eventListenerLock         sync.RWMutex
	eventListeners            map[string]map[string]EventListener
	outbound                  bool
	closeOnce                 sync.Once
	finishedChannel           chan bool
	logger                    zerolog.Logger
	connectionId              string
	onDisconnect              func(string)
	disconnected              bool
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
	if c.disconnected {
		c.logger.Debug().Msgf("[ID: %s][action_id: %s] connection disconnected, skipping command %s", c.connectionId, commandId, command.BuildMessage())
		return nil, fmt.Errorf("connection closed")
	}
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

	replyChannel := c.replyChannel()
	apiResponseChannel := c.apiResponseChannel()

	if replyChannel == nil || apiResponseChannel == nil {
		return nil, fmt.Errorf("channel configuration invalid (reply or response channel missing)")
	}

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
	case response := <-apiResponseChannel:
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

	plainEventChannel := c.plainEventChannel()
	xmlEventChannel := c.xmlEventChannel()
	jsonEventChannel := c.jsonEventChannel()
	disconnectChannel := c.disconnectChannel()
	c.logger.Debug().Msgf("[ID: %s][action_id: event_loop] starting event loop", c.connectionId)
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
	}
	if c.onDisconnect != nil {
		c.logger.Debug().Msgf("[ID: %s][action_id: context_loop] on disconnect is not null, calling", c.connectionId)
		c.onDisconnect(c.connectionId)
	}
	c.logger.Debug().Msgf("[ID: %s][action_id: context_loop] context loop finished", c.connectionId)
	return
}

func (c *Conn) responseChannel(contentType string) chan *RawResponse {
	c.responseChannelsReadMutex.Lock()
	defer c.responseChannelsReadMutex.Unlock()
	responseChan, ok := c.responseChannels[contentType]
	if !ok {
		return nil
	}
	return responseChan
}

func (c *Conn) authChannel() chan *RawResponse {
	c.responseChannelsReadMutex.Lock()
	defer c.responseChannelsReadMutex.Unlock()
	responseChan, ok := c.responseChannels[TypeAuthRequest]
	if !ok {
		return nil
	}
	return responseChan
}

func (c *Conn) replyChannel() chan *RawResponse {
	c.responseChannelsReadMutex.Lock()
	defer c.responseChannelsReadMutex.Unlock()
	responseChan, ok := c.responseChannels[TypeReply]
	if !ok {
		return nil
	}
	return responseChan
}

func (c *Conn) apiResponseChannel() chan *RawResponse {
	c.responseChannelsReadMutex.Lock()
	defer c.responseChannelsReadMutex.Unlock()
	responseChan, ok := c.responseChannels[TypeAPIResponse]
	if !ok {
		return nil
	}
	return responseChan
}

func (c *Conn) plainEventChannel() chan *RawResponse {
	c.responseChannelsReadMutex.Lock()
	defer c.responseChannelsReadMutex.Unlock()
	responseChan, ok := c.responseChannels[TypeEventPlain]
	if !ok {
		return nil
	}
	return responseChan
}

func (c *Conn) xmlEventChannel() chan *RawResponse {
	c.responseChannelsReadMutex.Lock()
	defer c.responseChannelsReadMutex.Unlock()
	responseChan, ok := c.responseChannels[TypeEventXML]
	if !ok {
		return nil
	}
	return responseChan
}

func (c *Conn) disconnectChannel() chan *RawResponse {
	c.responseChannelsReadMutex.Lock()
	defer c.responseChannelsReadMutex.Unlock()
	responseChan, ok := c.responseChannels[TypeDisconnect]
	if !ok {
		return nil
	}
	return responseChan
}

func (c *Conn) jsonEventChannel() chan *RawResponse {
	c.responseChannelsReadMutex.Lock()
	defer c.responseChannelsReadMutex.Unlock()
	responseChan, ok := c.responseChannels[TypeAPIResponse]
	if !ok {
		return nil
	}
	return responseChan
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
			return
		}
		c.responseChanMutex.RLock()
		responseChan := c.responseChannel(response.GetHeader("Content-Type"))
		if responseChan == nil {
			return
		}
		ctx, cancel := context.WithTimeout(c.runningContext, 2*time.Second)
		defer cancel()

		if responseChan == nil {
			return
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
		c.responseChanMutex.RUnlock()
	}
}

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
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/AkronimBlack/eslgo/command"
	"github.com/google/uuid"
)

/*Conn ...*/
type Conn struct {
	conn              net.Conn
	writeLock         sync.Mutex
	runningContext    context.Context
	stopFunc          func()
	responseChannels  map[string]chan *RawResponse
	responseChanMutex sync.RWMutex
	eventListenerLock sync.RWMutex
	eventListeners    map[string]map[string]EventListener
	outbound          bool
	logger            Logger
}

const EndOfMessage = "\r\n\r\n"

//NewConnection exported constructor for alternative builds
//if no ctx passed in a new context will be used out of context.Background()
func NewConnection(c net.Conn, outbound bool, ctx context.Context, logger Logger) *Conn {
	if logger == nil {
		logger = NewLogger()
	}
	if ctx == nil {
		ctx = context.Background()
	}
	runningContext, stop := context.WithCancel(ctx)

	instance := &Conn{
		conn: c,
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
	}
	go instance.readLoop()
	go instance.eventLoop()
	return instance
}

/*RegisterEventListener ... */
func (c *Conn) RegisterEventListener(channelUUID string, listener EventListener) string {
	c.eventListenerLock.Lock()
	defer c.eventListenerLock.Unlock()

	id := uuid.New().String()
	if _, ok := c.eventListeners[channelUUID]; ok {
		c.eventListeners[channelUUID][id] = listener
	} else {
		c.eventListeners[channelUUID] = map[string]EventListener{id: listener}
	}
	return id
}

/*RemoveEventListener .. */
func (c *Conn) RemoveEventListener(channelUUID string, id string) {
	c.eventListenerLock.Lock()
	defer c.eventListenerLock.Unlock()

	if listeners, ok := c.eventListeners[channelUUID]; ok {
		delete(listeners, id)
	}
}

func (c *Conn) SendCommand(ctx context.Context, command command.Command) (*RawResponse, error) {
	c.writeLock.Lock()
	defer c.writeLock.Unlock()

	if deadline, ok := ctx.Deadline(); ok {
		_ = c.conn.SetWriteDeadline(deadline)
	}
	_, err := c.conn.Write([]byte(command.BuildMessage() + EndOfMessage))
	if err != nil {
		return nil, err
	}

	// Get response
	c.responseChanMutex.RLock()
	defer c.responseChanMutex.RUnlock()
	select {
	case response := <-c.responseChannels[TypeReply]:
		if response == nil {
			// We only get nil here if the channel is closed
			return nil, errors.New("connection closed")
		}
		return response, nil
	case response := <-c.responseChannels[TypeAPIResponse]:
		if response == nil {
			// We only get nil here if the channel is closed
			return nil, errors.New("connection closed")
		}
		return response, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *Conn) ExitAndClose() {
	c.logger.Debugf("Invoking ExitAndClose")
	// Attempt a graceful closing of the connection with FreeSWITCH
	ctx, cancel := context.WithTimeout(c.runningContext, time.Second)
	resp, err := c.SendCommand(ctx, command.Exit{})
	cancel()
	if err != nil || strings.Contains(resp.String(), "ERR") {
		c.logger.Debugf("Graceful closing failed. Forcing connection close")
		c.logger.Debugf("Error %s", err.Error())
		c.close()
	}
}

func (c *Conn) Close() {
	c.logger.Debugf("Invoking Close")
	c.close()
}

func (c *Conn) close() {
	c.logger.Debugf("Invoking close")
	// Allow users to do anything they need to do before we tear everything down
	c.stopFunc()
	c.responseChanMutex.Lock()
	defer c.responseChanMutex.Unlock()
	c.logger.Debugf("Closing response channels")
	for key, responseChan := range c.responseChannels {
		close(responseChan)
		delete(c.responseChannels, key)
	}
	c.logger.Debugf("Response channels closed")

	// Close the connection only after we have the response channel lock and we have deleted all response channels to ensure we don't receive on a closed channel
	err := c.conn.Close()
	if err != nil {
		c.logger.Errorf("failed closing connection with error %s", err.Error())
	}
	c.logger.Debugf("close finished")
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
	for {
		var event *Event
		var err error
		c.responseChanMutex.RLock()
		select {
		case raw := <-c.responseChannels[TypeEventPlain]:
			if raw == nil {
				// We only get nil here if the channel is closed
				c.responseChanMutex.RUnlock()
				return
			}
			event, err = readPlainEvent(raw.Body)
		case raw := <-c.responseChannels[TypeEventXML]:
			if raw == nil {
				// We only get nil here if the channel is closed
				c.responseChanMutex.RUnlock()
				return
			}
			event, err = readXMLEvent(raw.Body)
		case raw := <-c.responseChannels[TypeEventJSON]:
			if raw == nil {
				// We only get nil here if the channel is closed
				c.responseChanMutex.RUnlock()
				return
			}
			event, err = readJSONEvent(raw.Body)
		case <-c.responseChannels[TypeDisconnect]:
			c.logger.Debugf("Disconnect outbound connection %s", c.conn.RemoteAddr().String())
			c.Close()
		case <-c.responseChannels[TypeAuthRequest]:
			c.logger.Debugf("Ignoring auth request on outbound connection %s", c.conn.RemoteAddr().String())
		case <-c.runningContext.Done():
			c.logger.Debugf("context done with %s", c.runningContext.Err().Error())
			c.responseChanMutex.RUnlock()
			return
		}
		c.responseChanMutex.RUnlock()

		if err != nil {
			c.logger.Debugf("Error parsing event %s", err.Error())
			continue
		}

		c.callEventListener(event)
	}
}

func (c *Conn) readLoop() {
	for c.runningContext.Err() == nil {
		err := c.doMessage()
		if err != nil {
			c.logger.Debugf("Error receiving message %s", err.Error())
			break
		}
	}
}

func (c *Conn) doMessage() error {
	response, err := c.readResponse()
	if err != nil {
		return err
	}

	c.responseChanMutex.RLock()
	defer c.responseChanMutex.RUnlock()
	responseChan, ok := c.responseChannels[response.GetHeader("Content-Type")]
	if !ok && len(c.responseChannels) <= 0 {
		// We must have shutdown!
		return errors.New("no response channels")
	}

	// We have a handler
	if ok {
		// Only allow 5 seconds to allow the handler to receive hte message on the channel
		ctx, cancel := context.WithTimeout(c.runningContext, 5*time.Second)
		defer cancel()

		select {
		case responseChan <- response:
		case <-c.runningContext.Done():
			// Parent connection context has stopped we most likely shutdown in the middle of waiting for a handler to handle the message
			return c.runningContext.Err()
		case <-ctx.Done():
			// Do not return an error since this is not fatal but log since it could be a indication of problems
			c.logger.Error("No one to handle response. Is the connection overloaded or stopping? Possible block?", response)
		}
	} else {
		return errors.New("no response channel for Content-Type: " + response.GetHeader("Content-Type"))
	}
	return nil
}

func (c *Conn) outboundHandle(handler OutboundHandler, opts *Options) {
	var ctx context.Context
	var cancel context.CancelFunc
	ctx = context.Background()
	if opts.Timeout != 0 {
		ctx, cancel = context.WithTimeout(c.runningContext, opts.Timeout)
		cancel()
	}

	response, err := c.SendCommand(ctx, command.Connect{})
	if err != nil {
		c.logger.Errorf("Error connecting to %s error %s", c.conn.RemoteAddr().String(), err.Error())
		// Try closing cleanly first
		c.Close() // Not ExitAndClose since this error connection is most likely from communication failure
		return
	}
	handler(c.runningContext, c, response)
	// XXX This is ugly, the issue with short lived async sockets on our end is if they complete too fast we can actually
	// close the connection before FreeSWITCH is in a state to close the connection on their end. 25ms is an magic value
	// found by testing to have no failures on my test system. I started at 1 second and reduced as far as I could go.
	// TODO We should open a bug report on the FreeSWITCH GitHub at some point and remove this when fixed.
	// TODO This actually may be fixed: https://github.com/signalwire/freeswitch/pull/636
	time.Sleep(25 * time.Millisecond)
	if opts.Timeout != 0 {
		ctx, cancel = context.WithTimeout(c.runningContext, opts.Timeout)
		cancel()
	}
	_, _ = c.SendCommand(ctx, command.Exit{})
	c.ExitAndClose()
}

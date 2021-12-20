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
	"fmt"
	"net"
	"time"

	"github.com/AkronimBlack/eslgo/command"
)

type DialOpts struct {
	address, password string
	timeout           time.Duration
	onDisconnect      func()
	logger            Logger
}

func NewDialOpts(address string, password string, timeout time.Duration, onDisconnect func(), logger Logger) *DialOpts {
	return &DialOpts{address: address, password: password, timeout: timeout, onDisconnect: onDisconnect, logger: logger}
}

func Dial(ctx context.Context, opts *DialOpts) (*Conn, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	logger := opts.logger
	if logger == nil {
		logger = NewLogger()
	}

	c, err := net.DialTimeout("tcp", opts.address, opts.timeout)
	if err != nil {
		return nil, err
	}
	connection := NewConnection(c, false, ctx, logger)

	// First auth
	<-connection.responseChannels[TypeAuthRequest]
	err = connection.doAuth(connection.runningContext, command.Auth{Password: opts.password})
	if err != nil {
		// Try to gracefully disconnect, we have the wrong password.
		connection.ExitAndClose()
		if opts.onDisconnect != nil {
			go opts.onDisconnect()
		}
		return nil, err
	} else {
		logger.Debugf("Successfully authenticated %s", connection.conn.RemoteAddr())
	}

	// Inbound only handlers
	go connection.authLoop(command.Auth{Password: opts.password})
	go connection.disconnectLoop(opts.onDisconnect)

	return connection, nil
}

func (c *Conn) disconnectLoop(onDisconnect func()) {
	select {
	case <-c.responseChannels[TypeDisconnect]:
		c.Close()
		if onDisconnect != nil {
			onDisconnect()
		}
		return
	case <-c.runningContext.Done():
		return
	}
}

func (c *Conn) authLoop(auth command.Auth) {
	for {
		select {
		case <-c.responseChannels[TypeAuthRequest]:
			err := c.doAuth(c.runningContext, auth)
			if err != nil {
				c.logger.Errorf("Failed to auth with error %s", err.Error())
				// Close the connection, we have the wrong password
				c.ExitAndClose()
				return
			} else {
				c.logger.Debugf("Successfully authenticated %s\n", c.conn.RemoteAddr())
			}
		case <-c.runningContext.Done():
			return
		}
	}
}

func (c *Conn) doAuth(ctx context.Context, auth command.Auth) error {
	response, err := c.SendCommand(ctx, auth)
	if err != nil {
		return err
	}
	if !response.IsOk() {
		return fmt.Errorf("failed to auth %#v", response)
	}
	return nil
}

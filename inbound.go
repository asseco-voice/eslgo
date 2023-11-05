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
	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"log"
	"net"
	"time"

	"github.com/AkronimBlack/eslgo/command"
)

func Dial(address, password string, timeout time.Duration, onDisconnect func(string), logger zerolog.Logger) (*Conn, error) {
	c, err := net.DialTimeout("tcp", address, timeout)
	if err != nil {
		return nil, err
	}
	connection := NewConnection(c, false, logger, uuid.New().String(), onDisconnect)

	// First auth
	<-connection.responseChannels[TypeAuthRequest]
	err = connection.doAuth(connection.runningContext, command.Auth{Password: password})
	if err != nil {
		// Try to gracefully disconnect, we have the wrong password.
		connection.ExitAndClose()
		return nil, err
	} else {
		log.Printf("Sucessfully authenticated %s\n", connection.conn.RemoteAddr())
	}

	// Inbound only handlers
	go connection.authLoop(command.Auth{Password: password})
	//go connection.disconnectLoop()

	return connection, nil
}

//func (c *Conn) disconnectLoop() {
//	select {
//	case <-c.responseChannels[TypeDisconnect]:
//		c.logger.Warn().Msgf("[ID: %s] [disconnectLoop] connection disconnected", c.connectionId)
//		c.Close()
//		if c.onDisconnect != nil {
//			c.onDisconnect()
//		}
//		return
//	case <-c.runningContext.Done():
//		c.logger.Warn().Msgf("[ID: %s] [disconnectLoop] connection running context ended", c.connectionId)
//		return
//	}
//}

func (c *Conn) authLoop(auth command.Auth) {

	for {
		select {
		case <-c.responseChannels[TypeAuthRequest]:
			err := c.doAuth(c.runningContext, auth)
			if err != nil {
				c.logger.Error().Err(err).Msgf("failed to authenticate")
				// Close the connection, we have the wrong password
				c.ExitAndClose()
				return
			} else {
				c.logger.Debug().Msgf("successfully authenticated %s\n", c.conn.RemoteAddr())
			}
		case <-c.runningContext.Done():
			c.logger.Warn().Err(c.runningContext.Err()).Msgf("context done in auth loop")
			return
		}
	}
}

func (c *Conn) doAuth(ctx context.Context, auth command.Auth) error {
	c.logger.Debug().Msg("running authentication")
	response, err := c.SendCommand(ctx, auth)
	if err != nil {
		return err
	}
	if !response.IsOk() {
		return fmt.Errorf("failed to auth %#v", response)
	}
	return nil
}

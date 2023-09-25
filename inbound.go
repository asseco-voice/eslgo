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
	"log"
	"net"
	"time"

	"github.com/AkronimBlack/eslgo/command"
)

func Dial(address, password string, timeout time.Duration, onDisconnect func()) (*Conn, error) {
	c, err := net.DialTimeout("tcp", address, timeout)
	if err != nil {
		return nil, err
	}
	connection := NewConnection(c, false, onDisconnect, address, password)

	// First auth
	err = <-connection.authenticated
	if err != nil {
		return nil, err
	}
	return connection, nil
}

func (c *Conn) doAuth(ctx context.Context, auth command.Auth) error {
	log.Println("Authorizing ....")
	response, err := c.SendCommand(ctx, auth)
	if err != nil {
		return err
	}
	if !response.IsOk() {
		return fmt.Errorf("failed to auth %#v", response)
	}
	return nil
}

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
	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"log"
	"net"
	"time"

	"github.com/AkronimBlack/eslgo/command"
)

const (
	tcp  = "tcp"
	tcp4 = "tcp4"
	tcp6 = "tcp6"
	udp  = "udp"
	udp4 = "udp4"
	udp6 = "udp6"
)

type OutboundHandler func(ctx context.Context, conn *Conn, connectResponse *RawResponse)

func NewOptions(network string, timout time.Duration, logger zerolog.Logger, onDisconnect func(string)) *Options {
	return &Options{
		Network:      network,
		Timeout:      timout,
		Logger:       logger,
		OnDisconnect: onDisconnect,
	}
}

/*Options allow of customizing listener*/
type Options struct {
	//Allow for context timout. 0 will mean indefinite
	Timeout time.Duration `json:"duration"`
	// Defaults to "tcp"
	// Known networks are "tcp", "tcp4" (IPv4-only), "tcp6" (IPv6-only),
	// "udp", "udp4" (IPv4-only), "udp6" (IPv6-only), "ip", "ip4"
	// (IPv4-only), "ip6" (IPv6-only), "unix", "unixgram" and
	// "unixpacket".
	Network      string `json:"network"`
	Logger       zerolog.Logger
	OnDisconnect func(string)
}

/*ListenAndServe start listener with given options */
func ListenAndServe(address string, handler OutboundHandler, opts *Options) error {
	network := tcp
	if opts.Network != "" {
		network = opts.Network
	}
	listener, err := net.Listen(network, address)
	if err != nil {
		return err
	}
	opts.Logger.Debug().Msgf("listening for new ESL connections on %s", listener.Addr().String())
	for {
		c, err := listener.Accept()
		if err != nil {
			break
		}
		opts.Logger.Debug().Msgf("new outbound connection from %s", c.RemoteAddr().String())
		conn := NewConnection(c, true, opts.Logger, uuid.New().String(), opts.OnDisconnect)
		//go conn.dummyLoop()
		// Does not call the handler directly to ensure closing cleanly
		go conn.outboundHandle(handler, opts)
	}
	opts.Logger.Warn().Msg("ListenAndServe server shutting down")
	return errors.New("connection closed")
}

func (c *Conn) outboundHandle(handler OutboundHandler, opts *Options) {
	c.logger.Debug().Msgf("[ID: %s] outboundHandle called", c.connectionId)
	var ctx context.Context
	var cancel context.CancelFunc
	ctx = context.Background()
	if opts.Timeout != 0 {
		ctx, cancel = context.WithTimeout(c.runningContext, opts.Timeout)
		cancel()
	}

	response, err := c.SendCommand(ctx, command.Connect{})
	if err != nil {
		c.logger.Error().Err(err).Msgf("[ID: %s] failed connecting to %s, calling close", c.connectionId, c.conn.RemoteAddr().String())
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
	//c.ExitAndClose()
}

func (c *Conn) dummyLoop() {
	select {
	case <-c.responseChannels[TypeDisconnect]:
		log.Println("Disconnect outbound connection", c.conn.RemoteAddr())
		return
	case <-c.responseChannels[TypeAuthRequest]:
		log.Println("Ignoring auth request on outbound connection", c.conn.RemoteAddr())
	case <-c.runningContext.Done():
		log.Printf("Context done %s", c.runningContext.Err().Error())
		return
	}
}

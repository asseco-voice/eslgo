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

func NewOptions(network string, timout time.Duration, logger Logger, ctx context.Context) *Options {
	return &Options{
		Network: network,
		Timeout: timout,
		Logger:  logger,
		Ctx:     ctx,
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
	Network string `json:"network"`
	//Allow for closing on context done or error
	Ctx    context.Context `json:"-"`
	Logger Logger
}

/*ListenAndServe start listener with given options */
func ListenAndServe(address string, handler OutboundHandler, opts *Options) error {
	ctx := opts.Ctx
	if ctx == nil {
		ctx = context.Background()
	}

	logger := opts.Logger
	if logger == nil {
		logger = NewLogger()
	}

	network := tcp
	if opts.Network != "" {
		network = opts.Network
	}

	listener, err := net.Listen(network, address)
	if err != nil {
		return err
	}
	defer listener.Close()

	//make conn channel to give it some room
	connCh := make(chan net.Conn, 100)

	logger.Debugf("Listening for new ESL connections on %s", listener.Addr().String())
	go listenerLoop(listener, ctx, connCh, logger)
	for {
		select {
		case <-ctx.Done():
			logger.Debugf("context done with %s", ctx.Err().Error())
			return fmt.Errorf("context done with %s", ctx.Err().Error())
		case c := <-connCh:
			//use listener context to close all running connections on context.Done()
			conn := NewConnection(c, true, ctx, logger)
			//go conn.dummyLoop()
			// Does not call the handler directly to ensure closing cleanly
			go conn.outboundHandle(handler, opts)
		}
	}
}

func listenerLoop(listener net.Listener, ctx context.Context, ch chan net.Conn, logger Logger) {
	_, cancel := context.WithCancel(ctx)
	for {
		c, err := listener.Accept()
		if err != nil {
			logger.Errorf("failed accepting connection with error %s", err.Error())
			cancel()
			return
		}
		ch <- c
	}
}

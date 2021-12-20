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
package main

import (
	"context"
	"fmt"
	"github.com/AkronimBlack/eslgo/command"
	"github.com/google/uuid"
	"log"
	"net/http"
	"time"

	"github.com/AkronimBlack/eslgo"
)

func main() {
	go func() {
		fmt.Println(http.ListenAndServe("localhost:6060", nil))
	}()
	opts := eslgo.NewDialOpts("10.135.11.97:8021", "ClueCon", 2*time.Second, func() {
		fmt.Println("Inbound Connection Disconnected")
	}, nil)

	ctx := context.Background()

	// Connect to FreeSWITCH
	conn, err := eslgo.Dial(ctx, opts)
	if err != nil {
		fmt.Println("Error connecting", err)
		return
	}

	channelUuid := uuid.New().String()

	// Create a basic context
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	response, err := conn.OriginateCall(
		ctx,
		true,
		eslgo.Leg{CallURL: "user/777"},
		eslgo.Leg{CallURL: "&park()"},
		map[string]string{"origination_uuid": channelUuid})
	fmt.Println("Call Originated: ", response, err)

	ch := make(chan *eslgo.Event, 10)

	conn.RegisterEventListener(eslgo.EventListenAll, func(event *eslgo.Event) {
		ch <- event
	})

	resp, err := conn.SendCommand(ctx, command.Event{
		Format: "plain",
		Listen: []string{"all"},
	})

	log.Print(resp, err)

	for {
		msg := <-ch
		log.Print(msg.String())
	}

	conn.ExitAndClose()
}

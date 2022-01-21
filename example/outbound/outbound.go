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
	"log"

	"github.com/AkronimBlack/eslgo"
)

func main() {
	// Start listening, this is a blocking function
	log.Fatalln(eslgo.ListenAndServe(":8084", handleConnection, nil))
}

func handleConnection(ctx context.Context, conn *eslgo.Conn, response *eslgo.RawResponse) {
	fmt.Printf("Got connection! %#v\n", response)
	finished := make(chan bool)
	conn.SetFinishedChannel(finished)

	response, err := conn.OriginateCall(
		ctx,
		false,
		eslgo.Leg{CallURL: "user/100"},
		eslgo.Leg{CallURL: "&playback(misc/ivr-to_hear_screaming_monkeys.wav)"},
		map[string]string{})

	fmt.Println("Call Originated: ", response, err)
	if err != nil {
		return
	}

	if !response.IsOk() {
		return
	}
	fmt.Println("Waiting for connection finished event")
	<-finished
	fmt.Println("Connection finished")

}

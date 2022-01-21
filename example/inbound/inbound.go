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
	"time"

	"github.com/AkronimBlack/eslgo"
)

func main() {
	// Connect to FreeSWITCH
	conn, err := eslgo.Dial("127.0.0.1:8021", "ClueCon", 2*time.Second)
	if err != nil {
		fmt.Println("Error connecting", err)
		return
	}
	finished := make(chan bool)
	conn.SetFinishedChannel(finished)

	// Create a basic context
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Place the call in the background(bgapi) to user 100 and playback an audio file as the bLeg and no exported variables
	response, err := conn.OriginateCall(
		ctx,
		true,
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
	// Close the connection after sleeping for a bit
	time.Sleep(60 * time.Second)
	conn.ExitAndClose()
	fmt.Println("Waiting for connection finished event")
	<-finished
	fmt.Println("Connection finished")
}

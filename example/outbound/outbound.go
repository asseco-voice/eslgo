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
	"io"
	"log"
	"net/http"
	"os"
	"runtime"
	"time"

	"github.com/AkronimBlack/eslgo"
	_ "net/http/pprof"
)

func main() {
	// Start listening, this is a blocking function
	//memory log
	go memoryLog()
	//run pprof endpoint
	go func() {
		fmt.Println(http.ListenAndServe("0.0.0.0:6060", nil))
	}()
	log.Fatalln(eslgo.ListenAndServe("0.0.0.0:8040", handleConnection, eslgo.NewOptions("tcp", 0, nil, context.Background())))
}

func handleConnection(ctx context.Context, conn *eslgo.Conn, response *eslgo.RawResponse) {
	log.Print("New connection")
	ch := make(chan *eslgo.Event, 10)
	//log.Print(response.String())
	channelUuid := response.GetHeader("Unique-Id")
	log.Println(channelUuid)

	_, err := conn.SendCommand(ctx, command.Event{
		Format: "plain",
		Listen: []string{"all"},
	})

	if err != nil {
		log.Panic(err)
		return
	}

	conn.RegisterEventListener(channelUuid, func(event *eslgo.Event) {
		ch <- event
	})
	conn.RegisterEventListener(eslgo.EventListenAll, func(event *eslgo.Event) {
		ch <- event
	})

	err = conn.AnswerCall(ctx, channelUuid)
	for {
		msg := <-ch
		log.Print(msg.String())
	}

	conn.ExitAndClose()
}

func metricLogger() *log.Logger {
	var err error
	var metricLogFile *os.File
	if metricLogFile, err = os.OpenFile("metric_log.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666); err != nil {
		log.Panic(err.Error())
	}
	metricLog := log.New(io.MultiWriter(metricLogFile, os.Stdout), "", 0)
	return metricLog
}

func memoryLog() {
	mLog := metricLogger()
	mem := &runtime.MemStats{}
	for {
		runtime.ReadMemStats(mem)
		mLog.Printf("Time       = %v", time.Now().String())
		mLog.Printf("CPU        = %v", runtime.NumCPU())
		mLog.Printf("Alloc      = %v", bToKb(mem.Alloc))
		mLog.Printf("HeapAlloc  = %v", bToKb(mem.HeapAlloc))
		mLog.Printf("TotalAlloc = %v", bToKb(mem.TotalAlloc))
		mLog.Printf("Sys        = %v", bToKb(mem.Sys))
		mLog.Printf("NumGC      = %v", mem.NumGC)
		mLog.Printf("-----------------")
		time.Sleep(3 * time.Second)
	}
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}
func bToKb(b uint64) uint64 {
	return b / 1024
}

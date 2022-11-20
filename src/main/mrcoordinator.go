package main

//
// start the coordinator process, which is implemented
// in ../mr/coordinator.go
//
// go run mrcoordinator.go pg*.txt
//
// Please do not change this file.
//

import (
	"fmt"
	"os"
	"time"

	"6.824/mr"
	log "github.com/sirupsen/logrus"
)

func initLog() {
	log.SetFormatter(
		&log.TextFormatter{
			DisableColors: false,
			FullTimestamp: true,
		},
	)
	log.SetLevel(log.ErrorLevel)
}

func main() {
	initLog()
	log.Info("Launch the Map reduce coordinator...")
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrcoordinator inputfiles...\n")
		os.Exit(1)
	}

	m := mr.MakeCoordinator(os.Args[1:], 10)
	log.Info("coordinator is running...")
	for m.Done() == false {
		time.Sleep(time.Second)
	}
	log.Info("coordinator done.")
	time.Sleep(time.Second)
}

package main

import (
	"fmt"

	persistentlog "github.com/noahdw/plog"
)

func main() {
	log, err := persistentlog.NewPeristentLog()
	if err != nil {
		fmt.Printf("Error: %s\n", err.Error())
		return
	}
	defer log.Close()

	log.StoreValue("This is some text 123456789")
	log.StoreValue("omegaepsilon")
	fmt.Printf("maxLogIndex %d\n", log.GetMaxLogIndex())
}

package main

import (
	"flag"
	"log"
	"net/http"

	"github.com/gocircuit/circuit/client"
)

var flagDiscover = flag.String("discover", "228.8.8.8:8822", "Multicast UDP discovery address.")
var flagMaxJobs = flag.Int("maxjobs", 2, "Maximum number of concurrent jobs per worker.")

func connect(discoverAddr string) *client.Client {
	defer func() {
		if r := recover(); r != nil {
			log.Fatalf("Could not connect to circuit: %v", r)
		}
	}()
	return client.DialDiscover(discoverAddr, nil)
}

func main() {
	flag.Parse()
	log.Println("Connecting via multicast", *flagDiscover)
	conn := connect(*flagDiscover)
	log.Println("Connected to circuit.")
	controller := NewController(conn, *flagMaxJobs)
	http.HandleFunc("/add", Http{controller}.AddJob)
	http.HandleFunc("/status", Http{controller}.ShowStatus)
	log.Fatal(http.ListenAndServe(":8080", nil))
}

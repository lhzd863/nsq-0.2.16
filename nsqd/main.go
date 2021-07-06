package main

import (
	"github.com/lhzd863/nsq-0.2.16/nsq"
	"github.com/lhzd863/nsq-0.2.16/util"
	"crypto/md5"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

var (
	showVersion     = flag.Bool("version", false, "print version string")
	httpAddress     = flag.String("http-address", "0.0.0.0:4151", "<addr>:<port> to listen on for HTTP clients")
	tcpAddress      = flag.String("tcp-address", "0.0.0.0:4150", "<addr>:<port> to listen on for TCP clients")
	memQueueSize    = flag.Int64("mem-queue-size", 10000, "number of messages to keep in memory (per topic)")
	maxBytesPerFile = flag.Int64("max-bytes-per-file", 104857600, "number of bytes per diskqueue file before rolling")
	syncEvery       = flag.Int64("sync-every", 2500, "number of messages between diskqueue syncs")
	msgTimeoutMs    = flag.Int64("msg-timeout", 60000, "time (ms) to wait before auto-requeing a message")
	dataPath        = flag.String("data-path", "", "path to store disk-backed messages")
	workerId        = flag.Int64("worker-id", 0, "unique identifier (int) for this worker (will default to a hash of hostname)")
	verbose         = flag.Bool("verbose", false, "enable verbose logging")
	statsdAddress   = flag.String("statsd-address", "", "UDP <addr>:<port> of a statsd daemon for writing stats")
	statsdInterval  = flag.Int("statsd-interval", 30, "seconds between pushing to statsd")
	lookupdTCPAddrs = util.StringArray{}
)

func init() {
	flag.Var(&lookupdTCPAddrs, "lookupd-tcp-address", "lookupd TCP address (may be given multiple times)")
}

var nsqd *NSQd
var protocols = map[int32]nsq.Protocol{}

func main() {
	flag.Parse()

	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}

	if *showVersion {
		fmt.Printf("nsqd v%s\n", util.BINARY_VERSION)
		return
	}

	if *workerId == 0 {
		h := md5.New()
		io.WriteString(h, hostname)
		*workerId = int64(crc32.ChecksumIEEE(h.Sum(nil)) % 1024)
	}

	tcpAddr, err := net.ResolveTCPAddr("tcp", *tcpAddress)
	if err != nil {
		log.Fatal(err)
	}

	httpAddr, err := net.ResolveTCPAddr("tcp", *httpAddress)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("nsqd v%s", util.BINARY_VERSION)
	log.Printf("worker id %d", *workerId)

	exitChan := make(chan int)
	signalChan := make(chan os.Signal, 1)
	go func() {
		<-signalChan
		exitChan <- 1
	}()
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	if *statsdAddress != "" {
		underHostname := fmt.Sprintf("%s_%d", strings.Replace(hostname, ".", "_", -1), httpAddr.Port)
		prefix := fmt.Sprintf("nsq.%s.", underHostname)
		go statsdLoop(*statsdAddress, prefix, *statsdInterval)
	}

	options := NewNsqdOptions()
	options.memQueueSize = *memQueueSize
	options.dataPath = *dataPath
	options.maxBytesPerFile = *maxBytesPerFile
	options.syncEvery = *syncEvery
	options.msgTimeout = time.Duration(*msgTimeoutMs) * time.Millisecond

	nsqd = NewNSQd(*workerId, options)
	nsqd.tcpAddr = tcpAddr
	nsqd.httpAddr = httpAddr
	nsqd.lookupdTCPAddrs = lookupdTCPAddrs

	nsqd.LoadMetadata()
	nsqd.Main()
	<-exitChan
	nsqd.Exit()
}

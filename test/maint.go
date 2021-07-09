package main

import (
	"compress/gzip"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/lhzd863/nsq-0.2.16/nsq"
	"github.com/lhzd863/nsq-0.2.16/util"
)

var (
	showVersion      = flag.Bool("version", false, "print version string")
	topic            = flag.String("topic", "", "nsq topic")
	channel          = flag.String("channel", "nsq_to_file", "nsq channel")
	maxInFlight      = flag.Int("max-in-flight", 1000, "max number of messages to allow in flight")
	verbose          = flag.Bool("verbose", false, "verbose logging")
	nsqdTCPAddrs     = util.StringArray{}
	lookupdHTTPAddrs = util.StringArray{}
)

func init() {
	flag.Var(&nsqdTCPAddrs, "nsqd-tcp-address", "nsqd TCP address (may be given multiple times)")
	flag.Var(&lookupdHTTPAddrs, "lookupd-http-address", "lookupd HTTP address (may be given multiple times)")
}

type FileLogger struct {
	out        *os.File
	gzipWriter *gzip.Writer
	filename   string
	logChan    chan *Message
}

type Message struct {
	*nsq.Message
	returnChannel chan *nsq.FinishedMessage
}

type SyncMsg struct {
	m             *nsq.FinishedMessage
	returnChannel chan *nsq.FinishedMessage
}

func (l *FileLogger) HandleMessage(m *nsq.Message, responseChannel chan *nsq.FinishedMessage) {
	l.logChan <- &Message{m, responseChannel}
}

func router(r *nsq.Reader, f *FileLogger, termChan chan os.Signal, hupChan chan os.Signal) {
	pos := 0
	output := make([]*Message, *maxInFlight)
	sync := false
	ticker := time.NewTicker(time.Duration(30) * time.Second)
	closing := false

	for {
		select {
		case <-termChan:
			ticker.Stop()
			r.Stop()
			// ensures that we keep flushing whatever is left in the channels
			closing = true
		case <-hupChan:
			f.Close()
			f.updateFile()
			sync = true
		case <-ticker.C:
			f.updateFile()
			sync = true
		case m := <-f.logChan:
			if f.updateFile() {
				sync = true
			}
			_, err := f.Write(m.Body)
			if err != nil {
				log.Fatalf("ERROR: writing message to disk - %s", err.Error())
			}
			_, err = f.Write([]byte("\n"))
			if err != nil {
				log.Fatalf("ERROR: writing newline to disk - %s", err.Error())
			}
			output[pos] = m
			pos++
			if pos == *maxInFlight {
				sync = true
			}
		}

		if closing || sync || r.IsStarved() {
			if pos > 0 {
				log.Printf("syncing %d records to disk", pos)
				err := f.Sync()
				if err != nil {
					log.Fatalf("ERROR: failed syncing messages - %s", err.Error())
				}
				for pos > 0 {
					pos--
					m := output[pos]
					m.returnChannel <- &nsq.FinishedMessage{m.Id, 0, true}
					output[pos] = nil
				}
			}
			sync = false
		}
	}
}

func (f *FileLogger) Close() {
	if f.out != nil {
		if f.gzipWriter != nil {
			f.gzipWriter.Close()
		}
		f.out.Close()
		f.out = nil
	}
}
func (f *FileLogger) Write(p []byte) (n int, err error) {
	fmt.Printf(string(p))
	return len(p), nil
}

func (f *FileLogger) Sync() error {
	return nil
}

func (f *FileLogger) updateFile() bool {
	return true
}

func main() {
	flag.Parse()

	if *showVersion {
		fmt.Printf("nsq_to_file v%s\n", util.BINARY_VERSION)
		return
	}

	if *topic == "" || *channel == "" {
		log.Fatalf("--topic and --channel are required")
	}

	if *maxInFlight < 0 {
		log.Fatalf("--max-in-flight must be > 0")
	}

	if len(nsqdTCPAddrs) == 0 && len(lookupdHTTPAddrs) == 0 {
		log.Fatalf("--nsqd-tcp-address or --lookupd-http-address required.")
	}
	if len(nsqdTCPAddrs) != 0 && len(lookupdHTTPAddrs) != 0 {
		log.Fatalf("use --nsqd-tcp-address or --lookupd-http-address not both")
	}

	hupChan := make(chan os.Signal, 1)
	termChan := make(chan os.Signal, 1)
	signal.Notify(hupChan, syscall.SIGHUP)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM)

	f := &FileLogger{
		logChan: make(chan *Message, 1),
	}

	r, err := nsq.NewReader(*topic, *channel)
	if err != nil {
		log.Fatalf(err.Error())
	}
	r.SetMaxInFlight(*maxInFlight)
	r.VerboseLogging = *verbose

	r.AddAsyncHandler(f)
	go router(r, f, termChan, hupChan)

	for _, addrString := range nsqdTCPAddrs {
		err := r.ConnectToNSQ(addrString)
		if err != nil {
			log.Fatalf(err.Error())
		}
	}

	for _, addrString := range lookupdHTTPAddrs {
		log.Printf("lookupd addr %s", addrString)
		err := r.ConnectToLookupd(addrString)
		if err != nil {
			log.Fatalf(err.Error())
		}
	}
	<-r.ExitChan
}

// This is an NSQ client that reads the specified topic/channel
// and performs HTTP requests (GET/POST) to the specified endpoints

package main

import (
	"github.com/nsqio/nsq/nsq"
	"github.com/nsqio/nsq/util"
	"bytes"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"
)

const (
	ModeAll = iota
	ModeRoundRobin
)

var (
	showVersion      = flag.Bool("version", false, "print version string")
	topic            = flag.String("topic", "", "nsq topic")
	channel          = flag.String("channel", "nsq_to_http", "nsq channel")
	maxInFlight      = flag.Int("max-in-flight", 200, "max number of messages to allow in flight")
	verbose          = flag.Bool("verbose", false, "enable verbose logging")
	numPublishers    = flag.Int("n", 100, "number of concurrent publishers")
	roundRobin       = flag.Bool("round-robin", false, "enable round robin mode")
	throttleFraction = flag.Float64("throttle-fraction", 1.0, "publish only a fraction of messages")
	httpTimeoutMs    = flag.Int("http-timeout-ms", 20000, "timeout for HTTP connect/read/write (each)")
	statusEvery      = flag.Int("status-every", 250, "the # of requests between logging status (per handler), 0 disables")
	getAddrs         = util.StringArray{}
	postAddrs        = util.StringArray{}
	nsqdTCPAddrs     = util.StringArray{}
	lookupdHTTPAddrs = util.StringArray{}
)

func init() {
	flag.Var(&postAddrs, "post", "HTTP address to make a POST request to.  data will be in the body (may be given multiple times)")
	flag.Var(&getAddrs, "get", "HTTP address to make a GET request to. '%s' will be printf replaced with data (may be given multiple times)")
	flag.Var(&nsqdTCPAddrs, "nsqd-tcp-address", "nsqd TCP address (may be given multiple times)")
	flag.Var(&lookupdHTTPAddrs, "lookupd-http-address", "lookupd HTTP address (may be given multiple times)")
}

type Durations []time.Duration

func (s Durations) Len() int {
	return len(s)
}

func (s Durations) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s Durations) Less(i, j int) bool {
	return s[i] < s[j]
}

type Publisher interface {
	Publish(string, []byte) error
}

type PublishHandler struct {
	Publisher
	addresses util.StringArray
	counter   uint64
	mode      int
	reqs      Durations
	id        int
}

func (ph *PublishHandler) HandleMessage(m *nsq.Message) error {
	var startTime time.Time

	if *throttleFraction < 1.0 && rand.Float64() > *throttleFraction {
		return nil
	}

	if *statusEvery > 0 {
		startTime = time.Now()
	}

	switch ph.mode {
	case ModeAll:
		for _, addr := range ph.addresses {
			err := ph.Publish(addr, m.Body)
			if err != nil {
				return err
			}
		}
	case ModeRoundRobin:
		idx := ph.counter % uint64(len(ph.addresses))
		err := ph.Publish(ph.addresses[idx], m.Body)
		if err != nil {
			return err
		}
		ph.counter++
	}

	if *statusEvery > 0 {
		duration := time.Now().Sub(startTime)
		ph.reqs = append(ph.reqs, duration)
	}

	if *statusEvery > 0 && len(ph.reqs) >= *statusEvery {
		var total time.Duration
		for _, v := range ph.reqs {
			total += v
		}
		avgMs := (total.Seconds() * 1000) / float64(len(ph.reqs))

		sort.Sort(ph.reqs)
		p95Ms := percentile(95.0, ph.reqs, len(ph.reqs)).Seconds() * 1000
		p99Ms := percentile(99.0, ph.reqs, len(ph.reqs)).Seconds() * 1000

		log.Printf("handler(%d): finished %d requests - 99th: %.02fms - 95th: %.02fms - avg: %.02fms",
			ph.id, *statusEvery, p99Ms, p95Ms, avgMs)

		ph.reqs = ph.reqs[:0]
	}

	return nil
}

func percentile(perc float64, arr []time.Duration, length int) time.Duration {
	indexOfPerc := int(math.Ceil(((perc / 100.0) * float64(length)) + 0.5))
	if indexOfPerc >= length {
		indexOfPerc = length - 1
	}
	return arr[indexOfPerc]
}

type PostPublisher struct{}

func (p *PostPublisher) Publish(addr string, msg []byte) error {
	buf := bytes.NewBuffer(msg)
	resp, err := HttpPost(addr, buf)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		return errors.New(fmt.Sprintf("got status code %d", resp.StatusCode))
	}
	return nil
}

type GetPublisher struct{}

func (p *GetPublisher) Publish(addr string, msg []byte) error {
	endpoint := fmt.Sprintf(addr, url.QueryEscape(string(msg)))
	resp, err := HttpGet(endpoint)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		return errors.New(fmt.Sprintf("got status code %d", resp.StatusCode))
	}
	return nil
}

func main() {
	var publisher Publisher
	var addresses util.StringArray
	var mode int

	flag.Parse()

	if *showVersion {
		fmt.Printf("nsq_to_http v%s\n", util.BINARY_VERSION)
		return
	}

	if *topic == "" || *channel == "" {
		log.Fatalf("--topic and --channel are required")
	}

	if *maxInFlight < 0 {
		log.Fatalf("--max-in-flight must be > 0")
	}

	if len(nsqdTCPAddrs) == 0 && len(lookupdHTTPAddrs) == 0 {
		log.Fatalf("--nsqd-tcp-address or --lookupd-http-address required")
	}
	if len(nsqdTCPAddrs) > 0 && len(lookupdHTTPAddrs) > 0 {
		log.Fatalf("use --nsqd-tcp-address or --lookupd-http-address not both")
	}

	if len(getAddrs) == 0 && len(postAddrs) == 0 {
		log.Fatalf("--get or --post required")
	}
	if len(getAddrs) > 0 && len(postAddrs) > 0 {
		log.Fatalf("use --get or --post not both")
	}
	if len(getAddrs) > 0 {
		for _, get := range getAddrs {
			if strings.Count(get, "%s") != 1 {
				log.Fatal("invalid GET address - must be a printf string")
			}
		}
	}

	if *roundRobin {
		mode = ModeRoundRobin
	}

	if *throttleFraction > 1.0 || *throttleFraction < 0.0 {
		log.Fatalf("Throttle fraction must be between 0.0 and 1.0")
	}

	hupChan := make(chan os.Signal, 1)
	termChan := make(chan os.Signal, 1)
	signal.Notify(hupChan, syscall.SIGHUP)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM)

	if len(postAddrs) > 0 {
		publisher = &PostPublisher{}
		addresses = postAddrs
	} else {
		publisher = &GetPublisher{}
		addresses = getAddrs
	}

	r, err := nsq.NewReader(*topic, *channel)
	if err != nil {
		log.Fatalf(err.Error())
	}
	r.SetMaxInFlight(*maxInFlight)
	r.VerboseLogging = *verbose

	for i := 0; i < *numPublishers; i++ {
		handler := &PublishHandler{
			Publisher: publisher,
			addresses: addresses,
			mode:      mode,
			reqs:      make(Durations, 0, *statusEvery),
			id:        i,
		}
		r.AddHandler(handler)
	}

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

	select {
	case <-r.ExitChan:
	case <-hupChan:
		r.Stop()
	case <-termChan:
		r.Stop()
	}
}

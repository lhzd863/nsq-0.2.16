package nsq

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// returned from ConnectToNSQ() when already connected
var ErrAlreadyConnected = errors.New("already connected")

// Handler is the synchronous interface to Reader.
//
// Implement this interface for handlers that return whether or not message
// processing completed successfully.
//
// When the return value is nil Reader will automatically handle FINishing.
//
// When the returned value is non-nil Reader will automatically handle REQueing.
type Handler interface {
	HandleMessage(message *Message) error
}

// AsyncHandler is the asynchronous interface to Reader.
//
// Implement this interface for handlers that wish to defer responding until later.
// This is particularly useful if you want to batch work together.
//
// An AsyncHandler must send:
//
//     &FinishedMessage{messageID, requeueDelay, true|false}
//
// To the supplied responseChannel to indicate that a message is processed.
type AsyncHandler interface {
	HandleMessage(message *Message, responseChannel chan *FinishedMessage)
}

// FinishedMessage is the data type used over responseChannel in AsyncHandlers
type FinishedMessage struct {
	Id             MessageID
	RequeueDelayMs int
	Success        bool
}

// FailedMessageLogger is an interface that can be implemented by handlers that wish
// to receive a callback when a message is deemed "failed" (i.e. the number of attempts
// exceeded the Reader specified MaxAttemptCount)
type FailedMessageLogger interface {
	LogFailedMessage(message *Message)
}

type incomingMessage struct {
	*Message
	responseChannel chan *FinishedMessage
}

type nsqConn struct {
	net.Conn
	r                *bufio.Reader
	addr             string
	stopFlag         int32
	finishedMessages chan *FinishedMessage
	messagesInFlight int64
	messagesReceived uint64
	messagesFinished uint64
	messagesRequeued uint64
	rdyCount         int64
	readTimeout      time.Duration
	writeTimeout     time.Duration
	stopper          sync.Once
	dying            chan struct{}
	drainReady       chan struct{}
}

func newNSQConn(addr string, readTimeout time.Duration, writeTimeout time.Duration) (*nsqConn, error) {
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		return nil, err
	}

	nc := &nsqConn{
		Conn:             conn,
		r:                bufio.NewReader(conn),
		addr:             addr,
		finishedMessages: make(chan *FinishedMessage),
		readTimeout:      readTimeout,
		writeTimeout:     writeTimeout,
		dying:            make(chan struct{}, 1),
		drainReady:       make(chan struct{}),
	}

	nc.SetWriteDeadline(time.Now().Add(nc.writeTimeout))
	_, err = conn.Write(MagicV2)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("[%s] failed to write magic - %s", addr, err.Error())
	}

	return nc, nil
}

func (c *nsqConn) String() string {
	return c.addr
}

func (c *nsqConn) Read(p []byte) (int, error) {
	c.SetReadDeadline(time.Now().Add(c.readTimeout))
	return c.r.Read(p)
}

func (c *nsqConn) Write(p []byte) (int, error) {
	c.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	return c.Conn.Write(p)
}

func (c *nsqConn) sendCommand(buf *bytes.Buffer, cmd *Command) error {
	buf.Reset()
	err := cmd.Write(buf)
	if err != nil {
		return err
	}
	_, err = buf.WriteTo(c)
	return err
}

// Reader is a high-level type to consume from NSQ.
//
// A Reader instance is supplied handler(s) that will be executed
// concurrently via goroutines to handle processing the stream of messages
// consumed from the specified topic/channel.  See: AsyncHandler and Handler
// for details on implementing those interfaces to create handlers.
//
// If configured, it will poll nsqlookupd instances and handle connection (and
// reconnection) to any discovered nsqds.
type Reader struct {
	TopicName           string        // name of topic to subscribe to
	ChannelName         string        // name of channel to subscribe to
	LookupdPollInterval time.Duration // seconds between polling lookupd's (+/- random 1/10th this value for jitter)
	MaxAttemptCount     uint16        // maximum number of times this reader will attempt to process a message
	DefaultRequeueDelay time.Duration // the default duration when REQueueing
	MaxRequeueDelay     time.Duration // the maximum duration when REQueueing (for doubling backoff)
	VerboseLogging      bool          // enable verbose logging
	ShortIdentifier     string        // an identifier to send to nsqd when connecting (defaults: short hostname)
	LongIdentifier      string        // an identifier to send to nsqd when connecting (defaults: long hostname)
	ReadTimeout         time.Duration // the deadline set for network reads
	WriteTimeout        time.Duration // the deadline set for network writes
	MessagesReceived    uint64        // an atomic counter - # of messages received
	MessagesFinished    uint64        // an atomic counter - # of messages FINished
	MessagesRequeued    uint64        // an atomic counter - # of messages REQueued
	ExitChan            chan int      // read from this channel to block your main loop

	// internal variables
	maxInFlight        int
	incomingMessages   chan *incomingMessage
	nsqConnections     map[string]*nsqConn
	lookupdExitChan    chan int
	lookupdRecheckChan chan int
	stopFlag           int32
	runningHandlers    int32
	messagesInFlight   int64
	lookupdHTTPAddrs   []string
	stopHandler        sync.Once
}

// NewReader creates a new instance of Reader for the specified topic/channel
//
// The returned Reader instance is setup with sane default values.  To modify
// configuration, update the values on the returned instance before connecting.
func NewReader(topic string, channel string) (*Reader, error) {
	if !IsValidTopicName(topic) {
		return nil, errors.New("invalid topic name")
	}

	if !IsValidChannelName(channel) {
		return nil, errors.New("invalid channel name")
	}

	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("ERROR: unable to get hostname %s", err.Error())
	}
	q := &Reader{
		TopicName:           topic,
		ChannelName:         channel,
		incomingMessages:    make(chan *incomingMessage),
		ExitChan:            make(chan int),
		nsqConnections:      make(map[string]*nsqConn),
		MaxAttemptCount:     5,
		LookupdPollInterval: 120 * time.Second,
		lookupdExitChan:     make(chan int),
		lookupdRecheckChan:  make(chan int, 1), // used at connection close to force a possible reconnect
		DefaultRequeueDelay: 90 * time.Second,
		MaxRequeueDelay:     15 * time.Minute,
		ShortIdentifier:     strings.Split(hostname, ".")[0],
		LongIdentifier:      hostname,
		ReadTimeout:         DefaultClientTimeout,
		WriteTimeout:        time.Second,
		maxInFlight:         1,
	}
	return q, nil
}

// ConnectionMaxInFlight calculates the per-connection max-in-flight count.
//
// This may change dynamically based on the number of connections to nsqd the Reader
// is responsible for.
func (q *Reader) ConnectionMaxInFlight() int {
	b := float64(q.maxInFlight)
	s := b / float64(len(q.nsqConnections))
	return int(math.Min(math.Max(1, s), b))
}

// IsStarved indicates whether any connections for this reader are blocked on processing
// before being able to receive more messages (ie. RDY count of 0 and not exiting)
func (q *Reader) IsStarved() bool {
	for _, conn := range q.nsqConnections {
		threshold := int64(float64(atomic.LoadInt64(&conn.rdyCount)) * 0.85)
		if atomic.LoadInt64(&conn.messagesInFlight) >= threshold &&
			atomic.LoadInt32(&conn.stopFlag) != 1 {
			return true
		}
	}
	return false
}

// SetMaxInFlight sets the maximum number of messages this reader instance
// will allow in-flight.
//
// If already connected, it updates the reader RDY state for each connection.
func (q *Reader) SetMaxInFlight(maxInFlight int) {
	if atomic.LoadInt32(&q.stopFlag) == 1 {
		return
	}

	if maxInFlight > MaxReadyCount {
		log.Printf("WARNING: tried to SetMaxInFlight() > %d, truncating...", MaxReadyCount)
		maxInFlight = MaxReadyCount
	}

	if q.maxInFlight == maxInFlight {
		return
	}
	q.maxInFlight = maxInFlight

	for _, c := range q.nsqConnections {
		q.updateReady(c)
	}
}

// MaxInFlight returns the configured maximum number of messages to allow in-flight.
func (q *Reader) MaxInFlight() int {
	return q.maxInFlight
}

// ConnectToLookupd adds a nsqlookupd address to the list for this Reader instance.
//
// If it is the first to be added, it initiates an HTTP request to discover nsqd
// producers for the configured topic.
//
// A goroutine is spawned to handle continual polling.
func (q *Reader) ConnectToLookupd(addr string) error {
	// make a HTTP req to the lookupd, and ask it for endpoints that have the
	// topic we are interested in.
	// this is a go loop that fires every x seconds
	for _, x := range q.lookupdHTTPAddrs {
		if x == addr {
			return errors.New("lookupd address already exists")
		}
	}
	q.lookupdHTTPAddrs = append(q.lookupdHTTPAddrs, addr)

	// if this is the first one, kick off the go loop
	if len(q.lookupdHTTPAddrs) == 1 {
		q.queryLookupd()
		go q.lookupdLoop()
	}

	return nil
}

// poll all known lookup servers every LookupdPollInterval
func (q *Reader) lookupdLoop() {
	// add some jitter so that multiple consumers discovering the same topic,
	// when restarted at the same time, dont all connect at once.
	rand.Seed(time.Now().UnixNano())
	time.Sleep(time.Duration(rand.Int63n(int64(q.LookupdPollInterval / 10))))
	ticker := time.Tick(q.LookupdPollInterval)
	for {
		select {
		case <-ticker:
			q.queryLookupd()
		case <-q.lookupdRecheckChan:
			q.queryLookupd()
		case <-q.lookupdExitChan:
			return
		}
	}
}

// make a HTTP req to the /lookup endpoint on each lookup server
// to find what nsq's provide the topic we are consuming.
// for any new topics, initiate a connection to those NSQ's
func (q *Reader) queryLookupd() {
	for _, addr := range q.lookupdHTTPAddrs {
		endpoint := fmt.Sprintf("http://%s/lookup?topic=%s", addr, url.QueryEscape(q.TopicName))

		log.Printf("LOOKUPD: querying %s", endpoint)

		data, err := ApiRequest(endpoint)
		if err != nil {
			log.Printf("ERROR: lookupd %s - %s", addr, err.Error())
			continue
		}

		// {"data":{"channels":[],"producers":[{"address":"jehiah-air.local", "tpc_port":4150, "http_port":4151}],"timestamp":1340152173},"status_code":200,"status_txt":"OK"}
		producers, _ := data.Get("producers").Array()
		for _, producer := range producers {
			producerData, _ := producer.(map[string]interface{})
			address := producerData["address"].(string)
			//port := int(producerData["tcp_port"].(float64))
                        port, _ := strconv.Atoi(fmt.Sprintf("%v",producerData["tcp_port"]))

			// make an address, start a connection
			joined := net.JoinHostPort(address, strconv.Itoa(port))
			err = q.ConnectToNSQ(joined)
			if err != nil && err != ErrAlreadyConnected {
				log.Printf("ERROR: failed to connect to nsqd (%s) - %s", joined, err.Error())
				continue
			}
		}
	}
}

// ConnectToNSQ takes a nsqd address to connect directly to.
//
// It is recommended to use ConnectToLookupd so that topics are discovered
// automatically.  This method is useful when you want to connect to a single, local,
// instance.
func (q *Reader) ConnectToNSQ(addr string) error {
	var buf bytes.Buffer

	if atomic.LoadInt32(&q.stopFlag) == 1 {
		return errors.New("reader stopped")
	}

	if atomic.LoadInt32(&q.runningHandlers) == 0 {
		return errors.New("no handlers")
	}

	_, ok := q.nsqConnections[addr]
	if ok {
		return ErrAlreadyConnected
	}

	log.Printf("[%s] connecting to nsqd", addr)

	connection, err := newNSQConn(addr, q.ReadTimeout, q.WriteTimeout)
	if err != nil {
		return err
	}

	cmd := Subscribe(q.TopicName, q.ChannelName)
	err = connection.sendCommand(&buf, cmd)
	if err != nil {
		connection.Close()
		return fmt.Errorf("[%s] failed to subscribe to %s:%s - %s", q.TopicName, q.ChannelName, err.Error())
	}

	q.nsqConnections[connection.String()] = connection

	go q.readLoop(connection)
	go q.finishLoop(connection)

	return nil
}

func handleError(q *Reader, c *nsqConn, errMsg string) {
	log.Printf(errMsg)
	atomic.StoreInt32(&c.stopFlag, 1)
	if len(q.nsqConnections) == 1 && len(q.lookupdHTTPAddrs) == 0 {
		// This is the only remaining connection, so stop the queue
		atomic.StoreInt32(&q.stopFlag, 1)
	}
}

func (q *Reader) readLoop(c *nsqConn) {
	// prime our ready state
	err := q.updateReady(c)
	if err != nil {
		handleError(q, c, fmt.Sprintf("[%s] failed to send initial ready - %s", c, err.Error()))
		q.stopFinishLoop(c)
		return
	}

	for {
		if atomic.LoadInt32(&c.stopFlag) == 1 || atomic.LoadInt32(&q.stopFlag) == 1 {
			// start the connection close
			if atomic.LoadInt64(&c.messagesInFlight) == 0 {
				q.stopFinishLoop(c)
			} else {
				log.Printf("[%s] delaying close of FinishedMesages channel; %d outstanding messages", c, c.messagesInFlight)
			}
			log.Printf("[%s] stopped read loop ", c)
			break
		}

		resp, err := ReadResponse(c)
		if err != nil {
			handleError(q, c, fmt.Sprintf("[%s] error reading response %s", c, err.Error()))
			continue
		}

		frameType, data, err := UnpackResponse(resp)
		if err != nil {
			handleError(q, c, fmt.Sprintf("[%s] error (%s) unpacking response %d %s", c, err.Error(), frameType, data))
			continue
		}

		switch frameType {
		case FrameTypeMessage:
			msg, err := DecodeMessage(data)
			if err != nil {
				handleError(q, c, fmt.Sprintf("[%s] error (%s) decoding message %s", c, err.Error(), data))
				continue
			}

			remain := atomic.AddInt64(&c.rdyCount, -1)
			atomic.AddUint64(&c.messagesReceived, 1)
			atomic.AddUint64(&q.MessagesReceived, 1)
			atomic.AddInt64(&c.messagesInFlight, 1)
			atomic.AddInt64(&q.messagesInFlight, 1)

			if q.VerboseLogging {
				log.Printf("[%s] (remain %d) FrameTypeMessage: %s - %s", c, remain, msg.Id, msg.Body)
			}

			q.incomingMessages <- &incomingMessage{msg, c.finishedMessages}
		case FrameTypeResponse:
			switch {
			case bytes.Equal(data, []byte("CLOSE_WAIT")):
				// server is ready for us to close (it ack'd our StartClose)
				// we can assume we will not receive any more messages over this channel
				// (but we can still write back responses)
				log.Printf("[%s] received ACK from nsqd - now in CLOSE_WAIT", c)
				atomic.StoreInt32(&c.stopFlag, 1)
			case bytes.Equal(data, []byte("_heartbeat_")):
				var buf bytes.Buffer
				log.Printf("[%s] received heartbeat from nsqd", c)
				err := c.sendCommand(&buf, Nop())
				if err != nil {
					handleError(q, c, fmt.Sprintf("[%s] error sending NOP - %s", c, err.Error()))
					return
				}
			}
		case FrameTypeError:
			log.Printf("[%s] error from nsqd %s", c, data)
		default:
			log.Printf("[%s] unknown message type %d", c, frameType)
		}

		q.updateReady(c)
	}
}

func (q *Reader) finishLoop(c *nsqConn) {
	var buf bytes.Buffer

	for {
		select {
		case <-c.dying:
			log.Printf("[%s] breaking out of finish loop ", c)
			// Indicate drainReady because we will not pull any more off finishedMessages
			c.drainReady <- struct{}{}
			return
		case msg := <-c.finishedMessages:
			// Decrement this here so it is correct even if we can't respond to nsqd
			atomic.AddInt64(&q.messagesInFlight, -1)
			atomic.AddInt64(&c.messagesInFlight, -1)

			if msg.Success {
				if q.VerboseLogging {
					log.Printf("[%s] finishing %s", c, msg.Id)
				}
				err := c.sendCommand(&buf, Finish(msg.Id))
				if err != nil {
					log.Printf("[%s] error finishing %s - %s", c, msg.Id, err.Error())
					q.stopFinishLoop(c)
					continue
				}
				atomic.AddUint64(&c.messagesFinished, 1)
				atomic.AddUint64(&q.MessagesFinished, 1)
			} else {
				if q.VerboseLogging {
					log.Printf("[%s] requeuing %s", c, msg.Id)
				}
				err := c.sendCommand(&buf, Requeue(msg.Id, msg.RequeueDelayMs))
				if err != nil {
					log.Printf("[%s] error requeueing %s - %s", c, msg.Id, err.Error())
					q.stopFinishLoop(c)
					continue
				}
				atomic.AddUint64(&c.messagesRequeued, 1)
				atomic.AddUint64(&q.MessagesRequeued, 1)
			}

			if atomic.LoadInt64(&c.messagesInFlight) == 0 &&
				(atomic.LoadInt32(&c.stopFlag) == 1 || atomic.LoadInt32(&q.stopFlag) == 1) {
				q.stopFinishLoop(c)
				continue
			}
		}
	}
}

func (q *Reader) stopFinishLoop(c *nsqConn) {
	c.stopper.Do(func() {
		log.Printf("[%s] beginning stopFinishLoop logic", c)
		// This doesn't block because dying has buffer of 1
		c.dying <- struct{}{}

		// Drain the finishedMessages channel
		go func() {
			<-c.drainReady
			for atomic.AddInt64(&c.messagesInFlight, -1) >= 0 {
				<-c.finishedMessages
			}
		}()
		c.Close()
		delete(q.nsqConnections, c.String())

		log.Printf("there are %d connections left alive", len(q.nsqConnections))

		if len(q.nsqConnections) == 0 && len(q.lookupdHTTPAddrs) == 0 {
			// no lookupd entry means no reconnection
			if atomic.LoadInt32(&q.stopFlag) == 1 {
				q.stopHandlers()
			}
			return
		}

		// ie: we were the last one, and stopping
		if len(q.nsqConnections) == 0 && atomic.LoadInt32(&q.stopFlag) == 1 {
			q.stopHandlers()
		}

		if len(q.lookupdHTTPAddrs) != 0 && atomic.LoadInt32(&q.stopFlag) == 0 {
			// trigger a poll of the lookupd
			select {
			case q.lookupdRecheckChan <- 1:
			default:
			}
		}
	})
}

func (q *Reader) updateReady(c *nsqConn) error {
	var buf bytes.Buffer

	if atomic.LoadInt32(&c.stopFlag) != 0 {
		return nil
	}

	remain := atomic.LoadInt64(&c.rdyCount)
	mif := q.ConnectionMaxInFlight()
	// refill when at 1, or at 25% whichever comes first
	if remain <= 1 || remain < (int64(mif)/int64(4)) {
		if q.VerboseLogging {
			log.Printf("[%s] sending RDY %d (%d remain)", c, mif, remain)
		}
		atomic.StoreInt64(&c.rdyCount, int64(mif))
		err := c.sendCommand(&buf, Ready(mif))
		if err != nil {
			handleError(q, c, fmt.Sprintf("[%s] error sending RDY %d - %s", c, mif, err.Error()))
			return err
		}
	} else {
		if q.VerboseLogging {
			log.Printf("[%s] skip sending RDY (%d remain out of %d)", c, remain, mif)
		}
	}

	return nil
}

// Stop will gracefully stop the Reader
func (q *Reader) Stop() {
	var buf bytes.Buffer

	if !atomic.CompareAndSwapInt32(&q.stopFlag, 0, 1) {
		return
	}

	log.Printf("Stopping reader")

	if len(q.nsqConnections) == 0 {
		q.stopHandlers()
	} else {
		for _, c := range q.nsqConnections {
			err := c.sendCommand(&buf, StartClose())
			if err != nil {
				log.Printf("[%s] failed to start close - %s", c, err.Error())
			}
		}
		go func() {
			<-time.After(time.Duration(30) * time.Second)
			q.stopHandlers()
		}()
	}

	if len(q.lookupdHTTPAddrs) != 0 {
		q.lookupdExitChan <- 1
	}
}

func (q *Reader) stopHandlers() {
	q.stopHandler.Do(func() {
		log.Printf("closing incomingMessages")
		close(q.incomingMessages)
	})
}

// AddHandler adds a Handler for messages received by this Reader.
//
// See Handler for details on implementing this interface.
//
// It's ok to start more than one handler simultaneously, they
// are concurrently executed in goroutines.
func (q *Reader) AddHandler(handler Handler) {
	atomic.AddInt32(&q.runningHandlers, 1)
	log.Println("starting Handler go-routine")
	go func() {
		for {
			message, ok := <-q.incomingMessages
			if !ok {
				log.Printf("closing Handler (after self.incomingMessages closed)")
				if atomic.AddInt32(&q.runningHandlers, -1) == 0 {
					q.ExitChan <- 1
				}
				break
			}

			err := handler.HandleMessage(message.Message)
			if err != nil {
				log.Printf("ERR: handler returned %s for msg %s %s", err.Error(), message.Id, message.Body)
			}

			// message passed the max number of attempts
			if err != nil && q.MaxAttemptCount > 0 && message.Attempts > q.MaxAttemptCount {
				log.Printf("WARNING: msg attempted %d times. giving up %s %s", message.Attempts, message.Id, message.Body)
				logger, ok := handler.(FailedMessageLogger)
				if ok {
					logger.LogFailedMessage(message.Message)
				}
				message.responseChannel <- &FinishedMessage{message.Id, 0, true}
				continue
			}

			// linear delay
			requeueDelay := q.DefaultRequeueDelay * time.Duration(message.Attempts)
			// bound the requeueDelay to configured max
			if requeueDelay > q.MaxRequeueDelay {
				requeueDelay = q.MaxRequeueDelay
			}

			message.responseChannel <- &FinishedMessage{message.Id, int(requeueDelay / time.Millisecond), err == nil}
		}
	}()
}

// AddAsyncHandler adds an AsyncHandler for messages received by this Reader.
//
// See AsyncHandler for details on implementing this interface.
//
// It's ok to start more than one handler simultaneously, they
// are concurrently executed in goroutines.
func (q *Reader) AddAsyncHandler(handler AsyncHandler) {
	atomic.AddInt32(&q.runningHandlers, 1)
	log.Println("starting AsyncHandler go-routine")
	go func() {
		for {
			message, ok := <-q.incomingMessages
			if !ok {
				log.Printf("closing AsyncHandler (after self.incomingMessages closed)")
				if atomic.AddInt32(&q.runningHandlers, -1) == 0 {
					q.ExitChan <- 1
				}
				break
			}

			// message passed the max number of attempts
			// note: unfortunately it's not straight forward to do this after passing to async handler, so we don't.
			if q.MaxAttemptCount > 0 && message.Attempts > q.MaxAttemptCount {
				log.Printf("WARNING: msg attempted %d times. giving up %s %s", message.Attempts, message.Id, message.Body)
				logger, ok := handler.(FailedMessageLogger)
				if ok {
					logger.LogFailedMessage(message.Message)
				}
				message.responseChannel <- &FinishedMessage{message.Id, 0, true}
				continue
			}

			handler.HandleMessage(message.Message, message.responseChannel)
		}
	}()
}

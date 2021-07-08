package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lhzd863/nsq-0.2.16/nsq"
)

type StreamServer struct {
	sync.RWMutex // embed a r/w mutex
	clients      []*StreamReader
	messageCount uint64
}

func (s *StreamServer) Set(sr *StreamReader) {
	s.Lock()
	defer s.Unlock()
	s.clients = append(s.clients, sr)
}

func (s *StreamServer) Del(sr *StreamReader) {
	s.Lock()
	defer s.Unlock()
	n := make([]*StreamReader, len(s.clients)-1)
	for _, x := range s.clients {
		if x != sr {
			n = append(n, x)
		}
	}
	s.clients = n
}

func (s *StreamServer) ServeStart() {
	for {
		topicName := "test"
		channelName := "test"

		r, err := nsq.NewReader(topicName, channelName)
		r.SetMaxInFlight(100)
		if err != nil {
			log.Println(err.Error())
			return
		}

		sr := &StreamReader{
			topic:       topicName,
			channel:     channelName,
			reader:      r,
			connectTime: time.Now(),
		}
		s.Set(sr)

		r.AddHandler(sr)

		// TODO: handle the error cases better (ie. at all :) )
		nsqAddrs := make([]string, 0)
		nsqAddrs = append(nsqAddrs, "106.75.249.244:4160")
		errors := ConnectToNSQAndLookupd(r, nsqAddrs, make([]string, 0))
		log.Printf("connected to NSQ %v", errors)

		// this read allows us to detect clients that disconnect
		// sr.reader.Stop()
		go func() { sr.reader.Stop() }()

		go sr.HeartbeatLoop()
		time.Sleep(time.Duration(3) * time.Second)
	}
}

var streamServer *StreamServer

type StreamReader struct {
	sync.RWMutex // embed a r/w mutex
	topic        string
	channel      string
	reader       *nsq.Reader
	req          *http.Request
	conn         net.Conn
	bufrw        *bufio.ReadWriter
	connectTime  time.Time
}

func ConnectToNSQAndLookupd(r *nsq.Reader, nsqAddrs []string, lookupd []string) error {
	for _, addrString := range nsqAddrs {
		err := r.ConnectToNSQ(addrString)
		if err != nil {
			return err
		}
	}

	for _, addrString := range lookupd {
		log.Printf("lookupd addr %s", addrString)
		err := r.ConnectToLookupd(addrString)
		if err != nil {
			return err
		}
	}

	return nil
}

func (sr *StreamReader) HeartbeatLoop() {
	heartbeatTicker := time.NewTicker(30 * time.Second)
	defer func() {
		sr.conn.Close()
		heartbeatTicker.Stop()
		streamServer.Del(sr)
	}()
	for {
		select {
		case <-sr.reader.ExitChan:
			return
		case ts := <-heartbeatTicker.C:
			sr.Lock()
			fmt.Println(fmt.Sprintf("{\"_heartbeat_\":%d}\n", ts.Unix()))
			sr.Unlock()
		}
	}
}

func (sr *StreamReader) HandleMessage(message *nsq.Message) error {
	sr.Lock()
	log.Println(message.Body)
	sr.Unlock()
	atomic.AddUint64(&streamServer.messageCount, 1)
	return nil
}

func main() {
	s := &StreamServer{}
	s.ServeStart()
}

package main

import (
	"../nsq"
	"github.com/bmizerany/assert"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"testing"
	"time"
)

// ensure that we can push a message through a topic and get it out of a channel
func TestPutMessage(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	nsqd := NewNSQd(1, NewNsqdOptions())
	defer nsqd.Exit()

	topicName := "test_put_message" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	channel1 := topic.GetChannel("ch")

	var id nsq.MessageID
	msg := nsq.NewMessage(id, []byte("test"))
	topic.PutMessage(msg)

	outputMsg := <-channel1.clientMsgChan
	assert.Equal(t, msg.Id, outputMsg.Id)
	assert.Equal(t, msg.Body, outputMsg.Body)
}

// ensure that both channels get the same message
func TestPutMessage2Chan(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	nsqd := NewNSQd(1, NewNsqdOptions())
	defer nsqd.Exit()

	topicName := "test_put_message_2chan" + strconv.Itoa(int(time.Now().Unix()))
	topic := nsqd.GetTopic(topicName)
	channel1 := topic.GetChannel("ch1")
	channel2 := topic.GetChannel("ch2")

	var id nsq.MessageID
	msg := nsq.NewMessage(id, []byte("test"))
	topic.PutMessage(msg)

	outputMsg1 := <-channel1.clientMsgChan
	assert.Equal(t, msg.Id, outputMsg1.Id)
	assert.Equal(t, msg.Body, outputMsg1.Body)

	outputMsg2 := <-channel2.clientMsgChan
	assert.Equal(t, msg.Id, outputMsg2.Id)
	assert.Equal(t, msg.Body, outputMsg2.Body)
}

func TestInFlightWorker(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	options := NewNsqdOptions()
	options.msgTimeout = 300 * time.Millisecond
	nsqd := NewNSQd(1, options)
	defer nsqd.Exit()

	topic := nsqd.GetTopic("topic")
	channel := topic.GetChannel("channel")

	for i := 0; i < 1000; i++ {
		msg := nsq.NewMessage(<-nsqd.idChan, []byte("test"))
		channel.StartInFlightTimeout(msg, NewClientV2(nil))
	}

	assert.Equal(t, len(channel.inFlightMessages), 1000)
	assert.Equal(t, len(channel.inFlightPQ), 1000)

	time.Sleep(350 * time.Millisecond)

	assert.Equal(t, len(channel.inFlightMessages), 0)
	assert.Equal(t, len(channel.inFlightPQ), 0)
}

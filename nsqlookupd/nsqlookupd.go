package main

import (
	"github.com/lhzd863/nsq-0.2.16/util"
	"log"
	"net"
)

type NSQLookupd struct {
	tcpAddr      *net.TCPAddr
	httpAddr     *net.TCPAddr
	tcpListener  net.Listener
	httpListener net.Listener
	waitGroup    util.WaitGroupWrapper
	DB           *RegistrationDB
}

func NewNSQLookupd() *NSQLookupd {
	return &NSQLookupd{
		DB: NewRegistrationDB(),
	}
}

func (l *NSQLookupd) Main() {
	tcpListener, err := net.Listen("tcp", l.tcpAddr.String())
	if err != nil {
		log.Fatalf("FATAL: listen (%s) failed - %s", l.tcpAddr, err.Error())
	}
	l.tcpListener = tcpListener
	l.waitGroup.Wrap(func() { util.TcpServer(tcpListener, &TcpProtocol{protocols: protocols}) })

	httpListener, err := net.Listen("tcp", l.httpAddr.String())
	if err != nil {
		log.Fatalf("FATAL: listen (%s) failed - %s", l.httpAddr, err.Error())
	}
	l.httpListener = httpListener
	l.waitGroup.Wrap(func() { httpServer(httpListener) })

}

func (l *NSQLookupd) Exit() {

	if l.tcpListener != nil {
		l.tcpListener.Close()
	}

	if l.httpListener != nil {
		l.httpListener.Close()
	}
	l.waitGroup.Wait()

}

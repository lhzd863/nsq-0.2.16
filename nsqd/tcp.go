package main

import (
	"github.com/lhzd863/nsq-0.2.16/nsq"
	"github.com/lhzd863/nsq-0.2.16/util"
	"log"
	"net"
)

type TcpProtocol struct {
	util.TcpHandler
	protocols map[int32]nsq.Protocol
}

func (p *TcpProtocol) Handle(clientConn net.Conn) {
	log.Printf("TCP: new client(%s)", clientConn.RemoteAddr())

	protocolMagic, err := nsq.ReadMagic(clientConn)
	if err != nil {
		log.Printf("ERROR: failed to read protocol version - %s", err.Error())
		return
	}

	log.Printf("CLIENT(%s): desired protocol %d", clientConn.RemoteAddr(), protocolMagic)

	prot, ok := p.protocols[protocolMagic]
	if !ok {
		nsq.SendFramedResponse(clientConn, nsq.FrameTypeError, []byte("E_BAD_PROTOCOL"))
		log.Printf("ERROR: client(%s) bad protocol version %d", clientConn.RemoteAddr(), protocolMagic)
		return
	}

	err = prot.IOLoop(clientConn)
	if err != nil {
		log.Printf("ERROR: client(%s) - %s", clientConn.RemoteAddr(), err.Error())
		return
	}
}

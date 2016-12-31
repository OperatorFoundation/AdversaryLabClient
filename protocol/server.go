package protocol

import (
	"github.com/go-mangos/mangos"
	"github.com/go-mangos/mangos/protocol/rep"
	"github.com/go-mangos/mangos/transport/tcp"
)

type Responder func([]byte) []byte

type Server struct {
	sock mangos.Socket
}

func Listen(url string) Server {
	var sock mangos.Socket
	var err error

	if sock, err = rep.NewSocket(); err != nil {
		die("can't get new rep socket: %s", err)
	}

	sock.AddTransport(tcp.NewTransport())
	if err = sock.Listen(url); err != nil {
		die("can't listen on rep socket: %s", err.Error())
	}

	return Server{
		sock: sock,
	}
}

func (self Server) Accept(responder Responder) []byte {
	var err error
	var msg []byte
	var response []byte

	// Could also use sock.RecvMsg to get header
	msg, err = self.sock.Recv()
	//	fmt.Println("server received request:", string(msg))

	response = responder(msg)

	err = self.sock.Send(response)
	if err != nil {
		die("can't send reply: %s", err.Error())
	}

	return msg
}

func Ok(request []byte) []byte {
	return []byte("success")
}

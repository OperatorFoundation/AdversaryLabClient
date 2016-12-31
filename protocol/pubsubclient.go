package protocol

import (
	"fmt"

	"github.com/ugorji/go/codec"

	"github.com/go-mangos/mangos"
	"github.com/go-mangos/mangos/protocol/sub"
	"github.com/go-mangos/mangos/transport/tcp"
)

type PubsubClient struct {
	sock  mangos.Socket
	Rules chan Rule
}

func PubsubConnect(url string) PubsubClient {
	var sock mangos.Socket
	var err error

	if sock, err = sub.NewSocket(); err != nil {
		die("can't get new req socket: %s", err.Error())
	}

	sock.AddTransport(tcp.NewTransport())

	if err = sock.Dial(url); err != nil {
		die("can't dial on req socket: %s", err.Error())
	}

	err = sock.SetOption(mangos.OptionSubscribe, []byte(""))
	if err != nil {
		die("cannot subscribe: %s", err.Error())
	}

	rules := make(chan Rule)

	go pump(sock, rules)

	return PubsubClient{
		sock:  sock,
		Rules: rules,
	}
}

func pump(sock mangos.Socket, rules chan Rule) {
	var err error
	var msg []byte

	for {
		msg, err = sock.Recv()
		if err != nil {
			fmt.Println("Error reading subscription", err)
			return
		}

		var value = NamedType{}
		var h = NamedTypeHandle()
		var dec = codec.NewDecoderBytes(msg, h)
		var err = dec.Decode(&value)
		if err != nil {
			fmt.Println("Failed to decode")
			fmt.Println(err.Error())
			continue
		}

		switch value.Name {
		case "protocol.Rule":
			rule := RuleFromMap(value.Value.(map[interface{}]interface{}))
			rules <- rule
		default:
			fmt.Println("Unknown request type")
			fmt.Println(value)
		}
	}
}

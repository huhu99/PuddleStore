package pkg

import (
	"github.com/go-zookeeper/zk"
	tapestry "tapestry/pkg"
)

const TapNodePrefix = "/node-"

// Tapestry is a wrapper for a single Tapestry node. It is responsible for
// maintaining a zookeeper connection and implementing methods we provide
type Tapestry struct {
	tap *tapestry.Node
	zk  *zk.Conn
}

type TapestryMeta struct {
	ID      string
	Address string
}

// NewTapestry creates a new tapestry struct
func NewTapestry(tap *tapestry.Node, zkAddr string) (*Tapestry, error) {
	// set up a zookeeper connection and return a Tapestry struct
	zkConn, err := ConnectZk(zkAddr)
	if err != nil {
		return nil, err
	}

	path := TapRoot + TapNodePrefix
	nodeData, err := encodeTapestryMeta(TapestryMeta{ID: tap.ID(), Address: tap.Addr()})
	if err != nil {
		return nil, err
	}

	_, err = zkConn.CreateProtectedEphemeralSequential(path, nodeData, zk.WorldACL(zk.PermAll))
	if err != nil {
		return nil, err
	}

	return &Tapestry{tap: tap, zk: zkConn}, nil
}

// GracefulExit closes the zookeeper connection and gracefully shuts down the tapestry node
func (t *Tapestry) GracefulExit() {
	Out.Printf("Graceful Exit: Tapestry %s", t.tap.Addr())
	t.zk.Close()
	err := t.tap.Leave()
	if err != nil {
		Out.Println(err)
	}
	return
}

func encodeTapestryMeta(in TapestryMeta) ([]byte, error) {
	buf, err := encodeMsgPack(in)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeTapestryMeta(data []byte) (*TapestryMeta, error) {
	var in TapestryMeta
	if err := decodeMsgPack(data, &in); err != nil {
		return nil, err
	}
	return &in, nil
}

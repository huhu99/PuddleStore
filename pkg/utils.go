package pkg

import (
	"bytes"
	"github.com/hashicorp/go-msgpack/codec"
)

type blockIdx = uint64

// inode is the metadata of a file,
// and it's encoded and directly stored under 'Path' in the ZooKeeper
type inode struct {
	IsDir     bool
	Filename  string
	Path      string
	Size      uint64
	BlockSize uint64
	Blocks    []string
}

// Decode reverses the encode operation on a byte slice input
func decodeMsgPack(buf []byte, out interface{}) error {
	r := bytes.NewBuffer(buf)
	hd := codec.MsgpackHandle{}
	dec := codec.NewDecoder(r, &hd)
	return dec.Decode(out)
}

// Encode writes an encoded object to a new bytes buffer
func encodeMsgPack(in interface{}) (*bytes.Buffer, error) {
	buf := bytes.NewBuffer(nil)
	hd := codec.MsgpackHandle{}
	enc := codec.NewEncoder(buf, &hd)
	err := enc.Encode(in)
	return buf, err
}

func encodeInode(in inode) ([]byte, error) {
	buf, err := encodeMsgPack(in)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeInode(data []byte) (*inode, error) {
	var in inode
	if err := decodeMsgPack(data, &in); err != nil {
		return nil, err
	}
	return &in, nil
}

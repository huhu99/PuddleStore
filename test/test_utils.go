package test

import (
	"fmt"
	"github.com/go-zookeeper/zk"
	puddlestore "puddlestore/pkg"
	"testing"
)

func basicSetup(t *testing.T, numReplica int, numTapestry int, blockSize uint64) (
	puddlestore.Config,
	*puddlestore.Cluster,
	puddlestore.Client,
) {
	config := puddlestore.DefaultConfig()
	config.NumReplicas = numReplica
	config.NumTapestry = numTapestry
	config.BlockSize = blockSize

	cluster, err := puddlestore.CreateCluster(config)
	if err != nil {
		t.Fatal(err)
	}

	client, err := cluster.NewClient()
	if err != nil {
		t.Fatal(err)
	}
	return config, cluster, client
}

func testWriteFile(client puddlestore.Client, path string, offset uint64, data []byte) error {
	fd, err := client.Open(path, true, true)
	if err != nil {
		return err
	}
	defer func(client puddlestore.Client, fd int) {
		err = client.Close(fd)
		if err != nil {
			fmt.Println(err)
		}
	}(client, fd)

	return client.Write(fd, offset, data)
}

func testReadFile(client puddlestore.Client, path string, offset, size uint64) ([]byte, error) {
	fd, err := client.Open(path, true, false)
	if err != nil {
		return nil, err
	}
	defer func(client puddlestore.Client, fd int) {
		err = client.Close(fd)
		if err != nil {
			fmt.Println(err)
		}
	}(client, fd)

	return client.Read(fd, offset, size)
}

// clean up zookeeper at the end of the test
func cleanZooKeeper(zkConn *zk.Conn) {
	_ = removeZk(zkConn, puddlestore.TapRoot)
	_ = removeZk(zkConn, puddlestore.LockRoot)
	_ = removeZk(zkConn, puddlestore.PuddlestoreRoot)
	zkConn.Close()
}

func removeZk(zkConn *zk.Conn, path string) error {
	children, _, err := zkConn.Children(path)
	if err != nil {
		return err
	}
	for _, c := range children {
		childP := ""
		if path == "/" {
			childP = path + c
		} else {
			childP = path + "/" + c
		}

		err = removeZk(zkConn, childP)
		if err != nil {
			return err
		}
	}

	err = zkConn.Delete(path, -1)
	return err
}

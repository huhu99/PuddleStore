package test

import (
	puddlestore "puddlestore/pkg"
	tapestry "tapestry/pkg"
	"testing"
	"time"
)

// Shut down all tapestry nodes, read will fail.
func TestNoTapestry(t *testing.T) {
	config, cluster, client := basicSetup(t, 1, 1, 64)
	zkConn, _ := puddlestore.ConnectZk(config.ZkAddr)
	defer cluster.Shutdown()
	defer cleanZooKeeper(zkConn)
	defer client.Exit()

	tapNodes := cluster.TestGetTapestry()

	err := testWriteFile(client, "/test", 0, []byte("test"))
	if err != nil {
		t.Fatal(err)
	}

	for _, node := range tapNodes {
		node.GracefulExit()
	}

	time.Sleep(time.Second)

	_, err = testReadFile(client, "/test", 0, 10)
	if err == nil {
		t.Fatal("read should fail, but success")
	}
}

// Shut down a tapestry node, making not enough nodes for the flush.
func TestNotEnoughNode(t *testing.T) {
	config, cluster, client := basicSetup(t, 2, 2, 64)
	zkConn, _ := puddlestore.ConnectZk(config.ZkAddr)
	defer cluster.Shutdown()
	defer cleanZooKeeper(zkConn)
	defer client.Exit()

	tapNodes := cluster.TestGetTapestry()

	data := []byte("test")
	fd, err := client.Open("/test", true, true)
	if err != nil {
		t.Fatal(err)
	}

	err = client.Write(fd, 0, data)
	if err != nil {
		t.Fatal(err)
	}

	err = client.Close(fd)
	if err != nil {
		t.Fatal(err)
	}

	tapNodes[0].GracefulExit()

	time.Sleep(tapestry.REPUBLISH)

	fd, err = client.Open("/test", true, true)
	if err != nil {
		t.Fatal(err)
	}

	err = client.Write(fd, 0, data)
	if err != nil {
		t.Fatal(err)
	}

	err = client.Close(fd)
	if err == nil {
		t.Fatalf("expect flush to fail due to lack of tapestry node")
	}
}

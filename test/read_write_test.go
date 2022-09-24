package test

import (
	"bytes"
	"math/rand"
	puddlestore "puddlestore/pkg"
	"strconv"
	"strings"
	"testing"
)

// Set write flag of file to false, writing to that file should fail.
func TestFileWriteFlagFalse(t *testing.T) {
	config, cluster, client := basicSetup(t, 2, 2, 64)
	zkConn, _ := puddlestore.ConnectZk(config.ZkAddr)
	defer cluster.Shutdown()
	defer cleanZooKeeper(zkConn)
	defer client.Exit()

	fd, err := client.Open("/a", true, false)
	if err != nil {
		t.Fatal(err)
	}

	if err = client.Write(fd, 0, []byte{1}); err == nil {
		t.Fatal("Should err when writing at file with write flag false")
	}
}

// Read from empty file should always return empty data.
func TestReadEmptyFile(t *testing.T) {
	config, cluster, client := basicSetup(t, 2, 2, 64)
	zkConn, _ := puddlestore.ConnectZk(config.ZkAddr)
	defer cluster.Shutdown()
	defer cleanZooKeeper(zkConn)
	defer client.Exit()

	path := "/a.txt"
	var outData []byte
	var err error

	if outData, err = testReadFile(client, path, 0, 10); err != nil {
		t.Fatal(err)
	}
	if len(outData) != 0 {
		t.Fatalf("expected empty data, get: %v", outData)
	}

	if outData, err = testReadFile(client, path, 10, 100); err != nil {
		t.Fatal(err)
	}
	if len(outData) != 0 {
		t.Fatalf("expected empty data, get: %v", outData)
	}

	if outData, err = testReadFile(client, path, 100, 100); err != nil {
		t.Fatal(err)
	}
	if len(outData) != 0 {
		t.Fatalf("expected empty data, get: %v", outData)
	}
}

func TestReadWrite(t *testing.T) {
	config, cluster, client := basicSetup(t, 2, 2, 10)
	zkConn, _ := puddlestore.ConnectZk(config.ZkAddr)
	defer cluster.Shutdown()
	defer cleanZooKeeper(zkConn)
	defer client.Exit()

	path := "/a"
	offset := uint64(185)

	r := rand.New(rand.NewSource(10))
	blocks := 20
	data := make([]byte, config.BlockSize*uint64(blocks))
	r.Read(data)

	if err := testWriteFile(client, path, offset, data); err != nil {
		t.Fatal(err)
	}

	var outData []byte
	var err error
	if outData, err = testReadFile(client, path, offset, uint64(len(data))); err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(data, outData) {
		t.Fatalf("Expected: %v, Got: %v", data, outData)
	}
}

// Read variable size of data from file and check.
func TestReadWriteVariableReadSize(t *testing.T) {
	config, cluster, client := basicSetup(t, 2, 2, 10)
	zkConn, _ := puddlestore.ConnectZk(config.ZkAddr)
	defer cluster.Shutdown()
	defer cleanZooKeeper(zkConn)
	defer client.Exit()

	path := "/a"

	r := rand.New(rand.NewSource(10))
	blocks := 50
	data := make([]byte, config.BlockSize*uint64(blocks))
	r.Read(data)

	if err := testWriteFile(client, path, 0, data); err != nil {
		t.Fatal(err)
	}

	var outData []byte
	var err error

	if outData, err = testReadFile(client, path, 0, 10); err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(data[0:10], outData) {
		t.Fatalf("Expected: %v, Got: %v", data[0:10], outData)
	}

	if outData, err = testReadFile(client, path, 10, 50); err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(data[10:60], outData) {
		t.Fatalf("Expected: %v, Got: %v", data[10:60], outData)
	}

	if outData, err = testReadFile(client, path, 60, 10); err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(data[60:70], outData) {
		t.Fatalf("Expected: %v, Got: %v", data[60:70], outData)
	}
}

// Overwrite part of the file and check.
func TestReadAfterOverwrite(t *testing.T) {
	config, cluster, client := basicSetup(t, 2, 2, 10)
	zkConn, _ := puddlestore.ConnectZk(config.ZkAddr)
	defer cluster.Shutdown()
	defer cleanZooKeeper(zkConn)
	defer client.Exit()

	path := "/a"
	var err error

	if err = testWriteFile(client, path, 0, []byte(strings.Repeat("a", 128))); err != nil {
		t.Fatal(err)
	}

	if err = testWriteFile(client, path, 32, []byte(strings.Repeat("b", 64))); err != nil {
		t.Fatal(err)
	}

	var outData []byte
	if outData, err = testReadFile(client, path, 0, 128); err != nil {
		t.Fatal(err)
	}
	out := string(outData)

	expectedString := strings.Repeat("a", 32) + strings.Repeat("b", 64) + strings.Repeat("a", 32)
	if expectedString != out {
		t.Fatalf("Expected: %v, Got: %v", expectedString, out)
	}
}

// Writing beyond the file boundary should automatically fill the file with zero bytes.
func TestWriteAtOffsetInEmptyFile(t *testing.T) {
	config, cluster, client := basicSetup(t, 2, 2, 10)
	zkConn, _ := puddlestore.ConnectZk(config.ZkAddr)
	defer cluster.Shutdown()
	defer cleanZooKeeper(zkConn)
	defer client.Exit()

	err := testWriteFile(client, "/b", 63, []byte{1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}

	err = testWriteFile(client, "/b", 65, []byte{1, 2, 4})
	if err != nil {
		t.Fatal(err)
	}

	data, err := testReadFile(client, "/b", 0, 90)
	if err != nil {
		t.Fatal(err)
	}

	expected := append(make([]byte, 63), []byte{1, 2, 1, 2, 4}...)
	if bytes.Compare(data, expected) != 0 {
		t.Fatalf("Expected %v, but get %v\n", expected, data)
	}
}

// Writing beyond the file boundary should automatically fill the file with zero bytes.
func TestWriteAtOffsetInEmptyFile2(t *testing.T) {
	config, cluster, client := basicSetup(t, 2, 2, 10)
	zkConn, _ := puddlestore.ConnectZk(config.ZkAddr)
	defer cluster.Shutdown()
	defer cleanZooKeeper(zkConn)
	defer client.Exit()

	err := testWriteFile(client, "/b", 63, []byte{1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}

	err = testWriteFile(client, "/b", 80, []byte{1, 2, 4})
	if err != nil {
		t.Fatal(err)
	}

	data, err := testReadFile(client, "/b", 0, 83)
	if err != nil {
		t.Fatal(err)
	}

	expected := make([]byte, 83)
	copy(expected[63:66], []byte{1, 2, 3})
	copy(expected[80:83], []byte{1, 2, 4})
	if bytes.Compare(data, expected) != 0 {
		t.Fatalf("Expected %v, but get %v\n", expected, data)
	}
}

func TestWriteAtOffsetInEmptyFile3(t *testing.T) {
	config, cluster, client := basicSetup(t, 2, 2, 10)
	zkConn, _ := puddlestore.ConnectZk(config.ZkAddr)
	defer cluster.Shutdown()
	defer cleanZooKeeper(zkConn)
	defer client.Exit()

	err := testWriteFile(client, "/b", 23, []byte{1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}

	err = testWriteFile(client, "/b", 65, []byte{1, 2, 4})
	if err != nil {
		t.Fatal(err)
	}

	data, err := testReadFile(client, "/b", 0, 100)
	if err != nil {
		t.Fatal(err)
	}

	expected := make([]byte, 68)
	copy(expected[23:26], []byte{1, 2, 3})
	copy(expected[65:68], []byte{1, 2, 4})
	if bytes.Compare(data, expected) != 0 {
		t.Fatalf("Expected %v, but get %v\n", expected, data)
	}
}

// Two clients one writing and one reading.
func TestReadWriteTwoClients(t *testing.T) {
	config, cluster, client := basicSetup(t, 2, 2, 10)
	zkConn, _ := puddlestore.ConnectZk(config.ZkAddr)
	defer cluster.Shutdown()
	defer cleanZooKeeper(zkConn)
	defer client.Exit()

	client1, err := cluster.NewClient()
	if err != nil {
		t.Fatal(err)
	}
	client2, err := cluster.NewClient()
	if err != nil {
		t.Fatal(err)
	}
	defer client1.Exit()
	defer client2.Exit()

	// c1 write, c2 read
	in := "test"
	if err := testWriteFile(client1, "/file", 0, []byte(in)); err != nil {
		t.Fatal(err)
	}
	var out []byte
	if out, err = testReadFile(client2, "/file", 0, 5); err != nil {
		t.Fatal(err)
	}
	if string(out) != in {
		t.Fatalf("Expected: %v, Got: %v", in, string(out))
	}

	// c2 append, c1 read
	in = "append"
	if err := testWriteFile(client2, "/file", 5, []byte(in)); err != nil {
		t.Fatal(err)
	}

	if out, err = testReadFile(client1, "/file", 5, 11); err != nil {
		t.Fatal(err)
	}
	if string(out) != in {
		t.Fatalf("Expected: %v, Got: %v", in, string(out))
	}

	// c1 overwrite, c2 read
	in = "overwrite"
	if err := testWriteFile(client1, "/file", 0, []byte(in)); err != nil {
		t.Fatal(err)
	}
	if out, err = testReadFile(client2, "/file", 0, 11); err != nil {
		t.Fatal(err)
	}
	if string(out) != "overwrite"+"nd" {
		t.Fatalf("Expected: %v, Got: %v", in, string(out))
	}
	err = client1.Remove("/file")
	if err != nil {
		t.Fatal(err)
	}
	_, err = client2.Open("/file", false, false)
	if err == nil {
		t.Fatal(err)
	}
	_, err = client1.Open("/file", false, false)
	if err == nil {
		t.Fatal(err)
	}
}

// Read and write multiple times.
func TestReadWriteExtensive(t *testing.T) {
	config, cluster, client := basicSetup(t, 2, 2, 10)
	zkConn, _ := puddlestore.ConnectZk(config.ZkAddr)
	defer cluster.Shutdown()
	defer cleanZooKeeper(zkConn)
	defer client.Exit()

	dir0 := "/test0"
	err := client.Mkdir(dir0)
	if err != nil {
		t.Fatal(err)
	}
	dir1 := "/test0/test1"
	err = client.Mkdir(dir1)
	if err != nil {
		t.Fatal(err)
	}
	in := "file1"
	err = testWriteFile(client, dir1+"/"+in, 0, []byte(in))
	if err != nil {
		t.Fatal(err)
	}
	_, err = testReadFile(client, dir1+"/file1", 0, 64)
	if err != nil {
		t.Fatal(err)
	}

	err = testWriteFile(client, dir0+"/file0", 0, []byte("file0"))
	children, err := client.List(dir1)
	if children[0] != "file1" {
		t.Fatalf("Got %v; Expected %v", children[0], "file1")
	}

	err = client.Remove(dir1)
	if err != nil {
		t.Fatal(err)
	}
	children, err = client.List("/test0")
	if err != nil {
		t.Fatal(err)
	}
	if len(children) != 1 && children[0] != "file0" {
		t.Fatalf("Got %v; Expected %v", children[0], "file0")
	}

	err = client.Remove("/test0")
	if err != nil {
		t.Fatal(err)
	}
	children, err = client.List("/")
	if err != nil {
		t.Fatal(err)
	}
	if len(children) != 0 {
		t.Errorf("Expected empty string slice, got %v", children)
	}
}

// Write to some files, delete the intermediate directory on the file's path, then read file should fail.
func TestReadWriteAfterRemoveIntermediateDirectory(t *testing.T) {
	config, cluster, client := basicSetup(t, 2, 2, 10)
	zkConn, _ := puddlestore.ConnectZk(config.ZkAddr)
	defer cluster.Shutdown()
	defer cleanZooKeeper(zkConn)
	defer client.Exit()

	dir0 := "/d0"
	dir1 := dir0 + "/d1"
	dir2 := dir1 + "/d2"

	f0 := dir0 + "/file0"
	f1 := dir1 + "/file1"
	f2 := dir2 + "/file2"

	_ = client.Mkdir(dir0)
	_ = client.Mkdir(dir1)
	_ = client.Mkdir(dir2)

	err := testWriteFile(client, f0, 0, []byte("test"))
	if err != nil {
		t.Fatal(err)
	}
	err = testWriteFile(client, f1, 0, []byte("test"))
	if err != nil {
		t.Fatal(err)
	}
	err = testWriteFile(client, f2, 0, []byte("test"))
	if err != nil {
		t.Fatal(err)
	}

	_ = client.Remove(dir1)
	_, err = testReadFile(client, f2, 0, 64)
	if err == nil {
		t.Fatal("read should fail after deletion")
	}

	_, err = testReadFile(client, f1, 0, 64)
	if err == nil {
		t.Fatal("read should fail after deletion")
	}

	data, err := testReadFile(client, f0, 0, 64)

	if err != nil {
		t.Fatal("read should success")
	}

	if string(data) != "test" {
		t.Fatal("Expect: test, Got: ", string(data))
	}
}

// Concurrent read and write to a counter in the file.
func TestMultiClientConcurrentIncrement(t *testing.T) {
	config, cluster, client := basicSetup(t, 2, 2, 10)
	zkConn, _ := puddlestore.ConnectZk(config.ZkAddr)
	defer cluster.Shutdown()
	defer cleanZooKeeper(zkConn)
	defer client.Exit()

	in := "0"
	if err := testWriteFile(client, "/b", 0, []byte(in)); err != nil {
		t.Fatal(err)
	}

	finishChan := make(chan bool)
	finish := func(f chan<- bool) {
		f <- true
	}

	for i := 0; i < 20; i++ {
		go func() {
			defer finish(finishChan)

			tempClient, _ := cluster.NewClient()
			fd, err := tempClient.Open("/b", false, true)
			if err != nil {
				t.Error(err)
				return
			}

			out, err := tempClient.Read(fd, 0, 1)
			if err != nil {
				t.Error(err)
				return
			}

			num, _ := strconv.Atoi(string(out))

			in := strconv.Itoa(num + 1)
			if err := tempClient.Write(fd, 0, []byte(in)); err != nil {
				t.Error(err)
				return
			}
			if err := tempClient.Close(fd); err != nil {
				t.Error(err)
				return
			}
		}()
	}

	for i := 0; i < 20; i++ {
		<-finishChan
	}

	out, err := testReadFile(client, "/b", 0, 2)
	if err != nil {
		t.Fatal(err)
	}

	num, err := strconv.Atoi(string(out))
	if err != nil {
		t.Fatal(err)
	}
	if num != 20 {
		t.Fatalf("Expected num 20, got %v", num)
	}
}

// Multiple client performing read, overwrite and append operations.
func TestMultipleClientReadOverwriteAppend(t *testing.T) {
	config, cluster, mainClient := basicSetup(t, 2, 2, 10)
	zkConn, _ := puddlestore.ConnectZk(config.ZkAddr)
	defer cluster.Shutdown()
	defer cleanZooKeeper(zkConn)
	defer mainClient.Exit()

	in := "0"
	var err error
	if err = testWriteFile(mainClient, "/read", 0, []byte(in)); err != nil {
		t.Fatal(err)
	}
	if err = testWriteFile(mainClient, "/overwrite", 0, []byte(in)); err != nil {
		t.Fatal(err)
	}
	if err = testWriteFile(mainClient, "/append", 0, []byte(in)); err != nil {
		t.Fatal(err)
	}

	finishChan := make(chan bool)
	finish := func(f chan<- bool) {
		f <- true
	}

	for i := 0; i < 5; i++ {
		go func() {
			defer finish(finishChan)

			c, _ := cluster.NewClient()
			defer c.Exit()

			// read
			fdRead, _ := c.Open("/read", false, false)
			readOut, _ := c.Read(fdRead, 0, 1)
			if string(readOut) != in {
				t.Errorf("client read failed, expected %v, got %v", in, string(readOut))
				return
			}
			err := c.Close(fdRead)
			if err != nil {
				t.Error(err)
				return
			}

			// overwrite
			fdWrite, _ := c.Open("/overwrite", false, true)
			overWrite, _ := c.Read(fdWrite, 0, 1)
			num, _ := strconv.Atoi(string(overWrite))
			count := strconv.Itoa(num + 1)
			err = c.Write(fdWrite, 0, []byte(count))
			if err != nil {
				t.Error(err)
				return
			}
			err = c.Close(fdWrite)
			if err != nil {
				t.Error(err)
				return
			}

			//append
			fdAppend, _ := c.Open("/append", false, true)
			toAppend, _ := c.Read(fdAppend, 0, 64)
			err = c.Write(fdAppend, uint64(len(toAppend)), []byte("1"))
			err = c.Close(fdAppend)
			if err != nil {
				return
			}
		}()
	}

	for i := 0; i < 5; i++ {
		<-finishChan
	}

	readOut, err := testReadFile(mainClient, "/read", 0, 2)
	if err != nil {
		t.Fatal(err)
	}

	if string(readOut) != "0" {
		t.Fatalf("Read failed; Expected %v, got %v", "0", string(readOut))
	}
	writeOut, err := testReadFile(mainClient, "/overwrite", 0, 2)
	if err != nil {
		t.Fatal(err)
	}
	if string(writeOut) != "5" {
		t.Fatalf("Overwrite failed; Expected %v, got %v", "0", string(writeOut))
	}
	appendOut, err := testReadFile(mainClient, "/append", 0, 64)
	if err != nil {
		t.Fatal(err)
	}
	if string(appendOut) != "011111" {
		t.Fatalf("Append failed; Expected %v, got %v", "011111", string(appendOut))
	}
}

package test

import (
	puddlestore "puddlestore/pkg"
	"sync"
	"testing"
	"time"
)

// Create a default cluster.
func TestCreateCluster(t *testing.T) {
	cluster, err := puddlestore.CreateCluster(puddlestore.DefaultConfig())
	if err != nil {
		t.Errorf(err.Error())
	}
	defer cluster.Shutdown()
}

// Create a file whose parent directory does not exist, should fail.
func TestCreateFileNoParent(t *testing.T) {
	config, cluster, client := basicSetup(t, 2, 2, 64)
	zkConn, _ := puddlestore.ConnectZk(config.ZkAddr)
	defer cluster.Shutdown()
	defer cleanZooKeeper(zkConn)
	defer client.Exit()

	if _, err := client.Open("/a/b", true, false); err == nil {
		t.Fatalf("create should fail when parent directory does not exist")
	}
}

// Create a file "/a", should not be able to create a file "/a/b".
func TestCreateFileUnderFile(t *testing.T) {
	config, cluster, client := basicSetup(t, 2, 2, 64)
	zkConn, _ := puddlestore.ConnectZk(config.ZkAddr)
	defer cluster.Shutdown()
	defer cleanZooKeeper(zkConn)
	defer client.Exit()

	if _, err := client.Open("/a", true, false); err != nil {
		t.Fatal(err)
	}

	if _, err := client.Open("/a/b", true, false); err == nil {
		t.Fatalf("Expected error when creating file under file")
	}
}

// Open a non-existent file without creating it, should fail.
func TestCreateFlagFalse(t *testing.T) {
	config, cluster, client := basicSetup(t, 2, 2, 64)
	zkConn, _ := puddlestore.ConnectZk(config.ZkAddr)
	defer cluster.Shutdown()
	defer cleanZooKeeper(zkConn)
	defer client.Exit()

	if _, err := client.Open("/a", false, false); err == nil {
		t.Fatal("Open should fail if create flag is false and file does not exist")
	}
}

// Read, write and close an unopened file, should fail.
func TestOperateUnopened(t *testing.T) {
	config, cluster, client := basicSetup(t, 2, 2, 64)
	zkConn, _ := puddlestore.ConnectZk(config.ZkAddr)
	defer cluster.Shutdown()
	defer cleanZooKeeper(zkConn)
	defer client.Exit()

	if err := client.Close(100); err == nil {
		t.Fatalf("Close should fail if the file is not opened")
	}

	if _, err := client.Read(100, 0, 100); err == nil {
		t.Fatalf("Read should fail if the file is not opened")
	}

	if err := client.Write(100, 0, []byte{100}); err == nil {
		t.Fatalf("Write should fail if the file is not opened")
	}
}

// Create a basic directory "/a"
func TestMkdirBasic(t *testing.T) {
	config, cluster, client := basicSetup(t, 2, 2, 64)
	zkConn, _ := puddlestore.ConnectZk(config.ZkAddr)
	defer cluster.Shutdown()
	defer cleanZooKeeper(zkConn)
	defer client.Exit()

	if err := client.Mkdir("/a"); err != nil {
		t.Fatal(err)
	}

	if _, err := client.List("/a"); err != nil {
		t.Fatal(err)
	}
}

// Create a directory whose parent directory does not exist, should fail.
func TestMkdirParentNotExist(t *testing.T) {
	config, cluster, client := basicSetup(t, 2, 2, 64)
	zkConn, _ := puddlestore.ConnectZk(config.ZkAddr)
	defer cluster.Shutdown()
	defer cleanZooKeeper(zkConn)
	defer client.Exit()

	if err := client.Mkdir("/a/b"); err == nil {
		t.Fatalf("Mkdir should fail if parent directory does not exist")
	}
}

// Create a directory whose with a file in its path, should fail.
func TestMkdirParentNotDir(t *testing.T) {
	config, cluster, client := basicSetup(t, 2, 2, 64)
	zkConn, _ := puddlestore.ConnectZk(config.ZkAddr)
	defer cluster.Shutdown()
	defer cleanZooKeeper(zkConn)
	defer client.Exit()

	fd, err := client.Open("/a", true, true)
	if err != nil {
		t.Fatal(err)
	}
	err = client.Close(fd)
	if err != nil {
		t.Fatal(err)
	}

	err = client.Mkdir("/a/b")
	if err == nil {
		t.Fatalf("Mkdir should fail if parent is not a directory")
	}
}

// Create a directory that already exists, should err.
func TestMkdirAlreadyExist(t *testing.T) {
	config, cluster, client := basicSetup(t, 2, 2, 64)
	zkConn, _ := puddlestore.ConnectZk(config.ZkAddr)
	defer cluster.Shutdown()
	defer cleanZooKeeper(zkConn)
	defer client.Exit()

	if err := client.Mkdir("/a"); err != nil {
		t.Fatal(err)
	}

	if err := client.Mkdir("/a"); err == nil {
		t.Fatalf("Mkdir should fail if the directory already exists, actually success")
	}
}

// Create a root directory and sub-directory within it, then remove the root directory.
func TestRemoveDir(t *testing.T) {
	config, cluster, client := basicSetup(t, 2, 2, 64)
	zkConn, _ := puddlestore.ConnectZk(config.ZkAddr)
	defer cluster.Shutdown()
	defer cleanZooKeeper(zkConn)
	defer client.Exit()

	var err error
	if err = client.Mkdir("/a"); err != nil {
		t.Fatal(err)
	}

	if err = client.Mkdir("/a/b"); err != nil {
		t.Fatal(err)
	}

	if err = client.Remove("/a"); err != nil {
		t.Fatal(err)
	}

	if _, err = client.List("/a"); err == nil {
		t.Fatal("expect path /a to be invalid")
	}

	children, err := client.List("/")
	if err != nil {
		t.Fatal(err)
	}

	if len(children) != 0 {
		t.Fatal("expected empty children list, found: ", children)
	}
}

// Remove a non-existent file, should fail.
func TestRemoveNonExist(t *testing.T) {
	config, cluster, client := basicSetup(t, 2, 2, 64)
	zkConn, _ := puddlestore.ConnectZk(config.ZkAddr)
	defer cluster.Shutdown()
	defer cleanZooKeeper(zkConn)
	defer client.Exit()

	if err := client.Remove("/a"); err == nil {
		t.Fatalf("Remove should fail if item does not exist")
	}
}

// Open with write enabled should block all concurrent open.
func TestOpenWriteShouldBlockAll(t *testing.T) {
	config, cluster, mainClient := basicSetup(t, 2, 2, 64)
	zkConn, _ := puddlestore.ConnectZk(config.ZkAddr)
	defer cluster.Shutdown()
	defer cleanZooKeeper(zkConn)
	defer mainClient.Exit()

	path := "/test"
	fd, err := mainClient.Open(path, true, true)
	if err != nil {
		t.Fatal(err)
	}

	clientA, err := cluster.NewClient()
	if err != nil {
		t.Fatal(err)
	}
	defer clientA.Exit()

	clientB, err := cluster.NewClient()
	if err != nil {
		t.Fatal(err)
	}
	defer clientB.Exit()

	flagLock := &sync.Mutex{}
	aOpenFinished := false
	bOpenFinished := false

	go func() {
		var errA error
		fdA, errA := clientA.Open(path, false, true)
		if errA != nil {
			t.Error(errA)
		}
		flagLock.Lock()
		aOpenFinished = true
		flagLock.Unlock()

		_ = clientA.Close(fdA)
		return
	}()

	go func() {
		var errB error
		fdB, errB := clientB.Open(path, false, false)
		if errB != nil {
			t.Error(errB)
		}
		flagLock.Lock()
		bOpenFinished = true
		flagLock.Unlock()

		_ = clientB.Close(fdB)
		return
	}()

	time.Sleep(100 * time.Millisecond)
	flagLock.Lock()
	examineA := aOpenFinished
	examineB := bOpenFinished
	flagLock.Unlock()

	if examineA || examineB {
		t.Fatalf("expect sub-client to be blocked, get: clientA %v, clientB %v", examineA, examineB)
	}

	err = mainClient.Close(fd)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(500 * time.Millisecond)
	flagLock.Lock()
	examineA = aOpenFinished
	examineB = bOpenFinished
	flagLock.Unlock()

	if !examineA || !examineB {
		t.Fatalf("expect sub-clients open complete, get: clientA %v, clientB %v", examineA, examineB)
	}
}

// Read-only open should only block open with write enabled.
func TestOpenReadShouldOnlyBlockWrite(t *testing.T) {
	config, cluster, mainClient := basicSetup(t, 2, 2, 64)
	zkConn, _ := puddlestore.ConnectZk(config.ZkAddr)
	defer cluster.Shutdown()
	defer cleanZooKeeper(zkConn)
	defer mainClient.Exit()

	path := "/test"
	fd, err := mainClient.Open(path, true, false)
	if err != nil {
		t.Fatal(err)
	}

	clientA, err := cluster.NewClient()
	if err != nil {
		t.Fatal(err)
	}
	defer clientA.Exit()

	clientB, err := cluster.NewClient()
	if err != nil {
		t.Fatal(err)
	}
	defer clientB.Exit()

	flagLock := &sync.Mutex{}
	aOpenFinished := false
	bOpenFinished := false

	go func() {
		var errA error
		fdA, errA := clientA.Open(path, false, false)
		if errA != nil {
			t.Error(errA)
		}
		flagLock.Lock()
		aOpenFinished = true
		flagLock.Unlock()

		_ = clientA.Close(fdA)
		return
	}()

	time.Sleep(10 * time.Millisecond)

	go func() {
		var errB error
		fdB, errB := clientB.Open(path, false, true)
		if errB != nil {
			t.Error(errB)
		}
		flagLock.Lock()
		bOpenFinished = true
		flagLock.Unlock()

		_ = clientB.Close(fdB)
		return
	}()

	time.Sleep(500 * time.Millisecond)
	flagLock.Lock()
	examineA := aOpenFinished
	examineB := bOpenFinished
	flagLock.Unlock()

	if !examineA {
		t.Fatalf("expect read sub-client to not be block, actually got blocked")

	}

	if examineB {
		t.Fatalf("expect write sub-client to be blocked, actually not")
	}

	err = mainClient.Close(fd)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(500 * time.Millisecond)

	flagLock.Lock()
	examineA = aOpenFinished
	examineB = bOpenFinished
	flagLock.Unlock()

	if !examineA || !examineB {
		t.Fatalf("expect sub-clients open complete, get: clientA %v, clientB %v", examineA, examineB)
	}
}

// Open should block remove
func TestOpenRemoveBlock(t *testing.T) {
	config, cluster, clientA := basicSetup(t, 2, 2, 64)
	zkConn, _ := puddlestore.ConnectZk(config.ZkAddr)
	defer cluster.Shutdown()
	defer cleanZooKeeper(zkConn)
	defer clientA.Exit()

	fd, err := clientA.Open("/test", true, false)
	if err != nil {
		t.Fatal(err)
	}

	mtx := &sync.Mutex{}
	removeFinished := false
	go func() {
		clientB, err := cluster.NewClient()
		if err != nil {
			t.Error(err)
			return
		}
		defer clientB.Exit()
		err = clientB.Remove("/test")
		if err != nil {
			t.Error(err)
			return
		}
		mtx.Lock()
		removeFinished = true
		mtx.Unlock()
	}()

	time.Sleep(100 * time.Millisecond)
	mtx.Lock()
	checkFlag := removeFinished
	mtx.Unlock()

	if checkFlag {
		t.Fatal("remove should be blocked")
	}

	err = clientA.Close(fd)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(200 * time.Millisecond)
	mtx.Lock()
	checkFlag = removeFinished
	mtx.Unlock()

	if !removeFinished {
		t.Fatal("remove should finish after close")
	}
}

// List empty directory, should not contain any file.
func TestListEmpty(t *testing.T) {
	config, cluster, client := basicSetup(t, 2, 2, 64)
	zkConn, _ := puddlestore.ConnectZk(config.ZkAddr)
	defer cluster.Shutdown()
	defer cleanZooKeeper(zkConn)
	defer client.Exit()

	children, err := client.List("/")
	if err != nil {
		t.Fatal(err)
	}

	if len(children) > 0 {
		t.Errorf("Expected no files, but get %v", children)
	}
}

// List file should only return filename.
func TestListFile(t *testing.T) {
	config, cluster, client := basicSetup(t, 2, 2, 64)
	zkConn, _ := puddlestore.ConnectZk(config.ZkAddr)
	defer cluster.Shutdown()
	defer cleanZooKeeper(zkConn)
	defer client.Exit()

	if _, err := client.Open("/a", true, false); err != nil {
		t.Fatal(err)
	}

	children, err := client.List("/a")
	if err != nil {
		t.Fatal(err)
	}

	if len(children) != 1 {
		t.Fatalf("list on file should only return filename")
	}

	if children[0] != "a" {
		t.Fatalf("list returns a different filename")
	}
}

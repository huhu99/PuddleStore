package pkg

import (
	"errors"
	"fmt"
	"github.com/go-zookeeper/zk"
	"math/rand"
	"path"
	"strings"
	"sync"
	tapestry "tapestry/pkg"
	"time"
)

// Client is a puddlestore client interface that will communicate with puddlestore nodes
type Client interface {
	// Open a file and returns a file descriptor. If the `create` is true and the
	// file does not exist, create the file. If `create` is false and the file does not exist,
	// return an error. If `write` is true, then flush the resulting inode on Close(). If `write`
	// is false, no need to flush the inode to zookeeper. If `Open` is successful, the returned
	// file descriptor should be unique to the file. The client is responsible for keeping
	// track of local file descriptors. Using `Open` allows for file-locking and
	// multi-operation transactions.
	Open(path string, create, write bool) (int, error)

	// Close the file and flushes its contents to the distributed filesystem.
	// The updated closed file should be able to be opened again after successfully closing it.
	// We only flush changes to the file on close to ensure copy-on-write atomicity of operations.
	// Refer to the handout for more information on why this is necessary.
	Close(fd int) error

	// Read returns a `size` amount of bytes starting at `offset` in an opened file.
	// Reading at non-existent offset returns empty buffer and no error.
	// If offset+size exceeds file boundary, return as much as possible with no error.
	// Returns err if fd is not opened.
	Read(fd int, offset, size uint64) ([]byte, error)

	// Write `data` starting at `offset` on an opened file. Writing beyond the
	// file boundary automatically fills the file with zero bytes. Returns err if fd is not opened.
	// If the file was opened with write = true flag, `Write` should return an error.
	Write(fd int, offset uint64, data []byte) error

	// Mkdir creates directory at the specified path.
	// Returns error if any parent directory does not exist (non-recursive).
	Mkdir(path string) error

	// Remove a directory or file. Returns err if not exists.
	Remove(path string) error

	// List file & directory names (not full names) under `path`. Returns err if not exists.
	List(path string) ([]string, error)

	// Exit Release zk connection. Subsequent calls on Exit()-ed clients should return error.
	Exit()
}

const RetryAttempts = 3

type PuddleStoreClient struct {
	// Basic members for a puddlestore client
	blockSize   uint64
	numReplicas int
	zkConn      *zk.Conn

	// The counter for read pre-fetch statistics
	readBytesTotal     uint64
	readOperationTotal uint64
	readStatisticsMtx  *sync.Mutex

	// The file descriptor cache
	fdCache      map[fileDescriptorID]*FileDescriptor
	fdCacheMutex *sync.Mutex
	fdGenerator  func() fileDescriptorID
}

func (c *PuddleStoreClient) Open(path string, create, write bool) (fd int, err error) {
	Debug.Printf("Open %s\n", path)

	path = prunePath(path)
	exist, isDir, err := c.fileExistsAtPath(path)
	if err != nil {
		return -1, err
	}

	if exist {
		if isDir {
			// the file exists and is a directory, return error
			return -1, errors.New("can not open a directory")
		} else {
			// the file exists and is not a directory, open the file
			fd, err = c.openFileAtPath(path, write)
			return
		}
	} else if create {
		// the file does not exist at path
		// create the file, if 'create' is specified in parameters
		fd, err = c.createFileAtPath(path, write)
		return
	}
	// file does not exist at path, and create is not specified, return error
	return -1, errors.New(fmt.Sprintf("file does not exist at path: %s", path))
}

func (c *PuddleStoreClient) Close(fd int) (err error) {
	Debug.Printf("Close fd: %d\n", fd)

	c.fdCacheMutex.Lock()
	fileDes, exist := c.fdCache[fd]
	c.fdCacheMutex.Unlock()

	if !exist {
		// file descriptor not exists, return error
		return errors.New(fmt.Sprintf("unable to find file descriptor: %d", fd))
	}

	if fileDes.writeEnabled && fileDes.isDirty {
		// if write is specified, flush the content in memory
		Out.Println("flush file: ", fileDes.metaData.Path)
		err = fileDes.flush()
		if err != nil {
			return err
		}
	}

	// unlock the distributed lock
	err = fileDes.lock.Unlock()
	if err != nil {
		return err
	}

	// delete the file descriptor
	c.fdCacheMutex.Lock()
	delete(c.fdCache, fd)
	c.fdCacheMutex.Unlock()

	Out.Printf("close success: {fd: %d, path: %s}\n", fd, fileDes.metaData.Path)

	return nil
}

func (c *PuddleStoreClient) Read(fd int, offset, size uint64) ([]byte, error) {
	Debug.Printf("Read: {fd: %d, offset: %d, size: %d}\n", fd, offset, size)

	c.fdCacheMutex.Lock()
	fileDes, exist := c.fdCache[fd]
	c.fdCacheMutex.Unlock()

	if !exist {
		// file descriptor not exists, return error
		return nil, errors.New(fmt.Sprintf("unable to find file descriptor: %d", fd))
	}

	// adjust read size by considering history read statistics
	var averageReadBytes uint64
	c.readStatisticsMtx.Lock()
	if c.readOperationTotal == 0 {
		averageReadBytes = 0
	} else {
		averageReadBytes = c.readBytesTotal / c.readOperationTotal
	}
	c.readStatisticsMtx.Unlock()

	readSize := size
	if averageReadBytes > size {
		readSize = averageReadBytes
	}
	// read data from the file
	data, err := fileDes.read(offset, readSize)

	if err != nil {
		return nil, err
	}

	// accumulate the read statistics
	c.readStatisticsMtx.Lock()
	c.readBytesTotal += size
	c.readOperationTotal += 1
	c.readStatisticsMtx.Unlock()

	if uint64(len(data)) < size {
		// the file is actually even smaller than the original requested read size
		return data, nil
	}

	// The file is at least as large as the original requested read size
	// only return the requested size of data,
	// since pre-fetching can cause more data to be returned
	return data[:size], nil
}

func (c *PuddleStoreClient) Write(fd int, offset uint64, data []byte) error {
	Debug.Printf("Write: {fd: %d, offset: %d, size: %d}\n", fd, offset, len(data))

	c.fdCacheMutex.Lock()
	fileDes, exist := c.fdCache[fd]
	c.fdCacheMutex.Unlock()

	if !exist {
		// file descriptor not exists, return error
		return errors.New(fmt.Sprintf("unable to find file descriptor: %d", fd))
	}
	// write data into the file
	return fileDes.write(offset, data)
}

func (c *PuddleStoreClient) Mkdir(path string) error {
	Debug.Printf("Mkdir: %s\n", path)

	path = prunePath(path)
	exist, isDir, err := c.fileExistsAtPath(path)
	if err != nil {
		return err
	}

	if exist && isDir {
		// if the directory already exists, return error
		return errors.New("directory already exists")
	}
	// create the directory
	return c.createDirectoryAtPath(path)
}

func (c *PuddleStoreClient) Remove(path string) error {
	Debug.Printf("Remove: %s\n", path)

	path = prunePath(path)
	return c.removeItemAtPath(path)
}

func (c *PuddleStoreClient) List(itemPath string) ([]string, error) {
	Debug.Printf("List: %s\n", itemPath)
	// check if any item exists at path

	itemPath = prunePath(itemPath)
	exist, isDir, err := c.fileExistsAtPath(itemPath)
	if err != nil {
		return nil, err
	}

	if !exist {
		return nil, errors.New(fmt.Sprintf("no item exists at path: %s", itemPath))
	}

	if !isDir {
		filename := path.Base(itemPath)
		if strings.HasSuffix(itemPath, "/") {
			// "ls /dir/file.txt/" is not considered as a valid input
			return nil, errors.New(fmt.Sprintf("%s/ is not a directory", filename))
		}

		// the item is a file, return the file name
		return []string{filename}, nil
	}

	children, _, err := c.zkConn.Children(itemPath)
	if err != nil {
		return nil, err
	}

	return children, nil
}

func (c *PuddleStoreClient) Exit() {
	// close all file handles
	c.fdCacheMutex.Lock()
	fdCacheCpy := c.fdCache
	c.fdCacheMutex.Unlock()

	for id := range fdCacheCpy {
		for i := 0; i < RetryAttempts; i++ {
			err := c.Close(id)
			if err != nil {
				continue
			}
			break
		}
	}

	c.zkConn.Close()
	return
}

// fileExistsAtPath checks if file exists at the given path
// It also checks whether the item is a directory
func (c *PuddleStoreClient) fileExistsAtPath(path string) (exist bool, isDir bool, err error) {
	if path == PuddlestoreRoot {
		// we always assume the root directory exists
		return true, true, nil
	}

	// check if the path exists
	exist, _, err = c.zkConn.Exists(path)
	if err != nil {
		return false, false, err
	}

	if !exist {
		// file does not exist
		return false, false, nil
	}

	// get the file metadata to check if its directory
	data, _, err := c.zkConn.Get(path)
	if err != nil {
		return false, false, err
	}

	metaData, err := decodeInode(data)
	if err != nil {
		return false, false, err
	}

	return true, metaData.IsDir, nil
}

// createDirectoryAtPath creates a directory at the given path
// If the parent directory of the path does not exist, the method will return an error
func (c *PuddleStoreClient) createDirectoryAtPath(filePath string) error {
	// create directory inode structure
	filename := path.Base(filePath)
	dirPath := path.Dir(filePath)

	exist, isDir, err := c.fileExistsAtPath(dirPath)
	if err != nil {
		return err
	}

	if !exist {
		// parent directory does not exist
		return errors.New(fmt.Sprintf("parent directory does not exist: %s", dirPath))
	}

	if !isDir {
		// cannot create directory under a file
		return errors.New(fmt.Sprintf("%s is not a directory", dirPath))
	}

	metaData := inode{
		IsDir:     true,
		Filename:  filename,
		Path:      filePath,
		Size:      uint64(0),
		BlockSize: uint64(0),
		Blocks:    nil,
	}

	data, err := encodeInode(metaData)
	if err != nil {
		return err
	}

	// store the inode in the ZooKeeper
	zkFilePath, err := c.zkConn.Create(filePath, data, 0, zk.WorldACL(zk.PermAll))

	if err != nil {
		return err
	}

	if zkFilePath != filePath {
		return errors.New("internal error: created directory fail")
	}

	Out.Printf("create directory success: {path: %s}\n", filePath)

	return nil
}

// createFileAtPath creates a file at the given path
// If the parent directory of the path does not exist, the method will return an error
// The method will lock the distributed lock associated with the file descriptor
func (c *PuddleStoreClient) createFileAtPath(filePath string, write bool) (fileDescriptorID, error) {
	// create file inode structure
	filename := path.Base(filePath)
	dirPath := path.Dir(filePath)

	exist, isDir, err := c.fileExistsAtPath(dirPath)
	if err != nil {
		return -1, err
	}

	if !exist {
		// parent directory does not exist
		return -1, errors.New(fmt.Sprintf("parent directory does not exist: %s", dirPath))
	}

	if !isDir {
		// cannot create file under a file
		return -1, errors.New(fmt.Sprintf("%s is not a directory", dirPath))
	}

	metaData := inode{
		IsDir:     false,
		Filename:  filename,
		Path:      filePath,
		Size:      uint64(0),
		BlockSize: c.blockSize,
		Blocks:    make([]string, 0),
	}

	fd := &FileDescriptor{
		id:           c.fdGenerator(),
		writeEnabled: write,
		metaData:     &metaData,
		lock:         CreateRWDistLock(c.zkConn, filePath, zk.WorldACL(zk.PermAll)),
		client:       c,
		dataCache:    make(map[blockIdx][]byte),
	}

	// create path in the ZooKeeper
	data, err := encodeInode(metaData)
	if err != nil {
		return -1, err
	}

	zkFilePath, err := c.zkConn.Create(filePath, data, 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		return -1, err
	}

	if zkFilePath != filePath {
		return -1, errors.New("internal error: created directory fail")
	}

	// acquire the distributed lock for the file path
	if write {
		err = fd.lock.LockWrite()
	} else {
		err = fd.lock.LockRead()
	}
	if err != nil {
		return -1, err
	}

	// store the FileDescriptor in the client's fdCache
	c.fdCacheMutex.Lock()
	c.fdCache[fd.id] = fd
	c.fdCacheMutex.Unlock()

	Out.Printf("create file success: {fd: %d, path: %s}\n", fd.id, filePath)
	return fd.id, nil
}

// removeItemAtPath removes the item at the given path
func (c *PuddleStoreClient) removeItemAtPath(path string) error {
	exist, isDir, err := c.fileExistsAtPath(path)
	if err != nil {
		return err
	}

	if !exist {
		return errors.New(fmt.Sprintf("no item exists at path: %s", path))
	}

	children, _, err := c.zkConn.Children(path)
	if err != nil {
		return err
	}

	if !isDir {
		removeLock := CreateRWDistLock(c.zkConn, path, zk.WorldACL(zk.PermAll))
		err = removeLock.LockWrite()
		if err != nil {
			return err
		}
		defer func(removeLock *DistRWLock) {
			err := removeLock.Unlock()
			if err != nil {
				Out.Println("remove file unlock error: ", err)
			}
		}(removeLock)

	}
	for _, child := range children {
		err = c.removeItemAtPath(path + "/" + child)
		if err != nil {
			Out.Printf("delete error: %v\n", err)
			return err
		}
	}

	err = c.zkConn.Delete(path, -1)

	if err != nil {
		return err
	}

	Out.Printf("delete success: {path: %s}\n", path)
	return nil
}

// openFileAtPath opens the file at the given path.
// The method assumes that the item exists at the path and is not a directory.
// The method will lock the distributed lock associated with the file descriptor
// @param path: The path of the file.
// @param write: A boolean value indicating whether write is enabled for the file.
func (c *PuddleStoreClient) openFileAtPath(filePath string, write bool) (fileDescriptorID, error) {
	fd := &FileDescriptor{
		id:           c.fdGenerator(),
		writeEnabled: write,
		lock:         CreateRWDistLock(c.zkConn, filePath, zk.WorldACL(zk.PermAll)),
		client:       c,
		dataCache:    make(map[blockIdx][]byte),
	}

	// acquire the distributed lock for the file path
	var err error
	if write {
		err = fd.lock.LockWrite()
	} else {
		err = fd.lock.LockRead()
	}
	if err != nil {
		return -1, err
	}

	// read the inode metadata
	data, _, err := c.zkConn.Get(filePath)
	if err != nil {
		return -1, err
	}

	metaData, err := decodeInode(data)
	if err != nil {
		return -1, err
	}

	fd.metaData = metaData

	// store the FileDescriptor in the client's fdCache
	c.fdCacheMutex.Lock()
	c.fdCache[fd.id] = fd
	c.fdCacheMutex.Unlock()

	Out.Printf("open success: {fd: %d, path: %s}\n", fd.id, filePath)

	return fd.id, nil
}

func (c *PuddleStoreClient) getRandomTapestryClients(num int, exclude []*tapestry.Client) ([]*tapestry.Client, error) {
	tapNodes, _, err := c.zkConn.Children(TapRoot)
	if err != nil {
		return nil, err
	}

	if len(tapNodes) < num {
		return nil, errors.New(fmt.Sprintf("not enough nodes, %d requested but only have %d", num, len(tapNodes)))
	}

	rand.Seed(time.Now().UnixNano())
	shuffledIndices := rand.Perm(len(tapNodes))

	// convert exclude from slice to map for lookup
	excludeMap := make(map[string]bool, len(exclude))
	for _, node := range exclude {
		excludeMap[node.ID] = true
	}

	ret := make([]*tapestry.Client, 0)
	for _, idx := range shuffledIndices {
		tapNodePath := TapRoot + "/" + tapNodes[idx]
		tapMetaData, _, err := c.zkConn.Get(tapNodePath)
		if err != nil {
			continue
		}

		tapMeta, err := decodeTapestryMeta(tapMetaData)
		if err != nil {
			continue
		}

		if _, exist := excludeMap[tapMeta.ID]; exist {
			// if the node should be excluded, continue
			continue
		}
		newClient, err := tapestry.Connect(tapMeta.Address)
		if err != nil {
			continue
		}
		ret = append(ret, newClient)
	}

	if len(ret) < num {
		return nil, errors.New(fmt.Sprintf("not enough nodes, %d requested but only found %d", num, len(ret)))
	}
	return ret, nil
}

func (c *PuddleStoreClient) getAllTapestryClients() (clients []*tapestry.Client, err error) {
	tapNodes, _, err := c.zkConn.Children(TapRoot)
	if err != nil {
		return nil, err
	}

	clients = make([]*tapestry.Client, 0)
	for _, node := range tapNodes {
		tapNodePath := TapRoot + "/" + node
		tapMetaData, _, err := c.zkConn.Get(tapNodePath)
		if err != nil {
			continue
		}

		tapMeta, err := decodeTapestryMeta(tapMetaData)
		if err != nil {
			continue
		}

		newClient, err := tapestry.Connect(tapMeta.Address)
		if err != nil {
			continue
		}
		clients = append(clients, newClient)
	}
	return clients, nil
}

func prunePath(path string) string {
	path = PuddlestoreRoot + path
	if path[len(path)-1] == '/' {
		path = path[:len(path)-1]
	}
	return path
}

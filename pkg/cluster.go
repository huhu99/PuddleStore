package pkg

import (
	"github.com/go-zookeeper/zk"
	"sync"
	tapestry "tapestry/pkg"
	"time"
)

const TapRoot = "/tapestry"
const LockRoot = "/distLock"
const PuddlestoreRoot = "/puddlestore"

// Cluster is an interface for all nodes in a puddlestore cluster. One should be able to shut down
// this cluster and create a client for this cluster
type Cluster struct {
	config Config
	nodes  []*Tapestry
}

// Shutdown causes all tapestry nodes to gracefully exit
func (c *Cluster) Shutdown() {
	for _, node := range c.nodes {
		node.GracefulExit()
	}

	time.Sleep(time.Second)
}

// CreateCluster starts all nodes necessary for puddlestore
func CreateCluster(config Config) (*Cluster, error) {
	// IMPL:
	// Start your tapestry cluster with size config.NumTapestry. You should
	// also use the zkAddr (zookeeper address) found in the config and pass it to
	// your Tapestry constructor method

	zkConn, err := ConnectZk(config.ZkAddr)
	if err != nil {
		return nil, err
	}

	exist, _, err := zkConn.Exists(TapRoot)
	if err != nil {
		return nil, err
	}

	if !exist {
		_, err := zkConn.Create(TapRoot, make([]byte, 0), 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			return nil, err
		}
	}

	exist, _, err = zkConn.Exists(PuddlestoreRoot)
	if err != nil {
		return nil, err
	}

	if !exist {
		_, err = zkConn.Create(PuddlestoreRoot, make([]byte, 0), 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			return nil, err
		}
	}

	// create tapestry cluster
	tapNodes, err := tapestry.MakeRandomTapestries(10, config.NumTapestry)
	if err != nil {
		return nil, err
	}

	var cluster Cluster
	cluster.config = config

	// create Tapestry wrapper nodes
	for _, tap := range tapNodes {
		tapWrapper, err := NewTapestry(tap, config.ZkAddr)
		if err != nil {
			return nil, err
		}
		cluster.nodes = append(cluster.nodes, tapWrapper)
	}

	zkConn.Close()
	return &cluster, nil
}

// NewClient creates a new Puddlestore client
func (c *Cluster) NewClient() (Client, error) {
	// return a new PuddleStore Client that implements the Client interface
	zkConn, err := ConnectZk(c.config.ZkAddr)
	if err != nil {
		return nil, err
	}

	ret := &PuddleStoreClient{
		// basic members
		blockSize:   c.getBlockSize(),
		numReplicas: c.getNumReplicas(),
		zkConn:      zkConn,
		// read statistics lock for pre-fetching
		readStatisticsMtx: &sync.Mutex{},
		// file descriptor cache and its mutex
		fdCache:      make(map[fileDescriptorID]*FileDescriptor),
		fdCacheMutex: &sync.Mutex{},
		fdGenerator:  newFileDescriptorGenerator(),
	}

	return ret, nil
}

func (c *Cluster) TestGetTapestry() []*Tapestry {
	return c.nodes
}

func (c *Cluster) getNumReplicas() int {
	return c.config.NumReplicas
}

func (c *Cluster) getBlockSize() uint64 {
	return c.config.BlockSize
}

func newFileDescriptorGenerator() func() fileDescriptorID {
	i := 0
	lock := &sync.Mutex{}
	f := func() fileDescriptorID {
		lock.Lock()
		i++
		lock.Unlock()
		return i
	}
	return f
}

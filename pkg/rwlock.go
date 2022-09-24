package pkg

import (
	"fmt"
	"github.com/go-zookeeper/zk"
	"strconv"
	"strings"
)

const CommonPrefix = "dist-"
const RLockPrefix = CommonPrefix + "read-"
const WLockPrefix = CommonPrefix + "write-"

type DistRWLock struct {
	path     string
	acl      []zk.ACL
	lockPath string
	zkConn   *zk.Conn
}

// CreateRWDistLock CreateDistLock creates a distributed lock
func CreateRWDistLock(zkConn *zk.Conn, path string, acl []zk.ACL) *DistRWLock {
	dLock := &DistRWLock{
		path:     LockRoot + path,
		acl:      acl,
		lockPath: "",
		zkConn:   zkConn,
	}
	return dLock
}

func parseSeq(path string) (int, error) {
	parts := strings.Split(path, "-")
	return strconv.Atoi(parts[len(parts)-1])
}

func (lock *DistRWLock) GetNextLowerChild(contain string, seq int, children []string) (exist bool, lowerChild string) {
	lowerSeq := -1
	lowerChild = ""
	for _, c := range children {
		if strings.Contains(c, contain) {
			s, err := parseSeq(c)
			if err != nil {
				return false, ""
			}
			if s < seq && s > lowerSeq {
				lowerSeq = s
				lowerChild = c
			}
		}
	}
	if lowerSeq == -1 {
		return false, ""
	} else {
		return true, lowerChild
	}
}

func (lock *DistRWLock) createNode(root string, acl []zk.ACL) (path string, err error) {
	// Call create( ) to create a node with pathname "_locknode_/read-".
	for attempt := 0; attempt < RetryAttempts; attempt++ {
		path, err = lock.zkConn.CreateProtectedEphemeralSequential(root, []byte{}, acl)
		if err == nil {
			break
		} else if err == zk.ErrNoNode {
			components := strings.Split(lock.path, "/")
			parentDir := ""
			var exist bool
			for _, component := range components[1:] {
				parentDir += "/" + component
				exist, _, err = lock.zkConn.Exists(parentDir)
				if err != nil {
					return path, err
				}
				if exist {
					continue
				}
				_, err = lock.zkConn.Create(parentDir, []byte{}, 0, lock.acl)
				if err != nil && err != zk.ErrNodeExists {
					return path, err
				}
			}
		} else {
			return path, err
		}
	}
	return path, err
}

func (lock *DistRWLock) LockRead() (err error) {
	root := fmt.Sprintf("%s/%s", lock.path, RLockPrefix)

	// Call create( ) to create a node with pathname "_locknode_/read-".
	path, err := lock.createNode(root, zk.WorldACL(zk.PermAll))

	if err != nil {
		return err
	}

	seq, err := parseSeq(path)
	if err != nil {
		return err
	}

	for {
		// Call getChildren( ) on the lock node without setting the watch flag
		children, _, err := lock.zkConn.Children(lock.path)
		if err != nil {
			return err
		}

		exists, lowerChild := lock.GetNextLowerChild(WLockPrefix, seq, children)

		// If there are no children with a pathname starting with "write-" and having a lower sequence number than the node created in step 1, the client has the lock and can exit the protocol.
		if !exists {
			break
		}

		// Otherwise, call exists( ), with watch flag, set on the node in lock directory with pathname staring with "write-" having the next lowest sequence number.
		exists, _, ch, err := lock.zkConn.ExistsW(lock.path + "/" + lowerChild)
		if err != nil {
			return err
		}

		// If exists( ) returns false, goto step 2.
		if !exists {
			continue
		}

		// Otherwise, wait for a notification for the pathname from the previous step before going to step 2
		ev := <-ch
		if ev.Err != nil {
			return ev.Err
		}
	}

	lock.lockPath = path
	return nil
}

func (lock *DistRWLock) Unlock() (err error) {
	err = lock.zkConn.Delete(lock.lockPath, -1)
	if err != nil {
		return err
	}
	lock.lockPath = ""
	return nil
}

func (lock *DistRWLock) LockWrite() (err error) {
	root := fmt.Sprintf("%s/%s", lock.path, WLockPrefix)

	// Call create( ) to create a node with pathname "_locknode_/write-".
	path, err := lock.createNode(root, zk.WorldACL(zk.PermAll))
	if err != nil {
		return err
	}

	seq, err := parseSeq(path)
	if err != nil {
		return err
	}

	for {
		// Call getChildren( ) on the lock node without setting the watch flag
		children, _, err := lock.zkConn.Children(lock.path)
		if err != nil {
			return err
		}

		exists, lowerChild := lock.GetNextLowerChild(CommonPrefix, seq, children)

		// If there are no children with a lower sequence number than the node created in step 1, the client has the lock and the client exits the protocol.
		if !exists {
			break
		}

		// Otherwise, call exists( ), with watch flag, set on the node in lock directory with pathname staring with "write-" having the next lowest sequence number.
		exists, _, ch, err := lock.zkConn.ExistsW(lock.path + "/" + lowerChild)
		if err != nil {
			return err
		}

		// If exists( ) returns false, goto step 2.
		if !exists {
			continue
		}

		// Otherwise, wait for a notification for the pathname from the previous step before going to step 2
		ev := <-ch
		if ev.Err != nil {
			return ev.Err
		}
	}

	lock.lockPath = path
	return nil
}

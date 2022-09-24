package pkg

import (
	"errors"
	"fmt"
	"github.com/google/uuid"
	tapestry "tapestry/pkg"
)

type fileDescriptorID = int

type FileDescriptor struct {
	id           fileDescriptorID
	writeEnabled bool
	isDirty      bool
	metaData     *inode
	lock         *DistRWLock
	client       *PuddleStoreClient
	dataCache    map[blockIdx][]byte
	// TODO: first use mutex DistLock, then use RWLock for the required extra feature
}

// read content of the file
func (fd *FileDescriptor) read(offset uint64, size uint64) (ret []byte, err error) {
	// compute the bytes to read in total
	bytesToReadTotal := size

	ret = make([]byte, 0)

	if offset > fd.metaData.Size {
		// no content to read at all, return immediately
		return ret, nil
	}

	if offset+size > fd.metaData.Size {
		// clamp the read range so that it does not exceed the size of the file
		bytesToReadTotal = fd.metaData.Size - offset
	}

	// compute the index of the block to start reading
	idx := offset / fd.metaData.BlockSize
	startIdx := idx

	// compute the bytes to read from the first block
	bytesToReadCurrBlock := minUInt64(bytesToReadTotal, fd.metaData.BlockSize-offset%fd.metaData.BlockSize)

	for bytesToReadTotal > 0 {
		blockContent, exist := fd.dataCache[idx]
		if !exist {
			// local cache does not exist, fetch from Tapestry
			blockIdentifier := fd.metaData.Blocks[idx]
			blockContent, err = fd.fetchFromTapestry(blockIdentifier)
			if err != nil {
				return nil, err
			}
			// update local cache
			fd.dataCache[idx] = blockContent
		}
		// calculate the read start offset in the current block
		blockReadOffset := uint64(0)
		if idx == startIdx {
			// for the first block, we may not start reading from the start of the block
			blockReadOffset = offset % fd.metaData.BlockSize
		}

		// concatenate blockContent to ret
		ret = append(ret, blockContent[blockReadOffset:blockReadOffset+bytesToReadCurrBlock]...)

		// update bytesToReadTotal and bytesToReadCurrBlock
		bytesToReadTotal -= bytesToReadCurrBlock
		if bytesToReadTotal > fd.metaData.BlockSize {
			// we still have more than one block to read
			bytesToReadCurrBlock = fd.metaData.BlockSize
		} else {
			// we have exactly one block or part of the block to read
			bytesToReadCurrBlock = bytesToReadTotal
		}

		// increase index of the block
		idx += 1
	}

	Debug.Printf("read success: {size: %d, offset: %d, filename: %s}\n", len(ret), offset, fd.metaData.Filename)

	return ret, nil
}

func (fd *FileDescriptor) write(offset uint64, data []byte) (err error) {
	if !fd.writeEnabled {
		// check if write is enabled for the file
		return errors.New("write is not enabled for the file")
	}

	// compute the index of the block to start write into
	idx := offset / fd.metaData.BlockSize
	startIdx := idx

	if offset > fd.metaData.Size {
		// pad empty block until startIdx
		for i := uint64(len(fd.metaData.Blocks)); i < startIdx; i++ {
			blockIdentifier := uuid.New().String()
			fd.metaData.Blocks = append(fd.metaData.Blocks, blockIdentifier)
			fd.dataCache[i] = make([]byte, fd.metaData.BlockSize)
		}
	}

	// compute the bytes to write in total
	bytesToWriteTotal := uint64(len(data))
	// compute the bytes to write into the first block
	bytesToWriteCurrBlock := minUInt64(bytesToWriteTotal, fd.metaData.BlockSize-offset%fd.metaData.BlockSize)
	// initialize the handle of writing for the 'data'
	bytesToWriteOffset := uint64(0)

	for bytesToWriteTotal > 0 {
		if bytesToWriteCurrBlock > fd.metaData.BlockSize {
			return errors.New("fatal internal error: bytesToWriteCurrBlock is greater than block size")
		} else if bytesToWriteCurrBlock == fd.metaData.BlockSize {
			// if we are updating a whole block, simply update the local cache
			if _, exist := fd.dataCache[idx]; !exist {
				// allocate the memory if this block is not loaded before
				fd.dataCache[idx] = make([]byte, fd.metaData.BlockSize)
			}
			// update block identifier
			if idx >= uint64(len(fd.metaData.Blocks)) {
				// write exceeds the original size of the file, allocate new file block
				blockIdentifier := uuid.New().String()
				fd.metaData.Blocks = append(fd.metaData.Blocks, blockIdentifier)
			} else {
				// update a whole block that is already in cache, create new identifier
				fd.metaData.Blocks[idx] = uuid.New().String()
			}
			// copy block data
			copy(fd.dataCache[idx], data[bytesToWriteOffset:bytesToWriteOffset+bytesToWriteCurrBlock])
		} else {
			// only update a part of the block,
			// this can only happen when writing into the first and the last block
			_, exist := fd.dataCache[idx]
			var blockContent []byte
			if exist {
				// if the block is already in local cache, use it directly
				blockContent = fd.dataCache[idx]
				// create new identifier for the block
				fd.metaData.Blocks[idx] = uuid.New().String()
			} else if idx >= uint64(len(fd.metaData.Blocks)) {
				// write exceeds the size of the file, allocate new file block
				blockIdentifier := uuid.New().String()
				fd.metaData.Blocks = append(fd.metaData.Blocks, blockIdentifier)
				blockContent = make([]byte, fd.metaData.BlockSize)
			} else {
				// otherwise, we are still in range of the original file,
				// load the block from Tapestry
				blockIdentifier := fd.metaData.Blocks[idx]
				blockContent, err = fd.fetchFromTapestry(blockIdentifier)
				// create new identifier for the block
				fd.metaData.Blocks[idx] = uuid.New().String()
				if err != nil {
					return err
				}
			}

			// update the block content
			if idx == startIdx {
				// we are writing into the first block
				writeOffset := offset % fd.metaData.BlockSize
				copy(blockContent[writeOffset:writeOffset+bytesToWriteCurrBlock],
					data[bytesToWriteOffset:bytesToWriteOffset+bytesToWriteCurrBlock])
			} else {
				// we are writing into the last block, append to front
				copy(blockContent[0:bytesToWriteCurrBlock],
					data[bytesToWriteOffset:bytesToWriteOffset+bytesToWriteCurrBlock])
			}
			// update local cache
			fd.dataCache[idx] = blockContent
		}

		// update bytesToWriteTotal and bytesToWriteCurrBlock
		bytesToWriteTotal -= bytesToWriteCurrBlock
		bytesToWriteOffset += bytesToWriteCurrBlock
		if bytesToWriteTotal > fd.metaData.BlockSize {
			// we still have more than one block to write
			bytesToWriteCurrBlock = fd.metaData.BlockSize
		} else {
			// we have exactly one block or part of the block to write
			bytesToWriteCurrBlock = bytesToWriteTotal
		}

		// increase index of the block
		idx += 1
	}

	// update size of the file
	fd.metaData.Size = maxUInt64(fd.metaData.Size, offset+uint64(len(data)))

	fd.isDirty = true
	Debug.Printf("write success: {size: %d, offset: %d, filename: %s}\n", len(data), offset, fd.metaData.Filename)

	return nil
}

// flush the file content in memory to 'persistent' storage.
func (fd *FileDescriptor) flush() (err error) {
	// update metadata
	metaData, err := encodeInode(*fd.metaData)
	if err != nil {
		return err
	}

	_, err = fd.client.zkConn.Set(fd.metaData.Path, metaData, -1)
	if err != nil {
		return err
	}

	// write block content back into Tapestry
	for idx, blockContent := range fd.dataCache {
		blockIdentifier := fd.metaData.Blocks[idx]
		err = fd.storeAndReplicateBlock(blockIdentifier, blockContent, fd.client.numReplicas)
		if err != nil {
			return err
		}
	}

	return nil
}

// storeAndReplicateBlock stores the block on in the Tapestry cluster.
func (fd *FileDescriptor) storeAndReplicateBlock(blockIdentifier string, content []byte, numReplicas int) error {
	//tapClient, err := fd.client.getRandomTapestryClient()
	//if err != nil {
	//	return err
	//}
	//
	//// find the tapestry nodes that have already stored this block
	//replicas, _ := tapClient.Lookup(blockIdentifier)
	//
	//// update content on the replicas that have previously stored this block
	//for _, rep := range replicas {
	//	err = rep.Store(blockIdentifier, content)
	//	if err != nil {
	//		// update could fail if this replica has already crashed or become unavailable
	//		continue
	//	}
	//	numReplicas -= 1
	//}
	//
	//if numReplicas <= 0 {
	//	// we have replicated the block for numReplicas times, return success
	//	return nil
	//}

	// upon here, we are in either the following two cases
	// 1. This is a completely new block that has not been stored onto the tapestry before
	// 2. This is an old block, but some of its replicas became unavailable

	// randomly choose numReplicas tapestry node and establish client connection
	// we do need to exclude the node that have already stored the current block
	newReplicas, err := fd.client.getRandomTapestryClients(numReplicas, nil)
	if err != nil {
		Out.Printf("not enough tapestry nodes to perform replication: %d", numReplicas)
		return err
	}

	// for each new replica, store the block with retry
	maxRetry := 3
	replicateResult := make(chan bool)

	for i := 0; i < numReplicas; i++ {
		go replicateWithRetry(maxRetry, newReplicas[i], blockIdentifier, content, replicateResult)
	}

	// accumulate replication result
	successCnt := 0
	for i := 0; i < numReplicas; i++ {
		result := <-replicateResult
		if result {
			successCnt += 1
		}
	}

	if successCnt < numReplicas {
		return errors.New(fmt.Sprintf("replicate fail: expect %d, success %d", numReplicas, successCnt))
	}
	return nil
}

func replicateWithRetry(maxRetry int, c *tapestry.Client, key string, value []byte, replicateResult chan<- bool) {
	for i := 0; i < maxRetry; i++ {
		err := c.Store(key, value)
		if err != nil {
			// replicate fail, retry
			continue
		}
		// replicate success, return
		replicateResult <- true
		return
	}
	// replicate fail
	replicateResult <- false
	return
}

func (fd *FileDescriptor) fetchFromTapestry(blockIdentifier string) (data []byte, err error) {
	tapClients, err := fd.client.getAllTapestryClients()
	if err != nil {
		Out.Printf("fetch fail: %v\n", err)
		return nil, err
	}

	if len(tapClients) == 0 {
		err = errors.New("no available tapestry node")
		Out.Printf("fetch fail: %v\n", err)
		return nil, err
	}

	for _, tapClient := range tapClients {
		data, err = tapClient.Get(blockIdentifier)
		if err != nil {
			continue
		}
		return data, nil
	}
	Out.Printf("fetch fail: %v\n", err)
	return nil, err
}

func minUInt64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func maxUInt64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

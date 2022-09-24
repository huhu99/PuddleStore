# Puddlestore README

### Bugs in our code

We have passed all the test cases in the checkpoint.

### Extra features

Read-Write locks & Pre-fetching

Read lock: Acquire read lock when creating or opening a file with ```write``` set to false.

Write lock: Acquire write lock when creating or opening a file with ```write``` set to true. Or when removing items.

Pre-fetching: Adjust read size by considering history read statistics, use historical average read bytes to pre-fetch.

### Test Cases

- basic_test
    - TestCreateCluster:  Create a default cluster.
    - TestCreateFileNoParent: Create a file whose parent directory does not exist, should fail.
    - TestCreateFileUnderFile: Create a file "/a", should not be able to create a file "/a/b".
    - TestCreateFlagFalse: Open a non-existent file without creating it, should fail.
    - TestOperateUnopened: Read, write and close an unopened file, should fail.
    - TestMkdirBasic: Create a basic directory "/a"
    - TestMkdirParentNotExist: Create a directory whose parent directory does not exist, should fail.
    - TestMkdirParentNotDir: Create a directory whose with a file in its path, should fail.
    - TestMkdirAlreadyExist: Create a directory that already exists, should err.
    - TestRemoveDir: Create a root directory and sub-directory within it, then remove the root directory.
    - TestRemoveNonExist: Remove a non-existent file, should fail.
    - TestOpenWriteShouldBlockAll: Open with write enabled should block all concurrent open.
    - TestOpenReadShouldOnlyBlockWrite: Read-only open should only block open with write enabled.
    - TestOpenRemoveBlock: Open should block remove
    - TestListEmpty: List empty directory, should not contain any file.
    - TestListFile: List file should only return filename.

- read_write_test
    - TestFileWriteFlagFalse: Set write flag of file to false, writing to that file should fail.
    - TestReadEmptyFile: Read from empty file should always return empty data.
    - TestReadWriteVariableReadSize: Read variable size of data from file and check.
    - TestReadAfterOverwrite: Overwrite part of the file and check.
    - TestWriteAtOffsetInEmptyFile: Writing beyond the file boundary should automatically fill the file with zero bytes.
    - TestWriteAtOffsetInEmptyFile2: Writing beyond the file boundary should automatically fill the file with zero bytes.
    - TestReadWriteTwoClients: Two clients one writing and one reading.
    - TestReadWriteExtensive: Read and write multiple times.
    - TestReadWriteAfterRemoveIntermediateDirectory: Write to some files, delete the intermediate directory on the file's path, then read file should fail.
    - TestMultiClientConcurrentIncrement: Concurrent read and write to a counter in the file.
    - TestMultipleClientReadOverwriteAppend: Multiple client performing read, overwrite and append operations.
    
- tapestry_test
    - TestNoTapestry: Shut down all tapestry nodes, read will fail.
    - TestNotEnoughNode: Shut down a tapestry node, making not enough nodes for the flush.
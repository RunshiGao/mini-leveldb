# NoSQL Database with LSM-Tree

This project implements a simple NoSQL database system with a Log-Structured Merge-Tree (LSM-Tree) data structure. The system allows for basic operations like storing and retrieving key-value pairs in a persistent storage format.

## Design
Block size default 256 Bytes.  First block is metadata. Block 1-8 is for fcb.  
For data block, using linked allocation. For index, using indexed allocation.  
For sstable, optimized by sparse index to reduce block reads.

## Author
Runshi Gao(002793874)  
Yueyan Li(002190756)

## Prerequisites

- Python 3.x

## Installation

1. Clone the repository:

    ```bash
    git clone https://github.com/RunshiGao/my-rocksdb.git
    ```

2. Navigate to the project directory:

    ```bash
    cd my-rocksdb
    ```

3. Run the application:

    ```bash
    python my_rocksdb.py
    ```
Then you will be able to input commands

## Assumption and Limitation
* Allow only 8 input files
* Maximum 50 sstables, which is around 2MB file
* Assume on integer keys
* No Write-Ahead-Log so no crash recovery
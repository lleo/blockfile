/*
This Package implements version 1 of the blockfile API. The blockfile API
controls reading and writing blocks of data to and from a individual file.
*/
package blockfile

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"os"
)

// Header size of the base is (signature + version = 4 + 1)
// Header size of V1 is (base + uint32 = 5 + 4)
const HEADER_SIZE_V1 uint64 = HEADER_SIZE_BASE + 4

type BlockFileV1 struct {
	BlockFileBase
	fileName  string
	blockSize uint32
	file      *os.File
}

// Create and Initialize a new block file on disk and build at BlockFileV1
// struct.
func NewBlockFileV1(fname string, blockSize uint32) (BlockFile, error) {
	fmt.Printf("NewBlockFileV1: fname=%s, blockSize=%d\n", fname, blockSize)

	if blockSize == 0 {
		return nil, fmt.Errorf("blockfile.NewBlockFileV1: blockSize=0 is not allowed.")
	}

	file, err := os.OpenFile(fname, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0755)
	if err != nil {
		return nil, fmt.Errorf("blockfile.NewBlockFileV1: failed to os.OpenFile(%q, , os.O_RDWR|os.O_CREATE|os.O_EXCL, 0755); err=%q;\n", fname, err)
	}

	if err != nil {
		log.Fatal(err)
	}

	err = binary.Write(file, binary.BigEndian, SIGNATURE)
	if err != nil {
		return nil, fmt.Errorf("blockfile.NewBlockFileV1: failed to write encoded SIGNATURE; err=%q\n", err)
	}

	verBuf := []byte{V1}
	n, err := file.Write(verBuf)
	if err != nil {
		return nil, fmt.Errorf("blockfile.NewBlockFileV1: failed to write version; err=%q", err)
	}
	if n != 1 {
		return nil, fmt.Errorf("blockfile.NewBlockFileV1: failed to write version; expected n==1, found n=%d\n", n)
	}

	err = binary.Write(file, binary.BigEndian, blockSize)
	if err != nil {
		return nil, fmt.Errorf("blockfile.NewBlockFileV1: failed to write block size; err=%q\n", err)
	}

	return &BlockFileV1{BlockFileBase{1}, fname, blockSize, file}, nil
}

// Open an existing BlockFileV1 file.
func OpenBlockFileV1(fname string) (BlockFile, error) {
	fmt.Printf("blockfile.OpenBlockFileV1: fname=%q\n", fname)
	file, err := os.OpenFile(fname, os.O_RDWR, 0755)
	if err != nil {
		return nil, fmt.Errorf("blockfile.OpenBlockFileV1: failed to open file; err=%q\n", err)
	}

	headerBuf := make([]byte, HEADER_SIZE_V1)

	n, err := file.Read(headerBuf)
	if err != nil {
		return nil, fmt.Errorf("blockfile.OpenBlockFile: failed to read header; err=%q\n", err)
	}

	if uint64(n) != HEADER_SIZE_V1 {
		return nil, fmt.Errorf("blockfile.OpenBlockFile: failed to read first %d bytes of the header; read %d bytes\n", HEADER_SIZE_V1, n)
	}

	headerReader := bytes.NewReader(headerBuf)

	var sig uint32
	err = binary.Read(headerReader, binary.BigEndian, &sig)
	if err != nil {
		return nil, fmt.Errorf("blockfile.OpenBlockFileV1: failed to read signature; err=%q\n", err)
	}

	if sig != SIGNATURE {
		return nil, fmt.Errorf("blockfile.OpenBlockFileV1: signature does not match expected SIGNATURE. 0x%x != 0x%x\b", sig, SIGNATURE)
	}

	ver, err := headerReader.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("blockfile.OpenBlockFileV1: failed to read version; err=%q\n", err)
	}

	if ver != V1 {
		return nil, fmt.Errorf("blockfile.OpenBlockFileV1: expected version == %d; found version=%d", V1, ver)
	}

	var blockSize uint32
	err = binary.Read(headerReader, binary.BigEndian, &blockSize)
	if err != nil {
		return nil, fmt.Errorf("blockfile.OpenBlockFileV1: failed to read and convert the block size")
	}

	return &BlockFileV1{BlockFileBase{1}, fname, blockSize, file}, nil
}

// not implemented
func (bf *BlockFileV1) ReadBlock(blkId uint64) ([]byte, error) {
	pos := int64(HEADER_SIZE_V1 + blkId*uint64(bf.blockSize))
	buf := make([]byte, bf.blockSize)

	n, err := bf.file.ReadAt(buf, pos)
	if err != nil {
		return nil, fmt.Errorf("<*BlockFileV1>.ReadBlock: failed to read %d sized block at %d position; err=%q\n", len(buf), pos, err)
	}

	if n != len(buf) {
		fmt.Printf("<*BlockFileV1>.ReadBlock: only read %d bytes of %d long block\n", n, len(buf))
	}

	return buf, nil
}

// not implemented
func (bf *BlockFileV1) WriteBlock(blk []byte, blkId uint64) (int, error) {
	pos := int64(HEADER_SIZE_V1 + blkId*uint64(bf.blockSize))

	length := len(blk)
	fmt.Printf("blockfile.WriteBlock: before: len(blk)=%d\n", len(blk))
	if uint32(length) != bf.blockSize {
		if uint32(length) < bf.blockSize {
			rem := bf.blockSize - uint32(length)
			blk = append(blk, make([]byte, rem)...)
		} else {
			blk = blk[:bf.blockSize]
		}
	}
	fmt.Printf("blockfile.WriteBlock: after: len(blk)=%d\n", len(blk))

	n, err := bf.file.WriteAt(blk, pos)
	if err != nil {
		return n, err
	}

	err = nil
	if n != len(blk) {
		err = fmt.Errorf("didn't write whole block; expected to write %d bytes, wrote %d bytes; and blockSize=%d\n", len(blk), n, bf.blockSize)
	}

	return n, err
}

func (bf *BlockFileV1) FileName() string {
	return bf.fileName
}

func (bf *BlockFileV1) NumBlocks() (uint64, error) {
	finfo, err := bf.file.Stat()
	if err != nil {
		return 0, err
	}

	size := uint64(finfo.Size())  // finfo.Size() => int64
	blkSz := uint64(bf.blockSize) // bf.blockSize => uint32

	blkAreaSize := size - HEADER_SIZE_V1
	numBlks := blkAreaSize / blkSz

	rem := blkAreaSize % blkSz
	if rem != 0 {
		return numBlks, fmt.Errorf("The block area of this blockfile is NOT an integer number of block sizes; number of blocks %d + remainder %d\n", numBlks, rem)
	}

	return numBlks, nil
}

// bf.BlockSize() returns the block size this file was initialized with.
func (bf *BlockFileV1) BlockSize() uint32 {
	return bf.blockSize
}

// bf.Version() returns the version of this block file.
func (bf *BlockFileV1) Version() uint8 {
	return bf.version
}

func (bf *BlockFileV1) HeaderSize() uint64 {
	return HEADER_SIZE_V1
}

func (bf *BlockFileV1) Close() error {
	if bf.file == nil {
		log.Fatal("<*BlockFileV1>.Close: bf.file==nil")
	}

	err := bf.file.Close()
	if err != nil {
		fmt.Println("<*BlockFileV1>.Close: bf.file.Close() returned err; err=%q\n", err)
	}

	return err
}

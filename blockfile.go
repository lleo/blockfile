package blockfile

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"os"
)

const (
	_        = iota
	V1 uint8 = iota
	V2
	V3
)

const SIGNATURE uint32 = 0xb10cf11e

//var SIG_BYTE_ARRAY = [4]byte{0xb1, 0x0c, 0xf1, 0x1e}

const HEADER_SIZE_BASE uint64 = 5

type BlockFileBase struct {
	version uint8
}

type BlockFile interface {
	ReadBlock(uint64) ([]byte, error)
	WriteBlock([]byte, uint64) (int, error)
	FileName() string
	NumBlocks() (uint64, error)
	BlockSize() uint32
	Version() uint8
	HeaderSize() uint64
	Close() error
}

func NewBlockFile(fname string, ver uint8, bsize uint32) (BlockFile, error) {
	fmt.Printf("NewBlockFile: fname=%s; ver=%d; bsize=%d;\n", fname, ver, bsize)
	var bf BlockFile
	var err error

	switch ver {
	case V1:
		bf, err = NewBlockFileV1(fname, bsize)
	case V2:
		bf, err = nil, fmt.Errorf("V2 not implemented")
		//br, err = NewBlockFileV2(fname, bsize)
	case V3:
		bf, err = nil, fmt.Errorf("V3 not implemented")
		//bf, err = NewBlockFileV3(fname)
	default:
		bf, err = nil, fmt.Errorf("blockfile.OpenBlockFile: unsupported version; ver=%d\n", ver)
	}

	return bf, err
}

func OpenBlockFile(fname string) (BlockFile, error) {
	file, err := os.Open(fname)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	headerBuf := make([]byte, HEADER_SIZE_BASE)

	n, err := file.Read(headerBuf)
	if err != nil {
		return nil, fmt.Errorf("blockfile.OpenBlockFile: failed to read header; err=%q\n", err)
	}

	if uint64(n) != HEADER_SIZE_BASE {
		return nil, fmt.Errorf("blockfile.OpenBlockFile: failed to read first %d bytes of the header; read %d bytes\n", HEADER_SIZE_BASE, n)
	}

	headerReader := bytes.NewReader(headerBuf)

	var sig uint32
	err = binary.Read(headerReader, binary.BigEndian, &sig)
	if err != nil {
		return nil, fmt.Errorf("blockfile.OpenBlockFile: failed to read signature; err=%q\n", err)
	}

	ver, err := headerReader.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("blockfile.OpenBlockFile: failed to read version; err=%q\n", err)
	}

	if sig != SIGNATURE {
		return nil, fmt.Errorf("blockfile.OpenBlockFile: signature does not match expected SIGNATURE. 0x%x != 0x%x\b", sig, SIGNATURE)
	}

	if ver != 1 && ver != 2 && ver != 3 {
		return nil, fmt.Errorf("Version does not match an expected version (1, 2, or 3). ver=%d\n", ver)
	}

	var bf BlockFile
	//var err error //already declared

	switch ver {
	case V1:
		bf, err = OpenBlockFileV1(fname)
	case V2:
		bf, err = nil, fmt.Errorf("V2 not implemented")
		//bf, err = NewBlockFileV2(fname)
	case V3:
		bf, err = nil, fmt.Errorf("V3 not implemented")
		//bf, err = NewBlockFileV3(fname)
	default:
		bf, err = nil, fmt.Errorf("blockfile.OpenBlockFile: unsupported version; ver=%d\n", ver)
	}
	return bf, err
}

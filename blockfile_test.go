package blockfile

import (
	"flag"
	"log"
	"os"
	"testing"
)

var doesntExistFileName string = "doesntexist.bf"
var existsFileName string = "exists.bf"

const TEST_BLOCKSIZE uint32 = 64

func TestMain(m *testing.M) {
	flag.Parse()

	file, err := os.OpenFile(existsFileName, os.O_RDONLY|os.O_CREATE, 0644)
	if err != nil {
		log.Fatalf("Failed to create/open %s; err=%q", existsFileName, err)
	}
	file.Close()

	nofile, err := os.Open(doesntExistFileName)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Printf("File exists %s, but cannot open; err=%q\n", doesntExistFileName, err)
			errRemove := os.Remove(doesntExistFileName)
			if errRemove != nil {
				log.Fatalf("Failed to remove %s; err=%q\n", doesntExistFileName, errRemove)
			} else {
				log.Printf("Suceeded in removing %s\n", doesntExistFileName)
			}
		}
	} else if nofile != nil {
		nofile.Close()
		log.Printf("File exists %s and suceeded in opening it", doesntExistFileName)
		err = os.Remove(doesntExistFileName)
		if err != nil {
			log.Fatalf("Failed to remove %s; err=%q\n", doesntExistFileName, err)
		} else {
			log.Printf("Suceeded in removeing %s", doesntExistFileName)
		}
	}

	xit := m.Run()

	os.Remove(existsFileName)

	os.Exit(xit)
}

func TestNewBlkFileDoesntExist(t *testing.T) {
	bf, err := NewBlockFile(doesntExistFileName, V1, TEST_BLOCKSIZE)
	if err != nil {
		log.Printf("Failed to create blockfile name=%d; version=%d; blocksize=%d;", doesntExistFileName, V1, TEST_BLOCKSIZE)
		t.Fail()
	}
	bf.Close()
	os.Remove(doesntExistFileName)
}

func TestNewBlkFileExists(t *testing.T) {
	bf, err := NewBlockFile(existsFileName, V1, TEST_BLOCKSIZE)
	if err == nil {
		bf.Close()
		defer os.Remove(existsFileName)
		t.Fail()
	}
}

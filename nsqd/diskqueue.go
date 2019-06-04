package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"runtime"
	"sync"
	"sync/atomic"
)

// DiskQueue implements the BackendQueue interface
// providing a filesystem backed FIFO queue
type DiskQueue struct {
	sync.RWMutex

	// instatiation time metadata
	name            string
	dataPath        string
	maxBytesPerFile int64 // currently this cannot change once created
	syncEvery       int64 // number of writes per sync
	exitFlag        int32

	// run-time state (also persisted to disk)
	readPos      int64
	writePos     int64
	readFileNum  int64
	writeFileNum int64
	depth        int64

	// keeps track of the position where we have read
	// (but not yet sent over readChan)
	nextReadPos     int64
	nextReadFileNum int64

	readFile  *os.File
	writeFile *os.File
	reader    *bufio.Reader
	writeBuf  bytes.Buffer

	// exposed via ReadChan()
	readChan chan []byte

	// internal channels
	writeChan         chan []byte
	writeResponseChan chan error
	emptyChan         chan int
	emptyResponseChan chan error
	exitChan          chan int
	exitSyncChan      chan int
}

// NewDiskQueue instantiates a new instance of DiskQueue, retrieving metadata
// from the filesystem and starting the read ahead goroutine
func NewDiskQueue(name string, dataPath string, maxBytesPerFile int64, syncEvery int64) BackendQueue {
	d := DiskQueue{
		name:              name,
		dataPath:          dataPath,
		maxBytesPerFile:   maxBytesPerFile,
		readChan:          make(chan []byte),
		writeChan:         make(chan []byte),
		writeResponseChan: make(chan error),
		emptyChan:         make(chan int),
		emptyResponseChan: make(chan error),
		exitChan:          make(chan int),
		exitSyncChan:      make(chan int),
		syncEvery:         syncEvery,
	}

	// no need to lock here, nothing else could possibly be touching this instance
	err := d.retrieveMetaData()
	if err != nil && !os.IsNotExist(err) {
		log.Printf("ERROR: diskqueue(%s) failed to retrieveMetaData - %s", d.name, err.Error())
	}

	go d.ioLoop()

	return &d
}

// Depth returns the depth of the queue
func (d *DiskQueue) Depth() int64 {
	return atomic.LoadInt64(&d.depth)
}

// ReadChan returns the []byte channel for reading data
func (d *DiskQueue) ReadChan() chan []byte {
	return d.readChan
}

// Put writes a []byte to the queue
func (d *DiskQueue) Put(data []byte) error {
	d.RLock()
	defer d.RUnlock()

	if d.exitFlag == 1 {
		return errors.New("exiting")
	}

	d.writeChan <- data
	return <-d.writeResponseChan
}

// Close cleans up the queue and persists metadata
func (d *DiskQueue) Close() error {
	d.Lock()
	defer d.Unlock()

	log.Printf("DISKQUEUE(%s): closing", d.name)

	d.exitFlag = 1

	close(d.exitChan)
	// ensure that ioLoop has exited
	<-d.exitSyncChan

	if d.readFile != nil {
		d.readFile.Close()
		d.readFile = nil
	}

	if d.writeFile != nil {
		d.writeFile.Close()
		d.writeFile = nil
	}

	return d.sync()
}

// Empty destructively clears out any pending data in the queue
// by fast forwarding read positions and removing intermediate files
func (d *DiskQueue) Empty() error {
	d.RLock()
	defer d.RUnlock()

	if d.exitFlag == 1 {
		return errors.New("exiting")
	}

	d.emptyChan <- 1
	return <-d.emptyResponseChan
}

func (d *DiskQueue) doEmpty() error {
	log.Printf("DISKQUEUE(%s): emptying", d.name)

	if d.readFile != nil {
		d.readFile.Close()
		d.readFile = nil
	}

	// make a list of read file numbers to remove (later)
	numsToRemove := make([]int64, 0)
	for d.readFileNum < d.writeFileNum {
		numsToRemove = append(numsToRemove, d.readFileNum)
		d.readFileNum++
	}

	d.readFileNum = d.writeFileNum
	d.readPos = d.writePos
	d.nextReadFileNum = d.writeFileNum
	d.nextReadPos = d.writePos
	atomic.StoreInt64(&d.depth, 0)

	err := d.sync()
	if err != nil {
		log.Printf("ERROR: diskqueue(%s) failed to sync - %s", d.name, err.Error())
		return err
	}

	// only if we've successfully persisted metadata do we remove old files
	for _, num := range numsToRemove {
		fn := d.fileName(num)
		err := os.Remove(fn)
		if err != nil {
			return err
		}
	}

	return nil
}

// readOne performs a low level filesystem read for a single []byte
// while advancing read positions and rolling files, if necessary
func (d *DiskQueue) readOne() ([]byte, error) {
	var err error
	var msgSize int32

	if d.readFile == nil {
		curFileName := d.fileName(d.readFileNum)
		d.readFile, err = os.OpenFile(curFileName, os.O_RDONLY, 0600)
		if err != nil {
			return nil, err
		}

		log.Printf("DISKQUEUE(%s): readOne() opened %s", d.name, curFileName)

		if d.readPos > 0 {
			_, err = d.readFile.Seek(d.readPos, 0)
			if err != nil {
				d.readFile.Close()
				d.readFile = nil
				return nil, err
			}
		}

		d.reader = bufio.NewReader(d.readFile)
	}

	err = binary.Read(d.reader, binary.BigEndian, &msgSize)
	if err != nil {
		d.readFile.Close()
		d.readFile = nil
		return nil, err
	}

	readBuf := make([]byte, msgSize)
	_, err = io.ReadFull(d.reader, readBuf)
	if err != nil {
		d.readFile.Close()
		d.readFile = nil
		return nil, err
	}

	totalBytes := int64(4 + msgSize)

	// we only advance next* because we have not yet sent this to consumers
	// (where readFileNum, readPos will actually be advanced)
	d.nextReadPos = d.readPos + totalBytes
	d.nextReadFileNum = d.readFileNum

	// TODO: each data file should embed the maxBytesPerFile
	// as the first 8 bytes (at creation time) ensuring that
	// the value can change without affecting runtime
	if d.nextReadPos > d.maxBytesPerFile {
		if d.readFile != nil {
			d.readFile.Close()
			d.readFile = nil
		}

		d.nextReadFileNum++
		d.nextReadPos = 0
	}

	return readBuf, nil
}

// writeOne performs a low level filesystem write for a single []byte
// while advancing write positions and rolling files, if necessary
func (d *DiskQueue) writeOne(data []byte) error {
	var err error

	if d.writeFile == nil {
		curFileName := d.fileName(d.writeFileNum)
		d.writeFile, err = os.OpenFile(curFileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
		if err != nil {
			return err
		}

		log.Printf("DISKQUEUE(%s): writeOne() opened %s", d.name, curFileName)

		if d.writePos > 0 {
			_, err = d.writeFile.Seek(d.writePos, 0)
			if err != nil {
				d.writeFile.Close()
				d.writeFile = nil
				return err
			}
		}
	}

	dataLen := len(data)

	d.writeBuf.Reset()
	err = binary.Write(&d.writeBuf, binary.BigEndian, int32(dataLen))
	if err != nil {
		return err
	}

	_, err = d.writeBuf.Write(data)
	if err != nil {
		return err
	}

	// only write to the file once
	_, err = d.writeFile.Write(d.writeBuf.Bytes())
	if err != nil {
		d.writeFile.Close()
		d.writeFile = nil
		return err
	}

	totalBytes := int64(4 + dataLen)
	d.writePos += totalBytes
	atomic.AddInt64(&d.depth, 1)

	if d.writePos > d.maxBytesPerFile {
		d.writeFileNum++
		d.writePos = 0

		// sync every time we start writing to a new file
		err = d.sync()
		if err != nil {
			log.Printf("ERROR: diskqueue(%s) failed to sync - %s", d.name, err.Error())
		}

		if d.writeFile != nil {
			d.writeFile.Close()
			d.writeFile = nil
		}
	}

	return err
}

// sync fsyncs the current writeFile and persists metadata
func (d *DiskQueue) sync() error {
	if d.writeFile != nil {
		err := d.writeFile.Sync()
		if err != nil {
			d.writeFile.Close()
			d.writeFile = nil
			return err
		}
	}

	return d.persistMetaData()
}

// retrieveMetaData initializes state from the filesystem
func (d *DiskQueue) retrieveMetaData() error {
	var f *os.File
	var err error

	fileName := d.metaDataFileName()
	f, err = os.OpenFile(fileName, os.O_RDONLY, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = fmt.Fscanf(f, "%d\n%d,%d\n%d,%d\n",
		&d.depth,
		&d.readFileNum, &d.readPos,
		&d.writeFileNum, &d.writePos)
	if err != nil {
		return err
	}
	d.nextReadFileNum = d.readFileNum
	d.nextReadPos = d.readPos

	return nil
}

// persistMetaData atomically writes state to the filesystem
func (d *DiskQueue) persistMetaData() error {
	var f *os.File
	var err error

	fileName := d.metaDataFileName()
	tmpFileName := fileName + ".tmp"

	// write to tmp file
	f, err = os.OpenFile(tmpFileName, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}

	_, err = fmt.Fprintf(f, "%d\n%d,%d\n%d,%d\n",
		atomic.LoadInt64(&d.depth),
		d.readFileNum, d.readPos,
		d.writeFileNum, d.writePos)
	if err != nil {
		f.Close()
		return err
	}
	f.Sync()
	f.Close()

	// atomically rename
	return os.Rename(tmpFileName, fileName)
}

func (d *DiskQueue) metaDataFileName() string {
	return fmt.Sprintf(path.Join(d.dataPath, "%s.diskqueue.meta.dat"), d.name)
}

func (d *DiskQueue) fileName(fileNum int64) string {
	return fmt.Sprintf(path.Join(d.dataPath, "%s.diskqueue.%06d.dat"), d.name, fileNum)
}

// ioLoop provides the backend for exposing a go channel (via ReadChan())
// in support of multiple concurrent queue consumers
//
// it works by looping and branching based on whether or not the queue has data
// to read and blocking until data is either read or written over the appropriate
// go channels
//
// conveniently this also means that we're asynchronously reading from the filesystem
func (d *DiskQueue) ioLoop() {
	var dataRead []byte
	var err error
	var count int64
	var r chan []byte

	for {
		count++
		// dont sync all the time :)
		if count == d.syncEvery {
			err := d.sync()
			if err != nil {
				log.Printf("ERROR: diskqueue(%s) failed to sync - %s", d.name, err.Error())
			}
			count = 0
		}

		if (d.readFileNum < d.writeFileNum) || (d.readPos < d.writePos) {
			if d.nextReadPos == d.readPos {
				dataRead, err = d.readOne()
				if err != nil {
					log.Printf("ERROR: reading from diskqueue(%s) at %d of %s - %s",
						d.name, d.readPos, d.fileName(d.readFileNum), err.Error())
					// TODO: we assume that all read errors are recoverable...
					// it will probably turn out that this is a terrible assumption
					// as this could certainly result in an infinite busy loop
					runtime.Gosched()
					continue
				}
			}
			r = d.readChan
		} else {
			r = nil
		}

		select {
		// the Go channel spec dictates that nil channel operations (read or write)
		// in a select are skipped, we set r to d.readChan only when there is data to read
		// and reset it to nil after writing to the channel
		case r <- dataRead:
			oldReadFileNum := d.readFileNum
			d.readFileNum = d.nextReadFileNum
			d.readPos = d.nextReadPos
			atomic.AddInt64(&d.depth, -1)

			// see if we need to clean up the old file
			if oldReadFileNum != d.nextReadFileNum {
				// sync every time we start reading from a new file
				err = d.sync()
				if err != nil {
					log.Printf("ERROR: diskqueue(%s) failed to sync - %s", d.name, err.Error())
					continue
				}

				// only if we've successfully synced do we remove old files
				fn := d.fileName(oldReadFileNum)
				err = os.Remove(fn)
				if err != nil {
					log.Printf("ERROR: failed to Remove(%s) - %s", fn, err.Error())
				}
			}
		case <-d.emptyChan:
			d.emptyResponseChan <- d.doEmpty()
		case dataWrite := <-d.writeChan:
			d.writeResponseChan <- d.writeOne(dataWrite)
		case <-d.exitChan:
			goto exit
		}
	}

exit:
	log.Printf("DISKQUEUE(%s): closing ... ioLoop", d.name)
	d.exitSyncChan <- 1
}

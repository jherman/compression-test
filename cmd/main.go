package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/pkg/errors"
)

var (
	encoderOp = zstd.WithEncoderLevel(zstd.SpeedBetterCompression)
	zw        *zstd.Encoder
	zwMutex   *sync.Mutex
)

func init() {
	zwMutex = &sync.Mutex{}
}

func main() {
	go func() {
		i := 0

		for {
			err := CompressToFile("sample.txt", "sample.txt.zst")
			if err != nil {
				log.Fatal(err)
			}
			i++
			if i%1000 == 0 {
				runtime.GC()
				fmt.Println("sleeping for 5 minutes")
				time.Sleep(5 * time.Minute)
				fmt.Println("resuming")
			}
		}
	}()

	fmt.Printf("press enter key to stop...\n")

	os.Stdin.Read(make([]byte, 1))

	fmt.Printf("done")
}

// CompressToFile a file
func CompressToFile(in, out string) (err error) {
	zwMutex.Lock()

	var (
		file  *os.File
		zfile *os.File
	)

	defer func() {
		if zw != nil {
			zwErr := zw.Close()
			if err != nil {
				err = errors.Wrap(err, zwErr.Error())
			} else {
				err = zwErr
			}
		}

		if zfile != nil {
			fileErr := zfile.Close()
			if err != nil {
				err = errors.Wrap(err, fileErr.Error())
			} else {
				err = fileErr
			}
		}
		if file != nil {
			fileErr := file.Close()
			if err != nil {
				err = errors.Wrap(err, fileErr.Error())
			} else {
				err = fileErr
			}
		}

		zwMutex.Unlock()
	}()

	zw, err = getEncoder()
	if err != nil {
		return
	}

	file, err = os.Open(in)
	if err != nil {
		return
	}
	finfo, err := file.Stat()
	if err != nil {
		return
	}
	mode := finfo.Mode() // use the same mode for the output file

	// Output file.
	zfile, err = os.OpenFile(out, os.O_CREATE|os.O_WRONLY, mode)
	if err != nil {
		return
	}
	zw.Reset(zfile)

	// Compress.
	_, err = io.Copy(zw, file)
	if err != nil {
		return
	}

	return
}

func getEncoder() (*zstd.Encoder, error) {
	if zw == nil {
		newZw, writerErr := zstd.NewWriter(nil, encoderOp)
		if writerErr != nil {
			return nil, writerErr
		}

		zw = newZw
	}

	return zw, nil
}

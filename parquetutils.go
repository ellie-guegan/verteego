package main

import (
	"log"
	"strconv"

	"github.com/xitongsys/parquet-go/ParquetFile"
	"github.com/xitongsys/parquet-go/ParquetWriter"
	"github.com/xitongsys/parquet-go/parquet"
)

// ------------------------------------------------------------------
// save any struct slice into parquet file
// ------------------------------------------------------------------
func genericSaveParquetFile(data []interface{}, obj interface{}, outputParquetFile string) {
	fw, err := ParquetFile.NewLocalFileWriter(outputParquetFile)
	if err != nil {
		log.Println("Can't open file", err)
		return
	}

	pw, err := ParquetWriter.NewParquetWriter(fw, obj, 4)
	if err != nil {
		log.Println("Can't create parquet writer", err)
		return
	}

	pw.RowGroupSize = 128 * 1024 * 1024 //128M
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	numWritten := 0
	for i := 0; i < len(data); i++ {
		if i < 5 && debug {
			log.Printf("(%v, %T)\n", data[i], &data[i])
		}

		if err = pw.Write(data[i]); err != nil {
			log.Println("Write error", err)
		}
		numWritten++
	}
	if err = pw.WriteStop(); err != nil {
		log.Println("WriteStop error", err)
	}
	fw.Close()
	log.Println("Write finished, wrote " + strconv.Itoa(numWritten) + " data into parquet file.")
}

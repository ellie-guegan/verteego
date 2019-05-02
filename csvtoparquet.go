package main

import (
	"bufio"
	"encoding/csv"
	"io"
	"log"
	"os"
	"strconv"
)

type CountryEntry struct {
	Location  string  `parquet:"name=location, type=UTF8, encoding=PLAIN_DICTIONARY"`
	PopMale   float32 `parquet:"name=popmale, type=FLOAT"`
	PopFemale float32 `parquet:"name=popfemale, type=FLOAT"`
}

func createParquetFileFromCSV() {
	inputCSVFile := "./data/WPP2017_TotalPopulationBySex.2010.csv"
	outputParquetFile := "./data/WPP2017_TotalPopulationBySex.2010.parquet"

	// ------------------------------------------------------------------
	// load from CSV into structs
	// ------------------------------------------------------------------
	csvFile, _ := os.Open(inputCSVFile)
	reader := csv.NewReader(bufio.NewReader(csvFile))
	var entries []CountryEntry

	for {
		line, error := reader.Read()
		if error == io.EOF {
			break
		} else if error != nil {
			log.Fatal(error)
		}

		// parse float fields
		popMale, err := strconv.ParseFloat(line[2], 32)
		if err != nil {
			log.Fatal(err)
		}
		popFemale, err := strconv.ParseFloat(line[3], 32)
		if err != nil {
			log.Fatal(err)
		}

		entries = append(entries, CountryEntry{
			Location:  line[0],
			PopMale:   float32(popMale),
			PopFemale: float32(popFemale),
		})
	}
	log.Println("Loaded " + strconv.Itoa(len(entries)) + " countries from CSV.")

	// ------------------------------------------------------------------
	// save into parquet file
	// ------------------------------------------------------------------
	genericData := make([]interface{}, len(entries))
	for i, v := range entries {
		genericData[i] = v
	}

	genericSaveParquetFile(genericData, new(CountryEntry), outputParquetFile)
}

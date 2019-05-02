package main

import (
	"log"
)

// use for extra debug printouts
const debug = false

// number of countries to print top stats for
const topX = 10

func main() {
	// Enable line numbers in logging
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	log.Println("------- write CSV data into parquet file ----------------------")
	createParquetFileFromCSV()

	log.Println("------- load parquet file ----------------------")
	countries := loadParquetWorldData()

	log.Println("------- calculate stats ----------------------")
	countryStats := calculateStats(countries)

	log.Println("------- print some fun stats ----------------------")
	printFunStats(countryStats, topX)

	log.Println("------- write stats to parquet file ----------------------")
	outputParquetFile := "./data/WPP2017_TotalPopulationBySex.2010.statistics.parquet"
	genericData := make([]interface{}, len(countryStats))
	for i, v := range countryStats {
		genericData[i] = v
	}
	genericSaveParquetFile(genericData, new(CountryStat), outputParquetFile)
}

package main

import (
	"fmt"
	"log"
	"sort"
	"strconv"

	"github.com/xitongsys/parquet-go/ParquetFile"
	"github.com/xitongsys/parquet-go/ParquetReader"
)

func loadParquetWorldData() []CountryEntry {
	inputParquetFile := "./data/WPP2017_TotalPopulationBySex.2010.parquet"

	// read parquet file
	fr, err := ParquetFile.NewLocalFileReader(inputParquetFile)
	if err != nil {
		log.Println("Can't open file")
		return nil
	}

	pr, err := ParquetReader.NewParquetReader(fr, new(CountryEntry), 4)
	if err != nil {
		log.Println("Can't create parquet reader", err)
		return nil
	}

	numEntries := int(pr.GetNumRows())
	log.Println("Loaded " + strconv.Itoa(numEntries) + " entries.")
	countries := make([]CountryEntry, numEntries)
	err = pr.Read(&countries)
	if err != nil {
		log.Println("Read error", err)
	}

	pr.ReadStop()
	fr.Close()

	return countries
}

type CountryStat struct {
	Location          string  `parquet:"name=location, type=UTF8, encoding=PLAIN_DICTIONARY"`
	PopMale           float32 `parquet:"name=popmale, type=FLOAT"`
	PopFemale         float32 `parquet:"name=popfemale, type=FLOAT"`
	TotalPop          float32 `parquet:"name=totalpop, type=FLOAT"`
	FemaleToMaleRatio float32 `parquet:"name=femaletomaleratio, type=FLOAT"`
}

func calculateStats(countries []CountryEntry) []CountryStat {
	// ------------------------------------------------------------------
	// calculate total population and male-to-female ratio
	// ------------------------------------------------------------------

	var stats []CountryStat
	for i := 0; i < len(countries); i++ {
		totalPopulation := countries[i].PopMale + countries[i].PopFemale
		femaletomaleratio := countries[i].PopFemale / totalPopulation
		stats = append(stats, CountryStat{
			Location:          countries[i].Location,
			PopMale:           countries[i].PopMale,
			PopFemale:         countries[i].PopFemale,
			TotalPop:          totalPopulation,
			FemaleToMaleRatio: femaletomaleratio,
		})
	}

	return stats
}

func printFunStats(countryStats []CountryStat, topX int) {
	// ------------------------------------------------------------------
	// some of these stats can be checked online, for instance at
	// http://statisticstimes.com/demographics/countries-by-sex-ratio.php
	// ------------------------------------------------------------------

	topXStr := strconv.Itoa(topX)

	// Look at top and bottom countries by population
	log.Println(" ")
	log.Println("------- Top " + topXStr + " countries by population ----------------------")
	sort.Slice(countryStats, func(i, j int) bool {
		return countryStats[i].TotalPop > countryStats[j].TotalPop
	})
	for i := 0; i < topX; i++ {
		log.Println(strconv.Itoa(i) + "  ---  " + fmt.Sprintf("%v", countryStats[i]))
	}

	log.Println(" ")
	log.Println("------- Bottom " + topXStr + " countries by population ----------------------")
	sort.Slice(countryStats, func(i, j int) bool {
		return countryStats[i].TotalPop < countryStats[j].TotalPop
	})
	for i := 0; i < topX; i++ {
		log.Println(strconv.Itoa(len(countryStats)-i) + "  ---  " + fmt.Sprintf("%v", countryStats[i]))
	}

	// Look at top and bottom countries by gender ratio
	log.Println(" ")
	log.Println("------- Top " + topXStr + " countries in female to male ratio ----------------------")
	sort.Slice(countryStats, func(i, j int) bool {
		return countryStats[i].FemaleToMaleRatio > countryStats[j].FemaleToMaleRatio
	})
	for i := 0; i < topX; i++ {
		log.Println(strconv.Itoa(i) + "  ---  " + fmt.Sprintf("%v", countryStats[i]))
	}
	log.Println(" ")
	log.Println("------- Bottom " + topXStr + " countries in female to male ratio ----------------------")
	sort.Slice(countryStats, func(i, j int) bool {
		return countryStats[i].FemaleToMaleRatio < countryStats[j].FemaleToMaleRatio
	})
	for i := 0; i < topX; i++ {
		log.Println(strconv.Itoa(len(countryStats)-i) + "  ---  " + fmt.Sprintf("%v", countryStats[i]))
	}
}

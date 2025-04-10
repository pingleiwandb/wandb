package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	// Maximum size for a single random read (4MB)
	maxRandomSize = 4 * 1024 * 1024
)

type GenerationResult struct {
	SizeGB     int
	OutputFile string
	Duration   time.Duration
	Error      error
}

func generateLargeFile(sizeGB int, outputFile string, chunkSizeMB int) error {
	startTime := time.Now()

	// Convert GB to bytes
	totalSize := int64(sizeGB) * 1024 * 1024 * 1024

	// Convert chunk size from MB to bytes
	chunkSize := int64(chunkSizeMB) * 1024 * 1024

	// Calculate number of chunks
	numChunks := totalSize / chunkSize

	// Create output directory if it doesn't exist
	outputDir := filepath.Dir(outputFile)
	if outputDir != "" {
		if err := os.MkdirAll(outputDir, 0755); err != nil {
			return fmt.Errorf("failed to create output directory: %v", err)
		}
	}

	// Create the output file
	file, err := os.Create(outputFile)
	if err != nil {
		return fmt.Errorf("failed to create output file: %v", err)
	}
	defer file.Close()

	// Buffer for random data
	buffer := make([]byte, maxRandomSize)
	// Initialize random source with current time
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	for chunkNum := int64(0); chunkNum < numChunks; chunkNum++ {
		// Write chunk number (4 bytes)
		if err := binary.Write(file, binary.BigEndian, uint32(chunkNum)); err != nil {
			return fmt.Errorf("failed to write chunk number: %v", err)
		}

		// Generate random content for the rest of the chunk
		remainingSize := chunkSize - 4
		for remainingSize > 0 {
			currentSize := min(remainingSize, maxRandomSize)
			// Fill buffer with random bytes
			for i := 0; i < int(currentSize); i++ {
				buffer[i] = byte(r.Intn(256))
			}
			if _, err := file.Write(buffer[:currentSize]); err != nil {
				return fmt.Errorf("failed to write random data: %v", err)
			}
			remainingSize -= currentSize
		}

		// Print progress every 100 chunks
		if chunkNum%100 == 0 {
			progress := float64(chunkNum) / float64(numChunks) * 100
			fmt.Printf("Generating file %s %d/%d = %.2f%% done\n", outputFile, chunkNum, numChunks, progress)
		}
	}

	endTime := time.Now()
	totalTime := endTime.Sub(startTime)
	fmt.Printf("Generated file of size %dGB in %.2f seconds\n", sizeGB, totalTime.Seconds())
	fmt.Printf("Average write speed: %.2f GB/s\n", float64(sizeGB)/totalTime.Seconds())

	return nil
}

func generateLargeFiles(sizesGB []int, outputPrefix string, chunkSizeMB int) []GenerationResult {
	var wg sync.WaitGroup
	resultChan := make(chan GenerationResult, len(sizesGB))

	for _, sizeGB := range sizesGB {
		wg.Add(1)
		go func(sizeGB int, outputFile string) {
			defer wg.Done()
			startTime := time.Now()
			err := generateLargeFile(sizeGB, outputFile, chunkSizeMB)
			resultChan <- GenerationResult{
				SizeGB:     sizeGB,
				OutputFile: outputFile,
				Duration:   time.Since(startTime),
				Error:      err,
			}
		}(sizeGB, fmt.Sprintf("%s%dgb.bin", outputPrefix, sizeGB))
	}

	// Wait for all goroutines to complete
	wg.Wait()
	close(resultChan)

	// Collect results
	var allResults []GenerationResult
	for result := range resultChan {
		allResults = append(allResults, result)
	}

	return allResults
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

/*
# Generate files from 1GB to 5GB in parallel
go run gen_large_file.go -start-size 1 -end-size 5

# Generate files from 2GB to 10GB with custom prefix and chunk size
go run gen_large_file.go -start-size 2 -end-size 10 -output-prefix "upload/" -chunk-size 32
*/
func main() {
	var (
		startSizeGB  int
		endSizeGB    int
		outputPrefix string
		chunkSizeMB  int
	)

	flag.IntVar(&startSizeGB, "start-size", 1, "Starting size of files in GB")
	flag.IntVar(&endSizeGB, "end-size", 1, "Ending size of files in GB")
	flag.StringVar(&outputPrefix, "output-prefix", "large_file_", "Prefix for output files")
	flag.IntVar(&chunkSizeMB, "chunk-size", 16, "Size of each chunk in MB")
	flag.Parse()

	if startSizeGB > endSizeGB {
		fmt.Fprintf(os.Stderr, "Error: start-size must be less than or equal to end-size\n")
		os.Exit(1)
	}

	// Create array of sizes
	var sizesGB []int
	for size := startSizeGB; size <= endSizeGB; size++ {
		sizesGB = append(sizesGB, size)
	}

	// Generate files in parallel
	results := generateLargeFiles(sizesGB, outputPrefix, chunkSizeMB)

	// Print summary
	fmt.Println("\nGeneration Summary:")
	fmt.Println("------------------")
	for _, result := range results {
		if result.Error != nil {
			fmt.Printf("Failed to generate %dGB file: %v\n", result.SizeGB, result.Error)
		} else {
			fmt.Printf("Generated %dGB file in %.2f seconds (%.2f GB/s)\n",
				result.SizeGB,
				result.Duration.Seconds(),
				float64(result.SizeGB)/result.Duration.Seconds())
		}
	}
}

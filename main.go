package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"
)

func main() {
	// Verify and parse arguments
	op := flag.String("op", "sum", "Operation to be executed")
	column := flag.Int("col", 1, "CSV column on which to execute operation")
	flag.Parse()
	if err := run(flag.Args(), *op, *column, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(filenames []string, op string, column int, out io.Writer) error {
	var opFunc statsFunc
	if len(filenames) == 0 {
		return ErrNoFiles
	}
	if column < 1 {
		return fmt.Errorf("%w: %d", ErrInvalidColumn, column)
	}

	switch op {
	case "sum":
		opFunc = sum
	case "avg":
		opFunc = avg
	case "min":
		opFunc = min
	case "max":
		opFunc = max
	default:
		return fmt.Errorf("%w: %s", ErrInvalidOperation, op)
	}

	consolidate := make([]float64, 0)
	resCh := make(chan []float64)
	errCh := make(chan error)
	doneCh := make(chan struct{})
	filesCh := make(chan string)
	wg := sync.WaitGroup{}

	go func() {
		defer close(filesCh)
		for _, fname := range filenames {
			filesCh <- fname
		}
	}()

	for i := 0; i < runtime.NumCPU(); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for fname := range filesCh {
				f, err := os.Open(fname)
				if err != nil {
					errCh <- fmt.Errorf("cannot open file: %w", err)
					return
				}

				data, err := csv2float(f, column)
				if err != nil {
					errCh <- err
				}

				if err := f.Close(); err != nil {
					errCh <- err
				}

				resCh <- data
			}
		}()

	}

	go func() {
		wg.Wait()
		close(doneCh)
	}()
	// TODO try to improve the performance of min and max
	// by running the functions in multiple gorutines
	// or by trying to run the functions for each file that is read
	for {
		select {
		case err := <-errCh:
			return err
		case data := <-resCh:
			if op == "min" || op == "max" {
				// todo see if using goroutines here can improve the performance
				// spawn like 4 goroutines and divide the date between them to be processed
				minWg := sync.WaitGroup{}
				theData := make(chan float64)
				quarterLength := len(data) / 4
				start := 0
				endQuarter := quarterLength
				incrementQuarter := 2
				for i := 0; i < 4; i++ {
					minWg.Add(1)
					go func(start, end int) {
						defer minWg.Done()
						theData <- opFunc(data[start:end])
					}(start, endQuarter)
					start = endQuarter
					endQuarter = quarterLength * incrementQuarter
					incrementQuarter++
				}
				go func() {
					minWg.Wait()
					close(theData)
				}()

				for quarterData := range theData {
					consolidate = append(consolidate, quarterData)
				}

			} else {
				consolidate = append(consolidate, data...)
			}
		case <-doneCh:
			_, err := fmt.Fprintln(out, opFunc(consolidate))
			return err
		}
	}
}

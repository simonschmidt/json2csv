package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"strings"
	"unicode/utf8"
	"runtime"
)

type LineReader interface {
	ReadBytes(delim byte) (line []byte, err error)
}

var (
	inputFile   = flag.String("i", "", "/path/to/input.json (optional; default is stdin)")
	outputFile  = flag.String("o", "", "/path/to/output.json (optional; default is stdout)")
	outputDelim = flag.String("d", ",", "delimiter used for output values")
	nThreads    = flag.Int("t", -1, "Max number of threads (optional; deafult is number of cpus)")
	verbose     = flag.Bool("v", false, "verbose output (to stderr)")
	showVersion = flag.Bool("version", false, "print version string")
	printHeader = flag.Bool("p", false, "prints header to output")
	keys        = StringArray{}
)

func init() {
	flag.Var(&keys, "k", "fields to output")
}

func main() {
	flag.Parse()

	// GOMAXPROCS(0) doesn't do anything so environment variable etc
	// wont be overriden if -t 0 is given
	if *nThreads < 0 {
		*nThreads = runtime.NumCPU()
	}
	runtime.GOMAXPROCS(*nThreads)

	if *showVersion {
		fmt.Printf("json2csv v1.1-parallel\n")
		return
	}

	var reader *bufio.Reader
	var writer *csv.Writer
	if *inputFile != "" {
		file, err := os.OpenFile(*inputFile, os.O_RDONLY, 0600)
		if err != nil {
			log.Printf("Error %s opening input file %v", err, *inputFile)
			os.Exit(1)
		}
		reader = bufio.NewReader(file)
	} else {
		reader = bufio.NewReader(os.Stdin)
	}

	if *outputFile != "" {
		file, err := os.OpenFile(*outputFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
		if err != nil {
			log.Printf("Error %s opening output file %v", err, *outputFile)
			os.Exit(1)
		}
		writer = csv.NewWriter(file)
	} else {
		writer = csv.NewWriter(os.Stdout)
	}

	delim, _ := utf8.DecodeRuneInString(*outputDelim)
	writer.Comma = delim

	json2csv(reader, writer, keys, *printHeader)
}

func get_value(data map[string]interface{}, keyparts []string) string {
	if len(keyparts) > 1 {
		subdata, _ := data[keyparts[0]].(map[string]interface{})
		return get_value(subdata, keyparts[1:])
	} else if v, ok := data[keyparts[0]]; ok {
		switch v.(type) {
		case nil:
			return ""
		case float64:
			f, _ := v.(float64)
			if math.Mod(f, 1.0) == 0.0 {
				return fmt.Sprintf("%d", int(f))
			} else {
				return fmt.Sprintf("%f", f)
			}
		default:
			return fmt.Sprintf("%+v", v)
		}
	}

	return ""
}

func futureUnmarshal(line []byte, line_count int) chan map[string]interface{}{
	c := make (chan map[string]interface{})

	go func(){
		defer close(c)

		var data map[string]interface{}
		err := json.Unmarshal(line, &data)
		if err != nil {
			log.Printf("ERROR Decoding JSON at line %d: %s\n%s", line_count, err, line)
			return
		}
		c <- data
	}()

	return c
}

func jsonIterator(r LineReader, keys []string) <- chan chan map[string]interface{} {
	ch := make(chan chan map[string]interface{}, 1000)

	go func(){
		defer close(ch)

		var err error
		var line []byte
		line_count := 0

		for{
			if err == io.EOF{
				break
			}

			line, err = r.ReadBytes('\n')
			line_count++
			if err != nil && err != io.EOF {
				log.Printf("Input Error: %s", err)
				break
			}
			if len(line) == 0 {
				continue
			}

			ch <- (futureUnmarshal(line, line_count))
		}
	}()

	return ch
}

func json2csv(r LineReader, w *csv.Writer, keys []string, printHeader bool) {
	var expanded_keys [][]string
	for _, key := range keys {
		expanded_keys = append(expanded_keys, strings.Split(key, "."))
	}
	var n_keys = len(expanded_keys)
	var record = make([]string, n_keys)

	if printHeader {
		w.Write(keys)
		w.Flush()
	}

	for chandata := range jsonIterator(r, keys) {
		data := <-chandata

		// JSON error, skip
		if data == nil {
			continue
		}

		for i, expanded_key := range expanded_keys {
			record[i] = get_value(data, expanded_key)
		}

		w.Write(record)
		w.Flush()
	}
}

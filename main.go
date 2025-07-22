package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/kill-2/badmerger/lib"

	_ "github.com/kill-2/badmerger/storage/badgerdb"
	_ "github.com/kill-2/badmerger/storage/lotus"
)

func main() {
	dbW, err := lib.Open(storageOpts()...)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fail to open db %v", err)
		return
	}

	if os.Getenv("BADMERGER_DEL") != "" {
		defer dbW.Destroy()
	}

	defer dbW.Close()

	ch := make(chan map[string]any, 100)
	go readStdin(ch)
	if err := dbW.Recv(ch); err != nil {
		fmt.Fprintf(os.Stderr, "fail to Recv: %v\n", err)
		return
	}

	itW := dbW.NewIterator()
	for i := 1; i < len(os.Args); i++ {
		if os.Args[i] == "-k" && i+1 < len(os.Args) {
			parts := strings.Split(os.Args[i+1], ":")
			if len(parts) == 2 {
				itW = itW.WithPartialKey(parts[0])
			}
			i++
		} else if os.Args[i] == "-a" && i+1 < len(os.Args) {
			parts := strings.Split(os.Args[i+1], ":")
			operation := strings.Replace(strings.Replace(parts[1], "}", ")", -1), "{", "(", -1)
			if len(parts) == 2 {
				itW = itW.WithAgg(parts[0], operation)
			}
			i++
		}
	}

	itW.Iter(func(res map[string]any) error {
		b, err := json.Marshal(res)
		if err != nil {
			return fmt.Errorf("fail to marshal result into json: %v", err)
		}
		fmt.Println(string(b))
		return nil
	})
}

func readStdin(ch chan map[string]any) {
	defer close(ch)

	var i int32
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		var record map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			fmt.Fprintf(os.Stderr, "fail to parse as JSON: %v\n", err)
			return
		}
		record["_i_"] = i
		ch <- record
		i += 1
	}
}

func storageOpts() []lib.Opt {
	opts := []lib.Opt{lib.WithStorage("badger")}

	for i := 1; i < len(os.Args); i++ {
		if os.Args[i] == "-k" && i+1 < len(os.Args) {
			parts := strings.Split(os.Args[i+1], ":")
			if len(parts) == 2 {
				opts = append(opts, lib.WithKey(parts[0], parts[1]))
			}
			i++
		} else if os.Args[i] == "-v" && i+1 < len(os.Args) {
			parts := strings.Split(os.Args[i+1], ":")
			if len(parts) == 2 {
				opts = append(opts, lib.WithValue(parts[0], parts[1]))
			}
			i++
		} else if os.Args[i] == "-s" && i+1 < len(os.Args) {
			opts = append(opts, lib.WithStorage(os.Args[i+1]))
			i++
		} else if os.Args[i] == "-d" && i+1 < len(os.Args) {
			opts = append(opts, lib.WithDir(os.Args[i+1]))
			i++
		}
	}
	opts = append(opts, lib.WithKey("_i_", "int32"))

	return opts
}

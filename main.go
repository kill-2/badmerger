package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/kill-2/badmerger/lib"
)

func main() {
	var opts []lib.Opt
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
		}
	}
	opts = append(opts, lib.WithKey("_i_", "int32"))

	dbW, err := lib.New(os.Getenv("BADMERGER_TMP"), opts...)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fail to open db %v", err)
		return
	}

	defer dbW.Destroy()

	dbW.Add(func(txn *lib.TxnWrapper) error {
		scanner := bufio.NewScanner(os.Stdin)
		var i int32
		for scanner.Scan() {
			var record map[string]any
			if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
				return fmt.Errorf("fail to parse as JSON: %v", err)
			}
			record["_i_"] = i
			if err := txn.Add(record); err != nil {
				return err
			}
			i += 1
		}
		if err := scanner.Err(); err != nil {
			return fmt.Errorf("error in scanner: %v", err)
		}
		return nil
	})

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

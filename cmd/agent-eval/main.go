package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"

	"mirrorcore/internal/agenteval"
)

func main() {
	inputPath := flag.String("input", "", "JSONL evaluation dataset")
	outputPath := flag.String("output", "", "report path (default: stdout)")
	flag.Parse()

	if *inputPath == "" {
		fatal("-input is required")
	}
	if flag.NArg() != 0 {
		fatal("unexpected positional arguments")
	}

	input, err := os.Open(*inputPath)
	if err != nil {
		fatal("open input: %v", err)
	}
	defer input.Close()

	samples, err := agenteval.LoadJSONL(input)
	if err != nil {
		fatal("load evaluation data: %v", err)
	}
	report, err := agenteval.Evaluate(samples)
	if err != nil {
		fatal("evaluate data: %v", err)
	}

	var output io.Writer = os.Stdout
	if *outputPath != "" {
		file, err := os.Create(*outputPath)
		if err != nil {
			fatal("create report: %v", err)
		}
		defer file.Close()
		output = file
	}

	encoder := json.NewEncoder(output)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(report); err != nil {
		fatal("write report: %v", err)
	}
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "agent-eval: "+format+"\n", args...)
	os.Exit(2)
}

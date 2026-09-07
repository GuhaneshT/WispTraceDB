package main

import (
	"os"
	"strings"
)

func main() {
	content, err := os.ReadFile("cmd/cmd_test.go")
	if err != nil {
		panic(err)
	}

	// We'll replace all instances of wt.InsertSpan with a custom wrapper in tests.
	// But it's easier to just insert wt.WaitForIngest() right before any GetSpan, GetTrace, or RangeQuery.
	
	s := string(content)
	
	s = strings.ReplaceAll(s, "wt.GetSpan(", "wt.WaitForIngest()\n\twt.GetSpan(")
	s = strings.ReplaceAll(s, "wt.GetTrace(", "wt.WaitForIngest()\n\twt.GetTrace(")
	s = strings.ReplaceAll(s, "wt.RangeQuery(", "wt.WaitForIngest()\n\twt.RangeQuery(")
	
	// Also for live segments check where we expect flushes
	s = strings.ReplaceAll(s, "wt.LiveSegments(", "wt.WaitForIngest()\n\twt.LiveSegments(")

	// And wait before closing if not already
	// s = strings.ReplaceAll(s, "wt.Close()", "wt.WaitForIngest()\n\twt.Close()")

	os.WriteFile("cmd/cmd_test.go", []byte(s), 0644)
}

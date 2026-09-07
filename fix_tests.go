package main

import (
	"os"
	"strings"
)

func main() {
	b, err := os.ReadFile("cmd/cmd_test.go")
	if err != nil {
		panic(err)
	}

	s := string(b)
	
	// Fix the syntax error by changing it to method chaining
	s = strings.ReplaceAll(s, "wt.WaitForIngest()\n\twt.GetSpan(", "wt.WaitForIngest().GetSpan(")
	s = strings.ReplaceAll(s, "wt.WaitForIngest()\n\twt.GetTrace(", "wt.WaitForIngest().GetTrace(")
	s = strings.ReplaceAll(s, "wt.WaitForIngest()\n\twt.RangeQuery(", "wt.WaitForIngest().RangeQuery(")
	s = strings.ReplaceAll(s, "wt.WaitForIngest()\n\twt.LiveSegments(", "wt.WaitForIngest().LiveSegments(")

	os.WriteFile("cmd/cmd_test.go", []byte(s), 0644)
}

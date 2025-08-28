package main

import (
	"fmt"
	"strings"
)

func main() {
	lsm := newLSMTree()
	val, err := lsm.Get(strings.Repeat("d", kSize))
	fmt.Println(val, err)

	//lsm.Set(strings.Repeat("c", kSize), strings.Repeat("b", vSize))
	//lsm.Set(strings.Repeat("d", kSize), strings.Repeat("b", vSize))
	//lsm.Flush()
}

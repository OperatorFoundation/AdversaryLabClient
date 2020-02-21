package protocol

import (
	"fmt"
	"os"
)

func CheckError(err error) {
	if err != nil {
		fmt.Println("-> Error: ", err.Error())
		os.Exit(0)
	}
}

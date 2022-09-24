package pkg

import (
	"io/ioutil"
	"log"
	"os"
)

var Debug *log.Logger

// Out logs to stdout
var Out *log.Logger

func init() {
	Debug = log.New(ioutil.Discard, "", log.Ltime|log.Lshortfile)
	Out = log.New(os.Stdout, "", log.Ltime|log.Lshortfile)
}

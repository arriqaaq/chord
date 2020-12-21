# Chord
[WIP]
Implementation of Chord paper

# Paper
https://pdos.csail.mit.edu/papers/ton:chord/paper-ton.pdf

## Example Usage

```go
package main

import (
	"github.com/zebra-uestc/chord"
	"github.com/zebra-uestc/chord/mdoels"
	"log"
	"os"
	"os/signal"
	"time"
)

func createNode(id string, addr string, joinNode *models.Node) (*chord.Node, error) {

	cnf := chord.DefaultConfig()
	cnf.Id = id
	cnf.Addr = addr
	cnf.Timeout = 10 * time.Millisecond
	cnf.MaxIdle = 100 * time.Millisecond

	n, err := chord.NewNode(cnf, joinNode)
	return n, err
}


func main() {

	joinNode := chord.NewInode("1", "0.0.0.0:8001")

	h, err := createNode("8", "0.0.0.0:8003", joinNode)
	if err != nil {
		log.Fatalln(err)
		return
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
	h.Stop()
}
```


# References
This implementation helped me a lot in designing the code base
https://github.com/r-medina/gmaj

# TODO
- Add more test cases
- Add stats/prometheus stats
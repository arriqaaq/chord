package dhtnode

import (
	"github.com/zebra-uestc/chord"
	"github.com/zebra-uestc/chord/models"
)

type DhtNode struct {
	*chord.Node
	//*dhtnode.dht_node
}

func NewDhtNode(cnf *chord.Config, joinNode *models.Node) (*DhtNode, error) {
	node, error := chord.NewNode(cnf, joinNode)

	return &DhtNode{Node: node}, error
}

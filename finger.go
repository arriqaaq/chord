package chord

import (
	"bytes"
	"fmt"
	"math/big"

	cm "github.com/zebra-uestc/chord/models/chord"
)

type fingerTable []*fingerEntry

//m在默认cnf中为160
//初始化时，每个fingeEntry对应的node都为输入的参数node
func newFingerTable(node *cm.Node, m int) fingerTable {
	//fingerTable一共有m个fingerEntry
	ft := make([]*fingerEntry, m)
	for i := range ft {
		//每个i对应的offset不同，构造不同的fingerEntry
		ft[i] = newFingerEntry(fingerID(node.Id, i, m), node)
	}

	return ft
}

// fingerEntry represents a single finger table entry
type fingerEntry struct {
	Id         []byte   // ID hash of (n + 2^i) mod (2^m)
	RemoteNode *cm.Node // RemoteNode that Start points to
}

// newFingerEntry returns an allocated new finger entry with the attributes set
func newFingerEntry(id []byte, node *cm.Node) *fingerEntry {
	return &fingerEntry{
		Id:         id,
		RemoteNode: node,
	}
}

// Computes the offset by (n + 2^i) mod (2^m)
//fingerID为起始id转换为byte数组后的id再加上offset
func fingerID(n []byte, i int, m int) []byte {

	// Convert the ID to a bigint
	idInt := (&big.Int{}).SetBytes(n)

	// Get the offset
	two := big.NewInt(2)
	offset := big.Int{}
	offset.Exp(two, big.NewInt(int64(i)), nil)

	// Sum
	sum := big.Int{}
	sum.Add(idInt, &offset)

	// Get the ceiling
	ceil := big.Int{}
	ceil.Exp(two, big.NewInt(int64(m)), nil)

	// Apply the mod
	idInt.Mod(&sum, &ceil)

	// Add together
	return idInt.Bytes()
}

func (ft fingerTable) String() string {
	var buf bytes.Buffer

	buf.WriteString(fmt.Sprintf("FingerTable:"))

	for _, val := range ft {
		buf.WriteString(fmt.Sprintf(
			"\n\t{start:%v\tnodeID:%v %v}",
			IDToString(val.Id),
			IDToString(val.RemoteNode.Id),
			val.RemoteNode.Addr,
		))
	}

	return buf.String()
}

// FingerTableString takes a node and converts it's finger table into a string.
func (node *Node) FingerTableString() string {
	node.ftMtx.RLock()
	defer node.ftMtx.RUnlock()

	return node.fingerTable.String()
}

// called periodically. refreshes finger table entries.
// next stores the index of the next finger to fix.
//实时监听并更新fingerTable，next存储的是下一个需要更新的fingerEntry的index
func (n *Node) fixFinger(next int) int {
	// fmt.Println("fixFinger()... ", next)
	// nextHash := fingerID(n.Id, next, n.cnf.HashSize)
	nextHash := fingerID(n.Id, next, n.cnf.HashSize)
	//记录当前的后继节点
	succ, err := n.findSuccessor(nextHash)
	// nextNum := (next + 1) % n.cnf.HashSize
	nextNum := (next + 1) % n.cnf.HashSize
	if err != nil || succ == nil {
		fmt.Println("error: ", err, succ)
		fmt.Printf("finger lookup failed %x %x \n", n.Id, nextHash)
		// TODO: Check how to handle retry, passing ahead for now
		return nextNum
	}
	finger := newFingerEntry(nextHash, succ)
	n.ftMtx.Lock()
	//更新finger表
	n.fingerTable[next] = finger

	// aInt := (&big.Int{}).SetBytes(nextHash)
	// bInt := (&big.Int{}).SetBytes(finger.Node.Id)
	// fmt.Printf("finger entry %d, %d,%d\n", next, aInt, bInt)
	n.ftMtx.Unlock()
	//
	//fmt.Println(n.FingerTableString())

	return nextNum
}

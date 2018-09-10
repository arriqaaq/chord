package chord

import (
	"fmt"
	"github.com/arriqaaq/chord/internal"
	"math/big"
)

type fingerTable []*fingerEntry

func newFingerTable(node *internal.Node, m int) fingerTable {
	ft := make([]*fingerEntry, m)
	for i := range ft {
		ft[i] = newFingerEntry(fingerID(node.Id, i, m), node)
	}

	return ft
}

// fingerEntry represents a single finger table entry
type fingerEntry struct {
	Id   []byte         // ID hash of (n + 2^i) mod (2^m)
	Node *internal.Node // RemoteNode that Start points to
}

// newFingerEntry returns an allocated new finger entry with the attributes set
func newFingerEntry(id []byte, node *internal.Node) *fingerEntry {
	return &fingerEntry{
		Id:   id,
		Node: node,
	}
}

// Computes the offset by (n + 2^i) mod (2^m)
func fingerID(n []byte, i int, m int) []byte {

	// Convert the ID to a bigint
	idInt := big.Int{}
	idInt.SetBytes(n)

	// Get the offset
	two := big.NewInt(2)
	offset := big.Int{}
	offset.Exp(two, big.NewInt(int64(i)), nil)

	// Sum
	sum := big.Int{}
	sum.Add(&idInt, &offset)

	// Get the ceiling
	ceil := big.Int{}
	ceil.Exp(two, big.NewInt(int64(m)), nil)

	// Apply the mod
	idInt.Mod(&sum, &ceil)

	// Add together
	return padID(idInt.Bytes(), m)
}

// called periodically. refreshes finger table entries.
// next stores the index of the next finger to fix.
func (n *Node) fixFinger(next int) int {
	nextHash := fingerID(n.Id, next, n.cnf.HashSize)
	succ, err := n.findSuccessor(nextHash)
	nextNum := (next + 1) % n.cnf.HashSize
	if err != nil || succ == nil {
		fmt.Println("finger lookup failed", n.Id, nextHash)
		// TODO: this will keep retrying, check what to do
		// return next
		return nextNum
	}

	finger := newFingerEntry(nextHash, succ)
	n.ftMtx.Lock()
	n.fingerTable[next] = finger
	// fmt.Printf("finger entry %x,%x,%x\n", n.Id, nextHash, succ.Id)
	n.ftMtx.Unlock()

	return nextNum
}

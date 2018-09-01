package chord

import (
	"github.com/arriqaaq/chord/internal"
	"math/big"
)

type fingerTable []*fingerEntry

func newFingerTable(node *internal.Node) fingerTable {
	ft := make([]*fingerEntry, 8)
	for i := range ft {
		ft[i] = newFingerEntry(fingerMath(node.Id, i, 8), node)
	}

	return ft
}

// fingerEntry represents a single finger table entry
type fingerEntry struct {
	ID   []byte         // ID hash of (n + 2^i) mod (2^m)
	Node *internal.Node // RemoteNode that Start points to
}

// newFingerEntry returns an allocated new finger entry with the attributes set
func newFingerEntry(id []byte, node *internal.Node) *fingerEntry {
	return &fingerEntry{
		ID:   id,
		Node: node,
	}
}

// fingerMath does the `(n + 2^i) mod (2^m)` operation
// needed to update finger table entries.
func fingerMath(n []byte, i int, m int) []byte {
	iInt := big.NewInt(2)
	iInt.Exp(iInt, big.NewInt(int64(i)), big.NewInt(100))
	mInt := big.NewInt(2)
	mInt.Exp(mInt, big.NewInt(int64(m)), big.NewInt(100))

	res := &big.Int{} // res will pretty much be an accumulator
	res.SetBytes(n).Add(res, iInt).Mod(res, mInt)

	// return padID(res.Bytes())
	return res.Bytes()
}

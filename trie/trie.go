package trie

import (
	"fmt"
	"io"
	"io/ioutil"
	"sort"

	trie_pb "github.com/QubitProducts/triesbien/trie/proto"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/glog"
	"github.com/pkg/errors"
)

type Trie struct {
	root *trie_pb.Node
}

func NewTrie() *Trie {
	return &Trie{
		root: &trie_pb.Node{
			Children: []*trie_pb.Node{},
		},
	}
}

func (t *Trie) Unmarshal(r io.Reader) error {
	bytes, err := ioutil.ReadAll(r)
	if err != nil {
		return errors.Wrap(err, "could not read")
	}
	return proto.Unmarshal(bytes, t.root)
}

func (t *Trie) Marshal(buf io.Writer) error {
	bytes, err := proto.Marshal(t.root)
	if err != nil {
		return errors.Wrap(err, "could not unmarshal")
	}
	_, err = buf.Write(bytes)
	return errors.Wrap(err, "could not write")
}

func (t *Trie) String() string {
	childChars := make([]rune, len(t.root.Children))
	for i, c := range t.root.Children {
		childChars[i] = rune(c.Char)
	}
	return fmt.Sprintf("children: %s", string(childChars))
}

func (t *Trie) Lookup(value []rune) []uint32 {
	n := t.root
	for i := 0; i < len(value); i++ {
		child := findChild(n, value[i])
		if child == nil {
			return nil
		}
		n = child
	}
	return n.TopEntries
}

func (t *Trie) lookupOrInsert(value []rune) *trie_pb.Node {
	n := t.root
	for i := 0; i < len(value); i++ {
		child := findChild(n, value[i])
		if child != nil {
			n = child
		} else {
			child = &trie_pb.Node{
				Char:       uint32(value[i]),
				TopEntries: []uint32{},
				Children:   []*trie_pb.Node{},
			}
			n.Children = append(n.Children, child)
			n = child
		}
	}
	return n
}

func (t *Trie) Insert(value []rune, topEntries []uint32) {
	n := t.lookupOrInsert(value)
	n.TopEntries = topEntries
}

func (t *Trie) Append(value []rune, entry uint32) {
	n := t.lookupOrInsert(value)
	n.TopEntries = append(n.TopEntries, entry)
}

func (t *Trie) MergeUpwards(maxEntries int) {
	t.iterateLRN(t.root, func(e *trie_pb.Node) {
		if len(e.TopEntries) > maxEntries {
			e.TopEntries = e.TopEntries[0:maxEntries]
		}
		if len(e.Children) == 0 {
			return
		}
		maxChildEntries := 0
		glog.V(4).Infof("%v children", len(e.Children))
		for _, child := range e.Children {
			if len(child.TopEntries) > maxChildEntries {
				maxChildEntries = len(child.TopEntries)
			}
			glog.V(4).Infof("c: %v", child.TopEntries)
		}

	loop:
		// j is our position up the list of topEntries for each child
		for j := 0; j < maxChildEntries; j++ {
			for _, c := range e.Children {
				if len(c.TopEntries) <= j {
					continue
				}
				if len(e.TopEntries) >= maxEntries {
					break loop
				}

				e.TopEntries = append(e.TopEntries, c.TopEntries[j])
			}
		}
		sort.Slice(e.TopEntries, func(i, j int) bool {
			return e.TopEntries[i] < e.TopEntries[j]
		})
		glog.V(2).Infof("res: %v", e.TopEntries)
	})
}

func (t *Trie) iterateLRN(n *trie_pb.Node, cb func(*trie_pb.Node)) {
	for _, child := range n.Children {
		t.iterateLRN(child, cb)
	}
	cb(n)
}

func findChild(node *trie_pb.Node, c rune) *trie_pb.Node {
	for _, child := range node.Children {
		if child.Char == uint32(c) {
			return child
		}
	}
	return nil
}

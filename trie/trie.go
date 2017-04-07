package trie

import (
	"encoding/gob"
	"fmt"
	"io"
	"sort"

	"github.com/golang/glog"
)

type Trie struct {
	root *trieEntry
}

func NewTrie() *Trie {
	return &Trie{
		root: &trieEntry{
			Children: []*trieEntry{},
		},
	}
}

func (t *Trie) UnmarshalGob(buf io.Reader) error {
	return gob.NewDecoder(buf).Decode(t.root)
}

func (t *Trie) MarshalGob(buf io.Writer) error {
	return gob.NewEncoder(buf).Encode(t.root)
}

func (t *Trie) String() string {
	childChars := make([]rune, len(t.root.Children))
	for i, c := range t.root.Children {
		childChars[i] = c.Char
	}
	return fmt.Sprintf("children: %s", string(childChars))
}

func (t *Trie) Lookup(value []rune) []uint32 {
	n := t.root
	for i := 0; i < len(value); i++ {
		child := n.findChild(value[i])
		if child == nil {
			return nil
		}
		n = child
	}
	return n.TopEntries
}

func (t *Trie) lookupOrInsert(value []rune) *trieEntry {
	n := t.root
	for i := 0; i < len(value); i++ {
		child := n.findChild(value[i])
		if child != nil {
			n = child
		} else {
			child = &trieEntry{
				Char:       value[i],
				TopEntries: []uint32{},
				Children:   []*trieEntry{},
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
	t.iterateLRN(t.root, func(e *trieEntry) {
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

func (t *Trie) iterateLRN(n *trieEntry, cb func(*trieEntry)) {
	for _, child := range n.Children {
		t.iterateLRN(child, cb)
	}
	cb(n)
}

type trieEntry struct {
	Char       rune
	TopEntries []uint32
	Children   []*trieEntry
}

func (e trieEntry) findChild(c rune) *trieEntry {
	for _, child := range e.Children {
		if child.Char == c {
			return child
		}
	}
	return nil
}

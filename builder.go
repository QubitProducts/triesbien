package triesbien

import (
	"context"
	"encoding/binary"

	"github.com/QubitProducts/triesbien/trie"
	"github.com/golang/glog"
	"github.com/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb"
)

func WriteLevelDB(ctx context.Context, dbPath string, productChan <-chan string) error {
	db, err := leveldb.OpenFile(dbPath, nil)
	if err != nil {
		return errors.Wrap(err, "could not open leveldb")
	}
	defer db.Close()

	var i uint32
	for ; ; i++ {
		var item string
		var ok bool
		select {
		case <-ctx.Done():
			return ctx.Err()
		case item, ok = <-productChan:
		}
		if !ok {
			break
		}

		err := db.Put(toBS(i), []byte(item), nil)
		if err != nil {
			return errors.Wrap(err, "could not write to leveldb")
		}
	}
	return nil
}

func BuildTrie(ctx context.Context, trie *trie.Trie, config Config, catalogueChan <-chan string) error {
	var i uint32
	for ; ; i++ {
		var item string
		var ok bool
		select {
		case <-ctx.Done():
			return ctx.Err()
		case item, ok = <-catalogueChan:
		}
		if !ok {
			break
		}

		parts := config.Parser(item)
		glog.V(2).Infof("item: %v", item)
		for i := range parts {
			if len(parts[i]) > config.MaxLexemeLength {
				glog.V(2).Infof("truncating %v", parts[i])
				parts[i] = parts[i][0:config.MaxLexemeLength]
			}
			glog.V(4).Infof("part - %v", parts[i])
		}

		for _, part := range parts {
			trie.Append([]rune(part), i)
		}
	}

	trie.MergeUpwards(config.MaxBucketLength)
	return nil
}

func toBS(ix uint32) []byte {
	ret := make([]byte, 4)
	binary.LittleEndian.PutUint32(ret, ix)
	return ret
}

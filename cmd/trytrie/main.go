package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"os"
	"regexp"
	"runtime"
	"runtime/pprof"
	"strings"
	"time"
	"unicode"

	"github.com/QubitProducts/triesbien"
	"github.com/QubitProducts/triesbien/catalogue"
	"github.com/QubitProducts/triesbien/trie"
	"github.com/golang/glog"
	"github.com/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb"
	"golang.org/x/sync/errgroup"
)

var (
	searchQuery     = "tank"
	trieWrite       = false
	triePath        = "./data/trie.gob"
	maxLexemeLength = 10
	maxBucketLength = 1024
	leveldbPath     = "./data/leveldb"
	leveldbWrite    = false
	cataloguePath   = ""
	catalogueColumn = 1
	cpuProfile      = ""
	memProfile      = ""
)

func init() {
	flag.StringVar(&searchQuery, "search.query", searchQuery, "the query to run")
	flag.IntVar(&maxLexemeLength, "search.lexeme-length", maxLexemeLength, "the maximum length of any lexeme")
	flag.IntVar(&maxBucketLength, "search.bucket-length", maxBucketLength, "the maximum length of any bucket")
	flag.StringVar(&leveldbPath, "leveldb.path", leveldbPath, "path to the leveldb database")
	flag.BoolVar(&leveldbWrite, "leveldb.write", leveldbWrite, "write the product index to leveldb")
	flag.BoolVar(&trieWrite, "trie.write", trieWrite, "write the trie to disk (load from disk if false)")
	flag.StringVar(&triePath, "trie.path", triePath, "path to read/write trie from")
	flag.StringVar(&cataloguePath, "catalogue.path", cataloguePath, "path to the CSV dump of the catalogue")
	flag.IntVar(&catalogueColumn, "catalogue.column", catalogueColumn, "column in the CSV catalogue to index")
	flag.StringVar(&cpuProfile, "profile.cpu", cpuProfile, "file to dump the cpu profile into")
	flag.StringVar(&memProfile, "profile.mem", memProfile, "file to dump the mem profile into")
}

func main() {
	flag.Set("logtostderr", "true")
	flag.Parse()

	if cpuProfile != "" {
		f, err := os.Create(cpuProfile)
		if err != nil {
			glog.Fatal("could not create CPU profile: ", err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			glog.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	ctx := context.Background()

	grp, ctx := errgroup.WithContext(ctx)
	if leveldbWrite {
		levelDBChan := make(chan string)
		grp.Go(func() error {
			err := catalogue.CSVLoader(ctx, cataloguePath, catalogueColumn, levelDBChan)
			return errors.Wrap(err, "could not read catalogue for leveldb")
		})
		grp.Go(func() error {
			err := triesbien.WriteLevelDB(ctx, leveldbPath, levelDBChan)
			return errors.Wrap(err, "could not write leveldb")
		})
	}

	t := trie.NewTrie()
	config := triesbien.Config{
		Parser:          parseProductTitle,
		MaxLexemeLength: maxLexemeLength,
		MaxBucketLength: maxBucketLength,
	}
	if trieWrite {
		trieChan := make(chan string)
		grp.Go(func() error {
			err := catalogue.CSVLoader(ctx, cataloguePath, catalogueColumn, trieChan)
			return errors.Wrap(err, "could not read catalogue for trie")
		})
		grp.Go(func() error {
			err := triesbien.BuildTrie(ctx, t, config, trieChan)
			return errors.Wrap(err, "could not write leveldb")
		})
	} else {
		trieFile, err := os.Open(triePath)
		if err != nil {
			glog.Errorf("could not open trie path to read: %v", err)
			os.Exit(1)
		}
		defer trieFile.Close()

		err = t.UnmarshalGob(trieFile)
		if err != nil {
			glog.Errorf("could not read trie: %v", err)
			os.Exit(1)
		}
	}

	if err := grp.Wait(); err != nil {
		glog.Errorf("%v", err)
		os.Exit(1)
	}

	if trieWrite {
		trieFile, err := os.Create(triePath)
		if err != nil {
			glog.Errorf("could not open trie path to write: %v", err)
			os.Exit(1)
		}
		defer trieFile.Close()

		err = t.MarshalGob(trieFile)
		if err != nil {
			glog.Errorf("could not write trie: %v", err)
			os.Exit(1)
		}
	}

	db, err := leveldb.OpenFile(leveldbPath, nil)
	if err != nil {
		glog.Errorf("could not open leveldb: %v", err)
		os.Exit(1)
	}
	defer db.Close()

	if memProfile != "" {
		f, err := os.Create(memProfile)
		if err != nil {
			glog.Fatal("could not create memory profile: ", err)
		}
		runtime.GC() // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			glog.Fatal("could not write memory profile: ", err)
		}
		f.Close()
	}

	started := time.Now()
	res, err := triesbien.Query(t, db, config, searchQuery)
	if err != nil {
		glog.Errorf("query failed: %v", err)
		os.Exit(1)
	}
	glog.Infof("result in %v", time.Since(started))

	for _, r := range res {
		fmt.Println(r)
	}
}

func splitOn(r rune) bool {
	return unicode.IsSpace(r) || unicode.IsSymbol(r) || unicode.IsPunct(r)
}

var (
	validPart = regexp.MustCompile(`^[a-z0-9]*([a-z]|[0-9]{2})[a-z0-9]*$`)
)

func parseProductTitle(title string) []string {
	title = strings.ToLower(title)

	lastSplit := 0

	parts := []string{}

	for i, r := range title {
		if splitOn(r) {
			part := title[lastSplit:i]
			if validPart.MatchString(part) {
				parts = append(parts, part)
			}

			lastSplit = i + 1
		}
	}
	if lastSplit != len(title) {
		part := title[lastSplit:len(title)]
		if validPart.MatchString(part) {
			parts = append(parts, part)
		}
	}
	return parts
}

func toBS(ix uint32) []byte {
	ret := make([]byte, 4)
	binary.LittleEndian.PutUint32(ret, ix)
	return ret
}

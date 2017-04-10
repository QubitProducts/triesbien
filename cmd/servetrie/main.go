package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"
	"unicode"

	"github.com/QubitProducts/triesbien"
	"github.com/QubitProducts/triesbien/trie"
	"github.com/golang/glog"
	"github.com/pressly/chi"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/syndtr/goleveldb/leveldb"
)

var (
	triePath        = "./data/trie.pb"
	maxLexemeLength = 10
	maxBucketLength = 1024
	leveldbPath     = "./data/leveldb"
	addr            = ":3812"
)

func init() {
	flag.IntVar(&maxLexemeLength, "search.lexeme-length", maxLexemeLength, "the maximum length of any lexeme")
	flag.IntVar(&maxBucketLength, "search.bucket-length", maxBucketLength, "the maximum length of any bucket")
	flag.StringVar(&leveldbPath, "leveldb.path", leveldbPath, "path to the leveldb database")
	flag.StringVar(&triePath, "trie.path", triePath, "path to read/write trie from")
	flag.StringVar(&addr, "addr", addr, "address to serve on")
}

func main() {
	flag.Set("logtostderr", "true")
	flag.Parse()

	t := trie.NewTrie()
	config := triesbien.Config{
		Parser:          parseProductTitle,
		MaxLexemeLength: maxLexemeLength,
		MaxBucketLength: maxBucketLength,
	}
	trieFile, err := os.Open(triePath)
	if err != nil {
		glog.Errorf("could not open trie path to read: %v", err)
		os.Exit(1)
	}
	defer trieFile.Close()

	err = t.Unmarshal(trieFile)
	if err != nil {
		glog.Errorf("could not read trie: %v", err)
		os.Exit(1)
	}

	db, err := leveldb.OpenFile(leveldbPath, nil)
	if err != nil {
		glog.Errorf("could not open leveldb: %v", err)
		os.Exit(1)
	}
	defer db.Close()

	r := chi.NewRouter()

	r.Handle("/metrics", prometheus.Handler())
	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "./cmd/servetrie/index.html")
	})
	r.Get("/:query", func(w http.ResponseWriter, r *http.Request) {
		query := chi.URLParam(r, "query")

		started := time.Now()
		res, err := triesbien.Query(t, db, config, query)
		if err != nil {
			w.WriteHeader(500)
			fmt.Fprintf(w, "query failed: %v\n", err)
		}
		glog.Infof("result in %v", time.Since(started))

		if len(res) > 10 {
			res = res[0:10]
		}
		json.NewEncoder(w).Encode(res)
	})

	glog.Infof("listening on %v", addr)
	err = http.ListenAndServe(addr, r)
	if err != nil {
		glog.Errorf("failed to serve: %v", err)
		os.Exit(1)
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

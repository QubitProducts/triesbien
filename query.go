package triesbien

import (
	"strings"

	"github.com/QubitProducts/triesbien/trie"
	"github.com/golang/glog"
	"github.com/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb"
)

func Query(t *trie.Trie, db *leveldb.DB, config Config, query string) ([]string, error) {
	parts := config.Parser(query)

	results := make([][]uint32, len(parts))
	requireManualSearch := make([]string, 0)
	intersectionalResults := make([][]uint32, 0, len(parts))
	for i, part := range parts {
		if len(part) > config.MaxLexemeLength {
			requireManualSearch = append(requireManualSearch, part)
			part = part[0:config.MaxLexemeLength]
		}
		glog.V(2).Infof("Looking up %v\n", part)
		results[i] = t.Lookup([]rune(part))
		glog.V(2).Infof("%v results", len(results[i]))
		if glog.V(4) {
			glog.Infof("%v", results[i])
		}

		if len(results[i]) >= config.MaxBucketLength {
			if len(requireManualSearch) == 0 ||
				requireManualSearch[len(requireManualSearch)-1] != part {

				requireManualSearch = append(requireManualSearch, part)
			}
		} else {
			intersectionalResults = append(intersectionalResults, results[i])
		}
	}
	if glog.V(2) {
		glog.Infof("query parts requiring manual search: %v", strings.Join(requireManualSearch, ", "))
	}

	combinedResultIXs := []uint32{}
	if len(intersectionalResults) != 0 {
		glog.V(1).Infof("intersecting results")
		combinedResultIXs = resultIntersection(intersectionalResults)
	} else {
		glog.V(1).Infof("unioning results (nothing better to do)")
		combinedResultIXs = resultUnion(results)
	}
	combinedResults := make([]string, 0, len(combinedResultIXs))
	for _, ix := range combinedResultIXs {
		w, err := db.Get(toBS(ix), nil)
		if err != nil {
			return nil, errors.Wrap(err, "could not read index")
		}
		combinedResults = append(combinedResults, string(w))
	}

	if glog.V(2) {
		glog.Infof("combined results")
		for _, s := range combinedResults {
			glog.Info(s)
		}
	}

	manuallyFilteredResults := combinedResults
	if len(requireManualSearch) != 0 {
		manuallyFilteredResults = make([]string, 0, len(combinedResults))
		for _, v := range combinedResults {
			filtered := false
			valParts := config.Parser(v)
			for _, part := range requireManualSearch {
				found := false
				for _, valPart := range valParts {
					if strings.HasPrefix(valPart, part) {
						glog.V(2).Infof("found %v in %v", part, v)
						found = true
						break
					}
				}
				if !found {
					glog.V(2).Infof("didn't find %v in %v, filtering out", part, v)
					filtered = true
					break
				}
			}
			if !filtered {
				manuallyFilteredResults = append(manuallyFilteredResults, v)
			}
		}
	}

	return manuallyFilteredResults, nil
}

func resultIntersection(inp [][]uint32) []uint32 {
	if len(inp) == 0 {
		return nil
	}
	seen := inp[0]
	for i := 1; i < len(inp); i++ {
		seen = arrIntersection(seen, inp[i])
	}
	return seen
}

func arrIntersection(a, b []uint32) []uint32 {
	i := 0
	j := 0
	res := []uint32{}
	for {
		if i >= len(a) || j >= len(b) {
			return res
		} else if a[i] == b[j] {
			res = append(res, a[i])
			i++
			j++
		} else if a[i] < b[j] && i != len(a) {
			i++
		} else if j != len(b) {
			j++
		}
	}
}

func resultUnion(inp [][]uint32) []uint32 {
	if len(inp) == 0 {
		return nil
	}
	seen := inp[0]
	for i := 1; i < len(inp); i++ {
		seen = arrUnion(seen, inp[i])
	}
	return seen
}

func arrUnion(a, b []uint32) []uint32 {
	i := 0
	j := 0
	res := []uint32{}
	for {
		if i >= len(a) || j >= len(b) {
			return res
		} else if a[i] == b[j] {
			res = append(res, a[i])
			i++
			j++
		} else if a[i] < b[j] && i != len(a) {
			res = append(res, a[i])
			i++
		} else if j != len(b) {
			res = append(res, b[j])
			j++
		}
	}
}

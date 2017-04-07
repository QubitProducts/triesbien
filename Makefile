
PHONY: build-proto
build-proto:
	protoc --gofast_out=. ./trie/proto/trie.proto

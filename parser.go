package triesbien

type Config struct {
	Parser          Parser
	MaxLexemeLength int
	MaxBucketLength int
}

type Parser func(string) []string

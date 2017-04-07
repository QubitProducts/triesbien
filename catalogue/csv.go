package catalogue

import (
	"context"
	"encoding/csv"
	"io"
	"os"

	"github.com/golang/glog"
	"github.com/pkg/errors"
)

func CSVLoader(ctx context.Context, filename string, colIx int, rowChan chan<- string) error {
	defer close(rowChan)

	file, err := os.Open(filename)
	if err != nil {
		return errors.Wrap(err, "could not open csv catalogue file")
	}
	defer file.Close()

	keys := map[string]bool{}

	reader := csv.NewReader(file)
	for i := 0; ; i++ {
		record, err := reader.Read()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return errors.Wrap(err, "csv read failed")
		}

		if len(record) <= colIx {
			glog.Errorf("line %v did not have column %v", i, colIx)
		}

		if _, ok := keys[record[colIx]]; ok {
			continue
		}
		keys[record[colIx]] = true

		select {
		case <-ctx.Done():
			return ctx.Err()
		case rowChan <- record[colIx]:
		}
	}
}

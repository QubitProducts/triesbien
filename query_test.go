package triesbien

import (
	"reflect"
	"testing"
)

func TestArrIntersection(t *testing.T) {
	t.Parallel()

	cases := []struct {
		a, b     []uint32
		expected []uint32
	}{
		{
			a:        []uint32{1, 2, 3, 4},
			b:        []uint32{2, 3},
			expected: []uint32{2, 3},
		},
	}

	for _, c := range cases {
		c := c
		t.Run("", func(t *testing.T) {
			t.Parallel()

			got := arrIntersection(c.a, c.b)
			if !reflect.DeepEqual(got, c.expected) {
				t.Errorf("unexpected result\nGot: %v\nExpected: %v", got, c.expected)
			}
		})
	}
}

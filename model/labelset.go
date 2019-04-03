package model

import (
	"fmt"
	"sort"
	"strings"
)

// A LabelSet is a collection of LabelName and LabelValue pairs.
type LabelSet map[string]string

// Merge two LabelSets into new one
func (l LabelSet) Merge(sls LabelSet) LabelSet {
	r := make(LabelSet, len(l))
	for k, v := range l {
		r[k] = v
	}
	for k, v := range sls {
		r[k] = v
	}
	return r
}

func (l LabelSet) String() string {
	lstrs := make([]string, 0, len(l))
	for l, v := range l {
		lstrs = append(lstrs, fmt.Sprintf("%s=%q", l, v))
	}

	sort.Strings(lstrs)
	return fmt.Sprintf("{%s}", strings.Join(lstrs, ", "))
}

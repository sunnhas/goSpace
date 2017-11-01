package shared

import (
	"fmt"
	"strings"
)

// Labels is structure used to represent a set of labels.
// Labels purpose is to conveniently manipulate many labels.
type Labels map[string]Label

// Add adds a label l to label set ls.
// Add returns true if the label has been added, and false otherwise.
func (ls *Labels) Add(l Label) (b bool) {
	_, exists := (*ls)[l.Id()]
	if !exists {
		(*ls)[l.Id()] = l
	}

	b = !exists

	return b
}

// Retrieve returns a label l in the label set ls.
// Retrieve returns a reference to the label if it exists and nil otherwise.
func (ls *Labels) Retrieve(id string) (l *Label) {
	lbl, exists := (*ls)[id]
	if exists {
		l = &lbl
	} else {
		l = nil
	}

	return l
}

// Delete deletes a label l from label set ls.
// Delete returns true if the label has been deleted, and false otherwise.
func (ls *Labels) Delete(id string) (b bool) {
	_, exists := (*ls)[id]
	if exists {
		delete(*ls, id)
	}

	b = exists

	return b
}

// Labelling returns the labels identifiers present in the label set ls.
func (ls *Labels) Labelling() (labelling []string) {
	labelling = make([]string, len(*ls))

	i := 0
	for k, _ := range *ls {
		labelling[i] = k
		i += 1
	}

	return labelling
}

// String returns a print friendly representation of the labels set ls.
func (ls Labels) String() (s string) {
	ms := make([]string, len(ls))

	i := 0
	for _, v := range ls {
		ms[i] = fmt.Sprintf("%v", v)
		i += 1
	}

	s = fmt.Sprintf("{%s}", strings.Join(ms, ", "))

	return s
}

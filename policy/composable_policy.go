package policy

import (
	"fmt"
	"github.com/pspaces/gospace/container"
	"strings"
	"sync"
)

// Composable is a structure for containing composable policies.
type Composable struct {
	ActionMap *sync.Map // [Action]Label
	LabelMap  *sync.Map // [Label]AggregationPolicy
}

// NewComposable creates a composable policy cp from any amount of aggregation policies aps.
func NewComposable(aps ...Aggregation) (cp *Composable) {
	cp = &Composable{ActionMap: new(sync.Map), LabelMap: new(sync.Map)}

	for _, ap := range aps {
		cp.Add(ap)
	}

	return cp
}

// Add adds an aggregation policy ap to the composable policy cp.
// Add returns true if the aggregation policy ap has been added to the composable policy cp, and false otherwise.
func (cp *Composable) Add(ap Aggregation) (b bool) {
	b = cp != nil

	if b {
		a := (&ap).Action()
		l := (&ap).Label()

		as := (&a).Signature()
		_, existsLabel := cp.ActionMap.Load(as)

		b = !existsLabel
		if b {
			lid := l.ID()
			_, existsPolicy := cp.LabelMap.Load(lid)

			b = !existsPolicy
			if b {
				_, existsPolicy := cp.LabelMap.LoadOrStore(lid, ap)
				_, existsLabel := cp.ActionMap.LoadOrStore(as, l)
				b = !existsLabel && !existsPolicy
			}
		}
	}

	return b
}

// Find returns a reference to a label l given an action a.
func (cp *Composable) Find(a *Action) (l *container.Label) {
	b := cp != nil
	l = nil

	if b {
		as := a.Signature()
		val, exists := cp.ActionMap.Load(as)
		if exists {
			lbl := val.(container.Label)
			lbl = lbl.DeepCopy()
			l = &lbl
		}
	}

	return l
}

// Retrieve returns a reference to the aggregation policy ap with label l from the composable policy cp.
// Retrieve returns a reference if it exists and nil otherwise.
func (cp *Composable) Retrieve(l container.Label) (ap *Aggregation) {
	b := cp != nil
	ap = nil

	if b {
		lid := l.ID()
		val, exists := cp.LabelMap.Load(lid)
		if exists {
			pol := val.(Aggregation)
			ap = &pol
		}
	}

	return ap
}

// Delete removes an aggregation policy with label l from the composable policy cp.
// Delete returns true if an aggregation policy with label l has been deleted from the composable policy cp, and false otherwise.
func (cp *Composable) Delete(l container.Label) (b bool) {
	b = cp != nil

	if b {
		lid := l.ID()
		val, exists := cp.LabelMap.Load(lid)
		if exists {
			ap := val.(Aggregation)
			a := ap.Action()
			cp.LabelMap.Delete(l)
			cp.ActionMap.Delete(a)
		}

		b = exists
	}

	return b
}

// String returns a print friendly representation of a composable policy cp.
func (cp Composable) String() (s string) {
	var actionEntries, labelEntries []string

	entries := []string{}
	entry := make(chan string)

	go func() {
		cp.ActionMap.Range(func(k, v interface{}) bool {
			entry <- fmt.Sprintf("\t%v: %v", k, v)
			return true
		})
		close(entry)
	}()

	for entry := range entry {
		entries = append(entries, entry)
	}

	actionEntries = entries

	entries = []string{}
	entry = make(chan string)

	go func() {
		cp.LabelMap.Range(func(k, v interface{}) bool {
			entry <- fmt.Sprintf("\t%v: %v", k, v)
			return true
		})
		close(entry)
	}()

	for entry := range entry {
		entries = append(entries, entry)
	}

	labelEntries = entries

	refs := strings.Join(actionEntries, ",\n")

	if refs != "" {
		refs = fmt.Sprintf("\n%s\n", refs)
	}

	names := strings.Join(labelEntries, ",\n")

	if names != "" {
		names = fmt.Sprintf("\n%s\n", names)
	}

	s = fmt.Sprintf("%s%s%s%s%s%s", "{", refs, "}", "{", names, "}")

	return s
}

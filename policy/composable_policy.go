package policy

import (
	"fmt"
	"github.com/pspaces/gospace/shared"
	"strings"
	"sync"
)

// ComposablePolicy is a structure for containing composable policies.
type ComposablePolicy struct {
	ActionMap *sync.Map // [Action.String()]Label
	LabelMap  *sync.Map // [Label.String()]AggregationPolicy
}

// NewComposablePolicy creates a composable policy cp from any amount of aggregation policies aps.
func NewComposablePolicy(aps ...AggregationPolicy) (cp *ComposablePolicy) {
	cp = &ComposablePolicy{ActionMap: new(sync.Map), LabelMap: new(sync.Map)}

	for _, ap := range aps {
		cp.Add(ap)
	}

	return cp
}

// Add adds an aggregation policy ap to the composable policy cp.
// Add returns true if the aggregation policy ap has been added to the composable policy cp, and false otherwise.
func (cp *ComposablePolicy) Add(ap AggregationPolicy) (b bool) {
	b = cp != nil

	if b {
		a := (&ap).Action()
		l := (&ap).Label()

		as := (&a).Signature()
		_, existsLabel := cp.ActionMap.Load(as)

		b = !existsLabel
		if b {
			lid := l.Id()
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
func (cp *ComposablePolicy) Find(a *Action) (l *shared.Label) {
	b := cp != nil
	l = nil

	if b {
		as := a.Signature()
		val, exists := cp.ActionMap.Load(as)
		if exists {
			lbl := val.(shared.Label)
			lbl = lbl.DeepCopy()
			l = &lbl
		}
	}

	return l
}

// Retrieve returns a reference to the aggregation policy ap with label l from the composable policy cp.
// Retrieve returns a reference if it exists and nil otherwise.
func (cp *ComposablePolicy) Retrieve(l shared.Label) (ap *AggregationPolicy) {
	b := cp != nil
	ap = nil

	if b {
		lid := l.Id()
		val, exists := cp.LabelMap.Load(lid)
		if exists {
			pol := val.(AggregationPolicy)
			ap = &pol
		}
	}

	return ap
}

// Delete removes an aggregation policy with label l from the composable policy cp.
// Delete returns true if an aggregation policy with label l has been deleted from the composable policy cp, and false otherwise.
func (cp *ComposablePolicy) Delete(l shared.Label) (b bool) {
	b = cp != nil

	if b {
		lid := l.Id()
		val, exists := cp.LabelMap.Load(lid)
		if exists {
			ap := val.(AggregationPolicy)
			a := ap.Action()
			cp.LabelMap.Delete(l)
			cp.ActionMap.Delete(a)
		}

		b = exists
	}

	return b
}

// String returns a print friendly representation of a composable policy cp.
func (cp ComposablePolicy) String() (s string) {
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

package policy

import (
	. "github.com/pspaces/gospace/shared"
)

// ComposablePolicy is a structure for containing composable policies.
type ComposablePolicy map[string]AggregationPolicy

// NewComposablePolicy creates a composable policy cp from any amount of aggregation policies aps.
func NewComposablePolicy(aps ...AggregationPolicy) (cp ComposablePolicy) {
	cp = make(ComposablePolicy)

	for _, ap := range aps {
		cp.Add(ap)
	}

	return cp
}

// Add adds an aggregation policy ap to the composable policy cp.
// Add returns true if the aggregation policy ap has been added to the composable policy cp, and false otherwise.
func (cp *ComposablePolicy) Add(ap AggregationPolicy) (b bool) {
	l := (&ap).Label()
	_, exists := (*cp)[l.Id()]
	if !exists {
		(*cp)[l.Id()] = ap
	}

	b = !exists

	return b
}

// Retrieve returns a reference to the aggregation policy ap with label l from the composable policy cp.
// Retrieve returns a reference if it exists and nil otherwise.
func (cp *ComposablePolicy) Retrieve(l Label) (ap *AggregationPolicy) {
	policy, exists := (*cp)[l.Id()]
	if exists {
		ap = &policy
	} else {
		ap = nil
	}

	return ap
}

// Delete removes an aggregation policy with label l from the composable policy cp.
// Delete returns true if an aggregation policy with label l has been deleted from the composable policy cp, and false otherwise.
func (cp *ComposablePolicy) Delete(l Label) (b bool) {
	_, exists := (*cp)[l.Id()]
	if exists {
		delete(*cp, l.Id())
	}

	b = exists

	return b
}

package policy

import (
	. "github.com/pspaces/gospace/shared"
)

// AggregationPolicy is a structure defining how an aggregation operation should be transformed by an aggregation rule.
type AggregationPolicy struct {
	Lbl  Label
	Rule AggregationRule
}

// NewAggregationPolicy constructs a new policy given a label l an aggregation rule r.
func NewAggregationPolicy(l Label, r AggregationRule) (ap AggregationPolicy) {
	ap := AggregationPolicy{Lbl: l, Rule: r}
	return ap
}

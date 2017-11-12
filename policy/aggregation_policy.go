package policy

import (
	. "github.com/pspaces/gospace/shared"
)

// AggregationPolicy is a structure defining how an aggregation operation should be transformed by an aggregation rule.
type AggregationPolicy struct {
	Lbl     Label
	AggRule AggregationRule
}

// NewAggregationPolicy constructs a new policy given a label l an aggregation rule r.
func NewAggregationPolicy(l Label, r AggregationRule) (ap AggregationPolicy) {
	ap = AggregationPolicy{Lbl: l, AggRule: r}
	return ap
}

// Label returns the label l attached to aggregation policy ap.
func (ap *AggregationPolicy) Label() (l Label) {
	l = (*ap).Lbl
	return l
}

// Apply applies an aggregation policy onto the input action ia.
// Apply returns a modified action oa
func (*AggregationPolicy) Apply(ia Action) (oa Action) {
	return oa
}

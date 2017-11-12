package policy

// AggregationRule is a structure defining what transformations an action is subject to.
// The action is the object and the transformations are the subjects which will be applied to the action.
type AggregationRule struct {
	Object  Action
	Subject Transformations
}

// NewAggregationRule constructs a new policy given an action a and a list of transformation trs.
func NewAggregationRule(a Action, trs Transformations) (ar AggregationRule) {
	ar = AggregationRule{Object: a, Subject: trs}
	return ar
}

package gospace

import (
	pol "github.com/pspaces/gospace/policy"
	shr "github.com/pspaces/gospace/shared"
	spc "github.com/pspaces/gospace/space"
)

type Intertuple = shr.Intertuple

type Space = spc.Space
type Tuple = shr.Tuple
type Template = shr.Template

type Label = shr.Label
type Labels = shr.Labels
type LabelledTuple = shr.LabelledTuple

type Action = pol.Action
type AggregationRule = pol.AggregationRule
type AggregationPolicy = pol.AggregationPolicy
type ComposablePolicy = pol.ComposablePolicy
type Transformation = pol.Transformation
type Transformations = pol.Transformations

// NewSpace creates a structure that represents a space.
func NewSpace(name string, policy ...*ComposablePolicy) Space {
	return spc.NewSpace(name, policy...)
}

// NewRemoteSpace creates a structure that represents a remote space.
func NewRemoteSpace(name string) Space {
	return spc.NewRemoteSpace(name)
}

// SpaceFrame contains all interfaces that can operate on a space.
type SpaceFrame interface {
	spc.Interspace
	spc.Interstar
}

// CreateTuple creates a structure that represents a tuple.
func CreateTuple(fields ...interface{}) Tuple {
	return shr.CreateTuple(fields...)
}

// TupleFrame contains all interfaces that can operate on a tuple.
type TupleFrame interface {
	shr.Intertuple
}

// CreateTemplate creates a structure that represents a template.
func CreateTemplate(fields ...interface{}) Template {
	return shr.CreateTemplate(fields...)
}

// TemplateFrame contains all interfaces that can operate on a template.
type TemplateFrame interface {
	shr.Intertemplate
}

// NewLabel creates a structure that represents a label.
func NewLabel(id string) Label {
	return shr.NewLabel(id)
}

// NewLabels creates a structure that represents a collection of labels.
func NewLabels(ll ...Label) Labels {
	return shr.NewLabels(ll...)
}

// NewLabelledTuple creates a structure that represents a labelled tuple.
func NewLabelledTuple(fields ...interface{}) LabelledTuple {
	return shr.NewLabelledTuple(fields...)
}

// NewAction creates a structure that represents an action.
func NewAction(function interface{}, params ...interface{}) *Action {
	return pol.NewAction(function, params...)
}

// NewAggregationRule creates a structure that represents an aggregation rule.
func NewAggregationRule(a Action, trs Transformations) AggregationRule {
	return pol.NewAggregationRule(a, trs)
}

// NewAggregationPolicy creates a structure that represents an aggregation policy.
func NewAggregationPolicy(l Label, r AggregationRule) AggregationPolicy {
	return pol.NewAggregationPolicy(l, r)
}

// NewComposablePolicy creates a structure that represents a composable policy.
func NewComposablePolicy(ars ...AggregationPolicy) *ComposablePolicy {
	return pol.NewComposablePolicy(ars...)
}

// NewTransformation creates a structure for representing a transformation that can be applied to an action.
func NewTransformation(function interface{}, params ...interface{}) Transformation {
	return pol.NewTransformation(function, params...)
}

// NewTransformations creates a structure for representing collection of transformation that can be applied to an action.
func NewTransformations(trs ...*Transformation) *Transformations {
	return pol.NewTransformations(trs...)
}

package policy

import (
	"github.com/pspaces/gospace/shared"
)

// Action is a structure defining an operation.
type Action struct {
	Oper   string
	Sign   string
	Func   interface{}
	Params []interface{}
}

// NewAction creates an action given a function and optionally a parameter list params.
func NewAction(function interface{}, params ...interface{}) (a *Action) {
	operator := shared.FuncName(function)

	signature := shared.Signature(function)

	a = &Action{Oper: operator, Sign: signature, Func: function, Params: params}

	return a
}

// Name returns the operator name s of the action a.
func (a *Action) Operator() (s string) {
	s = (*a).Oper
	return s
}

// Function returns the function f associated to the action a.
func (a *Action) Function() (f interface{}) {
	f = (*a).Func
	return f
}

// Parameters returns the actual paramaters p which optionally can be applied to action a.
func (a *Action) Parameters() (p []interface{}) {
	p = (*a).Params
	return p
}

// Signature returns the function signature s belonging to an action a.
func (a *Action) Signature() (s string) {
	s = (*a).Sign
	return s
}

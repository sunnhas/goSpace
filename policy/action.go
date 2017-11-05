package policy

// Action is a structure defining an operation.
type Action struct {
	Function interface{}
	Params   []interface{}
}

// NewAction creates an action given a function and optionally a parameter list params.
func NewAction(function interface{}, params ...interface{}) (a Action) {
	a = Action{function, params}
	return a
}

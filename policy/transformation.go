package policy

// Transformation defines a structure for a transformation.
type Transformation struct {
	Function interface{}
	Params   []interface{}
}

// NewTransformation creates a new transformation from a function and optional parameter list params.
func NewTransformation(function interface{}, params ...interface{}) (tr Transformation) {
	tr = Transformation{function, params}
	return
}

// Function returns the function associated to the transformation.
func (tr Transformation) Function() interface{} {
	return tr.Function
}

// Parameters returns the parameters associated to the transformation.
func (tr Transformation) Parameters() []interface{} {
	return tr.Params
}

package function

// Applier interface describes for map functionality for collections.
type Applier interface {
	// Applier interface iterates over values and applies function fun on each value.
	Apply(fun func(interface{}) interface{})
}

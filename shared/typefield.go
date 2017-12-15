package shared

import (
	"reflect"
	"sync"
)

// TypeRegistry represtent a registry of types.
type TypeRegistry *sync.Map

// gtr maintains a global type registry.
var gtr = new(sync.Map)

// TypeField encapsulate a type.
type TypeField struct {
	TypeStr string
}

// CreateTypeField creates an encapsulation of a type.
func CreateTypeField(t reflect.Type) TypeField {
	ts := t.String()
	tf := TypeField{ts}

	_, exists := gtr.Load(ts)
	if !exists {
		gtr.Store(ts, t)
	}

	return tf
}

// Equal returns true if both type field a and b are quivalent, and false otherwise.
func (tf TypeField) Equal(tfo TypeField) (e bool) {
	rta := tf.GetType()
	rtb := tfo.GetType()
	e = rta == rtb
	return e
}

// GetType returns a type associated to this Typefield.
func (tf TypeField) GetType() (t reflect.Type) {
	ti, _ := gtr.Load(tf.TypeStr)
	t = ti.(reflect.Type)
	return t
}

// String returns the type string of this TypeField.
func (tf TypeField) String() string {
	return tf.TypeStr
}

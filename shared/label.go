package shared

import (
	"fmt"
)

// Interlabel is an internal interface for manipulating a label.
type Interlabel interface {
	Id() (id string)
	Value() (value interface{})
}

// Label is a structure used to label arbitrary data with additional data.
// Label has two fields: a unique string identifier and an optional interface value.
// The interpretation of the optional value depends on the application of the label.
type Label struct {
	id  string
	val interface{}
}

// NewLabel creates a new label with identifier id and an optional value v.
func NewLabel(id string, v ...interface{}) (l Label) {
	if len(v) == 1 {
		l = Label{id, v}
	} else {
		l = Label{id, nil}
	}

	return l
}

// Id returns label l's identifier id.
func (l *Label) Id() (id string) {
	id = (*l).id
	return id
}

// Value returns labels l's value v.
func (l *Label) Value() (v interface{}) {
	return (*l).val
}

// ParenthesisType returns a pair of strings that encapsulates the tuple.
// ParenthesisType is used in the String() method.
func (t Label) ParenthesisType() (string, string) {
	return "|", "|"
}

// Delimiter returns the delimiter used to seperated the values in label l.
// Delimiter is used in the String() method.
func (t Label) Delimiter() string {
	return ", "
}

// String returns a print friendly representation of label l.
func (l Label) String() (s string) {
	vs := ""

	if l.Value() != nil {
		vs = fmt.Sprintf(" - %v", l.Value())
	}

	s = fmt.Sprintf("|%s%s|", l.Id(), vs)

	return s
}

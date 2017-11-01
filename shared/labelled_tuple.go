package shared

import (
	"fmt"
	"reflect"
	"strings"
)

// LabelledTuple is a labelled tuple containing a list of fields and a label set.
// Fields can be any data type and is used to store data.
// TupleLabels is a label set that is associated to the tuple itself.
type LabelledTuple struct {
	Fields      []interface{}
	TupleLabels Labels
}

// NewLabelledTuple creates a labelled tuple according to the labels and values present in the fields.
func NewLabelledTuple(labels *Labels, fields ...interface{}) LabelledTuple {
	tf := make([]interface{}, len(fields))
	copy(tf, fields)

	ls := make(Labels)
	for _, v := range labels.Labelling() {
		labelp := labels.Retrieve(v)
		if labelp != nil {
			ls.Add(*labelp)
		}
	}

	tuple := LabelledTuple{tf, ls}

	return tuple
}

// Length returns the amount of fields of the tuple.
func (lt *LabelledTuple) Length() int {
	return len((*lt).Fields)
}

// GetFieldAt returns the i'th field of the tuple.
func (lt *LabelledTuple) GetFieldAt(i int) interface{} {
	return (*lt).Fields[i]
}

// SetFieldAt sets the i'th field of the tuple to the value of val.
func (lt *LabelledTuple) SetFieldAt(i int, val interface{}) {
	(*lt).Fields[i] = val
}

// Labels returns the label set belonging to the labelled tuple.
func (lt *LabelledTuple) Labels() (ls Labels) {
	return (*lt).TupleLabels
}

// MatchTemplate pattern matches labelled tuple t against the template tp.
// MatchTemplate discriminates between encapsulated formal fields and actual fields.
// MatchTemplate returns true if the template matches the labelled tuple and false otherwise.
func (lt *LabelledTuple) MatchTemplate(tp Template) bool {
	if (*lt).Length() != tp.Length() {
		return false
	} else if (*lt).Length() == 0 && tp.Length() == 0 {
		return true
	}

	// Run through corresponding fields of tuple and template to see if they are
	// matching.
	for i := 0; i < tp.Length(); i++ {
		tf := (*lt).GetFieldAt(i)
		tpf := tp.GetFieldAt(i)
		// Check if the field of the template is an encapsulated formal or actual field.
		if reflect.TypeOf(tpf) == reflect.TypeOf(TypeField{}) {
			if reflect.TypeOf(tf) != tpf.(TypeField).GetType() {
				return false
			}
		} else if !reflect.DeepEqual(tf, tpf) {
			return false
		}
	}

	return true
}

// ParenthesisType returns a pair of strings that encapsulates labelled tuple t.
// ParenthesisType is used in the String() method.
func (lt LabelledTuple) ParenthesisType() (string, string) {
	return "(", ")"
}

// Delimiter returns the delimiter used to seperate a labelled tuple t's fields.
// Delimiter is used in the String() method.
func (lt LabelledTuple) Delimiter() string {
	return ", "
}

// String returns a print friendly representation of the tuple.
func (lt LabelledTuple) String() (s string) {
	ld, rd := lt.ParenthesisType()

	delim := lt.Delimiter()

	strs := make([]string, lt.Length())

	for i, _ := range strs {
		field := lt.GetFieldAt(i)

		if field != nil {
			if reflect.TypeOf(field).Kind() == reflect.String {
				strs[i] = fmt.Sprintf("%s%s%s", "\"", field, "\"")
			} else {
				strs[i] = fmt.Sprintf("%v", field)
			}
		} else {
			strs[i] = "nil"
		}
	}

	s = fmt.Sprintf("%s%s%s%s%s", ld, lt.Labels(), " : ", strings.Join(strs, delim), rd)

	return s
}

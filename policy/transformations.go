package policy

import (
	"reflect"
)

// Transformations defines a structure for a collection of transformations to be applied.
type Transformations struct {
	Tmpl Transformation
	Mtch Transformation
	Rslt Transformation
}

// NewTransformations creates a collection of transformations to be applied.
// NewTransformations returns a pointer to the collection if exactly 3 types of transformations are specified, otherwise nil is returned.
func NewTransformations(tr ...*Transformation) (trs *Transformations) {
	if len(tr) != 3 {
		trs := nil
	} else {
		// TODO: Reduce the copy code to something better.
		trcopy := make([]*Transformation, len(tr))

		// Make a copy of the transformations.
		for i, _ := range trcopy {
			if tr[i] != nil {
				trcopy[i] = &NewTransformation(tr[i].Function(), tr[i].Parameters())
			} else {
				trcopy[i] = tr[i]
			}
		}

		trs = &Transformations{Tmpl: trcopy[0], Mtch: trcopy[1], Rslt: trcopy[2]}
	}

	return trs
}

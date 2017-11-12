package policy

// Transformations defines a structure for a collection of transformations to be applied.
type Transformations struct {
	Tmpl Transformation
	Mtch Transformation
	Rslt Transformation
}

// NewTransformations creates a collection of transformations to be applied.
// NewTransformations returns a pointer to a collection if exactly 3 types of transformations are specified, otherwise nil is returned.
func NewTransformations(tr ...*Transformation) (trs *Transformations) {
	if len(tr) != 3 {
		trs = nil
	} else {
		trc := make([]Transformation, len(tr))

		// Make a copy of the transformations.
		for i, _ := range tr {
			if tr[i] != nil {
				trans := tr[i]
				function := trans.Function()
				params := trans.Parameters()
				ntr := NewTransformation(function, params...)
				trc[i] = ntr
			} else {
				trc[i] = Transformation{}
			}
		}

		trs = &Transformations{Tmpl: trc[0], Mtch: trc[1], Rslt: trc[2]}
	}

	return trs
}

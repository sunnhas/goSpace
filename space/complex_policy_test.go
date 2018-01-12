package space

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"testing"

	"github.com/pspaces/gospace/container"
	"github.com/pspaces/gospace/function"
	"github.com/pspaces/gospace/policy"
)

// Coin picks a tuple randomly by a coin toss or creates an empty tuple.
func Coin(ts ...container.Intertuple) (r container.Intertuple) {
	upper := big.NewInt(int64(len(ts)))

	if upper.Int64() > 0 {
		coin, _ := rand.Int(rand.Reader, upper)
		r = ts[int(coin.Int64())]
	} else {
		t := container.NewTuple()
		r = &t
	}

	return r
}

// TemplateIdentity preservers the template.
func TemplateIdentity(i interface{}) (tp container.Template) {
	tpf := i.([]interface{})

	tp = container.NewTemplate(tpf...)

	return tp
}

// TupleIdentity preserves the tuple.
func TupleIdentity(i interface{}) (it container.Intertuple) {
	tf := i.([]interface{})

	t := container.NewTuple(tf...)
	it = &t

	return it
}

// ACLPolicy generates an policy by producing the Cartesian product between a set of functions and a multiset of templates.
// ACLPolicy adds aggregation policies to an existing composable policy and will not overwrite existing policies.
// Functions are operations related to a tuple space, but in principle could be more complicated.
// In ACLPolicy, templates, matched tuples and resulting tuple aggregation are preserved.
// ACLPolicy injects labels into the templates such that the templates become labelled.
// ACLPolicy returns all the generated label sets containing a single label and if the addition of all policies were successful.
func ACLPolicy(pol *policy.Composable, funs []interface{}, tps []container.Template) (slbls []container.Labels, b bool) {
	b = pol != nil && len(funs) > 0 && len(tps) > 0

	if b {
		spc := new(Space)
		polName := function.Name(ACLPolicy)
		slbls = make([]container.Labels, 0, len(funs)*len(tps))
		for _, fun := range funs {
			funName := function.Name(fun)
			for i, tp := range tps {
				sl := fmt.Sprintf("%s-%s-%d", polName, funName, i)
				l := container.NewLabel(sl)
				lm := container.NewLabels(l)
				slbls = append(slbls, lm)

				tf := tp.Fields()
				var ltf []interface{}
				if container.NewSignature(1, fun) == container.NewSignature(1, spc.PutAgg) ||
					container.NewSignature(1, fun) == container.NewSignature(1, spc.GetAgg) ||
					container.NewSignature(1, fun) == container.NewSignature(1, spc.QueryAgg) {
					ltf = make([]interface{}, len(tf)+1)
					copy(ltf[:2], []interface{}{tf[0], lm})
					copy(ltf[2:], tf[1:])
				} else {
					ltf = make([]interface{}, len(tf)+1)
					copy(ltf[:1], []interface{}{l})
					copy(ltf[1:], tf)
				}

				ltp := container.NewTemplate(ltf...)
				a := policy.NewAction(fun, ltp)
				templateTrans := policy.NewTransformation(TemplateIdentity)
				tupleTrans := policy.NewTransformation(TupleIdentity)
				resultTrans := policy.NewTransformation(TupleIdentity)
				transformations := policy.NewTransformations(&templateTrans, &tupleTrans, &resultTrans)
				rule := policy.NewAggregationRule(*a, *transformations)
				aggPol := policy.NewAggregation(l, rule)
				b = b && pol.Add(aggPol)
			}
		}
	}

	return slbls, b
}

// BenchmarkRandomDisclosurePolicy randomly picks a tuple from the matched tuples returns it according to a policy.
// Depending on operation, multiple tuples may be consumed and aggregated.
func BenchmarkRandomDisclosurePolicy(b *testing.B) {
	var i int
	var f float64
	var s string

	spc := new(Space)
	disclose := policy.NewComposable()
	funs := []interface{}{spc.PutAgg, spc.GetAgg, spc.QueryAgg}
	temps := []container.Template{container.NewTemplate(Coin, &s, &i, &f)}
	slabels, _ := ACLPolicy(disclose, funs, temps)

	aspc := NewSpace("tcp4://localhost:31415/aspc?CONN", disclose)

	b.ResetTimer()
	b.Run("Constructor-ComposablePolicy", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			disclose = policy.NewComposable()
		}
	})

	funs = []interface{}{spc.PutAgg, spc.GetAgg, spc.QueryAgg}
	temps = []container.Template{container.NewTemplate(Coin, &s, &i, &f)}

	b.Run("ACLPolicy-Random", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			disclose = policy.NewComposable()
			slabels, _ = ACLPolicy(disclose, funs, temps)
		}
	})

	// Put some unlabelled tuples.
	// aspc.Put("Alice", 25, 55.7844425, 12.5208206)
	// aspc.Put("Bob", 25, 55.7844425, 12.5208206)

	// Labels allowing PutAgg.
	lbls := slabels[0]

	b.Run("PutAgg-RandomDisclosure", func(b *testing.B) {

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			aspc.Put(lbls, "Alice", 0x010119701111, 60.0)
			aspc.Put(lbls, "Alice", 0x010119701111, 60.0)
			aspc.Put(lbls, "Bob", 0x010119702222, 75.0)
			aspc.Put(lbls, "Bob", 0x010119702222, 75.0)

			aspc.PutAgg(Coin, lbls, "Alice", &i, &f)
		}

	})

	b.Run("QryAgg-RandomDisclosure", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			aspc.Put(lbls, "Alice", 0x010119701111, 60.0)
			aspc.Put(lbls, "Alice", 0x010119701111, 60.0)
			aspc.Put(lbls, "Bob", 0x010119702222, 75.0)
			aspc.Put(lbls, "Bob", 0x010119702222, 75.0)

			aspc.QryAgg(Coin, lbls, "Alice", &i, &f)
		}

	})

	b.Run("GetAgg-RandomDisclosure", func(b *testing.B) {
		// Put some unlabelled tuples.
		// aspc.Put("Alice", 25, 55.7844425, 12.5208206)
		// aspc.Put("Bob", 25, 55.7844425, 12.5208206)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			aspc.Put(lbls, "Alice", 0x010119701111, 60.0)
			aspc.Put(lbls, "Alice", 0x010119701111, 60.0)
			aspc.Put(lbls, "Bob", 0x010119702222, 75.0)
			aspc.Put(lbls, "Bob", 0x010119702222, 75.0)

			aspc.GetAgg(Coin, lbls, "Alice", &i, &f)
		}

	})

	asz, _ := aspc.Size()
	fmt.Printf("Size of aspace: %d\n", asz)

	return
}

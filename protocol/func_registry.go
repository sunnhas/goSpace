package protocol

import (
	"github.com/pspaces/gospace/shared"
	"regexp"
	"runtime"
	"strings"
	"sync"
)

// Namespace is a representation of a location of a function.
type Namespace string

// DeepCopy creates a copy of the namespace.
func (ref *Namespace) DeepCopy() (ns Namespace) {
	ns = (*ref)[:len(*ref)] + ""
	return ns
}

// Function is a representation of a function.
type Function interface{}

// NamespaceDictionary is a representation for looking up namespaces names.
type NamespaceDictionary struct {
	RefLookUp  *sync.Map // [*Namespace]Namespace
	NameLookUp *sync.Map // [Namespace]*Namespace
}

// FunctionBind maintains a function binding.
type FunctionBinding struct {
	Binding *sync.Map // [*Namespace]Function
}

// LanguageBind is a structure to maintain a function mapping between two languages.
type LanguageBinding struct {
	InternalToExternal *sync.Map // [*Namespace]*Namespace
	ExternalToInternal *sync.Map // [*Namespace]*Namespace
}

// FunctionRegistry represents the available functions between languages.
type FunctionRegistry struct {
	NameDict NamespaceDictionary
	FuncBind FunctionBinding
	LangBind LanguageBinding
}

// NewNamespaceDictionary creates a two-way dictionary for looking up between a reference of a namespace and namespace itself.
func NewNamespaceDictionary() (nsd NamespaceDictionary) {
	rlu := new(sync.Map)
	nlu := new(sync.Map)
	nsd = NamespaceDictionary{RefLookUp: rlu, NameLookUp: nlu}
	return nsd
}

// Add will add a namespace ns to a namespace dictionary nsd.
// Add returns true if the addition of namespace ns was successful, and false otherwise.
func (nsd *NamespaceDictionary) Add(ns Namespace) (status bool) {
	nsc := (&ns).DeepCopy()
	_, nameExists := (*nsd).NameLookUp.LoadOrStore(nsc, &nsc)
	_, refExists := (*nsd).RefLookUp.LoadOrStore(&nsc, nsc)
	status = !nameExists && !refExists
	return status
}

// Remove will remove a namespace ns from the namespace dictionary nsd.
// Remove returns true if the removal of namespace ns, and false otherwise.
func (nsd *NamespaceDictionary) Remove(ns Namespace) (status bool) {
	ref, exists := (*nsd).NameLookUp.Load(ns)

	if exists {
		(*nsd).RefLookUp.Delete(ref)
		(*nsd).NameLookUp.Delete(ns)
	}

	status = exists

	return status
}

// Reference returns a reference ref for the namespace ns from namespace dictionary nsd.
func (nsd *NamespaceDictionary) Reference(ns Namespace) (ref *Namespace) {
	r, _ := (*nsd).NameLookUp.Load(ns)

	if r != nil {
		ref = r.(*Namespace)
	} else {
		ref = nil
	}

	return ref
}

// Value returns a namespace value ns from a namespace dictionary nsd given a reference ref.
func (nsd *NamespaceDictionary) Value(ref *Namespace) (ns Namespace) {
	n, _ := (*nsd).RefLookUp.Load(ref)

	if n != nil {
		ns = n.(Namespace)
	} else {
		ns = Namespace("")
	}

	return ns
}

// NewFunctionBinding creates a binding between a reference to a namespace and a function.
func NewFunctionBinding() (fb FunctionBinding) {
	b := new(sync.Map)
	fb = FunctionBinding{Binding: b}
	return fb
}

// Add a binding to fb between a namespace ref and function fun.
// Add returns true if the addition of the binding was successful, and false otherwise.
func (fb *FunctionBinding) Add(ref *Namespace, fun *Function) (status bool) {
	_, exists := (*fb).Binding.LoadOrStore(ref, fun)
	status = !exists
	return status
}

// Remove a binding from fb given a namespace reference ref.
// Remove returns true if the removal of the binding was successful, and false otherwise.
func (fb *FunctionBinding) Remove(ref *Namespace) (status bool) {
	_, exists := (*fb).Binding.Load(ref)

	if exists {
		(*fb).Binding.Delete(ref)
	}

	status = exists

	return status
}

// Binding returns a pointer to function fun given a namespace reference ref.
// Binding returns nil if no function is a attached to the namespace reference ref.
func (fb *FunctionBinding) Function(ref *Namespace) (fun *Function) {
	f, exists := (*fb).Binding.Load(ref)

	if exists {
		fun = f.(*Function)
	} else {
		fun = nil
	}

	return fun
}

// NewLanguageBinding creates a two-way binding lb between two namespaces in two different languages.
func NewLanguageBinding() (lb LanguageBinding) {
	ite := new(sync.Map)
	eti := new(sync.Map)
	lb = LanguageBinding{InternalToExternal: ite, ExternalToInternal: eti}
	return lb
}

// Add will add a new language binding between two namespaces a and b.
// Add returns true if the addition of namespace ns was successful, and false otherwise.
func (lb *LanguageBinding) Add(a *Namespace, b *Namespace) (status bool) {
	_, existsA := (*lb).InternalToExternal.LoadOrStore(a, b)
	_, existsB := (*lb).ExternalToInternal.LoadOrStore(b, a)
	status = !existsA && !existsB
	return status
}

// RemoveInternal removes a binding for the internal namespace ns from the language bindings dictionary lb.
// RemoveInternal returns true if the removal of the binding namespace ns was successful, and false otherwise.
func (lb *LanguageBinding) RemoveInternal(ns *Namespace) (status bool) {
	b, existsB := (*lb).InternalToExternal.Load(ns)
	a, existsA := (*lb).ExternalToInternal.Load(b)

	if existsA && existsB && ns == a {
		(*lb).InternalToExternal.Delete(a)
		(*lb).ExternalToInternal.Delete(b)
	}

	status = !existsA && !existsB && (ns == a)

	return status
}

// External returns the external namespace reference exter given an internal namespace reference inter.
// External returns nil if the internal namespace reference inter could not be found.
func (lb *LanguageBinding) External(inter *Namespace) (exter *Namespace) {
	b, existsB := (*lb).InternalToExternal.Load(inter)
	a, existsA := (*lb).ExternalToInternal.Load(b)

	if existsA && existsB && inter == a {
		exter = b.(*Namespace)
	} else {
		exter = nil
	}

	return exter
}

// Internal returns the internal namespace reference a given an external namespace reference b.
// Internal returns nil if the internal namespace reference a could not be found.
func (lb *LanguageBinding) Internal(exter *Namespace) (inter *Namespace) {
	a, existsA := (*lb).ExternalToInternal.Load(exter)
	b, existsB := (*lb).InternalToExternal.Load(a)

	if existsA && existsB && exter == b {
		inter = a.(*Namespace)
	} else {
		inter = nil
	}

	return inter
}

// NewFunctionRegistry creates a function registry.
func NewFunctionRegistry() (fr FunctionRegistry) {
	nsd := NewNamespaceDictionary()
	fb := NewFunctionBinding()
	lb := NewLanguageBinding()
	fr = FunctionRegistry{NameDict: nsd, FuncBind: fb, LangBind: lb}
	return fr
}

// Register performs a registring of function fun to function registry fr.
// Register returns true if registring of function fun was succesful, and false otherwise.
func (fr *FunctionRegistry) Register(fun Function) (status bool) {
	funcName := strings.Replace(shared.FuncName(fun), " ", "", -1)
	funcSign := strings.Replace(shared.Signature(fun), " ", "", -1)

	reVersion := regexp.MustCompile("(\\d)\\.(\\d)(\\.(\\d))?")
	goVersion := reVersion.FindString(runtime.Version())

	internalNamespace := Namespace(strings.Join([]string{funcName, ":", funcSign}, ""))
	externalNamespace := Namespace(strings.Join([]string{"func", "://", "golang", ":", goVersion, "/", funcName, ":", funcSign}, ""))

	addedInternal := (*fr).NameDict.Add(internalNamespace)
	addedExternal := (*fr).NameDict.Add(externalNamespace)

	if addedInternal && addedExternal {
		// Get the namespace references.
		refInternal := (*fr).NameDict.Reference(internalNamespace)
		refExternal := (*fr).NameDict.Reference(externalNamespace)

		// Bind the reference to a namespace to the reference of the function.
		bound := (*fr).FuncBind.Add(refInternal, &fun)

		// Create a language binding.
		if bound {
			status = (*fr).LangBind.Add(refInternal, refExternal)
		}
	}

	return status
}

// Unregister performs a unregistring of function fun from function registry fr.
// Unregister returns true if unregistring of function fun was succesful, and false otherwise.
func (fr *FunctionRegistry) Unregister(fun Function) (status bool) {
	funcName := strings.Replace(shared.FuncName(fun), " ", "", -1)
	funcSign := strings.Replace(shared.Signature(fun), " ", "", -1)

	reVersion := regexp.MustCompile("(\\d)\\.(\\d)(\\.(\\d))?")
	goVersion := reVersion.FindString(runtime.Version())

	internalNamespace := Namespace(strings.Join([]string{funcName, ":", funcSign}, ""))
	externalNamespace := Namespace(strings.Join([]string{"func", "://", "golang", ":", goVersion, "/", funcName, ":", funcSign}, ""))

	refInternal := (*fr).NameDict.Reference(internalNamespace)
	refExternal := (*fr).NameDict.Reference(externalNamespace)

	if refInternal != nil && refExternal != nil {
		// Remove the namespace references.
		(*fr).NameDict.Remove(internalNamespace)
		(*fr).NameDict.Remove(externalNamespace)

		// Remove the binding the reference to a namespace to the reference of the function.
		(*fr).FuncBind.Remove(refInternal)

		// Remove the language binding.
		(*fr).LangBind.RemoveInternal(refInternal)
	}

	status = refInternal != nil && refExternal != nil

	return status
}

// Check performs a check if function fun is registered in function registry fr.
// Check returns true if function fun is registered, and false otherwise.
func (fr *FunctionRegistry) Check(fun Function) (status bool) {
	funcName := strings.Replace(shared.FuncName(fun), " ", "", -1)
	funcSign := strings.Replace(shared.Signature(fun), " ", "", -1)

	reVersion := regexp.MustCompile("(\\d)\\.(\\d)(\\.(\\d))?")
	goVersion := reVersion.FindString(runtime.Version())

	internalNamespace := Namespace(strings.Join([]string{funcName, ":", funcSign}, ""))
	externalNamespace := Namespace(strings.Join([]string{"func", "://", "golang", ":", goVersion, "/", funcName, ":", funcSign}, ""))

	refInternal := (*fr).NameDict.Reference(internalNamespace)
	refExternal := (*fr).NameDict.Reference(externalNamespace)

	status = refInternal != nil && refExternal != nil

	return status
}

// Encode returns a pointer to an external namespace ns for function fun.
// Encode returns nil if the function could not be encoded.
func (fr *FunctionRegistry) Encode(fun Function) (exter *Namespace) {
	fr.Register(fun)

	funcName := strings.Replace(shared.FuncName(fun), " ", "", -1)
	funcSign := strings.Replace(shared.Signature(fun), " ", "", -1)

	internalNamespace := Namespace(strings.Join([]string{funcName, ":", funcSign}, ""))

	refInternal := (*fr).NameDict.Reference(internalNamespace)
	refExternal := (*fr).LangBind.External(refInternal)

	if refExternal != nil {
		refCopy := refExternal.DeepCopy()
		exter = &refCopy
	} else {
		exter = nil
	}

	return exter
}

// Decode returns a function fun given an external namespace ns.
// Decode returns nil if the function could not be found.
func (fr *FunctionRegistry) Decode(exter Namespace) (fun *Function) {
	refExternal := (*fr).NameDict.Reference(exter)
	refInternal := (*fr).LangBind.Internal(refExternal)
	fun = (*fr).FuncBind.Function(refInternal)
	return fun
}

package space

import (
	"encoding/gob"
	"fmt"
	"github.com/pspaces/gospace/function"
	"github.com/pspaces/gospace/policy"
	"github.com/pspaces/gospace/protocol"
	"github.com/pspaces/gospace/shared"
	"net"
	"reflect"
	"strconv"
	"sync"
)

// TupleSpace contains a set of tuples and it has a mutex lock associated with
// it to secure mutual exclusion.
// Furthermore a port number to locate it.
type TupleSpace struct {
	muTuples         *sync.RWMutex            // Lock for the tuples[].
	muWaitingClients *sync.Mutex              // Lock for the waitingClients[].
	tuples           []shared.Tuple           // Tuples in the tuple space.
	funReg           *function.Registry       // Function registry associated to the tuple space.
	pol              *policy.ComposablePolicy // Policy associated to the tuple space.
	port             string                   // Port number for the tuple space.
	waitingClients   []protocol.WaitingClient // Structure for clients that couldn't initially find a matching tuple.
}

// CreateTupleSpace creates a new tuple space.
func CreateTupleSpace(port int) (ts *TupleSpace) {
	gob.Register(shared.Template{})
	gob.Register(shared.Tuple{})
	gob.Register(shared.TypeField{})

	muTuples := new(sync.RWMutex)
	muWaitingClients := new(sync.Mutex)

	// TODO: Exchange capabilities instead and
	// TODO: make a mechanism capable of doing that.
	if function.GlobalRegistry == nil {
		fr := function.NewRegistry()
		function.GlobalRegistry = &fr
	}
	funcReg := *function.GlobalRegistry

	ts = &TupleSpace{
		muTuples:         muTuples,
		muWaitingClients: muWaitingClients,
		tuples:           []shared.Tuple{},
		funReg:           &funcReg,
		pol:              nil,
		port:             strconv.Itoa(port),
	}

	go ts.Listen()

	return ts
}

// Size return the number of tuples in the tuple space.
func (ts *TupleSpace) Size() int {
	return len(ts.tuples)
}

// put will call the nonblocking put method and places a success response on
// the response channel.
func (ts *TupleSpace) put(t *shared.Tuple, response chan<- bool) {
	ts.putP(t)
	response <- true
}

// putP will put a lock on the tuple space add the tuple to the list of
// tuples and unlock the list.
func (ts *TupleSpace) putP(t *shared.Tuple) {
	ts.muWaitingClients.Lock()

	// Perform a copy of the tuple.
	fc := make([]interface{}, t.Length())
	copy(fc, t.Fields())
	tc := shared.CreateTuple(fc...)

	fr := (*ts).funReg

	// Check if someone is waiting for the tuple that is about to be placed.
	for i := 0; i < len(ts.waitingClients); i++ {
		waitingClient := ts.waitingClients[i]
		// Extract the template from the waiting client and check if it
		// matches the tuple.
		temp := waitingClient.GetTemplate()

		if tc.Match(temp) {
			// If this is reached, the tuple matched the template and the
			// tuple is send to the response channel of the waiting client.
			clientResponse := waitingClient.GetResponseChan()
			funcEncode(fr, &tc)
			clientResponse <- &tc
			// Check if the client who was waiting for the tuple performed a get
			// or query operation.
			ts.removeClientAt(i)
			i--
			clientOperation := waitingClient.GetOperation()
			if clientOperation == protocol.GetRequest || clientOperation == protocol.GetAggRequest {
				// Unlock before exiting the method.
				ts.muWaitingClients.Unlock()
				return
			}
		}
	}

	// No waiting client performing Get matched the tuple. So unlock.
	ts.muWaitingClients.Unlock()

	// Place lock on tuples[] before adding the new tuple.
	ts.muTuples.Lock()
	defer ts.muTuples.Unlock()

	ts.tuples = append(ts.tuples, *t)
}

func (ts *TupleSpace) removeClientAt(i int) {
	ts.waitingClients = append(ts.waitingClients[:i], ts.waitingClients[i+1:]...)
}

// get will find the first tuple that matches the template temp and remove the
// tuple from the tuple space.
func (ts *TupleSpace) get(temp shared.Template, response chan<- *shared.Tuple) {
	ts.findTupleBlocking(temp, response, true)
}

// query will find the first tuple that matches the template temp.
func (ts *TupleSpace) query(temp shared.Template, response chan<- *shared.Tuple) {
	ts.findTupleBlocking(temp, response, false)
}

// findTupleBlocking will continuously search for a tuple that matches the
// template temp, making the method blocking.
// The boolean remove will denote if the found tuple should be removed or not
// from the tuple space.
// The found tuple is written to the channel response.
func (ts *TupleSpace) findTupleBlocking(temp shared.Template, response chan<- *shared.Tuple, remove bool) {
	// Seach for the a tuple in the tuple space.
	tuple := ts.findTuple(temp, remove)

	// Check if there was a tuple matching the template in the tuple space.
	if tuple != nil {
		// There was a tuple that matched the template. Write it to the
		// channel and return.
		response <- tuple
		return
	}
	// There was no tuple matching the template. Enter sleep.
	newWaitingClient := protocol.CreateWaitingClient(temp, response, remove)
	ts.addNewClient(newWaitingClient)
	return
}

// addNewClient will add the client to the list of waiting clients.
func (ts *TupleSpace) addNewClient(client protocol.WaitingClient) {
	ts.muWaitingClients.Lock()
	defer ts.muWaitingClients.Unlock()
	ts.waitingClients = append(ts.waitingClients, client)
}

// getP will find the first tuple that matches the template temp and remove the
// tuple from the tuple space.
func (ts *TupleSpace) getP(temp shared.Template, response chan<- *shared.Tuple) {
	ts.findTupleNonblocking(temp, response, true)
}

// queryP will find the first tuple that matches the template temp.
func (ts *TupleSpace) queryP(temp shared.Template, response chan<- *shared.Tuple) {
	ts.findTupleNonblocking(temp, response, false)
}

// findTupleNonblocking will search for a tuple that matches the template temp.
// The boolean remove will denote if the tuple should be removed or not from
// the tuple space.
// The found tuple is written to the channel response.
func (ts *TupleSpace) findTupleNonblocking(temp shared.Template, response chan<- *shared.Tuple, remove bool) {
	tuplePtr := ts.findTuple(temp, remove)
	response <- tuplePtr
}

// findTuple will run through the tuple space to see if it contains a tuple that
// matches the template temp.
// A lock is placed around the shared data.
// The boolean remove will denote if the tuple should be removed or not from
// the tuple space.
// If a match is found a pointer to the tuple is returned, otherwise nil is.
func (ts *TupleSpace) findTuple(temp shared.Template, remove bool) *shared.Tuple {
	if remove {
		ts.muTuples.Lock()
		defer ts.muTuples.Unlock()
	} else {
		ts.muTuples.RLock()
		defer ts.muTuples.RUnlock()
	}

	for i, t := range ts.tuples {
		// Perform a copy of the tuple.
		fc := make([]interface{}, t.Length())
		copy(fc, t.Fields())
		tc := shared.CreateTuple(fc...)

		if tc.Match(temp) {
			if remove {
				ts.removeTupleAt(i)
			}

			return &tc
		}
	}

	return nil
}

// getAll will return and remove every tuple from the tuple space.
func (ts *TupleSpace) getAll(temp shared.Template, response chan<- []shared.Tuple) {
	ts.findAllTuples(temp, response, true)
}

// queryAll will return every tuple from the tuple space.
func (ts *TupleSpace) queryAll(temp shared.Template, response chan<- []shared.Tuple) {
	ts.findAllTuples(temp, response, false)
}

// findAllTuples will make a copy a the tuples in the tuple space to a list.
// The boolean remove will denote if the tuple should be removed or not from
// the tuple space.
// NOTE: an empty list of tuples is a legal return value.
func (ts *TupleSpace) findAllTuples(temp shared.Template, response chan<- []shared.Tuple, remove bool) {
	if remove {
		ts.muTuples.Lock()
		defer ts.muTuples.Unlock()
	} else {
		ts.muTuples.RLock()
		defer ts.muTuples.RUnlock()
	}
	var tuples []shared.Tuple
	var removeIndex []int
	// Go through tuple space and collects matching tuples
	for i, t := range ts.tuples {
		// Perform a copy of the tuple.
		fc := make([]interface{}, t.Length())
		copy(fc, t.Fields())
		tc := shared.CreateTuple(fc...)

		if tc.Match(temp) {
			if remove {
				removeIndex = append(removeIndex, i)
			}

			tuples = append(tuples, tc)
		}
	}
	// Remove tuples from tuple space if it is a get operations
	for i := len(removeIndex) - 1; i >= 0; i-- {
		ts.removeTupleAt(removeIndex[i])
	}

	response <- tuples
}

// clearTupleSpace will reinitialise the list of tuples in the tuple space.
func (ts *TupleSpace) clearTupleSpace() {
	ts.tuples = []shared.Tuple{}
}

// removeTupleAt will removeTupleAt the tuple in the tuples space at index i.
func (ts *TupleSpace) removeTupleAt(i int) {
	//moves last tuple to place i, then removes last element from slice
	ts.tuples[i] = ts.tuples[ts.Size()-1]
	ts.tuples = ts.tuples[:ts.Size()-1]
}

// returnUnmatched stores unmatched tuples back to the tuple space ts given an action a.
// returnUnmatched returns true if tuples have been back to the tuple space, and false otherwise.
func (ts *TupleSpace) returnUnmatched(a *policy.Action, unmatched []shared.Intertuple) (b bool) {
	var spc Space
	b = a != nil && (*a).Sign.Func != shared.NewSignature(1, spc.QueryAgg)

	if b {
		var tuple shared.Tuple
		for _, ut := range unmatched {
			switch ut.(type) {
			case *shared.Tuple:
				tuple = *(ut.(*shared.Tuple))
				ts.putP(&tuple)
			case *shared.LabelledTuple:
				tuple = shared.Tuple(*(ut.(*shared.LabelledTuple)))
				ts.putP(&tuple)
			}
		}
	}

	return b
}

// Listen will listen and accept all incoming connections. Once a connection has
// been established, the connection is passed on to the handler.
func (ts *TupleSpace) Listen() {
	defer handleRecover(ts.Listen)

	var listener net.Listener
	var err error

	proto := "tcp4"

	listener, err = net.Listen(proto, ts.port)

	if err != nil {
		err := fmt.Sprintf("%s %s. %s: %s", "Could not create listener at ", ts.port, "Error", err)
		panic(err)
	}

	defer listener.Close()

	var conn net.Conn
	for {
		conn, err = listener.Accept()

		if err != nil {
			if conn != nil {
				conn.Close()
			}

			continue
		}

		go ts.handle(conn)
	}
}

// handle will read and decode the message from the connection.
// The decoded message will be passed on to the respective method.
func (ts *TupleSpace) handle(conn net.Conn) {
	defer handleRecover(ts.handle)

	// Make sure the connection closes when method returns.
	defer conn.Close()

	// Create decoder to the connection to receive the message.
	dec := gob.NewDecoder(conn)

	// Read the message from the connection through the decoder.
	var message protocol.Message
	err := dec.Decode(&message)

	// Error check for receiving message.
	if err != nil {
		err := fmt.Sprintf("%s %s. %s: %s", "Could not decode message from peer at", conn.RemoteAddr(), "Error", err)
		panic(err)
	}

	operation := message.GetOperation()
	fr := ts.funReg

	switch operation {
	case protocol.PutRequest:
		// Body of message must be a tuple.
		tuple := message.GetBody().(shared.Tuple)
		funcDecode(fr, &tuple)
		ts.handlePut(conn, tuple)
	case protocol.PutPRequest:
		// Body of message must be a tuple.
		tuple := message.GetBody().(shared.Tuple)
		funcDecode(fr, &tuple)
		ts.handlePutP(tuple)
	case protocol.PutAggRequest:
		// Body of message must be a function and a template.
		template := message.GetBody().(shared.Template)
		funcDecode(fr, &template)
		ts.handlePutAgg(conn, template)
	case protocol.GetRequest:
		// Body of message must be a template.
		template := message.GetBody().(shared.Template)
		funcDecode(fr, &template)
		ts.handleGet(conn, template)
	case protocol.GetPRequest:
		// Body of message must be a template.
		template := message.GetBody().(shared.Template)
		funcDecode(fr, &template)
		ts.handleGetP(conn, template)
	case protocol.GetAllRequest:
		// Body of message must be a template.
		template := message.GetBody().(shared.Template)
		funcDecode(fr, &template)
		ts.handleGetAll(conn, template)
	case protocol.GetAggRequest:
		// Body of message must be a function and a template.
		template := message.GetBody().(shared.Template)
		funcDecode(fr, &template)
		ts.handleGetAgg(conn, template)
	case protocol.SizeRequest:
		ts.handleSize(conn)
	case protocol.QueryRequest:
		// Body of message must be a template.
		template := message.GetBody().(shared.Template)
		funcDecode(fr, &template)
		ts.handleQuery(conn, template)
	case protocol.QueryPRequest:
		// Body of message must be a template.
		template := message.GetBody().(shared.Template)
		funcDecode(fr, &template)
		ts.handleQueryP(conn, template)
	case protocol.QueryAllRequest:
		// Body of message must be a template.
		template := message.GetBody().(shared.Template)
		funcDecode(fr, &template)
		ts.handleQueryAll(conn, template)
	case protocol.QueryAggRequest:
		// Body of message must be a function and a template.
		template := message.GetBody().(shared.Template)
		funcDecode(fr, &template)
		ts.handleQueryAgg(conn, template)
	default:
		err := fmt.Sprintf("%s %s. %s: %s", "Unsupported operation requested by peer at", conn.RemoteAddr(), "Message sent", message)
		panic(err)
	}

	return
}

// handlePut is a blocking method.
// The method will place the tuple t in the tuple space ts.
// The method will send a boolean value to the connection conn to tell whether
// or not the placement succeeded
func (ts *TupleSpace) handlePut(conn net.Conn, t shared.Tuple) {
	defer handleRecover(ts.handlePut)

	readChannel := make(chan bool)
	go ts.put(&t, readChannel)
	result := <-readChannel

	enc := gob.NewEncoder(conn)

	err := enc.Encode(result)

	if err != nil {
		panic("Could not encode tuple")
	}
}

// handlePutP is a nonblocking method.
// The method will try and place the tuple t in e tuple space ts.
func (ts *TupleSpace) handlePutP(t shared.Tuple) {
	go ts.putP(&t)
}

// handlePutAgg is a non-blocking method that will return an aggregated tuple from the tuple
// space and put it back into the tuple space.
func (ts *TupleSpace) handlePutAgg(conn net.Conn, temp shared.Template) {
	defer handleRecover(ts.handlePutAgg)

	fun := (temp.GetFieldAt(0)).(func(...shared.Intertuple) shared.Intertuple)

	spc := new(Space)
	var cp *policy.ComposablePolicy
	var ap *policy.AggregationPolicy
	var a *policy.Action

	// Find an applicable policy through the current action and template.
	cp = (*ts).pol
	if cp != nil {
		qa := policy.NewAction(spc.PutAgg, temp)
		l := cp.Find(qa)

		if l != nil {
			ap = ts.pol.Retrieve(*l)
		}

		a = qa
	}

	fields := make([]interface{}, temp.Length()-1)
	for i := 1; i < temp.Length(); i++ {
		fields[i-1] = temp.GetFieldAt(i)
	}

	template := templateTransform(ap, fields)

	readChannel := make(chan []shared.Tuple)
	go ts.getAll(template, readChannel)
	tuples := <-readChannel

	matched, unmatched := matchTransform(ap, cp, tuples)

	if cp != nil && ap == nil {
		ts.returnUnmatched(a, matched)
	}
	ts.returnUnmatched(a, unmatched)

	result := aggregate(ap, fun, matched)

	result = resultTransform(ap, result)

	var tuple shared.Tuple
	if ap != nil {
		switch result.(type) {
		case *shared.Tuple:
			tuple = *(result.(*shared.Tuple))
		case *shared.LabelledTuple:
			tuple = shared.Tuple(*(result.(*shared.LabelledTuple)))
		}
		ts.putP(&tuple)
	} else {
		tuple = *(result.(*shared.Tuple))
	}

	fr := (*ts).funReg
	if fr != nil {
		defer funcDecode(fr, result)
		funcEncode(fr, result)
	}

	if ap != nil {
		switch result.(type) {
		case *shared.Tuple:
			tuple = *(result.(*shared.Tuple))
		case *shared.LabelledTuple:
			tuple = shared.Tuple(*(result.(*shared.LabelledTuple)))
		}
	} else if cp != nil {
		tuple = shared.CreateTuple(nil)
	} else {
		tuple = *(result.(*shared.Tuple))
	}

	enc := gob.NewEncoder(conn)
	err := enc.Encode(tuple)

	if err != nil {
		panic("Could not encode tuple")
	}

	return
}

// handleGet is a blocking method.
// It will find a tuple matching the template temp and return it.
func (ts *TupleSpace) handleGet(conn net.Conn, temp shared.Template) {
	defer handleRecover(ts.handleGet)

	readChannel := make(chan *shared.Tuple)
	go ts.get(temp, readChannel)
	resultTuplePtr := <-readChannel

	fr := (*ts).funReg
	if fr != nil && resultTuplePtr != nil {
		defer funcDecode(fr, resultTuplePtr)
		funcEncode(fr, resultTuplePtr)
	}

	enc := gob.NewEncoder(conn)

	err := enc.Encode(*resultTuplePtr)

	if err != nil {
		panic("Could not encode tuple")
	}
}

// handleGetP is a nonblocking method.
// It will try to find a tuple matching the template temp and remove the tuple
// from the tuple space.
// As it may not find it, the method will send a boolean as well as the tuple
// to the connection conn.
func (ts *TupleSpace) handleGetP(conn net.Conn, temp shared.Template) {
	defer handleRecover(ts.handleGetP)

	readChannel := make(chan *shared.Tuple)
	go ts.getP(temp, readChannel)
	resultTuplePtr := <-readChannel

	fr := (*ts).funReg
	if fr != nil && resultTuplePtr != nil {
		defer funcDecode(fr, resultTuplePtr)
		funcEncode(fr, resultTuplePtr)
	}

	enc := gob.NewEncoder(conn)

	if resultTuplePtr == nil {
		result := []interface{}{false, shared.CreateTuple()}

		err := enc.Encode(result)

		if err != nil {
			panic("Could not encode the empty tuple")
		}
	} else {
		result := []interface{}{true, *resultTuplePtr}

		err := enc.Encode(result)

		if err != nil {
			panic("Could not encode the tuple")
		}
	}
}

// handleGetAll is a nonblocking method that will remove all tuples from the tuple
// space and send them in a list through the connection conn.
func (ts *TupleSpace) handleGetAll(conn net.Conn, temp shared.Template) {
	defer handleRecover(ts.handleGetAll)

	readChannel := make(chan []shared.Tuple)
	go ts.getAll(temp, readChannel)
	tupleList := <-readChannel

	fr := (*ts).funReg
	if fr != nil {
		for _, t := range tupleList {
			defer funcDecode(fr, &t)
			funcEncode(fr, &t)
		}
	}

	enc := gob.NewEncoder(conn)

	err := enc.Encode(tupleList)

	if err != nil {
		panic("Could not encode tuples")
	}
}

// handleGetAgg is a blocking method that will return an aggregated tuple from the tuple
// space in a list.
func (ts *TupleSpace) handleGetAgg(conn net.Conn, temp shared.Template) {
	defer handleRecover(ts.handleGetAgg)

	fun := (temp.GetFieldAt(0)).(func(...shared.Intertuple) shared.Intertuple)

	spc := new(Space)
	var cp *policy.ComposablePolicy
	var ap *policy.AggregationPolicy
	var a *policy.Action

	// Find an applicable policy through the current action and template.
	cp = (*ts).pol
	if cp != nil {
		qa := policy.NewAction(spc.GetAgg, temp)
		l := cp.Find(qa)

		if l != nil {
			ap = ts.pol.Retrieve(*l)
		}

		a = qa
	}

	fields := make([]interface{}, temp.Length()-1)
	for i := 1; i < temp.Length(); i++ {
		fields[i-1] = temp.GetFieldAt(i)
	}

	template := templateTransform(ap, fields)

	readChannel := make(chan []shared.Tuple)
	go ts.getAll(template, readChannel)
	tuples := <-readChannel

	matched, unmatched := matchTransform(ap, cp, tuples)

	if cp != nil && ap == nil {
		ts.returnUnmatched(a, matched)
	}
	ts.returnUnmatched(a, unmatched)

	result := aggregate(ap, fun, matched)

	result = resultTransform(ap, result)

	fr := (*ts).funReg
	if fr != nil {
		defer funcDecode(fr, result)
		funcEncode(fr, result)
	}

	var tuple shared.Tuple
	if ap != nil {
		switch result.(type) {
		case *shared.Tuple:
			tuple = *(result.(*shared.Tuple))
		case *shared.LabelledTuple:
			tuple = shared.Tuple(*(result.(*shared.LabelledTuple)))
		}
	} else if cp != nil {
		tuple = shared.CreateTuple(nil)
	} else {
		tuple = *(result.(*shared.Tuple))
	}

	enc := gob.NewEncoder(conn)
	err := enc.Encode(tuple)

	if err != nil {
		panic("Could not encode tuple")
	}

	return
}

// handleSize returns the size of this tuple space at this instant.
func (ts *TupleSpace) handleSize(conn net.Conn) {
	defer handleRecover(ts.handleSize)

	ts.muTuples.Lock()
	sz := ts.Size()
	ts.muTuples.Unlock()

	enc := gob.NewEncoder(conn)

	err := enc.Encode(sz)

	if err != nil {
		panic("Could not encode tuple space size")
	}

	return
}

// handleQuery is a blocking method.
// It will find a tuple matching the template temp.
// The found tuple will be send to the connection conn.
func (ts *TupleSpace) handleQuery(conn net.Conn, temp shared.Template) {
	defer handleRecover(ts.handleQuery)

	readChannel := make(chan *shared.Tuple)
	go ts.query(temp, readChannel)
	resultTuplePtr := <-readChannel

	fr := (*ts).funReg
	if fr != nil && resultTuplePtr != nil {
		defer funcDecode(fr, resultTuplePtr)
		funcEncode(fr, resultTuplePtr)
	}

	enc := gob.NewEncoder(conn)

	err := enc.Encode(*resultTuplePtr)

	funcDecode(fr, resultTuplePtr)

	if err != nil {
		panic("Could not encode tuple")
	}
}

// handleQueryP is a nonblocking method.
// It will try to find a tuple matching the template temp.
// As it may not find it, the method returns a boolean as well as the tuple.
func (ts *TupleSpace) handleQueryP(conn net.Conn, temp shared.Template) {
	defer handleRecover(ts.handleQueryP)

	readChannel := make(chan *shared.Tuple)
	go ts.queryP(temp, readChannel)
	resultTuplePtr := <-readChannel

	fr := (*ts).funReg
	if fr != nil && resultTuplePtr != nil {
		defer funcDecode(fr, resultTuplePtr)
		funcEncode(fr, resultTuplePtr)
	}

	enc := gob.NewEncoder(conn)

	if resultTuplePtr == nil {
		result := []interface{}{false, shared.CreateTuple()}

		err := enc.Encode(result)

		if err != nil {
			panic("Could not encode the empty tuple")
		}
	} else {
		result := []interface{}{true, *resultTuplePtr}

		err := enc.Encode(result)

		if err != nil {
			panic("Could not encode tuple")
		}
	}

	funcDecode(fr, resultTuplePtr)
}

// handleQueryAll is a blocking method that will return all tuples from the tuple
// space in a list.
func (ts *TupleSpace) handleQueryAll(conn net.Conn, temp shared.Template) {
	defer handleRecover(ts.handleQueryAll)

	readChannel := make(chan []shared.Tuple)
	go ts.queryAll(temp, readChannel)
	tupleList := <-readChannel

	fr := (*ts).funReg
	if fr != nil {
		for _, t := range tupleList {
			defer funcDecode(fr, &t)
			funcEncode(fr, &t)
		}
	}

	enc := gob.NewEncoder(conn)

	err := enc.Encode(tupleList)

	if err != nil {
		panic("Could not encode tuple")
	}
}

// handleQueryAgg is a blocking method that will return an aggregated tuple from the tuple
// space in a list.
func (ts *TupleSpace) handleQueryAgg(conn net.Conn, temp shared.Template) {
	defer handleRecover(ts.handleQueryAgg)

	fun := (temp.GetFieldAt(0)).(func(...shared.Intertuple) shared.Intertuple)

	spc := new(Space)
	var cp *policy.ComposablePolicy
	var ap *policy.AggregationPolicy

	// Find an applicable policy through the current action and template.
	cp = (*ts).pol
	if cp != nil {
		qa := policy.NewAction(spc.QueryAgg, temp)
		l := cp.Find(qa)
		if l != nil {
			ap = ts.pol.Retrieve(*l)
		}
	}

	fields := make([]interface{}, temp.Length()-1)
	for i := 1; i < temp.Length(); i++ {
		fields[i-1] = temp.GetFieldAt(i)
	}

	template := templateTransform(ap, fields)

	readChannel := make(chan []shared.Tuple)
	go ts.queryAll(template, readChannel)
	tuples := <-readChannel

	matched, _ := matchTransform(ap, cp, tuples)

	result := aggregate(ap, fun, matched)

	result = resultTransform(ap, result)

	fr := (*ts).funReg
	if fr != nil {
		defer funcDecode(fr, result)
		funcEncode(fr, result)
	}

	var tuple shared.Tuple
	if ap != nil {
		switch result.(type) {
		case *shared.Tuple:
			tuple = *(result.(*shared.Tuple))
		case *shared.LabelledTuple:
			tuple = shared.Tuple(*(result.(*shared.LabelledTuple)))
		}
	} else if cp != nil {
		tuple = shared.CreateTuple(nil)
	} else {
		tuple = *(result.(*shared.Tuple))
	}

	enc := gob.NewEncoder(conn)
	err := enc.Encode(tuple)

	if err != nil {
		panic("Could not encode tuple")
	}
}

// aggregate performs the aggregation of the tuple given an aggregation function fun and tuples ts.
func aggregate(ap *policy.AggregationPolicy, fun interface{}, ts []shared.Intertuple) (result shared.Intertuple) {
	b := fun != nil

	if b {
		fun := fun.(func(...shared.Intertuple) shared.Intertuple)

		if len(ts) > 1 {
			binfun := func(x shared.Intertuple, y shared.Intertuple) shared.Intertuple {
				params := []shared.Intertuple{x, y}
				return fun(params...)
			}

			init := ts[0]
			aggregate := init
			for _, val := range ts[1:] {
				aggregate = binfun(aggregate, val)
			}

			if aggregate != nil {
				result = aggregate
			} else {
				a := ap.Action()
				err := fmt.Sprintf("%s: %s", "Could not aggregate for action", a)
				panic(err)
			}
		} else if len(ts) == 1 {
			result = fun(ts...)
		} else {
			result = fun()
		}
	}

	return result
}

// templateTransform rewrites a template given an aggregation policy ap.
func templateTransform(ap *policy.AggregationPolicy, fields []interface{}) (template shared.Template) {
	var trans *policy.Transformation
	if ap != nil {
		trs := ap.AggRule.Transformations()
		trans = trs.Template()
	}

	if trans != nil {
		val, err := trans.Apply(fields...)

		if err == nil {
			template = val.(shared.Template)
		}
	} else {
		template = shared.CreateTemplate(fields...)
	}

	return template
}

// matchTransform takes an actions label and an aggregation policy ap and applies the policy to the matched tuples.
func matchTransform(ap *policy.AggregationPolicy, cp *policy.ComposablePolicy, tuples []shared.Tuple) (mt []shared.Intertuple, ut []shared.Intertuple) {
	var trans *policy.Transformation
	if ap != nil && cp != nil {
		trs := ap.AggRule.Transformations()
		trans = trs.Match()
	}

	if trans != nil {
		// Distinguish between labelled and unlabelled tuples.
		lts := make([]int, 0, len(tuples))
		uts := make([]int, 0, len(tuples))
		for i := range tuples {
			t := tuples[i]
			labelled := t.Length() >= 1 && reflect.TypeOf(t.GetFieldAt(0)) == reflect.TypeOf(shared.Labels{})
			if labelled {
				lts = append(lts, i)
			} else {
				uts = append(uts, i)
			}
		}

		// Check the labelled tuples are subject to the correct policy.
		a := ap.Action()
		al := ap.Label()
		ets := make([]int, 0, len(lts))
		nts := make([]int, 0, len(lts))
		for _, i := range lts {
			t := tuples[i]
			lt := shared.LabelledTuple(t)
			lp := shared.NewLabels(al)
			tupleLabels, labelsMatch := lt.MatchLabels(lp)

			// The label associated to the aggregation policy with the matching action a
			// must match at least one label b attached to the matched tuple and must contain
			// the same action.
			exact := false
			if labelsMatch {
				labelling := tupleLabels.Labelling()
				lbls := lt.Labels()
				for j := 0; j < len(labelling) && !exact; j++ {
					lbl := lbls.Retrieve(labelling[j])
					apb := cp.Retrieve(*lbl)
					b := apb.Action()
					exact = (&a).Equal(&b)
					if exact {
						ets = append(ets, i)
					}
				}
			}

			if !exact {
				nts = append(nts, i)
			}
		}

		mt = make([]shared.Intertuple, len(uts)+len(ets))
		ut = make([]shared.Intertuple, len(nts))

		// Transform unlabelled tuples by applying matching transformation.
		j := 0
		for _, i := range uts {
			ut := tuples[i]
			val, err := trans.Apply(ut.Fields()...)
			if err == nil {
				lbl := shared.NewLabels(al.DeepCopy())
				tf := (val.(shared.Intertuple)).Fields()
				ltf := make([]interface{}, len(tf)+1)
				copy(ltf[:1], []interface{}{lbl})
				copy(ltf[1:], tf)
				tlt := shared.NewLabelledTuple(ltf...)
				mt[j] = &tlt
				j++
			}
		}

		// Transform labelled tuples by applying matching transformation.
		for _, i := range ets {
			ult := shared.LabelledTuple(tuples[i])
			val, err := trans.Apply(ult.Fields()...)
			if err == nil {
				lbl := shared.NewLabels(al.DeepCopy())
				tf := (val.(shared.Intertuple)).Fields()
				ltf := make([]interface{}, len(tf)+1)
				copy(ltf[:1], []interface{}{lbl})
				copy(ltf[1:], tf)
				tlt := shared.NewLabelledTuple(ltf...)
				mt[j] = &tlt
				j++
			}
		}
	} else {
		mt = make([]shared.Intertuple, 0, len(tuples))
		for i := range tuples {
			mt = append(mt, &(tuples[i]))
		}
	}

	return mt, ut
}

// resultTransform rewrites a tuple result given an aggregation policy ap.
func resultTransform(ap *policy.AggregationPolicy, res shared.Intertuple) (mod shared.Intertuple) {
	var trans *policy.Transformation
	if ap != nil {
		trs := ap.AggRule.Transformations()
		trans = trs.Result()
	}

	if trans != nil {
		val, err := trans.Apply(res.Fields()...)
		if err == nil {
			lbl := ap.Label()
			lbls := shared.NewLabels((&lbl).DeepCopy())
			tf := (val.(shared.Intertuple)).Fields()
			ltf := make([]interface{}, len(tf)+1)
			copy(ltf[:1], []interface{}{lbls})
			copy(ltf[1:], tf)
			tlt := shared.NewLabelledTuple(ltf...)
			mod = &tlt
		}
	} else {
		mod = res
	}

	return mod
}

// funcEncode performs encoding of functions in a tuple or a template t.
func funcEncode(reg *function.Registry, i interface{}) {
	var fr *function.Registry

	if reg != nil {
		fr = reg
	}

	if fr != nil {
		t := i.(function.Applier)

		// Assume that any string field might contain a reference to a function.
		// TODO: Encapsulate the enconding of functions better than sending strings.
		// TODO: A reference type is actually needed in the tuple space, as this will
		// TODO: solve any issues with the code mobility via dictionary approach.
		t.Apply(func(field interface{}) interface{} {
			var val interface{}

			if field != nil && function.IsFunc(field) {
				fun := field
				fr.Register(fun)
				val = fr.Encode(fun).String()
			} else {
				val = field
			}

			return val
		})
	}

	return
}

// funcDecode performs function decoding on tuples or templates.
func funcDecode(reg *function.Registry, i interface{}) {
	var fr *function.Registry

	if reg != nil {
		fr = reg
	}

	if fr != nil {
		t := i.(function.Applier)

		// Assume that any string field might contain a reference to a function.
		// TODO: The decoding mechanism of functions might convert any tuples that contain namespace strings.
		// TODO: A reference type is actually needed in the tuple space, as this will
		// TODO: solve any issues with the code mobility via dictionary approach.
		t.Apply(func(field interface{}) interface{} {
			var val interface{}

			if field != nil && reflect.TypeOf(field) == reflect.TypeOf("") {
				fun := fr.Decode(function.NewNamespace(field.(string)))

				if fun != nil {
					val = (*fun)
				} else {
					val = field
				}
			} else {
				val = field
			}

			return val
		})
	}

	return
}

func handleRecover(caller interface{}) {
	if error := recover(); error != nil {
		fmt.Printf("%s: %s: \n\t%s: %s.\n", "gospace", function.FuncName(caller), "Recovered from error", error)
	}
}

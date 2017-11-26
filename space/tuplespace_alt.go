package space

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/pspaces/gospace/function"
	"github.com/pspaces/gospace/policy"
	"github.com/pspaces/gospace/protocol"
	"github.com/pspaces/gospace/shared"
	"log"
	"net"
	"strings"
	"sync"
	"time"
)

// Constants for logging errors occuring in this file.
var (
	tsAltBuf    bytes.Buffer
	tsAltLogger = log.New(&tsAltBuf, "gospace: ", log.Ldate|log.Ltime|log.Lmicroseconds|log.LUTC|log.Lshortfile)
)

// tsAltLog logs errors occuring in this file.
func tsAltLog(fun interface{}, e *error) {
	if *e != nil {
		fmt.Print(&tsAltBuf)
		tsAltLogger.Printf("%s: %s\n", function.FuncName(fun), *e)
	}
}

// NewSpaceAlt creates a representation of a new tuple space.
func NewSpaceAlt(url string, pol ...*policy.ComposablePolicy) (ptp *protocol.PointToPoint, ts *TupleSpace) {
	registerTypes()

	uri, err := shared.NewSpaceURI(url)

	if err == nil {
		muTuples := new(sync.RWMutex)
		muWaitingClients := new(sync.Mutex)
		tuples := []shared.Tuple{}

		// TODO: Exchange capabilities instead and
		// TODO: make a mechanism capable of doing that.
		if function.GlobalRegistry == nil {
			fr := function.NewRegistry()
			function.GlobalRegistry = &fr
		}
		funcReg := *function.GlobalRegistry

		port := strings.Join([]string{"", uri.Port()}, ":")

		ts = &TupleSpace{
			muTuples:         muTuples,
			muWaitingClients: muWaitingClients,
			tuples:           tuples,
			funReg:           &funcReg,
			port:             port,
		}

		if len(pol) == 1 {
			(*ts).pol = pol[0]
		}

		go ts.Listen()

		// TODO: This is not the best way of doing it since
		// TODO: a host can resolve to multiple addresses.
		// TODO: For now, accept this limitation, and fix it soon.
		ptp = protocol.CreatePointToPoint(uri.Space(), "localhost", uri.Port(), &funcReg)
	} else {
		ts = nil
		ptp = nil
	}

	return ptp, ts
}

// NewRemoteSpaceAlt creates a representaiton of a remote tuple space.
func NewRemoteSpaceAlt(url string) (ptp *protocol.PointToPoint, ts *TupleSpace) {
	registerTypes()

	uri, err := shared.NewSpaceURI(url)

	if err == nil {
		// TODO: Exchange capabilities instead and
		// TODO: make a mechanism capable of doing that.
		if function.GlobalRegistry == nil {
			fr := function.NewRegistry()
			function.GlobalRegistry = &fr
		}
		funcReg := *function.GlobalRegistry

		// TODO: This is not the best way of doing it since
		// TODO: a host can resolve to multiple addresses.
		// TODO: For now, accept this limitation, and fix it soon.
		ptp = protocol.CreatePointToPoint(uri.Space(), uri.Hostname(), uri.Port(), &funcReg)
	} else {
		ts = nil
		ptp = nil
	}

	return ptp, ts
}

// registerTypes registers all the types necessary for the implementation.
func registerTypes() {
	gob.Register(shared.Label{})
	gob.Register(shared.Labels{})
	gob.Register(shared.Template{})
	gob.Register(shared.Tuple{})
	gob.Register(shared.TypeField{})
	gob.Register([]interface{}{})
}

// Size will open a TCP connection to the PointToPoint and request the size of the tuple space.
func Size(ptp protocol.PointToPoint) (sz int, b bool) {
	var conn *net.Conn
	var err error

	defer tsAltLog(Size, &err)

	sz = -1
	b = false

	conn, err = establishConnection(ptp)

	if err != nil {
		return sz, b
	}

	defer (*conn).Close()

	err = sendMessage(conn, protocol.SizeRequest, "")

	if err != nil {
		return sz, b
	}

	sz, err = receiveMessageInt(conn)

	if err != nil {
		sz = -1
	} else {
		b = true
	}

	return sz, b
}

// Put will open a TCP connection to the PointToPoint and send the message,
// which includes the type of operation and tuple specified by the user.
// The method returns a boolean to inform if the operation was carried out with
// success or not.
func Put(ptp protocol.PointToPoint, tupleFields ...interface{}) (b bool) {
	var conn *net.Conn
	var err error

	defer tsAltLog(Put, &err)

	b = false

	t := shared.CreateTuple(tupleFields...)

	// Never time out and block until connection will be established.
	conn, err = establishConnection(ptp)

	// TODO: Yes this is a bad idea, and we are doing it for now until semantics
	// TODO: for what it means to block is established.
	for {
		if err == nil {
			break
		}

		if *conn != nil {
			(*conn).Close()
		}

		conn, err = establishConnection(ptp)
	}

	defer (*conn).Close()

	funcEncode(ptp.GetRegistry(), &t)

	err = sendMessage(conn, protocol.PutRequest, t)

	if err != nil {
		return b
	}

	b, err = receiveMessageBool(conn)

	if err != nil {
		b = false
		return b
	}

	return b
}

// PutP will open a TCP connection to the PointToPoint and send the message,
// which includes the type of operation and tuple specified by the user.
// As the method is nonblocking it wont wait for a response whether or not the
// operation was successful.
// The method returns a boolean to inform if the operation was carried out with
// any errors with communication.
func PutP(ptp protocol.PointToPoint, tupleFields ...interface{}) (b bool) {
	var conn *net.Conn
	var err error

	defer tsAltLog(PutP, &err)

	b = false

	t := shared.CreateTuple(tupleFields...)

	conn, err = establishConnection(ptp)

	if err != nil {
		return b
	}

	defer (*conn).Close()

	funcEncode(ptp.GetRegistry(), &t)

	err = sendMessage(conn, protocol.PutPRequest, t)

	if err != nil {
		return b
	} else {
		b = true
	}

	return b
}

// Get will open a TCP connection to the PointToPoint and send the message,
// which includes the type of operation and template specified by the user.
// The method returns a boolean to inform if the operation was carried out with
// any errors with communication.
func Get(ptp protocol.PointToPoint, tempFields ...interface{}) (b bool) {
	b = getAndQuery(ptp, protocol.GetRequest, tempFields...)
	return b
}

// Query will open a TCP connection to the PointToPoint and send the message,
// which includes the type of operation and template specified by the user.
// The method returns a boolean to inform if the operation was carried out with
// any errors with communication.
func Query(ptp protocol.PointToPoint, tempFields ...interface{}) (b bool) {
	b = getAndQuery(ptp, protocol.QueryRequest, tempFields...)
	return b
}

func getAndQuery(ptp protocol.PointToPoint, operation string, tempFields ...interface{}) (b bool) {
	var conn *net.Conn
	var err error

	defer tsAltLog(getAndQuery, &err)

	b = false

	tp := shared.CreateTemplate(tempFields...)

	// Never time out and block until connection will be established.
	conn, err = establishConnection(ptp)

	// Busy loop until a successful connection is established.
	// TODO: Yes this is a bad idea, and we are doing it for now until semantics
	// TODO: for what it means to block is established.
	for {
		if err == nil {
			break
		}

		if *conn != nil {
			(*conn).Close()
		}

		conn, err = establishConnection(ptp)
	}

	defer (*conn).Close()

	funcEncode(ptp.GetRegistry(), &tp)

	err = sendMessage(conn, operation, tp)

	if err != nil {
		return b
	}

	var t shared.Tuple
	t, err = receiveMessageTuple(conn)

	if err != nil {
		return b
	} else {
		b = true
	}

	funcDecode(ptp.GetRegistry(), &t)

	t.WriteToVariables(tempFields...)

	return b
}

// GetP will open a TCP connection to the PointToPoint and send the message,
// which includes the type of operation and template specified by the user.
// The function will return two bool values. The first denotes if a tuple was
// found, the second if there were any erors with communication.
func GetP(ptp protocol.PointToPoint, tempFields ...interface{}) (bool, bool) {
	return getPAndQueryP(ptp, protocol.GetPRequest, tempFields...)
}

// QueryP will open a TCP connection to the PointToPoint and send the message,
// which includes the type of operation and template specified by the user.
// The function will return two bool values. The first denotes if a tuple was
// found, the second if there were any erors with communication.
func QueryP(ptp protocol.PointToPoint, tempFields ...interface{}) (bool, bool) {
	return getPAndQueryP(ptp, protocol.QueryPRequest, tempFields...)
}

func getPAndQueryP(ptp protocol.PointToPoint, operation string, tempFields ...interface{}) (tb bool, sb bool) {
	var conn *net.Conn
	var err error

	defer tsAltLog(getPAndQueryP, &err)

	tb = false
	sb = false

	tp := shared.CreateTemplate(tempFields...)

	conn, err = establishConnection(ptp)

	if err != nil {
		return tb, sb
	}

	defer (*conn).Close()

	funcEncode(ptp.GetRegistry(), &tp)

	err = sendMessage(conn, operation, tp)

	if err != nil {
		return tb, sb
	}

	var t shared.Tuple
	tb, t, err = receiveMessageBoolAndTuple(conn)

	if err != nil {
		return tb, sb
	} else {
		sb = true
	}

	funcDecode(ptp.GetRegistry(), &t)

	if tb {
		t.WriteToVariables(tempFields...)
	}

	return tb, sb
}

// GetAll will open a TCP connection to the PointToPoint and send the message,
// which includes the type of operation specified by the user.
// The method is nonblocking and will return all tuples found in the tuple
// space as well as a bool to denote if there were any errors with the
// communication.
func GetAll(ptp protocol.PointToPoint, tempFields ...interface{}) (ts []shared.Tuple, b bool) {
	ts, b = getAllAndQueryAll(ptp, protocol.GetAllRequest, tempFields...)
	return ts, b
}

// QueryAll will open a TCP connection to the PointToPoint and send the message,
// which includes the type of operation specified by the user.
// The method is nonblocking and will return all tuples found in the tuple
// space as well as a bool to denote if there were any errors with the
// communication.
func QueryAll(ptp protocol.PointToPoint, tempFields ...interface{}) (ts []shared.Tuple, b bool) {
	ts, b = getAllAndQueryAll(ptp, protocol.QueryAllRequest, tempFields...)
	return ts, b
}

func getAllAndQueryAll(ptp protocol.PointToPoint, operation string, tempFields ...interface{}) (ts []shared.Tuple, b bool) {
	var conn *net.Conn
	var err error

	defer tsAltLog(getAllAndQueryAll, &err)

	ts = []shared.Tuple{}
	b = false

	tp := shared.CreateTemplate(tempFields...)

	conn, err = establishConnection(ptp)

	if err != nil {
		return ts, b
	}

	defer (*conn).Close()

	funcEncode(ptp.GetRegistry(), &tp)

	err = sendMessage(conn, operation, tp)

	if err != nil {
		return ts, b
	}

	ts, err = receiveMessageTupleList(conn)

	if err != nil {
		return ts, b
	} else {
		b = true
	}

	for _, t := range ts {
		funcDecode(ptp.GetRegistry(), &t)
	}

	return ts, b
}

// PutAgg will connect to a space and aggregate on matched tuples from the space according to a template.
// This method is nonblocking and will return a tuple and boolean state to denote if there were any errors
// with the communication. The tuple returned is the aggregation of tuples in the space.
// If no tuples are found it will create and put a new tuple from the template itself.
func PutAgg(ptp protocol.PointToPoint, fun interface{}, tempFields ...interface{}) (t shared.Tuple, b bool) {
	t, b = aggOperation(ptp, protocol.PutAggRequest, fun, tempFields...)
	return t, b
}

// GetAgg will connect to a space and aggregate on matched tuples from the space.
// This method is nonblocking and will return a tuple perfomed by the aggregation
// as well as a boolean state to denote if there were any errors with the communication.
// The resulting tuple is empty if no matching occurs or the aggregation function can not aggregate the matched tuples.
func GetAgg(ptp protocol.PointToPoint, fun interface{}, tempFields ...interface{}) (t shared.Tuple, b bool) {
	t, b = aggOperation(ptp, protocol.GetAggRequest, fun, tempFields...)
	return t, b
}

// QueryAgg will connect to a space and aggregated on matched tuples from the space.
// which includes the type of operation specified by the user.
// The method is nonblocking and will return a tuple found by aggregating the matched typles.
// The resulting tuple is empty if no matching occurs or the aggregation function can not aggregate the matched tuples.
func QueryAgg(ptp protocol.PointToPoint, fun interface{}, tempFields ...interface{}) (t shared.Tuple, b bool) {
	t, b = aggOperation(ptp, protocol.QueryAggRequest, fun, tempFields...)
	return t, b
}

func aggOperation(ptp protocol.PointToPoint, operation string, fun interface{}, tempFields ...interface{}) (t shared.Tuple, b bool) {
	var conn *net.Conn
	var err error

	defer tsAltLog(aggOperation, &err)

	t = shared.CreateTuple()
	b = false

	fields := make([]interface{}, len(tempFields)+1)
	fields[0] = fun
	copy(fields[1:], tempFields)
	tp := shared.CreateTemplate(fields...)

	conn, err = establishConnection(ptp)

	if err != nil {
		return t, b
	}

	defer (*conn).Close()

	funcEncode(ptp.GetRegistry(), &tp)

	err = sendMessage(conn, operation, tp)

	if err != nil {
		return t, b
	}

	t, err = receiveMessageTuple(conn)

	if err != nil {
		return t, b
	} else {
		b = true
	}

	funcDecode(ptp.GetRegistry(), &t)

	return t, b
}

// establishConnection will establish a connection to the PointToPoint ptp and
// return the Conn and error.
func establishConnection(ptp protocol.PointToPoint, timeout ...time.Duration) (*net.Conn, error) {
	var conn net.Conn
	var err error

	addr := ptp.GetAddress()

	proto := "tcp4"

	if len(timeout) == 0 {
		conn, err = net.Dial(proto, addr)
	} else {
		conn, err = net.DialTimeout(proto, addr, timeout[0])
	}

	return &conn, err
}

func sendMessage(conn *net.Conn, operation string, t interface{}) (err error) {
	gob.Register(t)
	gob.Register(shared.TypeField{})

	enc := gob.NewEncoder(*conn)

	message := protocol.CreateMessage(operation, t)

	err = enc.Encode(message)

	return err
}

func receiveMessageBool(conn *net.Conn) (b bool, err error) {
	dec := gob.NewDecoder(*conn)

	err = dec.Decode(&b)

	return b, err
}

func receiveMessageInt(conn *net.Conn) (i int, err error) {
	dec := gob.NewDecoder(*conn)

	err = dec.Decode(&i)

	return i, err
}

func receiveMessageTuple(conn *net.Conn) (t shared.Tuple, err error) {
	dec := gob.NewDecoder(*conn)

	err = dec.Decode(&t)

	return t, err
}

func receiveMessageBoolAndTuple(conn *net.Conn) (b bool, t shared.Tuple, err error) {
	dec := gob.NewDecoder(*conn)

	var result []interface{}
	err = dec.Decode(&result)

	b = result[0].(bool)
	t = result[1].(shared.Tuple)

	return b, t, err
}

func receiveMessageTupleList(conn *net.Conn) (ts []shared.Tuple, err error) {
	dec := gob.NewDecoder(*conn)

	err = dec.Decode(&ts)

	return ts, err
}

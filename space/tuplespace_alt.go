package space

import (
	"encoding/gob"
	"fmt"
	"github.com/choleraehyq/gofunctools/functools"
	"github.com/pspaces/gospace/function"
	"github.com/pspaces/gospace/policy"
	. "github.com/pspaces/gospace/protocol"
	. "github.com/pspaces/gospace/shared"
	"net"
	//	"reflect"
	"strings"
	"sync"
)

// NewSpaceAlt creates a representation of a new tuple space.
func NewSpaceAlt(url string, pol ...*policy.ComposablePolicy) (ptp *PointToPoint, ts *TupleSpace) {
	registerTypes()

	uri, err := NewSpaceURI(url)

	if err == nil {
		// TODO: This is not the best way of doing it since
		// TODO: a host can resolve to multiple addresses.
		// TODO: For now, accept this limitation, and fix it soon.
		muTuples := new(sync.RWMutex)
		muWaitingClients := new(sync.Mutex)
		tuples := []Tuple{}
		funReg := function.NewRegistry()
		port := strings.Join([]string{"", uri.Port()}, ":")

		ts = &TupleSpace{
			muTuples:         muTuples,
			muWaitingClients: muWaitingClients,
			tuples:           tuples,
			funReg:           &funReg,
			port:             port,
		}

		if len(pol) == 1 {
			(*ts).pol = pol[0]
		}

		go ts.Listen()

		ptp = CreatePointToPoint(uri.Space(), "localhost", uri.Port(), &funReg)
	} else {
		ts = nil
		ptp = nil
	}

	return ptp, ts
}

// NewRemoteSpaceAlt creates a representaiton of a remote tuple space.
func NewRemoteSpaceAlt(url string) (ptp *PointToPoint, ts *TupleSpace) {
	registerTypes()

	uri, err := NewSpaceURI(url)

	if err == nil {
		// TODO: This is not the best way of doing it since
		// TODO: a host can resolve to multiple addresses.
		// TODO: For now, accept this limitation, and fix it soon.
		funReg := function.NewRegistry()
		ptp = CreatePointToPoint(uri.Space(), uri.Hostname(), uri.Port(), &funReg)
	} else {
		ts = nil
		ptp = nil
	}

	return ptp, ts
}

// registerTypes registers all the types necessary for the implementation.
func registerTypes() {
	// Register default structures for communication.
	gob.Register(Label{})
	gob.Register(Labels{})
	gob.Register(Template{})
	gob.Register(Tuple{})
	gob.Register(TypeField{})
	gob.Register([]interface{}{})
}

// Put will open a TCP connection to the PointToPoint and send the message,
// which includes the type of operation and tuple specified by the user.
// The method returns a boolean to inform if the operation was carried out with
// success or not.
func Put(ptp PointToPoint, tupleFields ...interface{}) bool {
	t := CreateTuple(tupleFields...)

	conn, errDial := establishConnection(ptp)

	// Error check for establishing connection.
	if errDial != nil {
		fmt.Println("ErrDial:", errDial)
		return false
	}

	// Make sure the connection closes when method returns.
	defer conn.Close()

	// Perform function encoding.
	funcEncode(ptp.GetRegistry(), &t)

	errSendMessage := sendMessage(conn, PutRequest, t)

	// Error check for sending message.
	if errSendMessage != nil {
		fmt.Println("ErrSendMessage:", errSendMessage)
		return false
	}

	b, errReceiveMessage := receiveMessageBool(conn)

	// Error check for receiving response.
	if errReceiveMessage != nil {
		fmt.Println("ErrReceiveMessage:", errReceiveMessage)
		return false
	}

	// Return result.
	return b
}

// PutP will open a TCP connection to the PointToPoint and send the message,
// which includes the type of operation and tuple specified by the user.
// As the method is nonblocking it wont wait for a response whether or not the
// operation was successful.
// The method returns a boolean to inform if the operation was carried out with
// any errors with communication.
func PutP(ptp PointToPoint, tupleFields ...interface{}) bool {
	t := CreateTuple(tupleFields...)

	conn, errDial := establishConnection(ptp)

	// Error check for establishing connection.
	if errDial != nil {
		fmt.Println("ErrDial:", errDial)
		return false
	}

	// Make sure the connection closes when method returns.
	defer conn.Close()

	// Perform function encoding.
	funcEncode(ptp.GetRegistry(), &t)

	errSendMessage := sendMessage(conn, PutPRequest, t)

	// Error check for sending message.
	if errSendMessage != nil {
		fmt.Println("ErrSendMessage:", errSendMessage)
		return false
	}

	return true
}

// Get will open a TCP connection to the PointToPoint and send the message,
// which includes the type of operation and template specified by the user.
// The method returns a boolean to inform if the operation was carried out with
// any errors with communication.
func Get(ptp PointToPoint, tempFields ...interface{}) bool {
	return getAndQuery(ptp, GetRequest, tempFields...)
}

// Query will open a TCP connection to the PointToPoint and send the message,
// which includes the type of operation and template specified by the user.
// The method returns a boolean to inform if the operation was carried out with
// any errors with communication.
func Query(ptp PointToPoint, tempFields ...interface{}) bool {
	return getAndQuery(ptp, QueryRequest, tempFields...)
}

func getAndQuery(ptp PointToPoint, operation string, tempFields ...interface{}) bool {
	t := CreateTemplate(tempFields...)
	conn, errDial := establishConnection(ptp)

	// Error check for establishing connection.
	if errDial != nil {
		fmt.Println("ErrDial:", errDial)
		return false
	}

	// Make sure the connection closes when method returns.
	defer conn.Close()

	// Perform function encoding.
	funcEncode(ptp.GetRegistry(), &t)

	errSendMessage := sendMessage(conn, operation, t)

	// Error check for sending message.
	if errSendMessage != nil {
		fmt.Println("ErrSendMessage:", errSendMessage)
		return false
	}

	tuple, errReceiveMessage := receiveMessageTuple(conn)

	// Error check for receiving response.
	if errReceiveMessage != nil {
		fmt.Println("ErrReceiveMessage:", errReceiveMessage)
		return false
	}

	// Perform function decoding.
	funcDecode(ptp.GetRegistry(), &tuple)

	tuple.WriteToVariables(tempFields...)

	// Return result.
	return true
}

// GetP will open a TCP connection to the PointToPoint and send the message,
// which includes the type of operation and template specified by the user.
// The function will return two bool values. The first denotes if a tuple was
// found, the second if there were any erors with communication.
func GetP(ptp PointToPoint, tempFields ...interface{}) (bool, bool) {
	return getPAndQueryP(ptp, GetPRequest, tempFields...)
}

// QueryP will open a TCP connection to the PointToPoint and send the message,
// which includes the type of operation and template specified by the user.
// The function will return two bool values. The first denotes if a tuple was
// found, the second if there were any erors with communication.
func QueryP(ptp PointToPoint, tempFields ...interface{}) (bool, bool) {
	return getPAndQueryP(ptp, QueryPRequest, tempFields...)
}

func getPAndQueryP(ptp PointToPoint, operation string, tempFields ...interface{}) (bool, bool) {
	t := CreateTemplate(tempFields...)
	conn, errDial := establishConnection(ptp)

	// Error check for establishing connection.
	if errDial != nil {
		fmt.Println("ErrDial:", errDial)
		return false, false
	}

	// Make sure the connection closes when method returns.
	defer conn.Close()

	// Perform function encoding.
	funcEncode(ptp.GetRegistry(), &t)

	errSendMessage := sendMessage(conn, operation, t)

	// Error check for sending message.
	if errSendMessage != nil {
		fmt.Println("ErrSendMessage:", errSendMessage)
		return false, false
	}

	b, tuple, errReceiveMessage := receiveMessageBoolAndTuple(conn)

	// Error check for receiving response.
	if errReceiveMessage != nil {
		fmt.Println("ErrReceiveMessage:", errReceiveMessage)
		return false, false
	}

	// Perform function decoding.
	funcDecode(ptp.GetRegistry(), &tuple)

	if b {
		tuple.WriteToVariables(tempFields...)
	}

	// Return result.
	return b, true
}

// GetAll will open a TCP connection to the PointToPoint and send the message,
// which includes the type of operation specified by the user.
// The method is nonblocking and will return all tuples found in the tuple
// space as well as a bool to denote if there were any errors with the
// communication.
func GetAll(ptp PointToPoint, tempFields ...interface{}) ([]Tuple, bool) {
	return getAllAndQueryAll(ptp, GetAllRequest, tempFields...)
}

// QueryAll will open a TCP connection to the PointToPoint and send the message,
// which includes the type of operation specified by the user.
// The method is nonblocking and will return all tuples found in the tuple
// space as well as a bool to denote if there were any errors with the
// communication.
func QueryAll(ptp PointToPoint, tempFields ...interface{}) ([]Tuple, bool) {
	return getAllAndQueryAll(ptp, QueryAllRequest, tempFields...)
}

func getAllAndQueryAll(ptp PointToPoint, operation string, tempFields ...interface{}) ([]Tuple, bool) {
	t := CreateTemplate(tempFields...)
	conn, errDial := establishConnection(ptp)

	// Error check for establishing connection.
	if errDial != nil {
		fmt.Println("ErrDial:", errDial)
		return []Tuple{}, false
	}

	// Make sure the connection closes when method returns.
	defer conn.Close()

	// Perform function encoding.
	funcEncode(ptp.GetRegistry(), &t)

	// Initiallise dummy tuple.
	// TODO: Get rid of the dummy tuple.
	errSendMessage := sendMessage(conn, operation, t)

	// Error check for sending message.
	if errSendMessage != nil {
		fmt.Println("ErrSendMessage:", errSendMessage)
		return []Tuple{}, false
	}

	tuples, errReceiveMessage := receiveMessageTupleList(conn)

	// Error check for receiving response.
	if errReceiveMessage != nil {
		fmt.Println("ErrReceiveMessage:", errReceiveMessage)
		return []Tuple{}, false
	}

	// Perform function decoding.
	for tuple := range tuples {
		funcDecode(ptp.GetRegistry(), &tuple)
	}

	// Return result.
	return tuples, true
}

// PutAgg will connect to a space and aggregate on matched tuples from the space according to a template.
// This method is nonblocking and will return a tuple and boolean state to denote if there were any errors
// with the communication. The tuple returned is the aggregation of tuples in the space.
// If no tuples are found it will create and put a new tuple from the template itself.
func PutAgg(ptp PointToPoint, aggFunc interface{}, tempFields ...interface{}) (Tuple, bool) {
	return putAgg(ptp, aggFunc, tempFields...)
}

func putAgg(ptp PointToPoint, aggFunc interface{}, tempFields ...interface{}) (Tuple, bool) {
	var tuples []Tuple
	var result Tuple
	var err bool

	tuples, err = getAllAndQueryAll(ptp, GetAllRequest, tempFields...)
	temp := CreateTemplate(tempFields...)

	if err != false && len(tuples) > 0 {
		initTuple := temp.NewTuple()
		aggregate, _ := functools.Reduce(aggFunc, tuples, initTuple)
		result = aggregate.(Tuple)
	} else {
		result = CreateIntrinsicTuple(tempFields...)
	}

	tupleFields := make([]interface{}, result.Length())

	for i, _ := range tupleFields {
		tupleFields[i] = result.GetFieldAt(i)
	}

	err = err && Put(ptp, tupleFields...)

	return result, err
}

// GetAgg will connect to a space and aggregate on matched tuples from the space.
// This method is nonblocking and will return a tuple perfomed by the aggregation
// as well as a boolean state to denote if there were any errors with the communication.
// The resulting tuple is empty if no matching occurs or the aggregation function can not aggregate the matched tuples.
func GetAgg(ptp PointToPoint, aggFunc interface{}, tempFields ...interface{}) (Tuple, bool) {
	return getAggAndQueryAgg(ptp, GetAggRequest, aggFunc, tempFields...)
}

// QueryAgg will connect to a space and aggregated on matched tuples from the space.
// which includes the type of operation specified by the user.
// The method is nonblocking and will return a tuple found by aggregating the matched typles.
// The resulting tuple is empty if no matching occurs or the aggregation function can not aggregate the matched tuples.
func QueryAgg(ptp PointToPoint, aggFunc interface{}, tempFields ...interface{}) (Tuple, bool) {
	return getAggAndQueryAgg(ptp, QueryAggRequest, aggFunc, tempFields...)
}

func getAggAndQueryAgg(ptp PointToPoint, operation string, aggFunc interface{}, tempFields ...interface{}) (Tuple, bool) {
	fields := make([]interface{}, len(tempFields)+1)
	fields[0] = aggFunc
	copy(fields[1:], tempFields)

	t := CreateTemplate(fields...)
	conn, errDial := establishConnection(ptp)

	// Error check for establishing connection.
	if errDial != nil {
		fmt.Println("ErrDial:", errDial)
		return CreateTuple(nil), false
	}

	// Make sure the connection closes when method returns.
	defer conn.Close()

	// Perform function encoding.
	funcEncode(ptp.GetRegistry(), &t)

	errSendMessage := sendMessage(conn, operation, t)

	// Error check for sending message.
	if errSendMessage != nil {
		fmt.Println("ErrSendMessage:", errSendMessage)
		return CreateTuple(nil), false
	}

	tuple, errReceiveMessage := receiveMessageTuple(conn)

	// Error check for receiving response.
	if errReceiveMessage != nil {
		fmt.Println("ErrReceiveMessage:", errReceiveMessage)
		return CreateTuple(nil), false
	}

	// Perform function decoding.
	funcDecode(ptp.GetRegistry(), &tuple)

	// Return result.
	return tuple, true
}

// establishConnection will establish a connection to the PointToPoint ptp and
// return the Conn and error.
func establishConnection(ptp PointToPoint) (net.Conn, error) {
	addr := ptp.GetAddress()

	// Establish a connection to the PointToPoint using TCP to ensure reliability.
	conn, errDial := net.Dial("tcp4", addr)

	return conn, errDial
}

func sendMessage(conn net.Conn, operation string, t interface{}) error {
	// Create encoder to the connection.
	enc := gob.NewEncoder(conn)

	gob.Register(t)
	// Registrer typefield to match types.
	gob.Register(TypeField{})

	message := CreateMessage(operation, t)

	// Sends the message to the connection through the encoder.
	errEnc := enc.Encode(message)

	return errEnc
}

func receiveMessageBool(conn net.Conn) (bool, error) {
	// Create decoder to the connection to receive the response.
	dec := gob.NewDecoder(conn)

	// Read the response from the connection through the decoder.
	var b bool
	errDec := dec.Decode(&b)

	return b, errDec
}

func receiveMessageTuple(conn net.Conn) (Tuple, error) {
	// Create decoder to the connection to receive the response.
	dec := gob.NewDecoder(conn)

	// Read the response from the connection through the decoder.
	var tuple Tuple
	errDec := dec.Decode(&tuple)

	return tuple, errDec
}

func receiveMessageBoolAndTuple(conn net.Conn) (bool, Tuple, error) {
	// Create decoder to the connection to receive the response.
	dec := gob.NewDecoder(conn)

	// Read the response from the connection through the decoder.
	var result []interface{}
	errDec := dec.Decode(&result)

	// Extract the boolean and tuple from the result.
	b := result[0].(bool)
	tuple := result[1].(Tuple)

	return b, tuple, errDec
}

func receiveMessageTupleList(conn net.Conn) ([]Tuple, error) {
	// Create decoder to the connection to receive the response.
	dec := gob.NewDecoder(conn)

	// Read the response from the connection through the decoder.
	var tuples []Tuple
	errDec := dec.Decode(&tuples)

	return tuples, errDec
}

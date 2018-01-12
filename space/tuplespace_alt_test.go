package space

import (
	"reflect"
	"testing"

	"github.com/pspaces/gospace/container"
	"github.com/pspaces/gospace/protocol"
)

func TestPutUtilities(t *testing.T) {
	ptp, ts := NewSpaceAlt("9050")
	if !(ts.Size() == 0) {
		t.Errorf("Tuple space is not empty")
	}
	//ptp := protocol.CreatePointToPoint("Bookstore", "localhost", "9050")
	Put(*ptp, "hello", false)
	if !(reflect.DeepEqual(container.NewTuple("hello", false), ts.tuples[0])) {
		t.Errorf("Tuple space is not empty")
	}
}

func TestQueryAndGetUtilities(t *testing.T) {
	ptp, ts := NewSpaceAlt("9051")
	if !(ts.Size() == 0) {
		t.Errorf("Tuple space is not empty")
	}
	//ptp := protocol.CreatePointToPoint("Bookstore", "localhost", "9051", nil, nil)
	Put(*ptp, "hello", false)
	var s string
	qtuple, querySucceed := Query(*ptp, &s, false)
	s = qtuple.GetFieldAt(0).(string)

	if !(ts.Size() == 1) {
		t.Errorf("Tuple space should have one tuple")
	}
	var b bool
	gtuple, getSucceed := Get(*ptp, "hello", &b)
	b = gtuple.GetFieldAt(1).(bool)

	if !(ts.Size() == 0) {
		t.Errorf("Tuple space is not empty")
	}
	if b || !(s == "hello") || !getSucceed || !querySucceed {
		t.Errorf("Get or Query Failed")
	}
}

func TestPutPUtilities(t *testing.T) {
	_, ts := NewSpaceAlt("9053")
	if !(ts.Size() == 0) {
		t.Errorf("Tuple space is not empty")
	}
	ptp := protocol.CreatePointToPoint("Bookstore", "localhost", "9053", nil, nil)
	PutP(*ptp, "hello", false)
	var b bool
	Get(*ptp, "hello", &b)
	if b {
		t.Errorf("PutP Failed")
	}
}

func TestQueryPAndGetPUtilities(t *testing.T) {
	_, ts := NewSpaceAlt("9052")
	if !(ts.Size() == 0) {
		t.Errorf("Tuple space is not empty")
	}
	ptp := protocol.CreatePointToPoint("Bookstore", "localhost", "9052", nil, nil)
	Put(*ptp, "hello", false)

	var s string
	qtuple, queryPResult, queryPSucceed := QueryP(*ptp, &s, false)
	s = qtuple.GetFieldAt(0).(string)
	if !(ts.Size() == 1) {
		t.Errorf("Tuple space should have one tuple")
	}

	var b bool
	gtuple, getPResult, getPSucceed := GetP(*ptp, "hello", &b)
	b = gtuple.GetFieldAt(1).(bool)

	if !(ts.Size() == 0) {
		t.Errorf("Tuple space is not empty")
	}

	if b || !(s == "hello") || !getPSucceed || !queryPSucceed {
		t.Errorf("GetP or QueryP Failed")
	}

	if !getPResult || !queryPResult {
		t.Errorf("GetP or QueryP returned wrong boolean")
	}

	qtuple, queryPResult, queryPSucceed = QueryP(*ptp, &s, false)
	gtuple, getPResult, getPSucceed = GetP(*ptp, "hello", &b)

	if getPResult || queryPResult {
		t.Errorf("GetP or QueryP returned wrong boolean")
	}
}

func TestGetAllAndQueryAll(t *testing.T) {
	_, ts := NewSpaceAlt("9054")
	if !(ts.Size() == 0) {
		t.Errorf("Tuple space is not empty")
	}
	ptp := protocol.CreatePointToPoint("Bookstore", "localhost", "9054", nil, nil)
	Put(*ptp, 2, 2)
	Put(*ptp, 2, 2)
	Put(*ptp, 2, 3)
	Put(*ptp, 2, 3)
	Put(*ptp, 2, false)
	i := 1
	tuples, b := QueryAll(*ptp, 2, 2)
	tuple1 := container.NewTuple(2, 2)
	expectedTuples := []container.Tuple{tuple1, tuple1}
	if !reflect.DeepEqual(tuples, expectedTuples) {
		t.Errorf("QueryAll returned wrong tuple list %v - %v", tuples, expectedTuples)
	}
	if !b {
		t.Errorf("QueryAll returned wrong boolean")
	}
	tuple2 := container.NewTuple(2, 3)
	expectedTuples = []container.Tuple{tuple1, tuple1, tuple2, tuple2}
	tuples, b = GetAll(*ptp, 2, &i)
	if !reflect.DeepEqual(tuples, expectedTuples) {
		t.Errorf("GetAll returned wrong tuple list %v - %v", tuples, expectedTuples)
	}
	if !b {
		t.Errorf("GetAll returned wrong boolean")
	}
	if i != 1 {
		t.Errorf("i was overwritten")
	}
}

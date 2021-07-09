package schema

//go:generate stringer -type=Operation
//go:generate msgp

type Operation uint8

const (
	OpRemove  Operation = iota // Remove metric definition from index
	OpArchive                  // Archive metric definition
	OpRestore                  // Restore deleted/archived metric definition
)

// ControlMsg is a message to perform some operation on the specified definition
type ControlMsg struct {
	Defs []MetricDefinition
	Op   Operation
}

func (c *ControlMsg) Valid() bool {
	return c.Op <= OpRestore
}

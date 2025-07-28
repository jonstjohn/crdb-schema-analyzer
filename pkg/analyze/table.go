package analyze

import "fmt"

type Table struct {
	Database          string
	Name              string
	LogicalSizeBytes  uint64
	Owner             string
	EstimatedRowCount int
	Locality          string
	FKs               []FKConstraint
	ReferencedFKs     []FKConstraint
}

func (t Table) String() string {
	bytesPerRow := uint64(0)
	if t.EstimatedRowCount > 0 {
		bytesPerRow = t.LogicalSizeBytes / uint64(t.EstimatedRowCount)
	}
	return fmt.Sprintf("Database: %s, Name: %s, Locality: %s, Logical Size: %s, Row Count: %d, Avg Row Size: %s, FKs: %d, Referenced FKs: %d",
		t.Database, t.Name, t.Locality, formatBytes(t.LogicalSizeBytes), t.EstimatedRowCount, formatBytes(uint64(bytesPerRow)),
		len(t.FKs), len(t.ReferencedFKs))
}

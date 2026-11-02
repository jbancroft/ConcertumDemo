package minuetsql

type planNode interface{ isPlan() }

type createTablePlan struct{ name string }
type insertPlan struct {
	table string
	row   Row
}
type selectPlan struct {
	table string
	byID  bool
	id    uint64
}
type deletePlan struct {
	table string
	id    uint64
}

func (createTablePlan) isPlan() {}
func (insertPlan) isPlan()      {}
func (selectPlan) isPlan()      {}
func (deletePlan) isPlan()      {}

func Plan(stmt statement) (planNode, error) {
	switch n := stmt.(type) {
	case createTableStmt:
		return createTablePlan(n), nil
	case insertStmt:
		return insertPlan(n), nil
	case selectStmt:
		return selectPlan(n), nil
	case deleteStmt:
		return deletePlan(n), nil
	default:
		return nil, ErrUnsupportedSQL
	}
}

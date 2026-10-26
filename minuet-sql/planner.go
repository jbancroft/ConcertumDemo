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
		return createTablePlan{name: n.name}, nil
	case insertStmt:
		return insertPlan{table: n.table, row: n.row}, nil
	case selectStmt:
		return selectPlan{table: n.table, byID: n.byID, id: n.id}, nil
	case deleteStmt:
		return deletePlan{table: n.table, id: n.id}, nil
	default:
		return nil, ErrUnsupportedSQL
	}
}

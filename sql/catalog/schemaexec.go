package catalog

type CreateTableExec struct {
	Table *Table
}

func (c *CreateTableExec) Execute(txn Transaction) (ResultSet, error) {
	err := txn.CreateTable(c.Table)
	if err != nil {
		return nil, err
	}
	return &CreateTableResultSet{
		Name: c.Table.Name,
	}, nil
}

type DropTableExec struct {
	Table string
}

func (d *DropTableExec) Execute(txn Transaction) (ResultSet, error) {
	err := txn.DeleteTable(d.Table)
	if err != nil {
		return nil, err
	}
	return &DropTableResultSet{
		Name: d.Table,
	}, nil
}

package mapreduce

func SwitchTable(out Writer, tableIndex int) error {
	var switchTable struct {
		TableIndex int       `yson:"table_index,attr"`
		Value      *struct{} `yson:",value"`
	}

	switchTable.TableIndex = tableIndex
	return out.Write(switchTable)
}

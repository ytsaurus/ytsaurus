package storage

import (
	"fmt"
	"strings"

	"a.yandex-team.ru/yt/go/ytprof"
)

func (m *TableStorage) queryIDsMetadataPeriod(minTime int64, maxTime int64, limit int) string {
	return fmt.Sprintf("ProfIDHigh, ProfIDLow from [%s] where Timestamp >= %d and Timestamp <= %d limit %d",
		m.tableMetadata,
		minTime,
		maxTime,
		limit,
	)
}

func (m *TableStorage) queryMetadataPeriod(minTime int64, maxTime int64, limit int) string {
	return fmt.Sprintf("* from [%s] where Timestamp >= %d and Timestamp <= %d limit %d",
		m.tableMetadata,
		minTime,
		maxTime,
		limit,
	)
}

func (m *TableStorage) queryLookupData(IDs []ytprof.ProfID, limit int) string {
	var idTuples []string
	for _, id := range IDs {
		idTuples = append(idTuples, fmt.Sprintf("(%d, %d)", id.ProfIDHigh, id.ProfIDLow))
	}

	return fmt.Sprintf("* from [%s] where (ProfIDHigh, ProfIDLow) in (%s) limit %d",
		m.tableData,
		strings.Join(idTuples, ","),
		limit,
	)
}

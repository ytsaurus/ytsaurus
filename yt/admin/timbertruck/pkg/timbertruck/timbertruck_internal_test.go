package timbertruck

import (
	"fmt"
	"os"
	"path"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestStagedName(t *testing.T) {
	createTime := time.Time(time.Date(1992, time.September, 21, 8, 0, 0, 0, time.UTC))
	uuid := uuid.UUID([16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15})
	tempName := tempStagedName(createTime, uuid, ".txt")

	fmt.Fprintf(os.Stderr, "%v\n", tempName)

	parsedTime, isFinal := parseStagedPath(tempName, 100500)
	require.Equal(t, createTime, parsedTime)
	require.Equal(t, false, isFinal)

	parsedTime, isFinal = parseStagedPath(path.Join("/tmp", tempName), 100500)
	require.Equal(t, createTime, parsedTime)
	require.Equal(t, false, isFinal)

	finalName := finalStagedName(createTime, 100500, ".txt")
	parsedTime, isFinal = parseStagedPath(finalName, 100500)
	require.Equal(t, createTime, parsedTime)
	require.Equal(t, true, isFinal)

	parsedTime, isFinal = parseStagedPath(path.Join("/tmp", finalName), 100500)
	require.Equal(t, createTime, parsedTime)
	require.Equal(t, true, isFinal)

	parsedTime, isFinal = parseStagedPath(path.Join("/tmp", finalName), 100600)
	require.Equal(t, createTime, parsedTime)
	require.Equal(t, false, isFinal)
}

func TestPeriodMaxKeepsValueForOnePeriod(t *testing.T) {
	var m periodMax
	m.observe(10)
	m.observe(5)
	require.EqualValues(t, 10, m.value())

	m.rotate()
	m.observe(7)
	require.EqualValues(t, 10, m.value())

	m.rotate()
	require.EqualValues(t, 7, m.value())

	m.rotate()
	require.EqualValues(t, 0, m.value())
}

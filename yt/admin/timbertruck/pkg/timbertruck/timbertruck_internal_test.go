package timbertruck

import (
	"path"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestStagedName(t *testing.T) {
	createTime := time.Time(time.Date(1992, time.September, 21, 8, 0, 0, 0, time.UTC))
	uuid := uuid.UUID([16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15})

	stagedName := tempStagedName(createTime, uuid, ".txt")
	require.Equal(t, isTempStagedPath(stagedName), true)
	require.Equal(t, isTempStagedPath(path.Join("/tmp", stagedName)), true)
}

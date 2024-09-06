package timbertruck

import (
	"database/sql"
	_ "embed"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

//go:embed version1.txt
var version1Dump string

func TestMigrationFrom1(t *testing.T) {
	dir := t.TempDir()

	dbFile := path.Join(dir, "db.sqlite")

	func() {
		sqlite, err := sql.Open("sqlite3", dbFile)
		require.NoError(t, err)

		_, err = sqlite.Exec(version1Dump)
		require.NoError(t, err)

		err = sqlite.Close()
		require.NoError(t, err)
	}()

	ds, err := NewDatastore(dbFile)
	require.NoError(t, err)

	task, err := ds.TaskByPath("/proxy-logs/timbertruck/http-proxy-heh.json.log/staging/2024-09-03T21:00:06_ino:251534293.json.log")
	require.NoError(t, err)

	require.Equal(t, time.Date(2024, time.September, 3, 21, 15, 11, 395907779, time.FixedZone("+03:00", 3600*3)).UTC(), task.CompletionTime.UTC())

	task, err = ds.TaskByPath("/proxy-logs/timbertruck/http-proxy-heh.json.log/staging/2024-09-04T00:15:06_ino:251534482.json.log")
	require.NoError(t, err)

	require.Equal(t, time.Time{}, task.CompletionTime.UTC())

	err = ds.Close()
	require.NoError(t, err)

	backup, err := sql.Open("sqlite3", dbFile+".bak")
	require.NoError(t, err)

	var rowCount int
	err = backup.QueryRow("SELECT COUNT(*) FROM Tasks;").Scan(&rowCount)
	require.NoError(t, err)

	require.Equal(t, 14, rowCount)

	err = backup.Close()
	require.NoError(t, err)
}

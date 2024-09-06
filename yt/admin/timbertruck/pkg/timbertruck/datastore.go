package timbertruck

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"go.ytsaurus.tech/yt/admin/timbertruck/pkg/pipelines"
)

const timeLayout = time.RFC3339Nano

type Task struct {
	// Name of the stream this task belongs to.
	StreamName string

	// Name of the file in the internal storage.
	// i.e. it is hardlinked name of the file
	StagedPath string

	// Inode of the file.
	INode int64

	// Approximate task creation time.
	// Used to traverse task in order of their creation.
	CreationTime time.Time

	// Position of the first item that wasn't send yet
	EndPosition pipelines.FilePosition

	BoundTime time.Time

	CompletionTime time.Time
}

type rowScanner interface {
	Scan(dest ...any) error
}

func parseTime(s string) (t time.Time) {
	t, errParse := time.Parse(timeLayout, s)
	if errParse != nil {
		t = time.Time{}
	}
	return
}

func parseTask(scanner rowScanner, task *Task) (err error) {
	var createTimestamp string
	var boundTimestamp sql.NullString
	var completeTimestamp sql.NullString
	var endOffsetJSON []byte
	var iNode sql.NullInt64
	err = scanner.Scan(
		&task.StreamName,
		&task.StagedPath,
		&iNode,
		&createTimestamp,
		&endOffsetJSON,
		&boundTimestamp,
		&completeTimestamp,
	)
	if err == sql.ErrNoRows {
		err = ErrNotFound
		return
	} else if err != nil {
		panic(fmt.Sprintf("internal error: cannot decode task: %v", err))
	}

	if iNode.Valid {
		task.INode = iNode.Int64
	}

	task.CreationTime = parseTime(createTimestamp)
	if boundTimestamp.Valid {
		task.BoundTime = parseTime(boundTimestamp.String)
	}
	if completeTimestamp.Valid {
		task.CompletionTime = parseTime(completeTimestamp.String)
	}
	_ = json.Unmarshal(endOffsetJSON, &task.EndPosition)
	return
}

type Datastore struct {
	sqlite *sql.DB
}

var ErrNotFound = errors.New("not found")

func NewDatastore(fileName string) (db *Datastore, err error) {
	sqlite, err := sql.Open("sqlite3", fileName)
	if err != nil {
		return
	}

	// Avoid "database is locked" errors
	sqlite.SetMaxOpenConns(1)

	err = sqlite.Ping()
	if err != nil {
		return
	}

	db = &Datastore{sqlite}
	err = db.updateSchema(fileName + ".bak")

	if err != nil {
		return
	}

	err = db.resetPeeked()

	return
}

func (ds *Datastore) AddTask(task *Task) (err error) {
	timestamp := task.CreationTime.Format(timeLayout)
	var boundTimestamp sql.NullString
	if !task.BoundTime.IsZero() {
		boundTimestamp.String = task.BoundTime.Format(timeLayout)
		boundTimestamp.Valid = true
	}

	if !task.CompletionTime.IsZero() {
		panic("cannot add completed task")
	}

	endOffsetJSON := mustMarshalFilePosition(task.EndPosition)

	_, err = ds.sqlite.Exec(`
			INSERT INTO Tasks (
				StreamName,
				StagedPath,
				INode,
				CreationTime,
				EndPosition,
				BoundTime,
				CompletionTime
			) VALUES (?, ?, ?, ?, ?, ?, NULL);
		`,
		task.StreamName,
		task.StagedPath,
		task.INode,
		timestamp,

		endOffsetJSON,
		boundTimestamp,
	)
	return
}

func (ds *Datastore) ListActiveTasks() (tasks []Task, err error) {
	rows, err := ds.sqlite.Query(`
			SELECT
				StreamName,
				StagedPath,
				INode,
				CreationTime,
				EndPosition,
				BoundTime,
				CompletionTime
			FROM Tasks
			WHERE CompletionTime IS NULL
		`)
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		var t Task
		err = parseTask(rows, &t)
		if err != nil {
			return
		}
		tasks = append(tasks, t)
	}
	return
}

func (ds *Datastore) CompleteTask(stagedPath string, completionTime time.Time, taskError error) error {
	completionTimeStr := completionTime.Format(timeLayout)
	taskErrorStr := fmt.Sprintf("%v", taskError)
	result, err := ds.sqlite.Exec(`
			UPDATE Tasks
			SET
				CompletionTime = ?,
				Error = ?,
				Inode = NULL
			WHERE StagedPath = ?
		`,
		completionTimeStr,
		taskErrorStr,
		stagedPath,
	)
	if err != nil {
		return err
	}
	return checkSingleRowAffected(result)
}

func (ds *Datastore) CleanupOldCompletedTasks(completionTime time.Time) error {
	completionTimeStr := completionTime.Format(timeLayout)

	_, err := ds.sqlite.Exec(`
			DELETE FROM Tasks
			WHERE CompletionTime <= ?
		`,
		completionTimeStr,
	)
	return err
}

func (ds *Datastore) ActiveTaskByIno(ino int64, streamName string) (task Task, err error) {
	row := ds.sqlite.QueryRow(`
			SELECT
				StreamName,
				StagedPath,
				INode,
				CreationTime,
				EndPosition,
				BoundTime,
				CompletionTime
			FROM Tasks
			WHERE
				INode = ?
				AND StreamName = ?
		`,
		ino,
		streamName,
	)

	err = parseTask(row, &task)
	if err != nil {
		return
	}
	if !task.CompletionTime.IsZero() {
		panic("internal error: expected active task")
	}
	return
}

func (ds *Datastore) TaskByPath(path string) (task Task, err error) {
	row := ds.sqlite.QueryRow(`
			SELECT
				StreamName,
				StagedPath,
				INode,
				CreationTime,
				EndPosition,
				BoundTime,
				CompletionTime
			FROM Tasks
			WHERE StagedPath = ?
		`,
		path,
	)

	err = parseTask(row, &task)
	return
}

// Peek the task with the smallest timestamp
// return `ErrNotFound` if no task is available
func (ds *Datastore) PeekNextTask(streamName string) (task Task, err error) {
	row := ds.sqlite.QueryRow(`
			SELECT
				StreamName,
				StagedPath,
				INode,
				CreationTime,
				EndPosition,
				BoundTime,
				CompletionTime
			FROM Tasks
			WHERE StreamName = ? AND Peeked = 0 AND CompletionTime IS NULL
			ORDER BY CreationTime
			LIMIT 1
		`,
		streamName,
	)

	err = parseTask(row, &task)
	return
}

func (ds *Datastore) SetPeeked(streamName, fileName string) (err error) {
	result, err := ds.sqlite.Exec(`
			UPDATE Tasks
			SET Peeked = 1
			WHERE StreamName = ? AND StagedPath = ?
		`,
		streamName,
		fileName,
	)
	if err != nil {
		return
	}
	return checkSingleRowAffected(result)
}

func (ds *Datastore) ResetUnboundTask(streamName string, ino int64, boundTime time.Time) (err error) {
	boundTimeString := boundTime.Format(timeLayout)
	_, err = ds.sqlite.Exec(`
			BEGIN;

			UPDATE Tasks
			SET
				BoundTime = ?
			WHERE
				StreamName = ?
				AND Inode != ?
				AND BoundTime IS NULL
				AND CompletionTime IS NULL
			;

			UPDATE Tasks
			Set
				BoundTime = NULL
			WHERE
				StreamName = ?
				AND Inode = ?
				AND CompletionTime IS NULL;

			COMMIT;
		`,
		boundTimeString,
		streamName,
		ino,
		streamName,
		ino,
	)
	return
}

func (ds *Datastore) BoundAllTasks(streamName string, boundTime time.Time) (err error) {
	boundTimeString := boundTime.Format(timeLayout)
	_, err = ds.sqlite.Exec(`
		UPDATE Tasks
		SET
			BoundTime = ?
		WHERE
			StreamName = ?
			AND BoundTime IS NULL;
		`,
		boundTimeString,
		streamName,
	)
	return
}

func (ds *Datastore) UpdateEndPosition(stagedPath string, pos pipelines.FilePosition) (err error) {
	posJSON := mustMarshalFilePosition(pos)

	execResult, err := ds.sqlite.Exec(`
			UPDATE Tasks
			SET
				EndPosition = ?
			WHERE
				StagedPath = ?
			;
		`,
		posJSON,
		stagedPath,
	)
	if err != nil {
		panic(fmt.Sprintf("internal error: cannot update EndPosition: %v", err))
	}
	return checkSingleRowAffected(execResult)
}

func (ds *Datastore) withTransaction(fn func(*sql.Tx) error) (err error) {
	tx, err := ds.sqlite.Begin()
	if err != nil {
		return
	}
	defer tx.Rollback()

	err = fn(tx)
	if err != nil {
		return
	}
	err = tx.Commit()
	return
}

func (ds *Datastore) updateSchema(backupFile string) (err error) {
	schemaVersion := -1
	err = ds.sqlite.QueryRow("PRAGMA schema_version;").Scan(&schemaVersion)
	if err != nil {
		err = fmt.Errorf("cannot get schema_version: %w", err)
		return
	}

	userVersion := -1
	err = ds.sqlite.QueryRow("PRAGMA user_version;").Scan(&userVersion)
	if err != nil {
		err = fmt.Errorf("cannot get user_version: %w", err)
		return
	}

	backup := func() (err error) {
		_, err = ds.sqlite.Exec("VACUUM INTO ?;", backupFile)
		if err != nil {
			err = fmt.Errorf("cannot create backup into %s: %w", backupFile, err)
			return
		}
		return
	}

	createTable := func(tx *sql.Tx) (err error) {
		_, err = tx.Exec(`
			CREATE TABLE Tasks (
				TaskId INTEGER PRIMARY KEY,

				StreamName TEXT NOT NULL,
				StagedPath TEXT NOT NULL,
				Inode      INTEGER,

				CreationTime TEXT NOT NULL,
				BoundTime    TEXT,

				CompletionTime TEXT,
				Error          TEXT,

				EndPosition  TEXT NOT NULL,

				Peeked INTEGER NOT NULL DEFAULT 0,

				UNIQUE (Inode, StreamName),
				UNIQUE (StagedPath)
			);
			CREATE INDEX Tasks__Path ON Tasks (StagedPath);
			CREATE INDEX Tasks__StreamName_CreateTime ON Tasks (StreamName, CreationTime);
			CREATE INDEX Tasks__CompletionTime ON Tasks (CompletionTime);
			CREATE INDEX Tasks__Inode ON Tasks (Inode);

			PRAGMA user_version = 2;
		`)
		return
	}

	if userVersion == 0 && schemaVersion > 0 {
		// First version of our schema had a bug when we tried to update schema_version instead of user_version.
		userVersion = 1
	}

	switch userVersion {
	case 0:
		err = ds.withTransaction(createTable)
	case 1:
		err = backup()
		if err != nil {
			return
		}
		err = ds.withTransaction(func(tx *sql.Tx) error {
			_, err = tx.Exec(`
				ALTER TABLE Tasks RENAME TO TasksOld;
				DROP INDEX Tasks__Path;
				DROP INDEX Tasks__StreamName_CreateTime;
				DROP INDEX Tasks__CompletionTime;
				DROP INDEX Tasks__Inode;
			`)
			if err != nil {
				return err
			}

			err = createTable(tx)
			if err != nil {
				return err
			}

			_, err = tx.Exec(`
				INSERT INTO Tasks SELECT * FROM TasksOld;
				DROP TABLE TasksOld;
			`)
			if err != nil {
				return err
			}

			return nil
		})
	case 2:
		// Current schema version, do nothing
	default:
		err = fmt.Errorf("unknown schema_version: %v", userVersion)
	}

	return
}

func (ds *Datastore) resetPeeked() (err error) {
	_, err = ds.sqlite.Exec("UPDATE Tasks SET Peeked = 0;")
	return
}

func (ds *Datastore) Close() error {
	return ds.sqlite.Close()
}

func mustMarshalFilePosition(pos pipelines.FilePosition) []byte {
	result, err := json.Marshal(pos)
	if err != nil {
		panic(fmt.Sprintf("internal error: cannot marshal FilePosition: %v", err))
	}
	return result
}

func checkSingleRowAffected(sqlResult sql.Result) error {
	rowsAffected, err := sqlResult.RowsAffected()
	if err != nil {
		panic(fmt.Sprintf("internal error: cannot get rows affected: %v", err))
	}
	switch rowsAffected {
	case 0:
		return ErrNotFound
	case 1:
		return nil
	default:
		return fmt.Errorf("multiple rows affected")
	}
}

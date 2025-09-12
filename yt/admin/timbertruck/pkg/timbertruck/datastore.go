package timbertruck

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	sqlite3 "github.com/mattn/go-sqlite3"

	"go.ytsaurus.tech/yt/admin/timbertruck/pkg/pipelines"
)

const datastoreTimeLayout = time.RFC3339Nano

var ErrTaskLimitExceeded = errors.New("task limit exceeded")

func taskLimitExceededByCount(current, limit int) error {
	if current < limit {
		panic(fmt.Sprintf("internal error: current=%v < limit=%v", current, limit))
	}
	return fmt.Errorf("%w, too many tasks: %v (limit: %v)", ErrTaskLimitExceeded, current, limit)
}

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
	t, errParse := time.Parse(datastoreTimeLayout, s)
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
	} else if isDatabaseLocked(err) {
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
	logger *slog.Logger
}

var ErrNotFound = errors.New("not found")

func NewDatastore(logger *slog.Logger, fileName string) (db *Datastore, err error) {
	dsn := fmt.Sprintf("%s?_busy_timeout=%d&_journal_mode=WAL", fileName, 5000)
	sqlite, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return
	}

	sqlite.SetMaxOpenConns(4)
	sqlite.SetMaxIdleConns(4)

	err = sqlite.Ping()
	if err != nil {
		return
	}

	db = &Datastore{sqlite: sqlite, logger: logger}
	err = db.updateSchema(fileName + ".bak")

	if err != nil {
		return
	}

	err = db.resetPeeked()

	return
}

// Add task to the datastore.
//
// Return TaskLimitExceeded if task cannot be added due to limits.
// May emit warnings if limits can be exceeded soon.
func (ds *Datastore) AddTask(task *Task, maxActiveTasks int) (warns []error, err error) {
	timestamp := task.CreationTime.Format(datastoreTimeLayout)
	var boundTimestamp sql.NullString
	if !task.BoundTime.IsZero() {
		boundTimestamp.String = task.BoundTime.Format(datastoreTimeLayout)
		boundTimestamp.Valid = true
	}

	if !task.CompletionTime.IsZero() {
		panic("cannot add completed task")
	}

	endOffsetJSON := mustMarshalFilePosition(task.EndPosition)

	err = ds.withRetry(func() error {
		return ds.withTransaction(func(tx *sql.Tx) (err error) {
			row := tx.QueryRow(`
			SELECT
				COUNT(*)
			FROM
				Tasks
			WHERE
				CompletionTime IS NULL
				AND StreamName = ?
			;
		`, task.StreamName)
			var activeTaskCount int
			err = row.Scan(&activeTaskCount)
			if err != nil {
				err = fmt.Errorf("internal error: cannot scan active task count: %v", err)
				return
			}
			if activeTaskCount >= maxActiveTasks {
				return taskLimitExceededByCount(activeTaskCount, maxActiveTasks)
			} else if activeTaskCount >= maxActiveTasks*80/100 {
				warns = append(warns, fmt.Errorf(
					"limit of the active tasks will be reached soon, active tasks: %v (limit: %v)",
					activeTaskCount,
					maxActiveTasks,
				))
			}

			_, err = tx.Exec(`
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
		})
	})
	return
}

func (ds *Datastore) ListActiveTasks() (tasks []Task, err error) {
	err = ds.withRetry(func() error {
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
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var t Task
			if err := parseTask(rows, &t); err != nil {
				return err
			}
			tasks = append(tasks, t)
		}
		return nil
	})
	return
}

func (ds *Datastore) CompleteTask(stagedPath string, completionTime time.Time, taskError error) error {
	completionTimeStr := completionTime.Format(datastoreTimeLayout)
	taskErrorStr := fmt.Sprintf("%v", taskError)
	return ds.withRetry(func() error {
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
	})
}

func (ds *Datastore) CleanupOldCompletedTasks(completionTime time.Time) error {
	completionTimeStr := completionTime.Format(datastoreTimeLayout)

	return ds.withRetry(func() error {
		_, err := ds.sqlite.Exec(`
			DELETE FROM Tasks
			WHERE CompletionTime <= ?
		`,
			completionTimeStr,
		)
		return err
	})
}

func (ds *Datastore) ActiveTaskByIno(ino int64, streamName string) (task Task, err error) {
	err = ds.withRetry(func() error {
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
		return parseTask(row, &task)
	})
	if !task.CompletionTime.IsZero() {
		panic("internal error: expected active task")
	}
	return
}

func (ds *Datastore) TaskByPath(path string) (task Task, err error) {
	err = ds.withRetry(func() error {
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
		return parseTask(row, &task)
	})
	return
}

// Peek the task with the smallest timestamp
// return `ErrNotFound` if no task is available
func (ds *Datastore) PeekNextTask(streamName string) (task Task, err error) {
	err = ds.withRetry(func() error {
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
		return parseTask(row, &task)
	})
	return
}

func (ds *Datastore) SetPeeked(streamName, fileName string) error {
	return ds.withRetry(func() error {
		result, err := ds.sqlite.Exec(`
			UPDATE Tasks
			SET Peeked = 1
			WHERE StreamName = ? AND StagedPath = ?
		`,
			streamName,
			fileName,
		)
		if err != nil {
			return err
		}
		return checkSingleRowAffected(result)
	})
}

func (ds *Datastore) ResetUnboundTask(streamName string, ino int64, boundTime time.Time) error {
	boundTimeString := boundTime.Format(datastoreTimeLayout)
	return ds.withRetry(func() error {
		return ds.withTransaction(func(tx *sql.Tx) error {
			_, err := tx.Exec(`
                UPDATE Tasks
                SET BoundTime = ?
                WHERE
                    StreamName = ?
                    AND Inode != ?
                    AND BoundTime IS NULL
                    AND CompletionTime IS NULL
                ;`,
				boundTimeString, streamName, ino)
			if err != nil {
				return err
			}
			_, err = tx.Exec(`
                UPDATE Tasks
                SET BoundTime = NULL
                WHERE
                    StreamName = ?
                    AND Inode = ?
                    AND CompletionTime IS NULL
                ;`,
				streamName, ino)
			return err
		})
	})
}

func (ds *Datastore) BoundAllTasks(streamName string, boundTime time.Time) error {
	boundTimeString := boundTime.Format(datastoreTimeLayout)
	return ds.withRetry(func() error {
		_, err := ds.sqlite.Exec(`
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
		return err
	})
}

func (ds *Datastore) UpdateEndPosition(stagedPath string, pos pipelines.FilePosition) error {
	posJSON := mustMarshalFilePosition(pos)
	return ds.withRetry(func() error {
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
			return fmt.Errorf("cannot update EndPosition: %v", err)
		}
		return checkSingleRowAffected(execResult)
	})
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

func (ds *Datastore) resetPeeked() error {
	return ds.withRetry(func() error {
		_, err := ds.sqlite.Exec("UPDATE Tasks SET Peeked = 0;")
		return err
	})
}

func (ds *Datastore) Close() error {
	return ds.sqlite.Close()
}

// Retry wrapper for database locked
func (ds *Datastore) withRetry(fn func() error) error {
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = 50 * time.Millisecond
	b.MaxInterval = 5 * time.Second
	b.MaxElapsedTime = 5 * time.Minute

	return backoff.RetryNotify(func() error {
		err := fn()
		if err == nil {
			return nil
		}
		if isDatabaseLocked(err) {
			return err
		}
		return backoff.Permanent(err)
	}, b, ds.notifyRetryableError)
}

func (ds *Datastore) notifyRetryableError(err error, nextWait time.Duration) {
	ds.logger.Warn("Datastore error, will retry",
		"error", err, "error_struct", fmt.Sprintf("%#v", err), "backoff", nextWait.String())
}

func isDatabaseLocked(err error) bool {
	if err == nil {
		return false
	}
	var sqliteErr sqlite3.Error
	if errors.As(err, &sqliteErr) {
		return sqliteErr.Code == sqlite3.ErrBusy || sqliteErr.Code == sqlite3.ErrLocked
	}
	// Fallback: check for error message in case error type is not sqlite3.Error.
	if strings.Contains(err.Error(), "database is locked") || strings.Contains(err.Error(), "database table is locked") {
		return true
	}
	return false
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

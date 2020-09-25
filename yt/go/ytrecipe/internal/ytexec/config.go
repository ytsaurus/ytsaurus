package ytexec

import (
	"time"

	"a.yandex-team.ru/yt/go/ytrecipe/internal/job"
	"a.yandex-team.ru/yt/go/ytrecipe/internal/jobfs"
)

type ExecConfig struct {
	// PreparedFile пишется, после того как ytexec запустил операцию.
	PreparedFile string `json:"prepared_file" yson:"prepared_file"`
	// ResultFile пишется в конце работы ytexec.
	ResultFile string `json:"result_file" yson:"result_file"`

	ReadmeFile     string `json:"readme_file" yson:"readme_file"`
	DownloadScript string `json:"download_script" yson:"download_script"`

	ExecLog  string `json:"exec_log" yson:"exec_log"`
	JobLog   string `json:"job_log" yson:"job_log"`
	DmesgLog string `json:"dmesg_log" yson:"dmesg_log"`

	// YTTokenEnv задаёт имя переменной окружения, из которой нужно прочитать YT токен.
	YTTokenEnv string `json:"yt_token_env" yson:"yt_token_env"`
}

// Config хранит все настройки работы ytexec.
type Config struct {
	// Exec задаёт общий конфиг для ytexec.
	Exec ExecConfig `json:"exec" yson:"exec"`

	// Operation описывает параметры запуска операции.
	Operation job.OperationConfig `json:"operation" yson:"operation"`

	// Cmd описывает команду запуска.
	Cmd job.Cmd `json:"cmd" yson:"cmd"`

	// FS описывает файловую систему.
	//
	// ytexec загружает все файлы, симлинки и директории на YT, и воссоздаёт внутри джоба файловую
	// систему с такими же путями как на оригинальной машине. При передаче файлов, сохраняется
	// executable бит. read, write доступы, extended атрибуты, и file onwers не сохраняются.
	// При передаче core файлов, сохраняются sparse-дырки.
	FS jobfs.Config `json:"fs" yson:"fs"`
}

type PreparedFile struct {
	OperationID  string `json:"operation_id" yson:"operation_id"`
	OperationURL string `json:"operation_url" yson:"operation_url"`
}

type Statistics struct {
	UploadTime     time.Duration `json:"upload_time" yson:"upload_time"`
	SchedulingTime time.Duration `json:"scheduling_time" yson:"scheduling_time"`
	UnpackingTime  time.Duration `json:"unpacking_time" yson:"unpacking_time"`
	ExecutionTime  time.Duration `json:"execution_time" yson:"execution_time"`
	DownloadTime   time.Duration `json:"download_time" yson:"download_time"`
}

// ResultFile пишется в конце работы ytexec.
type ResultFile struct {
	ExitCode   int  `json:"exit_code" yson:"exit_code"`
	ExitSignal int  `json:"exit_signal" yson:"exit_signal"`
	IsOOM      bool `json:"is_oom" yson:"is_oom"`

	Statistics Statistics `json:"statistics" yson:"statistics"`
}

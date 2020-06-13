package ytexec

import (
	"time"

	"a.yandex-team.ru/yt/go/ytrecipe/internal/job"
	"a.yandex-team.ru/yt/go/ytrecipe/internal/jobfs"
)

type ExecConfig struct {
	PrepareFile    string `json:"prepared_file"`
	ReadmeFile     string `json:"readme_file"`
	DownloadScript string `json:"download_script"`
	ResultFile     string `json:"result_file"`

	ExecLog string `json:"exec_log"`
	JobLog  string `json:"job_log"`

	YTTokenEnv string `json:"yt_token_env"`
}

type Config struct {
	Exec      ExecConfig          `json:"exec"`
	Operation job.OperationConfig `json:"operation"`
	Cmd       job.Cmd             `json:"cmd"`
	FS        jobfs.Config        `json:"fs"`
}

type PreparedFile struct {
	OperationID  string `json:"operation_id"`
	OperationURL string `json:"operation_url"`
}

type Statistics struct {
	UploadTime     time.Duration `json:"upload_time"`
	SchedulingTime time.Duration `json:"scheduling_time"`
	UnpackingTime  time.Duration `json:"unpacking_time"`
	ExecutionTime  time.Duration `json:"execution_time"`
	DownloadTime   time.Duration `json:"download_time"`
}

type ResultFile struct {
	ExitCode   int  `json:"exit_code"`
	ExitSignal int  `json:"exit_signal"`
	IsOOM      bool `json:"is_oom"`

	Statistics Statistics `json:"statistics"`
}

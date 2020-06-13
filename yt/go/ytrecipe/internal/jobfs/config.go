package jobfs

type Config struct {
	UploadFile      []string `json:"upload_file"`
	UploadTarDir    []string `json:"upload_tar"`
	UploadStructure []string `json:"upload_structure"`

	StdoutFile string `json:"stdout_file"`
	StderrFile string `json:"stderr_file"`

	Outputs     []string `json:"outputs"`
	YTOutputs   []string `json:"yt_outputs"`
	CoredumpDir string   `json:"coredump_dir"`

	Ext4Dirs []string `json:"ext4_dirs"`

	Download map[string]string `json:"download"`
}

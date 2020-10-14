package job

import (
	"time"

	"a.yandex-team.ru/yt/go/ypath"
)

const (
	DefaultBaseLayer = ypath.Path("//porto_layers/base/xenial/porto_layer_search_ubuntu_xenial_app_lastest.tar.gz")

	MemoryReserve = 128 * (1 << 20)
	TmpfsReserve  = 256 * (1 << 20)

	OperationTimeout = time.Hour * 3
)

type OperationConfig struct {
	Cluster string `json:"cluster" yson:"cluster"`
	Pool    string `json:"pool" yson:"pool"`

	// Title операции. Должен быть заполнен строкой, по которой будет удобно искать операцию в архиве.
	//
	// Например: [TS] yt/yt/tests/integration/ytrecipe [test_layers.py] [0/3]
	Title string `json:"title" yson:"title"`

	// CypressRoot задаёт директорию для хранения всех данных.
	CypressRoot ypath.Path    `json:"cypress_root" yson:"cypress_root"`
	OutputTTL   time.Duration `json:"output_ttl" yson:"output_ttl"`
	BlobTTL     time.Duration `json:"blob_ttl" yson:"blob_ttl"`

	// CoordinateUpload включает оптимизированную загрузку артефактов.
	//
	// Обычная загрузка не координирует загрузку одного и того же файла между несколькими процессами.
	//
	// Если запустить одновременно 100 процессов ytexec с новым файлом, которого еще нет в кеше,
	// то все 100 пойдут загружать один и тот же файл в кипарис. В случае координации, процессы договорятся между
	// собой используя дин-таблицу. Один процесс займётся загрузкой, а 99 будут ждать результата.
	CoordinateUpload bool `json:"coordinate_upload" yson:"coordinate_upload"`

	// CPULimit будет поставлен в спеку операции.
	//
	// При этом, CPU reclaim для этой операции будет выключен. Это значит, что у джоба не будут забирать CPU,
	// если он потребляет меньше лимита.
	CPULimit float64 `json:"cpu_limit" yson:"cpu_limit"`

	// MemoryLimit задаёт максимальное потребление памяти в байтах.
	//
	// `memory_reserve_factor` для операции будет выставлен в 1.0. Это значит,
	// что шедулер будет сразу запускать операцию с нужной гарантией, и будет выключен
	// алгоритм подгонки `memory_limit`.
	//
	// Внутри джобы, будет создан вложенный контейнер, на котором будет включен memory контроллер.
	//
	// В случае OOM, команда будет убита, но джоб, операция и ytexec завершатся успешно. Информация о том,
	// что произошёл OOM будет доступна в ResultFile.
	MemoryLimit int `json:"memory_limit" yson:"memory_limit"`

	// TmpfsSize задаёт максимальный размер tmpfs в байтах.
	//
	// При запуске на YT, все файлы по умолчанию располагаются на tmpfs.
	//
	// Реальный размер tmpfs будет равен TmpfsSize + сумма размеров всех файлов.
	TmpfsSize int `json:"tmpfs_size" yson:"tmpfs_size"`

	// Timeout задаёт общий таймаут на работу операции.
	Timeout time.Duration `json:"timeout" yson:"timeout"`

	EnablePorto   bool `json:"enable_porto" yson:"enable_porto"`
	EnableNetwork bool `json:"enable_network" yson:"enable_network"`

	SpecPatch map[string]interface{} `json:"spec_patch" yson:"spec_patch"`
	TaskPatch map[string]interface{} `json:"task_patch" yson:"task_patch"`

	// EnableResearchFallback говорит ytexec обрабатывать ошибки отсутствия доступов
	// и запускать тест на общедоступных ресурсах кластера.
	//
	// В случае отсутствия доступа к пулу, тест будет запущен в research.
	//
	// В случае отсутствия доступа к кипарису, данные будут храниться в //tmp.
	EnableResearchFallback bool `json:"enable_research_fallback" yson:"enable_research_fallback"`

	// RunAsRoot включает запуск команды от рута.
	//
	// По умолчанию команда выполняется от пользователя с uid != 0.
	RunAsRoot bool `json:"run_as_root" yson:"run_as_root"`
}

const (
	OutputDir = "output"
	CacheDir  = "cache"
	TmpDir    = "tmp"
)

func (c *OperationConfig) TmpDir() ypath.Path {
	return c.CypressRoot.Child(TmpDir)
}

func (c *OperationConfig) CacheDir() ypath.Path {
	return c.CypressRoot.Child(CacheDir)
}

func (c *OperationConfig) OutputDir() ypath.Path {
	return c.CypressRoot.Child(OutputDir)
}

type Cmd struct {
	// Args задаёт команду и её аргументы.
	Args []string `json:"args" yson:"args"`
	// Cwd задаёт рабочую директорию команды.
	Cwd string `json:"cwd" yson:"cwd"`
	// Environ задаёт переменные окружения, с которыми будет запущена команда.
	// Формат VAR_NAME=VAR_VALUE.
	Environ []string `json:"environ" yson:"environ"`

	// *Timeout задают таймауты от старта выполнения команды, после которого команде будет послан сигнал.
	SIGUSR2Timeout time.Duration `json:"sigusr2_timeout" yson:"sigusr2_timeout"`
	SIGQUITTimeout time.Duration `json:"sigquit_timeout" yson:"sigquit_timeout"`
	SIGKILLTimeout time.Duration `json:"sigkill_timeout" yson:"sigkill_timeout"`
}

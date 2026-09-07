package mapreduce

import (
	"fmt"
	"os"
	"runtime/pprof"
	"strings"

	"go.ytsaurus.tech/yt/go/yson"
)

func startJobProfiler() func() {
	noop := func() {}

	var spec struct {
		Binary string `yson:"binary"`
		Type   string `yson:"type"`
	}
	raw := os.Getenv("YT_JOB_PROFILER_SPEC")
	if raw == "" {
		return noop
	}
	if err := yson.Unmarshal([]byte(raw), &spec); err != nil || spec.Binary != "user_job" {
		return noop
	}

	out, err := os.OpenFile(os.Getenv("YT_"+strings.ToUpper(spec.Type)+"_PROFILER_PATH"), os.O_WRONLY, 0)
	if err != nil {
		out = os.NewFile(8, "yt-job-profile")
	}

	switch spec.Type {
	case "cpu":
		if err := pprof.StartCPUProfile(out); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "job profiler: %v\n", err)
			_ = out.Close()
			return noop
		}
		return func() { pprof.StopCPUProfile(); _ = out.Close() }
	case "memory":
		return func() { _ = pprof.WriteHeapProfile(out); _ = out.Close() }
	}
	_ = out.Close()
	return noop
}

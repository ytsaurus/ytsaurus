package mapreduce

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"os"
)

// InsideJob determines whether current process is running inside a mapreduce job.
func InsideJob() bool {
	return os.Getenv("YT_JOB_ID") != ""
}

type jobArgs struct {
	job          string
	nOutputPipes int
}

func parseJobArgs(args []string) (job jobArgs, err error) {
	flags := flag.NewFlagSet("job", flag.ExitOnError)

	flags.StringVar(&job.job, "job", "", "")
	flags.IntVar(&job.nOutputPipes, "output-pipes", 0, "")

	err = flags.Parse(args)
	return
}

// JobMain runs user code inside mapreduce job.
//
// Binary that wishes to run mapreduce operations must place the following code
// at the beginning of the main() function.
//
//	if mapreduce.InsideJob() {
//	    os.Exit(mapreduce.JobMain())
//	}
func JobMain() int {
	args, err := parseJobArgs(os.Args[1:])
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "job: %+v\n", err)
	}

	var ctx jobContext
	if err = ctx.initEnv(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "job: %+v\n", err)
		return 5
	}

	stateBlob, err := readJobState(&ctx)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "job: %+v\n", err)
		return 6
	}

	state, err := decodeJob(bytes.NewBuffer(stateBlob))
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "job: %+v\n", err)
		return 7
	}

	if err = ctx.initPipes(args.nOutputPipes); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "job: %+v\n", err)
		return 3
	}

	outs := make([]io.Closer, args.nOutputPipes)

	switch job := state.Job.(type) {
	case Job:
		reader, err := ctx.createReader(state)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "job: %+v\n", err)
			return 3
		}
		writers := ctx.createWriters()
		for i, w := range writers {
			outs[i] = w
		}
		if err := job.Do(&ctx, reader, writers); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "job: %+v\n", err)
			return 1
		}
		if err := reader.Err(); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "job input reader error: %+v\n", err)
			return 4
		}
	case RawJob:
		for i, out := range ctx.out {
			outs[i] = out
		}
		if err := job.Do(&ctx, ctx.in, ctx.out); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "job: %+v\n", err)
			return 1
		}
	default:
		_, _ = fmt.Fprintf(os.Stderr, "unsupported job type: %T\n", job)
		return 1
	}

	for _, out := range outs {
		if err := out.Close(); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "job output writer error: %+v\n", err)
			return 4
		}
	}

	return 0
}

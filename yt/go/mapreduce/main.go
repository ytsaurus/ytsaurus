package mapreduce

import (
	"bytes"
	"flag"
	"fmt"
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

	if err = ctx.initPipes(state, args.nOutputPipes); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "job: %+v\n", err)
		return 3
	}

	if err := state.Job.Do(&ctx, ctx.in, ctx.writers()); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "job: %+v\n", err)
		return 1
	}

	if err := ctx.finish(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "job: %+v\n", err)
		return 4
	}

	return 0
}

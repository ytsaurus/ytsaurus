package mapreduce

import (
	"flag"
	"fmt"
	"os"
)

// InsideJob determines whether current process is running inside a mapreduce job.
func InsideJob() bool {
	return os.Getenv("YT_INSIDE_JOB") != ""
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

func JobMain() {
	args, err := parseJobArgs(os.Args[1:])
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "job: %+v\n", err)
	}

	job := NewJob(args.job)
	if job == nil {
		_, _ = fmt.Fprintf(os.Stderr, "job: unknown job type '%s'\n", args.job)
		os.Exit(2)
	}

	var ctx jobContext
	if err = ctx.initPipes(args.nOutputPipes); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "job: %+v\n", err)
		os.Exit(3)
	}

	if err := job.Do(&ctx, ctx.in, ctx.writers()); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "job: %+v\n", err)
		os.Exit(1)
	}

	os.Exit(0)
}

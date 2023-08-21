package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"time"

	"go.ytsaurus.tech/library/go/core/xerrors"
	"go.ytsaurus.tech/yt/go/mapreduce"
	"go.ytsaurus.tech/yt/go/mapreduce/spec"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/ythttp"
)

const cluster = "freud"

type EchoJob struct {
	mapreduce.Untyped
}

func (c *EchoJob) Do(ctx mapreduce.JobContext, in mapreduce.Reader, out []mapreduce.Writer) error {
	_, _ = fmt.Fprint(os.Stderr, "- Wait, is it all Vanilla?\n")

	cmd := exec.Command("/bin/echo", "- Always has been.")
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stderr

	return cmd.Run()
}

func init() {
	mapreduce.Register(&EchoJob{})
}

func Example() error {
	yc, err := ythttp.NewClient(&yt.Config{
		Proxy:             cluster,
		ReadTokenFromFile: true,
	})
	if err != nil {
		return err
	}

	jobs := map[string]mapreduce.Job{"job": &EchoJob{}}
	s := spec.Vanilla().AddVanillaTask("job", 1)
	s.MaxFailedJobCount = 1

	mr := mapreduce.New(yc)
	op, err := mr.Vanilla(s, jobs)
	if err != nil {
		return err
	}

	fmt.Printf("Operation: %s\n", yt.WebUIOperationURL(cluster, op.ID()))
	if err := op.Wait(); err != nil {
		return err
	}

	return dumpStderr(yc, op.ID())
}

// dumpStderr downloads and prints stderr of all operation jobs.
func dumpStderr(yc yt.Client, id yt.OperationID) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	jobs, err := yt.ListAllJobs(ctx, yc, id, nil)
	if err != nil {
		return xerrors.Errorf("unable to list operation jobs: %w", err)
	}

	for _, job := range jobs {
		data, err := yc.GetJobStderr(ctx, id, job.ID, nil)
		if err != nil {
			return xerrors.Errorf("unable to get stderr of job %s: %w", job.ID, err)
		}
		fmt.Printf("\nstderr of job %s:\n%s", job.ID, data)
	}

	return nil
}

func main() {
	if mapreduce.InsideJob() {
		os.Exit(mapreduce.JobMain())
	}

	if err := Example(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "error: %+v\n", err)
		os.Exit(1)
	}
}

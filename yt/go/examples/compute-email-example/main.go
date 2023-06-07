package main

import (
	"context"
	"fmt"
	"os"

	"go.ytsaurus.tech/yt/go/guid"
	"go.ytsaurus.tech/yt/go/mapreduce"
	"go.ytsaurus.tech/yt/go/mapreduce/spec"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/ythttp"
)

func init() {
	mapreduce.Register(&ComputeEmailJob{})
}

type ComputeEmailJob struct{}

type LoginRow struct {
	Name  string `yson:"name"`
	Login string `yson:"login"`
}

type EmailRow struct {
	Name  string `yson:"name"`
	Email string `yson:"email"`
}

func (*ComputeEmailJob) InputTypes() []any {
	return []any{&LoginRow{}}
}

func (*ComputeEmailJob) OutputTypes() []any {
	return []any{&EmailRow{}}
}

func (*ComputeEmailJob) Do(ctx mapreduce.JobContext, in mapreduce.Reader, out []mapreduce.Writer) error {
	for in.Next() {
		var login LoginRow
		in.MustScan(&login)

		email := EmailRow{
			Name:  login.Name,
			Email: login.Login + "@yandex-team.ru",
		}

		out[0].MustWrite(&email)
	}

	return nil
}

func Example() error {
	yc, err := ythttp.NewClient(&yt.Config{
		Proxy:             "freud",
		ReadTokenFromFile: true,
	})
	if err != nil {
		return err
	}

	mr := mapreduce.New(yc)
	inputTable := ypath.Path("//home/ermolovd/yt-tutorial/staff_unsorted")
	outputTable := ypath.Path("//tmp/" + guid.New().String())

	_, err = yt.CreateTable(context.Background(), yc, outputTable, yt.WithInferredSchema(&EmailRow{}))
	if err != nil {
		return err
	}

	op, err := mr.Map(&ComputeEmailJob{}, spec.Map().AddInput(inputTable).AddOutput(outputTable))
	if err != nil {
		return err
	}

	fmt.Printf("Operation: %s\n", yt.WebUIOperationURL("freud", op.ID()))
	err = op.Wait()
	if err != nil {
		return err
	}

	fmt.Printf("Output table: %s\n", yt.WebUITableURL("freud", outputTable))
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

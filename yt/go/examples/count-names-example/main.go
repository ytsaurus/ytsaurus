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
	mapreduce.Register(&CountNames{})
}

type CountNames struct{}

type LoginRow struct {
	Name  string `yson:"name"`
	Login string `yson:"login"`
}

type CountRow struct {
	Name  string `yson:"name"`
	Count int    `yson:"count"`
}

func (*CountNames) InputTypes() []any {
	return []any{&LoginRow{}}
}

func (*CountNames) OutputTypes() []any {
	return []any{&CountRow{}}
}

func (*CountNames) Do(ctx mapreduce.JobContext, in mapreduce.Reader, out []mapreduce.Writer) error {
	return mapreduce.GroupKeys(in, func(in mapreduce.Reader) error {
		var result CountRow
		for in.Next() {
			var login LoginRow
			in.MustScan(&login)

			result.Name = login.Name
			result.Count++
		}

		out[0].MustWrite(&result)
		return nil
	})
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
	sortedTable := ypath.Path("//tmp/" + guid.New().String())
	outputTable := ypath.Path("//tmp/" + guid.New().String())

	_, err = yt.CreateTable(context.Background(), yc, sortedTable)
	if err != nil {
		return err
	}

	_, err = yt.CreateTable(context.Background(), yc, outputTable, yt.WithInferredSchema(&CountRow{}))
	if err != nil {
		return err
	}

	op, err := mr.Sort(spec.Sort().
		SortByColumns("name").
		AddInput(inputTable).
		SetOutput(sortedTable))
	if err != nil {
		return err
	}

	fmt.Printf("Operation: %s\n", yt.WebUIOperationURL("freud", op.ID()))
	err = op.Wait()
	if err != nil {
		return err
	}

	op, err = mr.Reduce(&CountNames{}, spec.Reduce().
		ReduceByColumns("name").
		AddInput(sortedTable).
		AddOutput(outputTable))
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

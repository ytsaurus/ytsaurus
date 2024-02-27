package main

import (
	"context"
	"fmt"
	"os"

	"github.com/davecgh/go-spew/spew"

	"go.ytsaurus.tech/yt/go/guid"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/ythttp"
)

const (
	numberOfRows int    = 100
	cluster      string = "freud"
)

type Contact struct {
	Name  string `yson:"name"`
	Email string `yson:"email"`
	Phone string `yson:"phone"`
	Age   int    `yson:"age"`
}

func (c *Contact) Init() {
	c.Name = "Gopher"
	c.Email = "gopher@ytsaurus.tech"
	c.Phone = "+70000000000"
	c.Age = 27
}

func Example() error {
	yc, err := ythttp.NewClient(&yt.Config{
		Proxy:             cluster,
		ReadTokenFromFile: true,
	})
	if err != nil {
		return err
	}

	fakeContacts := make([]Contact, numberOfRows)
	for i := range fakeContacts {
		fakeContacts[i].Init()
	}
	fmt.Println("Generated contacts:")
	spew.Fdump(os.Stdout, fakeContacts)

	tableSchema, err := schema.Infer(Contact{})
	if err != nil {
		return err
	}

	fmt.Println("Inferred struct schema:")
	spew.Fdump(os.Stdout, tableSchema)
	ctx := context.Background()
	tablePath := ypath.Path("//tmp/go-table-example-" + guid.New().String())

	_, err = yt.CreateTable(ctx, yc, tablePath, yt.WithSchema(tableSchema))
	if err != nil {
		return err
	}
	fmt.Printf("Created table at https://yt.yandex-team.ru/%s/navigation?path=%s\n", cluster, tablePath.String())

	writer, err := yc.WriteTable(ctx, tablePath, nil)
	if err != nil {
		return err
	}

	fmt.Println("Writing rows to table...")
	for _, v := range fakeContacts {
		if err = writer.Write(v); err != nil {
			return err
		}
	}
	if err = writer.Commit(); err != nil {
		return err
	}
	fmt.Printf("Written and committed %v rows\n", len(fakeContacts))

	type Attrs struct {
		Rows int `yson:"row_count"`
	}
	var attrs Attrs
	if err = yc.GetNode(ctx, tablePath.Attrs(), &attrs, nil); err != nil {
		return err
	}
	fmt.Printf("YT table contains %v rows\n", attrs.Rows)

	reader, err := yc.ReadTable(ctx, tablePath, nil)
	if err != nil {
		return err
	}
	defer func() { _ = reader.Close() }()

	fmt.Println("Reading rows from table...")
	readContacts := make([]Contact, 0, attrs.Rows)

	for reader.Next() {
		var c Contact
		err = reader.Scan(&c)
		if err != nil {
			return err
		}

		readContacts = append(readContacts, c)
	}

	if reader.Err() != nil {
		return reader.Err()
	}

	fmt.Printf("Read %v rows:\n", len(readContacts))
	spew.Fdump(os.Stdout, readContacts)
	return nil
}

func main() {
	if err := Example(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "error: %+v\n", err)
		os.Exit(1)
	}
}

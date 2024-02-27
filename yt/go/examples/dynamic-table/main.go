package main

import (
	"context"
	"fmt"
	"os"

	"github.com/davecgh/go-spew/spew"

	"go.ytsaurus.tech/yt/go/guid"
	"go.ytsaurus.tech/yt/go/migrate"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/ythttp"
)

const (
	numberOfRows int    = 10
	cluster      string = "freud"
)

type Contact struct {
	ID    int    `yson:"id,key"`
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

	fakeContacts := make([]any, 0, numberOfRows)
	for i := 0; i < numberOfRows; i++ {
		var contact Contact
		contact.Init()
		contact.ID = i

		fakeContacts = append(fakeContacts, contact)
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

	if err := migrate.Create(ctx, yc, tablePath, tableSchema); err != nil {
		return err
	}
	fmt.Printf("Created table at https://yt.yandex-team.ru/%s/navigation?path=%s\n", cluster, tablePath.String())

	fmt.Println("Mounting table...")
	if err := migrate.MountAndWait(ctx, yc, tablePath); err != nil {
		return err
	}
	fmt.Println("Mounted table")

	fmt.Println("Inserting rows into table...")
	if err := yc.InsertRows(ctx, tablePath, fakeContacts, nil); err != nil {
		return err
	}
	fmt.Printf("Inserted %v rows\n", len(fakeContacts))

	type Attrs struct {
		Rows int `yson:"row_count"`
	}
	var attrs Attrs
	if err = yc.GetNode(ctx, tablePath.Attrs(), &attrs, nil); err != nil {
		return err
	}
	fmt.Printf("YT table contains %v rows\n", attrs.Rows)

	reader, err := yc.SelectRows(ctx, fmt.Sprintf("* from [%s] where id < 3", tablePath), nil)
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

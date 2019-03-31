package main

import (
	"context"
	"log"

	"a.yandex-team.ru/yt/go/schema"
	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yt/ythttp"

	"github.com/brianvoe/gofakeit"
	"github.com/google/uuid"
)

const (
	numberOfRows int    = 100
	cluster      string = "freud"
)

func failOnError(err error) {
	if err != nil {
		log.Fatalln(err)
	}
}

type Contact struct {
	Name  string `yson:"name"`
	Email string `yson:"email"`
	Phone string `yson:"phone"`
	Age   int    `yson:"age"`
}

func (c *Contact) Init() {
	c.Name = gofakeit.Name()
	c.Email = gofakeit.Email()
	c.Phone = gofakeit.Phone()
	c.Age = gofakeit.Number(18, 80)
}

func main() {
	yc, err := ythttp.NewClientCli(cluster)
	failOnError(err)

	fakeContacts := make([]Contact, numberOfRows)
	for i := range fakeContacts {
		fakeContacts[i].Init()
	}
	log.Println("Generated contacts:\n", fakeContacts)

	schema, err := schema.Infer(Contact{})
	failOnError(err)
	log.Println("Infered struct schema:\n", schema)
	ctx := context.Background()
	tablePath := ypath.Path("//tmp/go-table-example-" + uuid.New().String())

	_, err = yt.CreateTable(ctx, yc, tablePath, yt.WithSchema(schema))
	failOnError(err)
	log.Printf("Created table at https://yt.yandex-team.ru/%s/navigation?path=%s", cluster, tablePath.String())

	writer, err := yc.WriteTable(ctx, tablePath, nil)
	failOnError(err)

	log.Println("Writing rows to table...")
	for _, v := range fakeContacts {
		err = writer.Write(v)
		failOnError(err)
	}
	err = writer.Commit()
	failOnError(err)
	log.Printf("Written and commited %v rows", len(fakeContacts))

	type Attrs struct {
		Rows int `yson:"row_count"`
	}
	var attrs Attrs
	err = yc.GetNode(ctx, tablePath.Attrs(), &attrs, nil)
	failOnError(err)
	log.Printf("YT table contains %v rows", attrs.Rows)

	reader, err := yc.ReadTable(ctx, tablePath, nil)
	failOnError(err)

	log.Println("Reading rows from table...")
	readContacts := make([]Contact, attrs.Rows)
	var readerCursor int
	for reader.Next() {
		err = reader.Scan(&readContacts[readerCursor])
		failOnError(err)
		readerCursor++
	}
	log.Printf("Read %v rows:\n%v", len(readContacts), readContacts)
}

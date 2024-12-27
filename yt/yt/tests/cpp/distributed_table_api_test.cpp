#include "distributed_table_api_test.h"

////////////////////////////////////////////////////////////////////////////////

namespace NYT {
namespace NCppTests {

////////////////////////////////////////////////////////////////////////////////

using namespace NApi;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NRpc;
using namespace NSecurityClient;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTransactionClient;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TDistributedTableApiTest::CreateStaticTable(
    const TString& tablePath,
    const TString& schema)
{
    Table_ = tablePath;
    EXPECT_TRUE(tablePath.StartsWith("//tmp"));

    auto attributes = TYsonString("{dynamic=%false;schema=" + schema + "}");
    TCreateNodeOptions options;
    options.Attributes = ConvertToAttributes(attributes);
    options.Force = true;

    while (true) {
        TError error;

        try {
            error = WaitFor(Client_->CreateNode(Table_, EObjectType::Table, options));
        } catch (const std::exception& ex) {
            error = TError(ex);
        }

        if (error.IsOK()) {
            break;
        }
    }
}

void TDistributedTableApiTest::WriteTable(
    std::vector<TString> columnNames,
    std::vector<TString> rowStrings,
    bool append)
{
    DoWriteTable(
        std::move(columnNames),
        YsonRowsToUnversionedRows(std::move(rowStrings)),
        append);
}

std::vector<TString> TDistributedTableApiTest::ReadTable()
{
    return DoReadTable();
}

TDistributedWriteSessionWithCookies TDistributedTableApiTest::StartDistributedWriteSession(
    bool append,
    int cookieCount,
    std::optional<TTransactionId> txId)
{
    TDistributedWriteSessionStartOptions options = {};
    options.CookieCount = cookieCount;
    if (txId) {
        options.TransactionId = *txId;
    }
    return WaitFor(Client_
        ->StartDistributedWriteSession(
            MakeRichPath(append),
            options))
        .ValueOrThrow();
}

TSignedWriteFragmentResultPtr TDistributedTableApiTest::DistributedWriteTable(
    const TSignedWriteFragmentCookiePtr& cookie,
    std::vector<TString> columnNames,
    std::vector<TString> rowStrings)
{
    return DoDistributedWriteTable(
        cookie,
        std::move(columnNames),
        YsonRowsToUnversionedRows(std::move(rowStrings)));
}

void TDistributedTableApiTest::PingDistributedWriteSession(
    const TSignedDistributedWriteSessionPtr& session)
{
    WaitFor(NApi::PingDistributedWriteSession(session, Client_)).ThrowOnError();
}

void TDistributedTableApiTest::FinishDistributedWriteSession(
    TSignedDistributedWriteSessionPtr session,
    std::vector<TSignedWriteFragmentResultPtr> results,
    int chunkListsPerAttachRequest)
{
    YT_VERIFY(chunkListsPerAttachRequest > 0);

    WaitFor(Client_
        ->FinishDistributedWriteSession(
            TDistributedWriteSessionWithResults{
                .Session = std::move(session),
                .Results = std::move(results),
            },
            TDistributedWriteSessionFinishOptions{
                .MaxChildrenPerAttachRequest = chunkListsPerAttachRequest,
        }))
        .ThrowOnError();
}

NYPath::TRichYPath TDistributedTableApiTest::MakeRichPath(bool append)
{
    auto path = NYPath::TRichYPath(Table_);
    path.SetAppend(append);
    return path;
}

TSharedRange<TUnversionedRow> TDistributedTableApiTest::YsonRowsToUnversionedRows(
    std::vector<TString> rowStrings)
{
    auto rowBuffer = New<TRowBuffer>();
    std::vector<TUnversionedRow> rows;
    rows.reserve(std::ssize(rowStrings));
    for (const auto& rowString : rowStrings) {
        rows.push_back(rowBuffer->CaptureRow(YsonToSchemalessRow(rowString).Get()));
    }
    return MakeSharedRange(std::move(rows), std::move(rowBuffer));
}

void TDistributedTableApiTest::DoWriteTable(
    std::vector<TString> columnNames,
    TSharedRange<TUnversionedRow> rows,
    bool append)
{
    auto writer = WaitFor(Client_->CreateTableWriter(MakeRichPath(append)))
        .ValueOrThrow();

    const auto& nameTable = writer->GetNameTable();
    for (int i = 0; const auto& name : columnNames) {
        EXPECT_EQ(i++, nameTable->GetIdOrRegisterName(name));
    }
    auto readyEvent = VoidFuture;
    if (!writer->Write(rows)) {
        readyEvent = writer->GetReadyEvent();
    }

    WaitFor(readyEvent.Apply(BIND([writer] {
        return writer->Close();
    })))
        .ThrowOnError();
}

std::vector<TString> TDistributedTableApiTest::DoReadTable()
{
    auto reader = WaitFor(Client_->CreateTableReader(NYPath::TRichYPath(Table_)))
        .ValueOrThrow();

    std::vector<TString> rowStrings;

    while (auto batch = reader->Read()) {
        if (batch->IsEmpty()) {
            WaitFor(reader->GetReadyEvent())
                .ThrowOnError();
            continue;
        }

        auto batchRows = batch->MaterializeRows();
        rowStrings.reserve(std::ssize(rowStrings) + std::ssize(batchRows));

        for (const auto& row : batchRows) {
            rowStrings.push_back(KeyToYson(row));
        }
    }

    return rowStrings;
}

TSignedWriteFragmentResultPtr TDistributedTableApiTest::DoDistributedWriteTable(
    const TSignedWriteFragmentCookiePtr& cookie,
    std::vector<TString> columnNames,
    TSharedRange<TUnversionedRow> rows)
{
    auto writer = WaitFor(Client_->CreateTableFragmentWriter(cookie))
        .ValueOrThrow();

    const auto& nameTable = writer->GetNameTable();
    for (int i = 0; const auto& name : columnNames) {
        EXPECT_EQ(i++, nameTable->GetIdOrRegisterName(name));
    }
    auto readyEvent = VoidFuture;

    if (!writer->Write(rows)) {
        readyEvent = writer->GetReadyEvent();
    }

    WaitFor(readyEvent.Apply(BIND([writer] {
        return writer->Close();
    })))
        .ThrowOnError();

    return writer->GetWriteFragmentResult();
}

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

TEST_F(TDistributedTableApiTest, StartFinishNoTx)
{
    CreateStaticTable(
        /*tablePath*/ "//tmp/distributed_table_api_test",
        /*schema*/ "["
        "{name=v1;type=string};"
        "{name=v2;type=string}"
        "]");

    {
        auto sessionWithCookies = StartDistributedWriteSession(/*append*/ false, /*cookieCount*/0);
        FinishDistributedWriteSession(std::move(sessionWithCookies.Session));
    }

    {
        auto sessionWithCookies = StartDistributedWriteSession(/*append*/ true, /*cookieCount*/0);
        FinishDistributedWriteSession(std::move(sessionWithCookies.Session));
    }

    {
        auto sessionWithCookies = StartDistributedWriteSession(/*append*/ false, /*cookieCount*/1);
        FinishDistributedWriteSession(std::move(sessionWithCookies.Session));
    }

    {
        auto sessionWithCookies = StartDistributedWriteSession(/*append*/ true, /*cookieCount*/1);
        FinishDistributedWriteSession(std::move(sessionWithCookies.Session));
    }

    {
        auto sessionWithCookies = StartDistributedWriteSession(/*append*/ false, /*cookieCount*/0);
        PingDistributedWriteSession(sessionWithCookies.Session);
        FinishDistributedWriteSession(std::move(sessionWithCookies.Session));
    }

    {
        auto sessionWithCookies = StartDistributedWriteSession(/*append*/ true, /*cookieCount*/0);
        PingDistributedWriteSession(sessionWithCookies.Session);
        FinishDistributedWriteSession(std::move(sessionWithCookies.Session));
    }
}

TEST_F(TDistributedTableApiTest, StartFinishWithTx)
{
    CreateStaticTable(
        /*tablePath*/ "//tmp/distributed_table_api_test",
        /*schema*/ "["
        "{name=v1;type=string};"
        "{name=v2;type=string}"
        "]");

    for (int append : {true, false}) {
        Cerr << Format("StartFinishWithTx (Append: %v)", append) << '\n';
        {
            auto tx = WaitFor(
                Client_->StartTransaction(ETransactionType::Master))
                    .ValueOrThrow();
            auto sessionWithCookies = StartDistributedWriteSession(/*append*/ append, /*cookieCount*/ 0);
            FinishDistributedWriteSession(std::move(sessionWithCookies.Session));

            WaitFor(tx->Commit()).ThrowOnError();
        }

        {
            auto tx = WaitFor(
                Client_->StartTransaction(ETransactionType::Master))
                    .ValueOrThrow();
            auto sessionWithCookies = StartDistributedWriteSession(/*append*/ append, /*cookieCount*/ 0);
            FinishDistributedWriteSession(std::move(sessionWithCookies.Session));

            WaitFor(tx->Abort())
                .ThrowOnError();
        }
    }
}

TEST_F(TDistributedTableApiTest, StartWriteFinish)
{
    CreateStaticTable(
        /*tablePath*/ "//tmp/distributed_table_api_test",
        /*schema*/ "["
        "{name=v1;type=string};"
        "]");

    std::vector<TString> rowStrings = {
        "Foo",
        "Bar",
        "Baz",
    };

    std::vector<TString> inputRows;
    std::vector<TString> expectedRows;

    for (auto row : rowStrings) {
        inputRows.push_back("<id=0> " + row + ";");
        expectedRows.push_back("[\"" + row + "\";]");
    }

    {
        auto sessionWithCookies = StartDistributedWriteSession(/*append*/ false, /*cookieCount*/ 1);

        auto result = DistributedWriteTable(
            sessionWithCookies.Cookies[0],
            {
                "v1",
            },
            inputRows);

        PingDistributedWriteSession(sessionWithCookies.Session);

        FinishDistributedWriteSession(
            std::move(sessionWithCookies.Session),
            {std::move(result)}
        );
    }

    auto rowsDistributed = ReadTable();
    EXPECT_EQ(expectedRows, rowsDistributed);
}

TEST_F(TDistributedTableApiTest, StartWriteFinishAbort)
{
    CreateStaticTable(
        /*tablePath*/ "//tmp/distributed_table_api_test",
        /*schema*/ "["
        "{name=v1;type=string};"
        "]");

    std::vector<TString> rowStrings = {
        "Foo",
        "Bar",
        "Baz",
    };

    std::vector<TString> inputRows;
    std::vector<TString> expectedRows;

    for (auto row : rowStrings) {
        inputRows.push_back("<id=0> " + row + ";");
        expectedRows.push_back("[\"" + row + "\";]");
    }

    {
        auto tx = WaitFor(
            Client_->StartTransaction(ETransactionType::Master))
                .ValueOrThrow();
        auto [session, cookies] = StartDistributedWriteSession(
            /*append*/ false,
            /*cookieCount*/ 1,
            /*txId*/ tx->GetId());

        auto cookie = std::move(cookies[0]);
        auto result = DistributedWriteTable(
            cookie,
            {
                "v1",
            },
            inputRows);

        PingDistributedWriteSession(session);

        FinishDistributedWriteSession(
            std::move(session),
            {std::move(result)});

        WaitFor(tx->Abort())
            .ThrowOnError();
    }

    auto rowsDistributed = ReadTable();
    EXPECT_EQ(std::ssize(rowsDistributed), 0);
}

TEST_F(TDistributedTableApiTest, StartWriteAbortFinish)
{
    CreateStaticTable(
        /*tablePath*/ "//tmp/distributed_table_api_test",
        /*schema*/ "["
        "{name=v1;type=string};"
        "]");

    std::vector<TString> rowStrings = {
        "Foo",
        "Bar",
        "Baz",
    };

    std::vector<TString> inputRows;
    std::vector<TString> expectedRows;

    for (auto row : rowStrings) {
        inputRows.push_back("<id=0> " + row + ";");
        expectedRows.push_back("[\"" + row + "\";]");
    }

    {
        auto tx = WaitFor(
            Client_->StartTransaction(ETransactionType::Master))
                .ValueOrThrow();
        auto [session, cookies] = StartDistributedWriteSession(
            /*append*/ false,
            /*cookieCount*/ 1,
            /*txId*/ tx->GetId());

        auto cookie = std::move(cookies[0]);
        auto result = DistributedWriteTable(
            cookie,
            {
                "v1",
            },
            inputRows);

        PingDistributedWriteSession(session);

        WaitFor(tx->Abort())
            .ThrowOnError();

        EXPECT_ANY_THROW(FinishDistributedWriteSession(
            std::move(session),
            {std::move(result)}));
    }

    auto rowsDistributed = ReadTable();
    EXPECT_EQ(std::ssize(rowsDistributed), 0);
}

TEST_F(TDistributedTableApiTest, StartWriteTwiceFinishSameCookie)
{
    CreateStaticTable(
        /*tablePath*/ "//tmp/distributed_table_api_test",
        /*schema*/ "["
        "{name=v1;type=string};"
        "]");

    std::vector<TString> rowStrings = {
        "Foo",
        "Bar",
        "Baz",
    };

    std::vector<TString> inputRows;
    std::vector<TString> expectedRows;

    for (const auto& row : rowStrings) {
        inputRows.push_back("<id=0> " + row + ";");
        expectedRows.push_back("[\"" + row + "\";]");
    }

    {
        auto [session, cookies] = StartDistributedWriteSession(
            /*append*/ true,
            /*cookieCount*/ 1);

        auto cookie = std::move(cookies[0]);
        auto result1 = DistributedWriteTable(
            cookie,
            {
                "v1",
            },
            {
                inputRows[0],
                inputRows[1],
            });

        PingDistributedWriteSession(session);

        auto result2 = DistributedWriteTable(
            cookie,
            {
                "v1",
            },
            {
                inputRows[2],
            });

        PingDistributedWriteSession(session);

        FinishDistributedWriteSession(
            std::move(session),
            {std::move(result1), std::move(result2)});
    }

    auto rowsDistributed = ReadTable();
    EXPECT_EQ(expectedRows, rowsDistributed);
}

TEST_F(TDistributedTableApiTest, StartWriteTwiceFinishDifferentCookies)
{
    CreateStaticTable(
        /*tablePath*/ "//tmp/distributed_table_api_test",
        /*schema*/ "["
        "{name=v1;type=string};"
        "]");

    std::vector<TString> rowStrings = {
        "Foo",
        "Bar",
        "Baz",
    };

    std::vector<TString> inputRows;
    std::vector<TString> expectedRows;

    for (const auto& row : rowStrings) {
        inputRows.push_back("<id=0> " + row + ";");
        expectedRows.push_back("[\"" + row + "\";]");
    }

    {
        auto [session, cookies] = StartDistributedWriteSession(
            /*append*/ true,
            /*cookieCount*/ 2);

        auto cookie1 = cookies[0];
        auto cookie2 = cookies[1];
        auto result1 = DistributedWriteTable(
            cookie1,
            {
                "v1",
            },
            {
                inputRows[0],
                inputRows[1],
            });

        PingDistributedWriteSession(session);

        auto result2 = DistributedWriteTable(
            cookie2,
            {
                "v1",
            },
            {
                inputRows[2],
            });

        PingDistributedWriteSession(session);

        FinishDistributedWriteSession(
            std::move(session),
            {std::move(result1), std::move(result2)});
    }

    auto rowsDistributed = ReadTable();
    EXPECT_EQ(expectedRows, rowsDistributed);
}

TEST_F(TDistributedTableApiTest, StartWriteFailWriteSuccessFinish)
{
    CreateStaticTable(
        /*tablePath*/ "//tmp/distributed_table_api_test",
        /*schema*/ "["
        "{name=v1;type=string};"
        "]");

    std::vector<TString> rowStrings = {
        "Foo",
        "Bar",
        "Baz",
    };

    std::vector<TString> inputRows;
    std::vector<TString> expectedRows;

    for (const auto& row : rowStrings) {
        inputRows.push_back("<id=0> " + row + ";");
        expectedRows.push_back("[\"" + row + "\";]");
    }

    {
        auto [session, cookies] = StartDistributedWriteSession(
            /*append*/ true,
            /*cookieCount*/ 1);

        auto cookie = cookies[0];
        EXPECT_ANY_THROW(DistributedWriteTable(
            cookie,
            {
                "wrong_name here",
            },
            {
                inputRows[0],
                inputRows[1],
            }));

        PingDistributedWriteSession(session);

        auto result = DistributedWriteTable(
            cookie,
            {
                "v1",
            },
            {
                inputRows[2],
            });

        PingDistributedWriteSession(session);

        FinishDistributedWriteSession(
            std::move(session),
            {std::move(result)});
    }

    auto rowsDistributed = ReadTable();
    EXPECT_EQ(std::vector{
        expectedRows[2]
    }, rowsDistributed);
}

TEST_F(TDistributedTableApiTest, ManyChunksToAttach)
{
    CreateStaticTable(
        /*tablePath*/ "//tmp/distributed_table_api_test",
        /*schema*/ "["
        "{name=v1;type=string};"
        "]");

    std::vector<TString> rowStrings = {
        "Foo",
        "Bar",
        "Baz",
    };

    std::vector<TString> inputRows;
    std::vector<TString> expectedRows;

    for (const auto& row : rowStrings) {
        inputRows.push_back("<id=0> " + row + ";");
        expectedRows.push_back("[\"" + row + "\";]");
    }

    {
        auto [session, cookies] = StartDistributedWriteSession(
            /*append*/ true,
            /*cookieCount*/ 1);

        auto cookie = cookies[0];
        std::vector<TSignedWriteFragmentResultPtr> results;
        for (int i = 0; i < 3; ++i) {
            results.push_back(DistributedWriteTable(
                cookie,
                {
                    "v1",
                },
                {
                    inputRows[i],
                }));
        }

        PingDistributedWriteSession(session);

        FinishDistributedWriteSession(
            std::move(session),
            std::move(results),
            /*chunkListsPerAttachRequest*/ 1);
    }

    auto rowsDistributed = ReadTable();
    EXPECT_EQ(expectedRows, rowsDistributed);
}

TEST_F(TDistributedTableApiTest, SortedTableSimpleDistributedWrite)
{
    CreateStaticTable(
        /*tablePath*/ "//tmp/distributed_table_api_test",
        /*schema*/ "["
        "{name=v1;type=string;sort_order=ascending};"
        "]");

    std::vector<TString> rowStrings{
        "aa",
        "bb",
        "cc",
    };

    std::vector<TString> inputRows;
    std::vector<TString> expectedRows;

    for (auto row : rowStrings) {
        inputRows.push_back("<id=0> " + row + ";");
        expectedRows.push_back("[\"" + row + "\";]");
    }

    {
        auto [session, cookies] = StartDistributedWriteSession(
            /*append*/ true,
            /*cookieCount*/ 1);

        auto cookie = cookies[0];

        auto result = DistributedWriteTable(
            cookie,
            {
                "v1",
            },
            inputRows);

        FinishDistributedWriteSession(
            std::move(session),
            {std::move(result)});
    }

    auto rowsDistributed = ReadTable();
    EXPECT_EQ(expectedRows, rowsDistributed);
}

TEST_F(TDistributedTableApiTest, SortedTableTwoDistributedWritesOneCookie)
{
    CreateStaticTable(
        /*tablePath*/ "//tmp/distributed_table_api_test",
        /*schema*/ "["
        "{name=v1;type=string;sort_order=ascending};"
        "]");

    std::vector<TString> rowStrings{
        "aa",
        "bb",
        "cc",
    };

    std::vector<TString> inputRows;
    std::vector<TString> expectedRows;

    for (auto row : rowStrings) {
        inputRows.push_back("<id=0> " + row + ";");
        expectedRows.push_back("[\"" + row + "\";]");
    }

    {
        auto [session, cookies] = StartDistributedWriteSession(
            /*append*/ true,
            /*cookieCount*/ 1);

        auto cookie = cookies[0];

        auto result1 = DistributedWriteTable(
            cookie,
            {
                "v1",
            },
            {
                inputRows[0],
                inputRows[1],
            });

        auto result2 = DistributedWriteTable(
            cookie,
            {
                "v1",
            },
            {
                inputRows[2],
            });

        FinishDistributedWriteSession(
            std::move(session),
            {std::move(result1), std::move(result2)});
    }

    auto rowsDistributed = ReadTable();
    EXPECT_EQ(expectedRows, rowsDistributed);
}

TEST_F(TDistributedTableApiTest, SortedTableTwoDistributedWritesOneCookieInverted)
{
    CreateStaticTable(
        /*tablePath*/ "//tmp/distributed_table_api_test",
        /*schema*/ "["
        "{name=v1;type=string;sort_order=ascending};"
        "]");

    std::vector<TString> rowStrings{
        "aa",
        "bb",
        "cc",
    };

    std::vector<TString> inputRows;
    std::vector<TString> expectedRows;

    for (auto row : rowStrings) {
        inputRows.push_back("<id=0> " + row + ";");
        expectedRows.push_back("[\"" + row + "\";]");
    }

    {
        auto [session, cookies] = StartDistributedWriteSession(
            /*append*/ true,
            /*cookieCount*/ 1);

        auto cookie = cookies[0];

        auto result1 = DistributedWriteTable(
            cookie,
            {
                "v1",
            },
            {
                inputRows[2],
            });

        auto result2 = DistributedWriteTable(
            cookie,
            {
                "v1",
            },
            {
                inputRows[0],
                inputRows[1],
            });

        FinishDistributedWriteSession(
            std::move(session),
            {std::move(result1), std::move(result2)});
    }

    auto rowsDistributed = ReadTable();
    EXPECT_EQ(expectedRows, rowsDistributed);
}

TEST_F(TDistributedTableApiTest, SortedTableTwoDistributedWritesTwoCookies)
{
    CreateStaticTable(
        /*tablePath*/ "//tmp/distributed_table_api_test",
        /*schema*/ "["
        "{name=v1;type=string;sort_order=ascending};"
        "]");

    std::vector<TString> rowStrings{
        "aa",
        "bb",
        "cc",
    };

    std::vector<TString> inputRows;
    std::vector<TString> expectedRows;

    for (auto row : rowStrings) {
        inputRows.push_back("<id=0> " + row + ";");
        expectedRows.push_back("[\"" + row + "\";]");
    }

    {
        auto [session, cookies] = StartDistributedWriteSession(
            /*append*/ true,
            /*cookieCount*/ 2);

        auto cookie1 = cookies[0];
        auto cookie2 = cookies[1];
        auto result1 = DistributedWriteTable(
            cookie1,
            {
                "v1",
            },
            {
                inputRows[0],
                inputRows[1],
            });

        PingDistributedWriteSession(session);

        auto result2 = DistributedWriteTable(
            cookie2,
            {
                "v1",
            },
            {
                inputRows[2],
            });

        FinishDistributedWriteSession(
            std::move(session),
            {std::move(result1), std::move(result2)});
    }

    auto rowsDistributed = ReadTable();
    EXPECT_EQ(expectedRows, rowsDistributed);
}

TEST_F(TDistributedTableApiTest, SortedTableTwoDistributedWritesTwoCookiesInverted)
{
    CreateStaticTable(
        /*tablePath*/ "//tmp/distributed_table_api_test",
        /*schema*/ "["
        "{name=v1;type=string;sort_order=ascending};"
        "]");

    std::vector<TString> rowStrings{
        "aa",
        "bb",
        "cc",
    };

    std::vector<TString> inputRows;
    std::vector<TString> expectedRows;

    for (auto row : rowStrings) {
        inputRows.push_back("<id=0> " + row + ";");
        expectedRows.push_back("[\"" + row + "\";]");
    }

    {
        auto [session, cookies] = StartDistributedWriteSession(
            /*append*/ true,
            /*cookieCount*/ 2);

        auto cookie1 = cookies[0];
        auto cookie2 = cookies[1];

        auto result1 = DistributedWriteTable(
            cookie1,
            {
                "v1",
            },
            {
                inputRows[2],
            });

        PingDistributedWriteSession(session);

        auto result2 = DistributedWriteTable(
            cookie2,
            {
                "v1",
            },
            {
                inputRows[0],
                inputRows[1],
            });

        FinishDistributedWriteSession(
            std::move(session),
            {std::move(result1), std::move(result2)});
    }

    auto rowsDistributed = ReadTable();
    EXPECT_EQ(expectedRows, rowsDistributed);
}

TEST_F(TDistributedTableApiTest, SortedTableTwoDistributedWritesViolateUniqueness)
{
    CreateStaticTable(
        /*tablePath*/ "//tmp/distributed_table_api_test",
        /*schema*/ "<unique_keys=%true>["
        "{name=v1;type=string;sort_order=ascending};"
        "]");

    std::vector<TString> rowStrings{
        "aa",
        "bb",
        "cc",
    };

    std::vector<TString> inputRows;
    std::vector<TString> expectedRows;

    for (auto row : rowStrings) {
        inputRows.push_back("<id=0> " + row + ";");
        expectedRows.push_back("[\"" + row + "\";]");
    }

    {
        auto [session, cookies] = StartDistributedWriteSession(
            /*append*/ true,
            /*cookieCount*/ 2);

        auto cookie1 = cookies[0];
        auto cookie2 = cookies[1];

        auto result1 = DistributedWriteTable(
            cookie1,
            {
                "v1",
            },
            {
                inputRows[0],
            });

        PingDistributedWriteSession(session);

        auto result2 = DistributedWriteTable(
            cookie2,
            {
                "v1",
            },
            {
                inputRows[0],
            });

        EXPECT_THROW_WITH_SUBSTRING(
            FinishDistributedWriteSession(
            std::move(session),
            {std::move(result1), std::move(result2)}),
            "contains duplicate keys");
    }

    auto rowsDistributed = ReadTable();
    EXPECT_EQ(std::ssize(rowsDistributed), 0);
}

TEST_F(TDistributedTableApiTest, SortedTableTwoDistributedWritesOverlapKeyRanges)
{
    CreateStaticTable(
        /*tablePath*/ "//tmp/distributed_table_api_test",
        /*schema*/ "<unique_keys=%true>["
        "{name=v1;type=string;sort_order=ascending};"
        "]");

    std::vector<TString> rowStrings{
        "aa",
        "ba",
        "bb",
        "cc",
    };

    std::vector<TString> inputRows;
    std::vector<TString> expectedRows;

    for (auto row : rowStrings) {
        inputRows.push_back("<id=0> " + row + ";");
        expectedRows.push_back("[\"" + row + "\";]");
    }

    {
        auto [session, cookies] = StartDistributedWriteSession(
            /*append*/ true,
            /*cookieCount*/ 2);

        auto cookie1 = cookies[0];
        auto cookie2 = cookies[1];

        auto result1 = DistributedWriteTable(
            cookie1,
            {
                "v1",
            },
            {
                inputRows[0],
                inputRows[3],
            });

        auto result2 = DistributedWriteTable(
            cookie2,
            {
                "v1",
            },
            {
                inputRows[1],
                inputRows[2],
            });

        EXPECT_THROW_WITH_SUBSTRING(
            FinishDistributedWriteSession(
            std::move(session),
            {std::move(result1), std::move(result2)}),
            "is not sorted");
    }

    auto rowsDistributed = ReadTable();
    EXPECT_EQ(std::ssize(rowsDistributed), 0);
}

TEST_F(TDistributedTableApiTest, StartWriteCookieDuplicateResult)
{
    CreateStaticTable(
        /*tablePath*/ "//tmp/distributed_table_api_test",
        /*schema*/ "["
        "{name=v1;type=string};"
        "]");

    std::vector<TString> rowStrings = {
        "Foo",
        "Bar",
        "Baz",
    };

    std::vector<TString> inputRows;
    std::vector<TString> expectedRows;

    for (const auto& row : rowStrings) {
        inputRows.push_back("<id=0> " + row + ";");
        expectedRows.push_back("[\"" + row + "\";]");
    }

    {
        auto [session, cookies] = StartDistributedWriteSession(
            /*append*/ true,
            /*cookieCount*/ 1);

        auto cookie = std::move(cookies[0]);
        auto result1 = DistributedWriteTable(
            cookie,
            {
                "v1",
            },
            {
                inputRows[0],
                inputRows[1],
            });

        PingDistributedWriteSession(session);

        EXPECT_THROW_WITH_SUBSTRING(FinishDistributedWriteSession(
            std::move(session),
            {result1, result1}), "Duplicate chunk list ids");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NCppTests
} // namespace NYT

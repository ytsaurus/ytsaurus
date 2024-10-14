#include <yt/yt/tests/cpp/test_base/api_test_base.h>

#include <yt/yt/client/api/rowset.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/cypress_client/cypress_ypath_proxy.h>
#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/object_client/config.h>
#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/ytlib/table_client/table_ypath_proxy.h>

#include <yt/yt/client/api/distributed_table_session.h>
#include <yt/yt/client/api/internal_client.h>
#include <yt/yt/client/api/table_reader.h>
#include <yt/yt/client/api/table_writer.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/client/transaction_client/helpers.h>
#include <yt/yt/client/transaction_client/timestamp_provider.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/logging/config.h>
#include <yt/yt/core/logging/log_manager.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/yson/string.h>

#include <util/datetime/base.h>

#include <util/random/random.h>

////////////////////////////////////////////////////////////////////////////////

namespace NYT {
namespace NCppTests {
namespace {

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

class TDistributedTableApiTest
    : public TApiTestBase
{
public:
    static void CreateStaticTable(
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

    static void WriteTable(
        std::vector<TString> columnNames,
        std::vector<TString> rowStrings,
        bool append)
    {
        DoWriteTable(
            std::move(columnNames),
            YsonRowsToUnversionedRows(std::move(rowStrings)),
            append);
    }

    static std::vector<TString> ReadTable()
    {
        return DoReadTable();
    }

    static TDistributedWriteSessionPtr StartDistributedWriteSession(
        bool append,
        std::optional<TTransactionId> txId = {})
    {
        TDistributedWriteSessionStartOptions options = {};
        if (txId) {
            options.TransactionId = *txId;
        }
        return WaitFor(Client_
            ->StartDistributedWriteSession(
                MakeRichPath(append),
                options))
            .ValueOrThrow();
    }

    // Append is decided upon session opening.
    static void DistributedWriteTable(
        const TFragmentWriteCookiePtr& cookie,
        std::vector<TString> columnNames,
        std::vector<TString> rowStrings)
    {
        DoDistributedWriteTable(
            cookie,
            std::move(columnNames),
            YsonRowsToUnversionedRows(std::move(rowStrings)));
    }

    static void PingDistributedWriteSession(
        const TDistributedWriteSessionPtr& session)
    {
        WaitFor(session->Ping(Client_)).ThrowOnError();
    }

    static void FinishDistributedWriteSession(
        TDistributedWriteSessionPtr session,
        int chunkListsPerAttachRequest = 42)
    {
        YT_VERIFY(chunkListsPerAttachRequest > 0);

        WaitFor(Client_
            ->FinishDistributedWriteSession(
                std::move(session),
                TDistributedWriteSessionFinishOptions{
                    .MaxChildrenPerAttachRequest = chunkListsPerAttachRequest,
            }))
            .ThrowOnError();
    }

private:
    static inline TString Table_ = {};

    static NYPath::TRichYPath MakeRichPath(bool append)
    {
        auto path = NYPath::TRichYPath(Table_);
        path.SetAppend(append);
        return path;
    }

    static TSharedRange<TUnversionedRow> YsonRowsToUnversionedRows(
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

    static void DoWriteTable(
        std::vector<TString> columnNames,
        TSharedRange<TUnversionedRow> rows,
        bool append)
    {
        WaitFor(Client_
            ->CreateTableWriter(MakeRichPath(append))
            .Apply(BIND([rows, names = std::move(columnNames)] (const ITableWriterPtr& writer) {
                const auto& nameTable = writer->GetNameTable();
                for (int i = 0; const auto& name : names) {
                    EXPECT_EQ(i++, nameTable->GetIdOrRegisterName(name));
                }
                if (writer->Write(rows)) {
                    return writer->Close();
                }

                return writer->GetReadyEvent().Apply(BIND([writer] {
                    return writer->Close();
                }));
            })))
            .ThrowOnError();
    }

    static std::vector<TString> DoReadTable()
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

    static void DoDistributedWriteTable(
        const TFragmentWriteCookiePtr& cookie,
        std::vector<TString> columnNames,
        TSharedRange<TUnversionedRow> rows)
    {
        WaitFor(Client_
            ->CreateFragmentTableWriter(cookie)
            .Apply(BIND([rows, names = std::move(columnNames)] (const ITableWriterPtr& writer) {
                const auto& nameTable = writer->GetNameTable();
                for (int i = 0; const auto& name : names) {
                    EXPECT_EQ(i++, nameTable->GetIdOrRegisterName(name));
                }
                if (writer->Write(rows)) {
                    return writer->Close();
                }

                return writer->GetReadyEvent().Apply(BIND([writer] {
                    return writer->Close();
                }));
            })))
            .ThrowOnError();
    }
};

TEST_F(TDistributedTableApiTest, StartFinishNoTx)
{
    CreateStaticTable(
        /*tablePath*/ "//tmp/distributed_table_api_test",
        /*schema*/ "["
        "{name=v1;type=string};"
        "{name=v2;type=string}"
        "]");

    {
        auto session = StartDistributedWriteSession(/*append*/ false);
        FinishDistributedWriteSession(std::move(session));
    }

    {
        auto session = StartDistributedWriteSession(/*append*/ true);
        FinishDistributedWriteSession(std::move(session));
    }

    {
        auto session = StartDistributedWriteSession(/*append*/ false);
        PingDistributedWriteSession(session);
        FinishDistributedWriteSession(std::move(session));
    }

    {
        auto session = StartDistributedWriteSession(/*append*/ true);
        PingDistributedWriteSession(session);
        FinishDistributedWriteSession(std::move(session));
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
            auto session = StartDistributedWriteSession(/*append*/ append);
            FinishDistributedWriteSession(std::move(session));

            WaitFor(tx->Commit()).ThrowOnError();
        }

        {
            auto tx = WaitFor(
                Client_->StartTransaction(ETransactionType::Master))
                    .ValueOrThrow();
            auto session = StartDistributedWriteSession(/*append*/ append);
            FinishDistributedWriteSession(std::move(session));

            WaitFor(tx->Abort()).ThrowOnError();
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
        auto session = StartDistributedWriteSession(/*append*/ false);

        auto cookie = session->GiveCookie();
        DistributedWriteTable(
            cookie,
            {
                "v1",
            },
            inputRows);

        PingDistributedWriteSession(session);

        session->TakeCookie(std::move(cookie));

        FinishDistributedWriteSession(std::move(session));
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
        auto session = StartDistributedWriteSession(
            /*append*/ false,
            /*txId*/ tx->GetId());

        auto cookie = session->GiveCookie();
        DistributedWriteTable(
            cookie,
            {
                "v1",
            },
            inputRows);

        PingDistributedWriteSession(session);

        session->TakeCookie(std::move(cookie));

        FinishDistributedWriteSession(std::move(session));

        WaitFor(tx->Abort()).ThrowOnError();
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
        auto session = StartDistributedWriteSession(
            /*append*/ false,
            /*txId*/ tx->GetId());

        auto cookie = session->GiveCookie();
        DistributedWriteTable(
            cookie,
            {
                "v1",
            },
            inputRows);

        PingDistributedWriteSession(session);

        session->TakeCookie(std::move(cookie));

        WaitFor(tx->Abort()).ThrowOnError();

        EXPECT_ANY_THROW(FinishDistributedWriteSession(std::move(session)));
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
        auto session = StartDistributedWriteSession(/*append*/ true);

        auto cookie = session->GiveCookie();
        DistributedWriteTable(
            cookie,
            {
                "v1",
            },
            {
                inputRows[0],
                inputRows[1],
            });

        PingDistributedWriteSession(session);

        DistributedWriteTable(
            cookie,
            {
                "v1",
            },
            {
                inputRows[2],
            });

        PingDistributedWriteSession(session);

        session->TakeCookie(std::move(cookie));

        FinishDistributedWriteSession(std::move(session));
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
        auto session = StartDistributedWriteSession(/*append*/ true);

        auto cookie1 = session->GiveCookie();
        auto cookie2 = session->GiveCookie();
        DistributedWriteTable(
            cookie1,
            {
                "v1",
            },
            {
                inputRows[0],
                inputRows[1],
            });

        PingDistributedWriteSession(session);

        DistributedWriteTable(
            cookie2,
            {
                "v1",
            },
            {
                inputRows[2],
            });

        PingDistributedWriteSession(session);

        session->TakeCookie(std::move(cookie1));
        session->TakeCookie(std::move(cookie2));

        FinishDistributedWriteSession(std::move(session));
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
        auto session = StartDistributedWriteSession(/*append*/ true);

        auto cookie = session->GiveCookie();
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

        DistributedWriteTable(
            cookie,
            {
                "v1",
            },
            {
                inputRows[2],
            });

        PingDistributedWriteSession(session);

        session->TakeCookie(std::move(cookie));

        FinishDistributedWriteSession(std::move(session));
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
        auto session = StartDistributedWriteSession(/*append*/ true);

        auto cookie = session->GiveCookie();
        DistributedWriteTable(
            cookie,
            {
                "v1",
            },
            {
                inputRows[0],
            });

        DistributedWriteTable(
            cookie,
            {
                "v1",
            },
            {
                inputRows[1],
            });

        DistributedWriteTable(
            cookie,
            {
                "v1",
            },
            {
                inputRows[2],
            });

        PingDistributedWriteSession(session);

        session->TakeCookie(std::move(cookie));

        FinishDistributedWriteSession(std::move(session), /*chunkListsPerAttachRequest*/ 1);
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
        auto session = StartDistributedWriteSession(/*append*/ true);

        auto cookie = session->GiveCookie();

        DistributedWriteTable(
            cookie,
            {
                "v1",
            },
            inputRows);

        session->TakeCookie(std::move(cookie));

        FinishDistributedWriteSession(std::move(session));
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
        auto session = StartDistributedWriteSession(/*append*/ true);

        auto cookie = session->GiveCookie();

        DistributedWriteTable(
            cookie,
            {
                "v1",
            },
            {
                inputRows[0],
                inputRows[1],
            });

        DistributedWriteTable(
            cookie,
            {
                "v1",
            },
            {
                inputRows[2],
            });

        session->TakeCookie(std::move(cookie));

        FinishDistributedWriteSession(std::move(session));
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
        auto session = StartDistributedWriteSession(/*append*/ true);

        auto cookie = session->GiveCookie();

        DistributedWriteTable(
            cookie,
            {
                "v1",
            },
            {
                inputRows[2],
            });

        DistributedWriteTable(
            cookie,
            {
                "v1",
            },
            {
                inputRows[0],
                inputRows[1],
            });

        session->TakeCookie(std::move(cookie));

        FinishDistributedWriteSession(std::move(session));
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
        auto session = StartDistributedWriteSession(/*append*/ true);

        auto cookie1 = session->GiveCookie();
        DistributedWriteTable(
            cookie1,
            {
                "v1",
            },
            {
                inputRows[0],
                inputRows[1],
            });

        auto cookie2 = session->GiveCookie();
        DistributedWriteTable(
            cookie2,
            {
                "v1",
            },
            {
                inputRows[2],
            });

        session->TakeCookie(std::move(cookie1));
        session->TakeCookie(std::move(cookie2));

        FinishDistributedWriteSession(std::move(session));
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
        auto session = StartDistributedWriteSession(/*append*/ true);

        auto cookie2 = session->GiveCookie();
        DistributedWriteTable(
            cookie2,
            {
                "v1",
            },
            {
                inputRows[2],
            });

        auto cookie1 = session->GiveCookie();
        DistributedWriteTable(
            cookie1,
            {
                "v1",
            },
            {
                inputRows[0],
                inputRows[1],
            });

        session->TakeCookie(std::move(cookie1));
        session->TakeCookie(std::move(cookie2));

        FinishDistributedWriteSession(std::move(session));
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
        auto session = StartDistributedWriteSession(/*append*/ true);

        auto cookie1 = session->GiveCookie();
        DistributedWriteTable(
            cookie1,
            {
                "v1",
            },
            {
                inputRows[0],
            });

        auto cookie2 = session->GiveCookie();
        DistributedWriteTable(
            cookie2,
            {
                "v1",
            },
            {
                inputRows[0],
            });

        session->TakeCookie(std::move(cookie1));
        session->TakeCookie(std::move(cookie2));

        bool threw = false;

        try {
            FinishDistributedWriteSession(std::move(session));
        } catch (const std::exception& ex) {
            threw = true;
            EXPECT_TRUE(TString(ex.what()).Contains("contains duplicate keys"));
        }

        EXPECT_TRUE(threw);
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
        auto session = StartDistributedWriteSession(/*append*/ true);

        auto cookie1 = session->GiveCookie();
        DistributedWriteTable(
            cookie1,
            {
                "v1",
            },
            {
                inputRows[0],
                inputRows[3],
            });

        auto cookie2 = session->GiveCookie();
        DistributedWriteTable(
            cookie2,
            {
                "v1",
            },
            {
                inputRows[1],
                inputRows[2],
            });

        session->TakeCookie(std::move(cookie1));
        session->TakeCookie(std::move(cookie2));

        bool threw = false;

        try {
            FinishDistributedWriteSession(std::move(session));
        } catch (const std::exception& ex) {
            threw = true;
            EXPECT_TRUE(TString(ex.what()).Contains("is not sorted"));
        }

        EXPECT_TRUE(threw);
    }

    auto rowsDistributed = ReadTable();
    EXPECT_EQ(std::ssize(rowsDistributed), 0);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NCppTests
} // namespace NYT

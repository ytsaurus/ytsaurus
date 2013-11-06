#include <ytlib/object_client/public.h>

#include <ytlib/query_client/plan_fragment.h>

#include <ytlib/query_client/callbacks.h>
#include <ytlib/query_client/helpers.h>

#include <ytlib/table_client/chunk_meta_extensions.h>
#include <ytlib/table_client/table_chunk_meta.pb.h>

#include <ytlib/new_table_client/chunk_meta_extensions.h>
#include <ytlib/new_table_client/chunk_meta.pb.h>

#include <contrib/testing/framework.h>

////////////////////////////////////////////////////////////////////////////////

using namespace NYT;
using namespace NYT::NObjectClient;
using namespace NYT::NQueryClient;

using ::testing::HasSubstr;
using ::testing::ContainsRegex;

class TQueryPrepareTest
    : public ::testing::Test
    , public IPrepareCallbacks
{
    virtual TFuture<TErrorOr<TDataSplit>> GetInitialSplit(const NYPath::TYPath& path) override
    {
        if (path != "//t") {
            return MakeFuture(TErrorOr<TDataSplit>(TError(Sprintf(
                "Could not find table %s",
                ~path))));
        }

        TDataSplit dataSplit;

        ToProto(
            dataSplit.mutable_chunk_id(),
            MakeId(EObjectType::Table, 0x42, 0xffffffff, 0xdeadbabe));

        TKeyColumns keyColumns;
        keyColumns.push_back("k");
        keyColumns.push_back("n");
        SetKeyColumns(&dataSplit, keyColumns);

        TTableSchema tableSchema;
        tableSchema.Columns().push_back({ "k", EColumnType::Integer });
        tableSchema.Columns().push_back({ "n", EColumnType::Integer });
        tableSchema.Columns().push_back({ "a", EColumnType::Integer });
        tableSchema.Columns().push_back({ "b", EColumnType::Integer });
        SetTableSchema(&dataSplit, tableSchema);

        return MakeFuture(TErrorOr<TDataSplit>(std::move(dataSplit)));
    }
};

template <class TCallbacks, class TMatcher>
void ExpectThrowsWithDiagnostics(
    const Stroka& query,
    TCallbacks* callbacks,
    TMatcher matcher)
{
    using namespace ::testing;
    bool exceptionThrown = false;
    try {
        TPlanFragment::Prepare(query, callbacks);
    } catch (const std::exception& ex) {
        exceptionThrown = true;
        EXPECT_THAT(ex.what(), matcher);
    }
    EXPECT_TRUE(exceptionThrown);
}

TEST_F(TQueryPrepareTest, Simple)
{
    TPlanFragment::Prepare("a, b FROM [//t] WHERE k > 3", this);
    SUCCEED();
}

TEST_F(TQueryPrepareTest, BadSyntax)
{
    ExpectThrowsWithDiagnostics(
        "bazzinga mu ha ha ha",
        this,
        HasSubstr("syntax error"));
}

TEST_F(TQueryPrepareTest, BadTableName)
{
    ExpectThrowsWithDiagnostics(
        "a, b from [//bad/table]",
        this,
        HasSubstr("Could not find table //bad/table"));
}

TEST_F(TQueryPrepareTest, BadColumnName)
{
    ExpectThrowsWithDiagnostics(
        "foo from [//t]",
        this,
        HasSubstr("Table //t does not have column \"foo\" in its schema"));
    ExpectThrowsWithDiagnostics(
        "k from [//t] where bar = 1",
        this,
        HasSubstr("Table //t does not have column \"bar\" in its schema"));
}

TEST_F(TQueryPrepareTest, BadTypecheck)
{
    ExpectThrowsWithDiagnostics(
        "k from [//t] where a > 3.1415926",
        this,
        ContainsRegex("Type mismatch .* in expression \"a > 3.1415926\""));
}

////////////////////////////////////////////////////////////////////////////////


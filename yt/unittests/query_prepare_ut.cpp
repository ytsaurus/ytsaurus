#include <ytlib/object_client/public.h>

#include <ytlib/query_client/query_fragment.h>

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
    TCallbacks* callbacks,
    const Stroka& query,
    TMatcher matcher)
{
    using namespace ::testing;
    bool exceptionThrown = false;
    try {
        PrepareQueryFragment(callbacks, query);
    } catch (const std::exception& ex) {
        exceptionThrown = true;
        EXPECT_THAT(ex.what(), matcher);
    }
    EXPECT_TRUE(exceptionThrown);
}

TEST_F(TQueryPrepareTest, Simple)
{
    PrepareQueryFragment(this, "a, b FROM [//t] WHERE k > 3");
    SUCCEED();
}

TEST_F(TQueryPrepareTest, BadSyntax)
{
    ExpectThrowsWithDiagnostics(
        this,
        "bazzinga mu ha ha ha",
        ::testing::HasSubstr("syntax error"));
}

TEST_F(TQueryPrepareTest, BadTableName)
{
    ExpectThrowsWithDiagnostics(
        this,
        "a, b from [//bad/table]",
        ::testing::HasSubstr("Could not find table //bad/table"));
}

TEST_F(TQueryPrepareTest, BadColumnName)
{
    ExpectThrowsWithDiagnostics(
        this,
        "foo from [//t]",
        ::testing::HasSubstr("Table //t does not have column \"foo\" in its schema"));
    ExpectThrowsWithDiagnostics(
        this,
        "k from [//t] where bar = 1",
        ::testing::HasSubstr("Table //t does not have column \"bar\" in its schema"));
}

TEST_F(TQueryPrepareTest, BadTypecheck)
{
    ExpectThrowsWithDiagnostics(
        this,
        "k from [//t] where a > 3.1415926",
        ::testing::ContainsRegex("Type mismatch .* in expression \"a > 3.1415926\""));
}

////////////////////////////////////////////////////////////////////////////////


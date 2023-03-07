#include "modify_rows_test.h"

#include <yt/client/api/rowset.h>
#include <yt/client/api/transaction.h>

#include <yt/client/table_client/helpers.h>

#include <yt/core/yson/string.h>

namespace NYT {
namespace NCppTests {

using namespace NApi;
using namespace NConcurrency;
using namespace NTableClient;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TModifyRowsTest::SetUp()
{
    TTransactionStartOptions options;
    options.AutoAbort = false;

    Transaction_ = WaitFor(Client_->StartTransaction(NTransactionClient::ETransactionType::Tablet, options))
        .ValueOrThrow();
}

void TModifyRowsTest::TearDown()
{
    Transaction_.Reset();

    auto transaction = WaitFor(Client_->StartTransaction(NTransactionClient::ETransactionType::Tablet))
        .ValueOrThrow();

    for (const auto& key: Keys_) {
        auto preparedKey = PrepareUnversionedRow({"key", "value"}, "<id=0> " + ToString(key));
        transaction->DeleteRows(Table_, std::get<1>(preparedKey), std::get<0>(preparedKey));
    }

    WaitFor(transaction->Commit()).ValueOrThrow();

    Keys_.clear();
}


void TModifyRowsTest::SetUpTestCase()
{
    TDynamicTablesTestBase::SetUpTestCase();

    CreateTable(
        "//tmp/modify_rows_test", // tablePath
        "[" // schema
        "{name=key;type=int64;sort_order=ascending};"
        "{name=value;type=int64}]");
}

void TModifyRowsTest::WriteSimpleRow(
    i64 key,
    i64 value,
    std::optional<i64> sequenceNumber)
{
    Keys_.insert(key);

    const std::vector<TString> names = {"key", "value"};
    auto rowString = MakeRowString(key, value);

    auto preparedRow = PrepareUnversionedRow(names, rowString);

    TModifyRowsOptions options;
    options.SequenceNumber = sequenceNumber;

    Transaction_->WriteRows(
        Table_,
        std::get<1>(preparedRow),
        std::get<0>(preparedRow),
        options);

}

void TModifyRowsTest::SyncCommit()
{
    WaitFor(Transaction_->Commit()).ValueOrThrow();
}

void TModifyRowsTest::CheckTableContents(
    const std::vector<std::pair<i64, i64>>& simpleRows)
{
    THashSet<TString> expected;
    for (const auto& simpleRow: simpleRows) {
        auto simpleRowString = MakeRowString(simpleRow.first, simpleRow.second);
        auto rowString = ToString(YsonToSchemalessRow(simpleRowString));
        expected.insert(rowString);
    }

    auto res = WaitFor(Client_->SelectRows("* from [" + Table_ + "]")).
        ValueOrThrow();

    THashSet<TString> actual;
    for (const auto& row: res.Rowset->GetRows()) {
        actual.insert(ToString(row));
    }

    EXPECT_EQ(actual, expected);

    auto schema = ConvertTo<TTableSchema>(TYsonString(
        "<unique_keys=%false;strict=%true>[{name=key;type=int64};{name=value;type=int64};]"));

    auto expectedSchema = ConvertToYsonString(schema, EYsonFormat::Text).GetData();
    auto actualSchema = ConvertToYsonString(res.Rowset->Schema(), EYsonFormat::Text).GetData();
    EXPECT_EQ(expectedSchema, actualSchema);
}

TString TModifyRowsTest::MakeRowString(i64 key, i64 value)
{
    return "<id=0> " + ToString(key) + "; <id=1> " + ToString(value);
}

ITransactionPtr TModifyRowsTest::Transaction_;
THashSet<i64> TModifyRowsTest::Keys_;

////////////////////////////////////////////////////////////////////////////////

} // namespace NCppTests
} // namespace NYT


#include <yt/yt/flow/library/cpp/misc/retryable_transaction.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/unittests/mock/transaction.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NFlow {
namespace {

using namespace NApi;
using namespace NTableClient;
using namespace NQueryClient;
using namespace NYPath;

using ::testing::_;
using ::testing::Invoke;
using ::testing::StrictMock;
using TStrictMockTransaction = StrictMock<NApi::TMockTransaction>;
using TStrictMockTransactionPtr = TIntrusivePtr<TStrictMockTransaction>;

////////////////////////////////////////////////////////////////////////////////

TEST(TRetryableTransactionTest, Simple)
{
    auto retryableTransaction = CreateRetryableTransaction();

    EXPECT_TRUE(retryableTransaction->IsEmpty());

    auto nameTable = New<TNameTable>();
    i32 field = nameTable->GetIdOrRegisterName("field");
    auto path = std::string("//path");

    // Write empty modifications range.
    retryableTransaction->ModifyRows(TYPath(path), nameTable, MakeSharedRange(std::vector<NApi::TRowModification>(), New<TRowBuffer>()));
    EXPECT_TRUE(retryableTransaction->IsEmpty());

    // Write not empty modifications range.
    auto rowBuffer = New<TRowBuffer>();
    std::vector<NApi::TRowModification> rows;
    {
        auto row = rowBuffer->AllocateUnversioned(1);
        row[0] = rowBuffer->CaptureValue(MakeUnversionedStringValue("text", field));
        rows.push_back(NRowModifications::TWriteRow(row));
    }
    auto sharedRange = MakeSharedRange(std::move(rows), std::move(rowBuffer));
    retryableTransaction->ModifyRows(TYPath(path), nameTable, sharedRange);
    EXPECT_FALSE(retryableTransaction->IsEmpty());

    auto checker = [&] (
        const NYPath::TYPath& callPath,
        NTableClient::TNameTablePtr callNameTable,
        TSharedRange<NApi::TRowModification>
            modifications,
        const NApi::TModifyRowsOptions& /*options*/) {
        ASSERT_EQ(callPath, path);
        ASSERT_EQ(callNameTable, nameTable);
        ASSERT_EQ(modifications.Data(), sharedRange.Data());
    };

    for ([[maybe_unused]] int i : {0, 1}) {
        auto mockTransaction = New<TStrictMockTransaction>();

        EXPECT_CALL(*mockTransaction, ModifyRows(_, _, _, _))
            .WillOnce(Invoke(checker));

        retryableTransaction->DoAttempt(mockTransaction);
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST(TRetryableTransactionTest, OnAttemptResultDispatchesSubscribers)
{
    auto retryableTransaction = CreateRetryableTransaction();

    int firstCallCount = 0;
    int secondCallCount = 0;
    TCommitAttemptResult lastResult{.CommitResult = TError("Not yet fired")};

    retryableTransaction->SubscribeOnAttemptResult(BIND([&] (const TCommitAttemptResult& result) {
        ++firstCallCount;
        lastResult = result;
    }));
    retryableTransaction->SubscribeOnAttemptResult(BIND([&] (const TCommitAttemptResult& /*result*/) {
        ++secondCallCount;
    }));

    NApi::TTransactionCommitResult okResult;
    okResult.PrimaryCommitTimestamp = 42;
    retryableTransaction->OnAttemptResult(TCommitAttemptResult{.CommitResult = okResult});
    ASSERT_EQ(firstCallCount, 1);
    ASSERT_EQ(secondCallCount, 1);
    ASSERT_TRUE(lastResult.CommitResult.IsOK());
    ASSERT_EQ(lastResult.CommitResult.Value().PrimaryCommitTimestamp, 42u);

    retryableTransaction->OnAttemptResult(TCommitAttemptResult{.CommitResult = TError("Commit failed")});
    ASSERT_EQ(firstCallCount, 2);
    ASSERT_EQ(secondCallCount, 2);
    ASSERT_FALSE(lastResult.CommitResult.IsOK());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/ytlib/api/native/encoded_row_stream.h>

#include <yt/yt/client/api/formatted_table_reader.h>
#include <yt/yt/client/api/row_batch_reader.h>

#include <yt/yt/client/formats/config.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_batch.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/actions/invoker_util.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

namespace NYT::NApi::NNative {
namespace {

using namespace NConcurrency;
using namespace NFormats;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TEmptyBatchReader
    : public IRowBatchReader
{
public:
    TEmptyBatchReader(
        IInvokerPtr invoker,
        int emptyBatchCount,
        bool returnRow,
        bool asynchronousReady)
        : Invoker_(std::move(invoker))
        , EmptyBatchCount_(emptyBatchCount)
        , ReturnRow_(returnRow)
        , AsynchronousReady_(asynchronousReady)
    {
        NameTable_->GetIdOrRegisterName("x");
        TUnversionedOwningRowBuilder builder;
        builder.AddValue(MakeUnversionedInt64Value(42));
        Row_ = builder.FinishRow();
    }

    IUnversionedRowBatchPtr Read(const TRowBatchReadOptions& /*options*/) override
    {
        LastReadFiberId_ = GetCurrentFiberId();
        if (++ReadCount_ <= EmptyBatchCount_) {
            return CreateBatchFromUnversionedRows({});
        }
        if (ReturnRow_ && ReadCount_ == EmptyBatchCount_ + 1) {
            return CreateBatchFromUnversionedRows(MakeSharedRange(
                std::vector<TUnversionedRow>{Row_},
                MakeStrong(this)));
        }
        return nullptr;
    }

    const TNameTablePtr& GetNameTable() const override
    {
        return NameTable_;
    }

    TFuture<void> GetReadyEvent() const override
    {
        if (ReadCount_ < EmptyBatchCount_) {
            if (!AsynchronousReady_) {
                return OKFuture;
            }
            return BIND([]
            { })
                .AsyncVia(Invoker_)
                .Run();
        }
        Blocked_.TrySet();
        return Ready_.ToFuture();
    }

    TFuture<void> GetBlockedFuture() const
    {
        return Blocked_.ToFuture();
    }

    void Complete(const TError& error)
    {
        Ready_.Set(error);
    }

    TFiberId GetLastReadFiberId() const
    {
        return LastReadFiberId_;
    }

private:
    const IInvokerPtr Invoker_;
    const int EmptyBatchCount_;
    const bool ReturnRow_;
    const bool AsynchronousReady_;
    const TNameTablePtr NameTable_ = New<TNameTable>();
    const TPromise<void> Blocked_ = NewPromise<void>();
    const TPromise<void> Ready_ = NewPromise<void>();

    int ReadCount_ = 0;
    TFiberId LastReadFiberId_ = InvalidFiberId;
    TUnversionedOwningRow Row_;
};

enum class EReadOutcome
{
    Row,
    EndOfStream,
    Error,
    Cancel,
};

class TEncodedRowStreamTest
    : public testing::TestWithParam<std::tuple<EReadOutcome, bool>>
{ };

TEST_P(TEncodedRowStreamTest, ManyEmptyBatches)
{
    auto queue = New<TActionQueue>("EmptyBatches");
    auto [outcome, asynchronousReady] = GetParam();
    auto reader = New<TEmptyBatchReader>(
        queue->GetInvoker(),
        /*emptyBatchCount*/ 5'000,
        outcome == EReadOutcome::Row,
        asynchronousReady);
    auto stream = CreateEncodedRowStream(
        reader,
        reader->GetNameTable(),
        TFormat(EFormatType::Dsv),
        New<TTableSchema>(std::vector{TColumnSchema("x", EValueType::Int64)}),
        /*columns*/ std::nullopt,
        New<TControlAttributesConfig>());

    TFuture<TSharedRef> future;
    BIND([&] {
        future = stream->Read();
        if (!asynchronousReady) {
            EXPECT_EQ(reader->GetLastReadFiberId(), GetCurrentFiberId());
        }
    })
        .AsyncVia(queue->GetInvoker())
        .Run()
        .BlockingGet()
        .ThrowOnError();
    reader->GetBlockedFuture()
        .BlockingGet()
        .ThrowOnError();
    EXPECT_FALSE(future.IsSet());

    BIND([=] {
        if (outcome == EReadOutcome::Cancel) {
            EXPECT_TRUE(future.Cancel(TError("Client disconnected")));
        } else {
            reader->Complete(outcome == EReadOutcome::Error
                ? TError("Reader failed")
                : TError());
        }
    })
        .AsyncVia(queue->GetInvoker())
        .Run()
        .BlockingGet()
        .ThrowOnError();

    auto result = future.BlockingGet();
    if (outcome == EReadOutcome::Cancel) {
        EXPECT_FALSE(result.IsOK());
        EXPECT_THAT(ToString(result), testing::HasSubstr("Client disconnected"));
    } else if (outcome == EReadOutcome::Error) {
        EXPECT_FALSE(result.IsOK());
        EXPECT_THAT(ToString(result), testing::HasSubstr("Reader failed"));
    } else {
        ASSERT_TRUE(result.IsOK()) << ToString(result);
        if (outcome == EReadOutcome::Row) {
            EXPECT_EQ(result.Value().ToStringBuf(), "x=42\n");
            EXPECT_FALSE(stream->Read().BlockingGet().ValueOrThrow());
        } else {
            EXPECT_FALSE(result.Value());
        }
    }
}

INSTANTIATE_TEST_SUITE_P(
    Outcomes,
    TEncodedRowStreamTest,
    testing::Combine(
        testing::Values(EReadOutcome::Row, EReadOutcome::EndOfStream, EReadOutcome::Error, EReadOutcome::Cancel),
        testing::Bool()));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NApi::NNative

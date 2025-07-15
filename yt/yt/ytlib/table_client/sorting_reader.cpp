#include "sorting_reader.h"
#include "timing_reader.h"

#include <yt/yt/ytlib/chunk_client/dispatcher.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/data_statistics.pb.h>

#include <yt/yt/library/query/engine_api/column_evaluator.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/schemaless_row_reorderer.h>
#include <yt/yt/client/table_client/row_batch.h>
#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/misc/error.h>

namespace NYT::NTableClient {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NConcurrency;
using namespace NQueryClient;

using NChunkClient::TDataSliceDescriptor;

////////////////////////////////////////////////////////////////////////////////

struct TSchemalessSortingReaderTag
{ };

class TSortingReader
    : public ISchemalessMultiChunkReader
    , public TTimingReaderBase
{
public:
    TSortingReader(
        ISchemalessMultiChunkReaderPtr underlyingReader,
        TNameTablePtr nameTable,
        const std::vector<std::string>& outputColumns,
        TComparator comparator,
        TColumnEvaluatorPtr evaluator)
        : UnderlyingReader_(std::move(underlyingReader))
        , Comparator_(std::move(comparator))
        , RowBuffer_(New<TRowBuffer>(TSchemalessSortingReaderTag()))
        , Evaluator_(std::move(evaluator))
        , RowReorderer_(std::move(nameTable), RowBuffer_, /*deepCapture*/ true, outputColumns)
    {
        SetReadyEvent(BIND(&TSortingReader::DoOpen, MakeWeak(this))
            .AsyncVia(TDispatcher::Get()->GetReaderInvoker())
            .Run());
    }

    IUnversionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
    {
        if (!IsReadyEventSetAndOK()) {
            return CreateEmptyUnversionedRowBatch();
        }

        i64 startRowCount = ReadRowCount_;
        i64 endRowCount = startRowCount;
        i64 dataWeight = 0;
        while (endRowCount < std::ssize(Rows_) && endRowCount - startRowCount < options.MaxRowsPerRead && dataWeight < options.MaxDataWeightPerRead) {
            dataWeight += GetDataWeight(Rows_[endRowCount++]);
        }

        if (endRowCount == startRowCount) {
            return nullptr;
        }

        ReadDataWeight_ += dataWeight;
        ReadRowCount_ = endRowCount;

        return CreateBatchFromUnversionedRows(MakeSharedRange<TUnversionedRow>(
            TRange<TUnversionedRow>(Rows_.data() + startRowCount, Rows_.data() + endRowCount),
            MakeStrong(this)));
    }

    bool IsFetchingCompleted() const override
    {
        YT_VERIFY(UnderlyingReader_);
        return UnderlyingReader_->IsFetchingCompleted();
    }

    TDataStatistics GetDataStatistics() const override
    {
        YT_VERIFY(UnderlyingReader_);
        auto dataStatistics = UnderlyingReader_->GetDataStatistics();
        dataStatistics.set_row_count(ReadRowCount_);
        dataStatistics.set_data_weight(ReadDataWeight_);
        return dataStatistics;
    }

    TCodecStatistics GetDecompressionStatistics() const override
    {
        YT_VERIFY(UnderlyingReader_);
        return UnderlyingReader_->GetDecompressionStatistics();
    }

    std::vector<TChunkId> GetFailedChunkIds() const override
    {
        YT_VERIFY(UnderlyingReader_);
        return UnderlyingReader_->GetFailedChunkIds();
    }

    TInterruptDescriptor GetInterruptDescriptor(
        TRange<TUnversionedRow> /*unreadRows*/) const override
    {
        YT_ABORT();
    }

    void Interrupt() override
    {
        YT_ABORT();
    }

    void SkipCurrentReader() override
    {
        YT_ABORT();
    }

    const TNameTablePtr& GetNameTable() const override
    {
        return UnderlyingReader_->GetNameTable();
    }

    i64 GetTotalRowCount() const override
    {
        return Rows_.size();
    }

    i64 GetSessionRowIndex() const override
    {
        return ReadRowCount_;
    }

    i64 GetTableRowIndex() const override
    {
        return 0;
    }

    const TDataSliceDescriptor& GetCurrentReaderDescriptor() const override
    {
        YT_ABORT();
    }

private:
    const ISchemalessMultiChunkReaderPtr UnderlyingReader_;
    const TComparator Comparator_;

    const TRowBufferPtr RowBuffer_;
    const TColumnEvaluatorPtr Evaluator_;
    TSchemalessRowReorderer RowReorderer_;

    std::vector<TUnversionedRow> Rows_;
    i64 ReadRowCount_ = 0;
    i64 ReadDataWeight_ = 0;

    void DoOpen()
    {
        while (auto batch = UnderlyingReader_->Read()) {
            if (batch->IsEmpty()) {
                WaitFor(UnderlyingReader_->GetReadyEvent())
                    .ThrowOnError();
                continue;
            }

            for (auto row : batch->MaterializeRows()) {
                auto schemafulRow = RowReorderer_.ReorderRow(row);
                Evaluator_->EvaluateKeys(schemafulRow, RowBuffer_, /*preserveColumnsIds*/ false);
                Rows_.push_back(schemafulRow);
            }
        }

        std::sort(
            Rows_.begin(),
            Rows_.end(),
            [&] (auto lhsRow, auto rhsRow) {
                // Value types validation is disabled for performance reasons.
                auto lhsKey = TKey::FromRowUnchecked(lhsRow, Comparator_.GetLength());
                auto rhsKey = TKey::FromRowUnchecked(rhsRow, Comparator_.GetLength());
                return Comparator_.CompareKeys(lhsKey, rhsKey) < 0;
            });
    }
};

////////////////////////////////////////////////////////////////////////////////

ISchemalessMultiChunkReaderPtr CreateSortingReader(
    ISchemalessMultiChunkReaderPtr underlyingReader,
    TNameTablePtr nameTable,
    const std::vector<std::string>& outputColumns,
    TComparator comparator,
    TColumnEvaluatorPtr evaluator)
{
    return New<TSortingReader>(
        std::move(underlyingReader),
        std::move(nameTable),
        outputColumns,
        std::move(comparator),
        std::move(evaluator));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient

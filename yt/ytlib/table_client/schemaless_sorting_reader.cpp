#include "schemaless_sorting_reader.h"

#include <yt/ytlib/chunk_client/dispatcher.h>

#include <yt/client/chunk_client/proto/data_statistics.pb.h>

#include <yt/client/table_client/name_table.h>
#include <yt/client/table_client/schemaless_row_reorderer.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/misc/error.h>

namespace NYT::NTableClient {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NConcurrency;

using NChunkClient::TDataSliceDescriptor;
using NYT::TRange;

// Reasonable default for max data size per one read call.
static const i64 MaxDataSizePerRead = 16 * 1024 * 1024;
static const int RowsPerRead = 10000;

////////////////////////////////////////////////////////////////////////////////

class TSchemalessSortingReader
    : public ISchemalessMultiChunkReader
{
public:
    TSchemalessSortingReader(
        ISchemalessMultiChunkReaderPtr underlyingReader,
        TNameTablePtr nameTable,
        TKeyColumns keyColumns)
        : RowReorderer_(nameTable, keyColumns)
        , UnderlyingReader_(underlyingReader)
        , KeyColumns_(keyColumns)
    { 
        ReadyEvent_ = BIND(&TSchemalessSortingReader::DoOpen,
            MakeWeak(this))
            .AsyncVia(TDispatcher::Get()->GetReaderInvoker())
            .Run();
    }

    void DoOpen() 
    {
        std::vector<TUnversionedRow> rows;
        rows.reserve(RowsPerRead);

        while (UnderlyingReader_->Read(&rows)) {
            if (rows.empty()) {
                WaitFor(UnderlyingReader_->GetReadyEvent())
                    .ThrowOnError();
                continue;
            }

            for (auto& row : rows) {
                Rows_.push_back(RowReorderer_.ReorderRow(row));
            }
        }

        std::sort(
            Rows_.begin(),
            Rows_.end(),
            [&] (const TUnversionedOwningRow& lhs, const TUnversionedOwningRow& rhs) {
                return CompareRows(lhs, rhs, KeyColumns_.size()) < 0;
            });
    }

    virtual bool Read(std::vector<TUnversionedRow>* rows) override
    {
        YCHECK(rows->capacity() > 0);
        if (!ReadyEvent_.IsSet() || !ReadyEvent_.Get().IsOK()) {
            return true;
        }

        rows->clear();

        if (ReadRowCount_ == Rows_.size()) {
            return false;
        }

        i64 dataWeight = 0;
        while (ReadRowCount_ < Rows_.size() && rows->size() < rows->capacity() && dataWeight < MaxDataSizePerRead) {
            rows->push_back(Rows_[ReadRowCount_]);
            dataWeight += GetDataWeight(rows->back());
            ++ReadRowCount_;
        }
        ReadDataWeight_ += dataWeight;

        YCHECK(!rows->empty());
        return true;
    }

    virtual TFuture<void> GetReadyEvent() override
    {
        return ReadyEvent_;
    }

    virtual bool IsFetchingCompleted() const override
    {
        YCHECK(UnderlyingReader_);
        return UnderlyingReader_->IsFetchingCompleted();
    }

    virtual TDataStatistics GetDataStatistics() const override
    {
        YCHECK(UnderlyingReader_);
        auto dataStatistics = UnderlyingReader_->GetDataStatistics();
        dataStatistics.set_row_count(ReadRowCount_);
        dataStatistics.set_data_weight(ReadDataWeight_);
        return dataStatistics;
    }

    TCodecStatistics GetDecompressionStatistics() const override
    {
        YCHECK(UnderlyingReader_);
        return UnderlyingReader_->GetDecompressionStatistics();
    }

    virtual std::vector<TChunkId> GetFailedChunkIds() const override
    {
        YCHECK(UnderlyingReader_);
        return UnderlyingReader_->GetFailedChunkIds();
    }

    virtual TInterruptDescriptor GetInterruptDescriptor(
        TRange<TUnversionedRow> unreadRows) const override
    {
        Y_UNREACHABLE();
    }

    virtual void Interrupt() override
    {
        Y_UNREACHABLE();
    }

    virtual const TNameTablePtr& GetNameTable() const override
    {
        return UnderlyingReader_->GetNameTable();
    }

    virtual TKeyColumns GetKeyColumns() const override
    {
        return KeyColumns_;
    }

    virtual i64 GetTotalRowCount() const override
    {
        return Rows_.size();
    }

    virtual i64 GetSessionRowIndex() const override
    {
        return ReadRowCount_;
    }

    virtual i64 GetTableRowIndex() const override
    {
        Y_UNREACHABLE();
    }

private:
    TSchemalessRowReorderer RowReorderer_;

    ISchemalessMultiChunkReaderPtr UnderlyingReader_;
    TKeyColumns KeyColumns_;

    std::vector<TUnversionedOwningRow> Rows_;
    i64 ReadRowCount_ = 0;
    i64 ReadDataWeight_ = 0;

    TFuture<void> ReadyEvent_;

};

////////////////////////////////////////////////////////////////////////////////

ISchemalessMultiChunkReaderPtr CreateSchemalessSortingReader(
    ISchemalessMultiChunkReaderPtr underlyingReader,
    TNameTablePtr nameTable,
    TKeyColumns keyColumns)
{
    return New<TSchemalessSortingReader>(underlyingReader, nameTable, keyColumns);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient

#include "stdafx.h"

#include "schemaless_sorting_reader.h"

#include "name_table.h"
#include "schemaless_row_reorderer.h"

#include "core/concurrency/scheduler.h"
#include <core/misc/error.h>

namespace NYT {
namespace NTableClient {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NConcurrency;

// Reasonable default for max data size per one read call.
const i64 MaxDataSizePerRead = 16 * 1024 * 1024;

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
        , ReadRowCount_(0)
    { }

    virtual TFuture<void> Open() override
    {
        try {
            // ToDo(psushin): make it really async.
            WaitFor(UnderlyingReader_->Open())
                .ThrowOnError();

            std::vector<TUnversionedRow> rows;
            rows.reserve(10000);

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
                    return CompareRows(lhs.Get(), rhs.Get(), KeyColumns_.size()) < 0;
                });

            return VoidFuture;
        } catch (const std::exception& ex) {
            return MakeFuture(TError(ex));
        }
    }

    virtual bool Read(std::vector<TUnversionedRow> *rows) override
    {
        rows->clear();

        if (ReadRowCount_ == Rows_.size()) {
            return false;
        }

        i64 dataWeight = 0;
        while (ReadRowCount_ < Rows_.size() && rows->size() < rows->capacity() && dataWeight < MaxDataSizePerRead) {
            rows->push_back(Rows_[ReadRowCount_].Get());
            dataWeight += GetDataWeight(rows->back());
            ++ReadRowCount_;
        }

        YCHECK(!rows->empty());
        return true;
    }

    virtual TFuture<void> GetReadyEvent() override
    {
        YUNREACHABLE();
    }

    virtual int GetTableIndex() const override
    {
        return 0;
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
        return dataStatistics;
    }

    virtual std::vector<TChunkId> GetFailedChunkIds() const override
    {
        YCHECK(UnderlyingReader_);
        return UnderlyingReader_->GetFailedChunkIds();
    }

    virtual TNameTablePtr GetNameTable() const override
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
        YUNREACHABLE();
    }

    virtual i32 GetRangeIndex() const override
    {
        YUNREACHABLE();
    }

private:
    TSchemalessRowReorderer RowReorderer_;

    ISchemalessMultiChunkReaderPtr UnderlyingReader_;
    TKeyColumns KeyColumns_;

    std::vector<TUnversionedOwningRow> Rows_;
    i64 ReadRowCount_;

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

} // namespace NTableClient
} // namespace NYT

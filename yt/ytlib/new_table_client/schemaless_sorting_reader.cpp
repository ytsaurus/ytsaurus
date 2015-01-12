#include "stdafx.h"

#include "schemaless_sorting_reader.h"

#include "name_table.h"
#include "schemaless_row_reorderer.h"

#include "core/concurrency/scheduler.h"
#include <core/misc/error.h>

namespace NYT {
namespace NVersionedTableClient {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NConcurrency;

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
        // ToDo(psushin): make it really async.
        auto error = WaitFor(UnderlyingReader_->Open());
        if (!error.IsOK()) {
            return MakeFuture(error);
        }

        std::vector<TUnversionedRow> rows;
        rows.reserve(10000);

        while (UnderlyingReader_->Read(&rows)) {
            if (rows.empty()) {
                auto error = WaitFor(UnderlyingReader_->GetReadyEvent());
                if (!error.IsOK()) {
                    return MakeFuture(error);
                }
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
    }

    virtual bool Read(std::vector<TUnversionedRow> *rows) override
    {
        rows->clear();

        if (ReadRowCount_ == Rows_.size()) {
            return false;
        }

        while (ReadRowCount_ < Rows_.size() && rows->size() < rows->capacity()) {
            rows->push_back(Rows_[ReadRowCount_].Get());
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

    virtual i64 GetSessionRowCount() const override
    {
        return Rows_.size();
    }

    virtual i64 GetSessionRowIndex() const override
    {
        return ReadRowCount_;
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

} // namespace NVersionedTableClient
} // namespace NYT

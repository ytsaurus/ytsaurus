#include "config.h"
#include "name_table.h"
#include "schema.h"
#include "schemaful_reader.h"
#include "schemaless_reader.h"
#include "schemaless_reader_adapter.h"

#include <yt/ytlib/chunk_client/data_statistics.h>

#include <yt/core/misc/chunked_memory_pool.h>

namespace NYT {
namespace NTableClient {

using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TSchemalessReaderAdapter)

struct TSchemalessReaderAdapterPoolTag { };

class TSchemalessReaderAdapter
    : public ISchemalessReader
{
public:
    TSchemalessReaderAdapter(
        ISchemafulReaderPtr underlyingReader,
        TTableReaderOptionsPtr options,
        std::vector<int> idMapping,
        TNameTablePtr nameTable,
        TKeyColumns keyColumns,
        int tableIndex,
        int rangeIndex)
        : UnderlyingReader_(std::move(underlyingReader))
        , Options_(std::move(options))
        , IdMapping_(std::move(idMapping))
        , NameTable_(std::move(nameTable))
        , KeyColumns_(std::move(keyColumns))
        , TableIndex_(tableIndex)
        , RangeIndex_(rangeIndex)
        , MemoryPool_(TSchemalessReaderAdapterPoolTag())
    {
        if (Options_->EnableRangeIndex) {
            ++SystemColumnCount_;
            RangeIndexId_ = NameTable_->GetIdOrRegisterName(RangeIndexColumnName);
        }
        if (Options_->EnableTableIndex) {
            ++SystemColumnCount_;
            TableIndexId_ = NameTable_->GetIdOrRegisterName(TableIndexColumnName);
        }
    }

    virtual bool Read(std::vector<TUnversionedRow>* rows) override
    {
        rows->clear();
        MemoryPool_.Clear();

        if (ErrorPromise_.IsSet()) {
            return true;
        }

        auto hasMore = UnderlyingReader_->Read(rows);
        if (rows->empty()) {
            return hasMore;
        }

        YCHECK(hasMore);
        auto& rows_ = *rows;

        try {
            for (int index = 0; index < rows->size(); ++index) {
                auto row = TMutableUnversionedRow::Allocate(&MemoryPool_, rows_[index].GetCount() + SystemColumnCount_);
                row.SetCount(rows_[index].GetCount());

                for (int valueIndex = 0; valueIndex < row.GetCount(); ++valueIndex) {
                    const auto& value = rows_[index][valueIndex];
                    ValidateDataValue(value);
                    row[valueIndex] = value;
                    row[valueIndex].Id = IdMapping_[value.Id];
                }

                if (Options_->EnableRangeIndex) {
                    *row.End() = MakeUnversionedInt64Value(RangeIndex_, RangeIndexId_);
                    row.SetCount(row.GetCount() + 1);
                }
                if (Options_->EnableTableIndex) {
                    *row.End() = MakeUnversionedInt64Value(TableIndex_, TableIndexId_);
                    row.SetCount(row.GetCount() + 1);
                }

                rows_[index] = row;
            }
        } catch (const std::exception& ex) {
            rows_.clear();
            ErrorPromise_.Set(ex);
        }

        return true;
    }

    virtual TFuture<void> GetReadyEvent() override
    {
        if (ErrorPromise_.IsSet()) {
            return ErrorPromise_.ToFuture();
        } else {
            return UnderlyingReader_->GetReadyEvent();
        }
    }

    virtual const TNameTablePtr& GetNameTable() const override
    {
        return NameTable_;
    }

    virtual TKeyColumns GetKeyColumns() const override
    {
        return KeyColumns_;
    }

    virtual NChunkClient::NProto::TDataStatistics GetDataStatistics() const override
    {
        return UnderlyingReader_->GetDataStatistics();
    }

    virtual bool IsFetchingCompleted() const override
    {
        return false;
    }

    virtual std::vector<TChunkId> GetFailedChunkIds() const override
    {
        return std::vector<TChunkId>();
    }

private:
    const ISchemafulReaderPtr UnderlyingReader_;
    const TTableReaderOptionsPtr Options_;
    const std::vector<int> IdMapping_;
    const TNameTablePtr NameTable_;
    const TKeyColumns KeyColumns_;
    const int TableIndex_;
    const int RangeIndex_;

    TChunkedMemoryPool MemoryPool_;
    int TableIndexId_ = -1;
    int RangeIndexId_ = -1;
    int SystemColumnCount_ = 0;
    TPromise<void> ErrorPromise_ = NewPromise<void>();
};

DEFINE_REFCOUNTED_TYPE(TSchemalessReaderAdapter)

////////////////////////////////////////////////////////////////////////////////

ISchemalessReaderPtr CreateSchemalessReaderAdapter(
    ISchemafulReaderPtr underlyingReader,
    TTableReaderOptionsPtr options,
    TNameTablePtr nameTable,
    const TTableSchema& schema,
    const TColumnFilter& columnFilter,
    int tableIndex,
    int rangeIndex)
{
    std::vector<int> idMapping(schema.GetColumnCount());

    try {
        for (const auto& column : schema.Columns()) {
            idMapping[schema.GetColumnIndex(column)] = nameTable->GetIdOrRegisterName(column.Name);
        }
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Failed to update name table for schemaless reader adapter")
            << ex;
    }

    auto result = New<TSchemalessReaderAdapter>(
        std::move(underlyingReader),
        std::move(options),
        std::move(idMapping),
        std::move(nameTable),
        schema.GetKeyColumns(),
        tableIndex,
        rangeIndex);

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT

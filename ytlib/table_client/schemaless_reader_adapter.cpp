#include "schemaless_reader_adapter.h"
#include "name_table.h"
#include "schema.h"
#include "schemaful_reader.h"
#include "schemaless_reader.h"

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
        std::vector<int> idMapping,
        TNameTablePtr nameTable,
        TKeyColumns keyColumns)
        : UnderlyingReader_(std::move(underlyingReader))
        , IdMapping_(std::move(idMapping))
        , NameTable_(std::move(nameTable))
        , KeyColumns_(std::move(keyColumns))
        , MemoryPool_(TSchemalessReaderAdapterPoolTag())
    { }

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
                auto row = TMutableUnversionedRow::Allocate(&MemoryPool_, rows_[index].GetCount());
                for (int valueIndex = 0; valueIndex < row.GetCount(); ++valueIndex) {
                    const auto& value = rows_[index][valueIndex];
                    ValidateDataValue(value);
                    row[valueIndex] = value;
                    row[valueIndex].Id = IdMapping_[value.Id];
                }
                rows_[index] = row;
            }
        } catch (const std::exception& ex) {
            rows_.clear();
            ErrorPromise_.Set(ex);
        }

        RowCount_ += rows->size();

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

    virtual TNameTablePtr GetNameTable() const override
    {
        return NameTable_;
    }

    virtual TKeyColumns GetKeyColumns() const override
    {
        return KeyColumns_;
    }

    virtual NChunkClient::NProto::TDataStatistics GetDataStatistics() const override
    {
        NChunkClient::NProto::TDataStatistics dataStatistics;
        dataStatistics.set_row_count(RowCount_);
        return dataStatistics;
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
    const std::vector<int> IdMapping_;
    const TNameTablePtr NameTable_;
    const TKeyColumns KeyColumns_;

    TChunkedMemoryPool MemoryPool_;
    int RowCount_ = 0;
    TPromise<void> ErrorPromise_ = NewPromise<void>();
};

DEFINE_REFCOUNTED_TYPE(TSchemalessReaderAdapter)

////////////////////////////////////////////////////////////////////////////////

ISchemalessReaderPtr CreateSchemalessReaderAdapter(
    TSchemafulReaderFactory createReader,
    TNameTablePtr nameTable,
    const TTableSchema& schema,
    const TColumnFilter& columnFilter)
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

    auto underlyingReader = createReader(schema, columnFilter);

    auto result = New<TSchemalessReaderAdapter>(
        std::move(underlyingReader),
        std::move(idMapping),
        std::move(nameTable),
        schema.GetKeyColumns());

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT

#include "stdafx.h"

#include "schemaful_reader_adapter.h"

#include "name_table.h"
#include "schema.h"
#include "schemaful_reader.h"
#include "schemaless_reader.h"
#include "schemaless_row_reorderer.h"

#include <core/misc/chunked_memory_pool.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TSchemafulReaderAdapter)

struct TSchemafulReaderAdapterPoolTag { };

class TSchemafulReaderAdapter
    : public ISchemafulReader
{
public:
    TSchemafulReaderAdapter(
        ISchemalessReaderPtr underlyingReader,
        TNameTablePtr nameTable,
        const TTableSchema& schema,
        const TKeyColumns& keyColumns)
        : UnderlyingReader_(std::move(underlyingReader))
        , ReaderSchema_(schema)
        , RowReorderer_(TNameTable::FromSchema(schema), keyColumns)
        , MemoryPool_(TSchemafulReaderAdapterPoolTag())
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
            for (int i = 0; i < rows->size(); ++i) {
                auto row = RowReorderer_.ReorderKey(rows_[i], &MemoryPool_);
                for (int valueIndex = 0; valueIndex < ReaderSchema_.Columns().size(); ++valueIndex) {
                    ValidateDataValue(row[valueIndex]);
                    ValidateValueType(row[valueIndex], ReaderSchema_, valueIndex);
                }
                rows_[i] = row;
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

private:
    ISchemalessReaderPtr UnderlyingReader_;

    TTableSchema ReaderSchema_;

    TSchemalessRowReorderer RowReorderer_;
    TChunkedMemoryPool MemoryPool_;

    TPromise<void> ErrorPromise_ = NewPromise<void>();

};

DEFINE_REFCOUNTED_TYPE(TSchemafulReaderAdapter)

TFuture<ISchemafulReaderPtr> CreateSchemafulReaderAdapter(
    TSchemalessReaderFactory createReader,
    const TTableSchema& schema)
{
    TKeyColumns keyColumns;
    for (const auto& columnSchema : schema.Columns()) {
        keyColumns.push_back(columnSchema.Name);
    }

    auto nameTable = TNameTable::FromSchema(schema);
    TColumnFilter columnFilter(schema.Columns().size());

    auto underlyingReader = createReader(nameTable, columnFilter);

    auto result = New<TSchemafulReaderAdapter>(
        underlyingReader,
        nameTable,
        schema,
        keyColumns);

    return underlyingReader->Open().Apply(BIND([=] () -> ISchemafulReaderPtr {
        return result;
    }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT

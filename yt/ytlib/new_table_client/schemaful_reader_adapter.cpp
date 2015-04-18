#include "stdafx.h"

#include "schemaful_reader_adapter.h"

#include "name_table.h"
#include "schema.h"
#include "schemaful_reader.h"
#include "schemaless_reader.h"
#include "schemaless_row_reorderer.h"

#include <core/misc/chunked_memory_pool.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TSchemafulReaderAdapter)

struct TSchemafulReaderAdapterPoolTag { };

class TSchemafulReaderAdapter
    : public ISchemafulReader
{
public:
    explicit TSchemafulReaderAdapter(TSchemalessReaderFactory createReader)
        : CreateReader_(createReader)
        , MemoryPool_(TSchemafulReaderAdapterPoolTag())
    { }

    virtual TFuture<void> Open(const TTableSchema& schema) override
    {
        ReaderSchema_ = schema;
        auto nameTable = TNameTable::FromSchema(ReaderSchema_);
        TKeyColumns keyColumns;
        for (const auto& columnSchema : ReaderSchema_.Columns()) {
            keyColumns.push_back(columnSchema.Name);
        }

        TColumnFilter columnFilter(ReaderSchema_.Columns().size());

        RowReorderer_.reset(new TSchemalessRowReorderer(nameTable, keyColumns));
        UnderlyingReader_ = CreateReader_(nameTable, columnFilter);

        return UnderlyingReader_->Open();
    }

    virtual bool Read(std::vector<TUnversionedRow>* rows) override
    {
        MemoryPool_.Clear();
        auto hasMore = UnderlyingReader_->Read(rows);
        if (rows->empty()) {
            return hasMore;
        }

        YCHECK(hasMore);
        auto& rows_ = *rows;

        try {
            for (int i = 0; i < rows->size(); ++i) {
                rows_[i] = RowReorderer_->ReorderKey(rows_[i], &MemoryPool_);
                ValidateServerDataRow(rows_[i], 0, ReaderSchema_);
            }
        } catch (const std::exception& ex) {
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
    TSchemalessReaderFactory CreateReader_;

    TTableSchema ReaderSchema_;

    std::unique_ptr<TSchemalessRowReorderer> RowReorderer_;
    TChunkedMemoryPool MemoryPool_;

    TPromise<void> ErrorPromise_ = NewPromise<void>();

};

DEFINE_REFCOUNTED_TYPE(TSchemafulReaderAdapter)

ISchemafulReaderPtr CreateSchemafulReaderAdapter(TSchemalessReaderFactory createReader)
{
    return New<TSchemafulReaderAdapter>(createReader);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT

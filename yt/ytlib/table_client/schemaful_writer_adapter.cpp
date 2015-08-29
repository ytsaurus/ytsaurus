#include "stdafx.h"

#include "schemaful_writer_adapter.h"

#include "name_table.h"
#include "schema.h"
#include "schemaful_writer.h"
#include "schemaless_writer.h"
#include "schemaless_row_reorderer.h"

#include <core/misc/chunked_memory_pool.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TSchemafulWriterAdapter)

struct TSchemafulWriterAdapterPoolTag { };

class TSchemafulWriterAdapter
    : public ISchemafulWriter
{
public:
    explicit TSchemafulWriterAdapter(TSchemalessWriterFactory createWriter)
        : CreateWriter_(createWriter)
    { }

    virtual TFuture<void> Open(const TTableSchema& schema, const TKeyColumns& keyColumns = TKeyColumns()) override
    {
        YCHECK(!UnderlyingWriter_);
        auto nameTable = TNameTable::FromSchema(schema);
        UnderlyingWriter_ = CreateWriter_(nameTable);
        return UnderlyingWriter_->Open();
    }

    virtual bool Write(const std::vector<TUnversionedRow>& rows) override
    {
        return UnderlyingWriter_->Write(rows);
    }

    virtual TFuture<void> Close() override
    {
        return UnderlyingWriter_->Close();
    }

    virtual TFuture<void> GetReadyEvent() override
    {
        return UnderlyingWriter_->GetReadyEvent();
    }

private:
    ISchemalessWriterPtr UnderlyingWriter_;
    TSchemalessWriterFactory CreateWriter_;
};

DEFINE_REFCOUNTED_TYPE(TSchemafulWriterAdapter)

ISchemafulWriterPtr CreateSchemafulWriterAdapter(TSchemalessWriterFactory createWriter)
{
    return New<TSchemafulWriterAdapter>(createWriter);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT

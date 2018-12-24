#include "schemaful_writer_adapter.h"
#include "schemaless_row_reorderer.h"

#include <yt/client/table_client/schema.h>
#include <yt/client/table_client/name_table.h>
#include <yt/client/table_client/unversioned_writer.h>

#include <yt/core/misc/chunked_memory_pool.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TSchemafulWriterAdapter)

class TSchemafulWriterAdapter
    : public IUnversionedRowsetWriter
{
public:
    explicit TSchemafulWriterAdapter(IUnversionedWriterPtr underlyingWriter)
        : UnderlyingWriter_(std::move(underlyingWriter))
    { }

    virtual bool Write(TRange<TUnversionedRow> rows) override
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
    const IUnversionedWriterPtr UnderlyingWriter_;

};

DEFINE_REFCOUNTED_TYPE(TSchemafulWriterAdapter)

IUnversionedRowsetWriterPtr CreateSchemafulWriterAdapter(IUnversionedWriterPtr underlyingWriter)
{
    return New<TSchemafulWriterAdapter>(underlyingWriter);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient

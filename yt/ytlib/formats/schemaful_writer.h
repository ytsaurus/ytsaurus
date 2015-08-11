#pragma once

#include "public.h"

#include <core/misc/blob_output.h>

#include <core/concurrency/public.h>

#include <core/actions/future.h>

#include <core/yson/writer.h>

#include <ytlib/new_table_client/schemaful_writer.h>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

class TSchemafulWriter
    : public NVersionedTableClient::ISchemafulWriter
{
public:
    template <class TMakeWriter>
    explicit TSchemafulWriter(
        NConcurrency::IAsyncOutputStreamPtr stream,
        TMakeWriter&& makeWriter)
        : Stream_(stream)
        , Writer_(makeWriter(&Buffer_))
    { }

    virtual TFuture<void> Open(
        const NVersionedTableClient::TTableSchema& schema,
        const TNullable<NVersionedTableClient::TKeyColumns>& keyColumns) override;

    virtual TFuture<void> Close() override;

    virtual bool Write(const std::vector<NVersionedTableClient::TUnversionedRow>& rows) override;

    virtual TFuture<void> GetReadyEvent() override;

private:
    NConcurrency::IAsyncOutputStreamPtr Stream_;

    TBlobOutput Buffer_;
    std::unique_ptr<NYson::TYsonConsumerBase> Writer_;

    NVersionedTableClient::TTableSchema Schema_;

    TFuture<void> Result_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT


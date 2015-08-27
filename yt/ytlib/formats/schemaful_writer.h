#pragma once

#include "public.h"

#include <core/misc/blob_output.h>

#include <core/concurrency/public.h>

#include <core/actions/future.h>

#include <core/yson/writer.h>

#include <ytlib/table_client/schemaful_writer.h>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

class TSchemafulWriter
    : public NTableClient::ISchemafulWriter
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
        const NTableClient::TTableSchema& schema,
        const NTableClient::TKeyColumns& keyColumns) override;

    virtual TFuture<void> Close() override;

    virtual bool Write(const std::vector<NTableClient::TUnversionedRow>& rows) override;

    virtual TFuture<void> GetReadyEvent() override;

private:
    NConcurrency::IAsyncOutputStreamPtr Stream_;

    TBlobOutput Buffer_;
    std::unique_ptr<NYson::TYsonConsumerBase> Writer_;

    NTableClient::TTableSchema Schema_;

    TFuture<void> Result_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT


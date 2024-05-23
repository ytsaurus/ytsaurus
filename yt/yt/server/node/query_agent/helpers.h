#pragma once

#include "public.h"

#include <yt/yt/client/table_client/public.h>
#include <yt/yt/client/table_client/unversioned_writer.h>
#include <yt/yt/client/table_client/row_buffer.h>

namespace NYT::NQueryAgent {

////////////////////////////////////////////////////////////////////////////////

bool IsRetriableQueryError(const TError& error);

////////////////////////////////////////////////////////////////////////////////

class TSimpleRowsetWriter
    : public NTableClient::IUnversionedRowsetWriter
{
public:
    explicit TSimpleRowsetWriter(IMemoryChunkProviderPtr chunkProvider);

    TSharedRange<NTableClient::TUnversionedRow> GetRows() const;

    TFuture<TSharedRange<NTableClient::TUnversionedRow>> GetResult() const;

    TFuture<void> Close() override;

    bool Write(TRange<NTableClient::TUnversionedRow> rows) override;

    TFuture<void> GetReadyEvent() override;

    void Fail(const TError& error);

private:
    struct TSchemafulRowsetWriterBufferTag
    { };

    const TPromise<TSharedRange<NTableClient::TUnversionedRow>> Result_ = NewPromise<TSharedRange<NTableClient::TUnversionedRow>>();
    const NTableClient::TRowBufferPtr RowBuffer_;
    std::vector<NTableClient::TUnversionedRow> Rows_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryAgent

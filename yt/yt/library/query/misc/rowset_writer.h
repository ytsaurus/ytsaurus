#pragma once

#include <yt/yt/client/table_client/unversioned_writer.h>

namespace NYT::NQueryClient {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TSimpleRowsetWriter)

class TSimpleRowsetWriter
    : public IUnversionedRowsetWriter
{
public:
    explicit TSimpleRowsetWriter(IMemoryChunkProviderPtr chunkProvider);

    TSharedRange<TUnversionedRow> GetRows() const;

    TFuture<TSharedRange<TUnversionedRow>> GetResult() const;

    TFuture<void> Close() override;

    bool Write(TRange<TUnversionedRow> rows) override;

    TFuture<void> GetReadyEvent() override;

    void Fail(const TError& error);

    std::optional<NCrypto::TMD5Hash> GetDigest() const override;

private:
    struct TSchemafulRowsetWriterBufferTag
    { };

    const TPromise<TSharedRange<TUnversionedRow>> Result_ = NewPromise<TSharedRange<TUnversionedRow>>();
    const TRowBufferPtr RowBuffer_;
    std::vector<TUnversionedRow> Rows_;
};

DEFINE_REFCOUNTED_TYPE(TSimpleRowsetWriter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient

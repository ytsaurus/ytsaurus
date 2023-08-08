#pragma once

#include <yt/yt/client/formats/format.h>

#include <yt/yt/client/table_client/public.h>

#include <util/generic/buffer.h>

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

class TAnyToCompositeConverter
    : public NFormats::ISchemalessFormatWriter
{
public:
    TAnyToCompositeConverter(
        NFormats::ISchemalessFormatWriterPtr underlyingWriter,
        std::vector<NTableClient::TTableSchemaPtr>& schemas,
        const NTableClient::TNameTablePtr& nameTable);

    TFuture<void> GetReadyEvent() override;
    bool Write(TRange<NTableClient::TUnversionedRow> rows) override;
    bool WriteBatch(NTableClient::IUnversionedRowBatchPtr rowBatch) override;
    TBlob GetContext() const override;
    i64 GetWrittenSize() const override;
    TFuture<void> Close() override;
    TFuture<void> Flush() override;

private:
    NFormats::ISchemalessFormatWriterPtr UnderlyingWriter_;
    const int TableIndexId_;
    std::vector<std::vector<bool>> TableIndexToIsComposite_;
    const NTableClient::TRowBufferPtr RowBuffer_;
    std::vector<NTableClient::TUnversionedRow> ConvertedRows_;

private:
    NTableClient::TUnversionedRow ConvertAnyToComposite(NTableClient::TUnversionedRow row);
    TRange<NTableClient::TUnversionedRow> ConvertAnyToComposite(TRange<NTableClient::TUnversionedRow> rows);
};

DEFINE_REFCOUNTED_TYPE(TAnyToCompositeConverter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy

#pragma once

#include "public.h"
#include "format.h"
#include "helpers.h"

#include <yt/ytlib/table_client/public.h>
#include <yt/ytlib/table_client/schemaless_writer.h>

#include <yt/core/concurrency/public.h>

#include <yt/core/misc/blob_output.h>

#include <yt/core/yson/public.h>

#include <memory>
#include <limits>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

class TSchemalessFormatWriterBase
    : public ISchemalessFormatWriter
{
public:
    virtual TFuture<void> Open() override;

    virtual bool Write(const TRange<NTableClient::TUnversionedRow>& rows) override;

    virtual TFuture<void> GetReadyEvent() override;

    virtual TFuture<void> Close() override;

    virtual const NTableClient::TNameTablePtr& GetNameTable() const override;

    virtual const NTableClient::TTableSchema& GetSchema() const override;

    virtual TBlob GetContext() const override;

    virtual i64 GetWrittenSize() const override;

protected:
    const NTableClient::TNameTablePtr NameTable_;
    const NConcurrency::IAsyncOutputStreamPtr Output_;
    const bool EnableContextSaving_;
    const TControlAttributesConfigPtr ControlAttributesConfig_;
    const int KeyColumnCount_;

    const std::unique_ptr<NTableClient::TNameTableReader> NameTableReader_;

    NTableClient::TOwningKey LastKey_;
    NTableClient::TKey CurrentKey_;

    TSchemalessFormatWriterBase(
        NTableClient::TNameTablePtr nameTable,
        NConcurrency::IAsyncOutputStreamPtr output,
        bool enableContextSaving,
        TControlAttributesConfigPtr controlAttributesConfig,
        int keyColumnCount);

    TBlobOutput* GetOutputStream();

    void TryFlushBuffer(bool force);
    virtual void FlushWriter();

    virtual void DoWrite(const TRange<NTableClient::TUnversionedRow>& rows) = 0;

    bool CheckKeySwitch(NTableClient::TUnversionedRow row, bool isLastRow);

    bool IsSystemColumnId(int id) const;
    bool IsTableIndexColumnId(int id) const;
    bool IsRangeIndexColumnId(int id) const;
    bool IsRowIndexColumnId(int id) const;

    int GetRangeIndexColumnId() const;
    int GetRowIndexColumnId() const;
    int GetTableIndexColumnId() const;

    // This is suitable only for switch-based control attributes,
    // e.g. in such formats as YAMR or YSON.
    void WriteControlAttributes(NTableClient::TUnversionedRow row);
    virtual void WriteTableIndex(i64 tableIndex);
    virtual void WriteRangeIndex(i64 rangeIndex);
    virtual void WriteRowIndex(i64 rowIndex);

    bool HasError() const;
    void RegisterError(const TError& error);

private:
    TBlobOutput CurrentBuffer_;
    TSharedRef PreviousBuffer_;

    int RowIndexId_ = -1;
    int RangeIndexId_ = -1;
    int TableIndexId_ = -1;

    i64 RangeIndex_ = std::numeric_limits<i64>::min();
    i64 TableIndex_ = std::numeric_limits<i64>::min();
    i64 RowIndex_ = std::numeric_limits<i64>::min();

    bool EnableRowControlAttributes_;

    TError Error_;

    i64 WrittenSize_ = 0;

    void DoFlushBuffer();
};

////////////////////////////////////////////////////////////////////////////////

class TSchemalessWriterAdapter
    : public TSchemalessFormatWriterBase
{
public:
    TSchemalessWriterAdapter(
        NTableClient::TNameTablePtr nameTable,
        NConcurrency::IAsyncOutputStreamPtr output,
        bool enableContextSaving,
        TControlAttributesConfigPtr controlAttributesConfig,
        int keyColumnCount);

    void Init(const TFormat& format);

private:
    std::unique_ptr<NYson::IFlushableYsonConsumer> Consumer_;
    bool SkipNullValues_;

    template <class T>
    void WriteControlAttribute(
        NTableClient::EControlAttribute controlAttribute,
        T value);

    void ConsumeRow(NTableClient::TUnversionedRow row);

    virtual void DoWrite(const TRange<NTableClient::TUnversionedRow>& rows) override;
    virtual void FlushWriter() override;

    virtual void WriteTableIndex(i64 tableIndex) override;
    virtual void WriteRangeIndex(i64 rangeIndex) override;
    virtual void WriteRowIndex(i64 rowIndex) override;
};

////////////////////////////////////////////////////////////////////////////////

void WriteYsonValue(NYson::IYsonConsumer* writer, const NTableClient::TUnversionedValue& value);

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT

#pragma once

#include "public.h"
#include "config.h"
#include "helpers.h"
#include "dsv_table.h"
#include "schemaless_writer_adapter.h"

#include <ytlib/table_client/public.h>

#include <core/misc/enum.h>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

class TDsvWriterBase
{
public:
    explicit TDsvWriterBase(TDsvFormatConfigPtr config);

protected:
    TDsvFormatConfigPtr Config_;
    TDsvTable Table_;

    void EscapeAndWrite(const TStringBuf& string, bool inKey, TOutputStream* stream);
};

////////////////////////////////////////////////////////////////////////////////

class TSchemalessDsvWriter
    : public TSchemalessFormatWriterBase
    , public TDsvWriterBase
{
public:
    TSchemalessDsvWriter(
        NTableClient::TNameTablePtr nameTable,
        bool enableContextSaving,
        NConcurrency::IAsyncOutputStreamPtr output,
        TDsvFormatConfigPtr config = New<TDsvFormatConfig>());

    virtual void DoWrite(const std::vector<NTableClient::TUnversionedRow>& rows) override;

    virtual void WriteTableIndex(int tableIndex) override;

    virtual void WriteRangeIndex(i32 rangeIndex) override;

    virtual void WriteRowIndex(i64 rowIndex) override;

private:
    int TableIndex_ = 0;

    void WriteValue(const NTableClient::TUnversionedValue& value);
    void FinalizeRow(bool firstValue);
};

DEFINE_REFCOUNTED_TYPE(TSchemalessDsvWriter)

////////////////////////////////////////////////////////////////////////////////

// YsonNode is written as follows:
//  * Each element of list is ended with RecordSeparator
//  * Items in map are separated with FieldSeparator
//  * Key and Values in map are separated with KeyValueSeparator
class TDsvNodeConsumer
    : public TDsvWriterBase
    , public TFormatsConsumerBase
{
public:
    explicit TDsvNodeConsumer(
        TOutputStream* stream,
        TDsvFormatConfigPtr config = New<TDsvFormatConfig>());

    // IYsonConsumer overrides.
    virtual void OnStringScalar(const TStringBuf& value) override;
    virtual void OnInt64Scalar(i64 value) override;
    virtual void OnUint64Scalar(ui64 value) override;
    virtual void OnDoubleScalar(double value) override;
    virtual void OnBooleanScalar(bool value) override;
    virtual void OnEntity() override;
    virtual void OnBeginList() override;
    virtual void OnListItem() override;
    virtual void OnEndList() override;
    virtual void OnBeginMap() override;
    virtual void OnKeyedItem(const TStringBuf& key) override;
    virtual void OnEndMap() override;
    virtual void OnBeginAttributes() override;
    virtual void OnEndAttributes() override;

private:
    bool AllowBeginList_;
    bool AllowBeginMap_;

    bool BeforeFirstMapItem_;
    bool BeforeFirstListItem_;

    TOutputStream* Stream_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT

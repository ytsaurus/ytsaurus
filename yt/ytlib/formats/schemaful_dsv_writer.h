#pragma once

#include "public.h"
#include "config.h"
#include "helpers.h"
#include "schemaful_dsv_table.h"

#include <core/misc/blob.h>
#include <core/misc/nullable.h>

#include <core/concurrency/async_stream.h>

#include <ytlib/table_client/public.h>

#include <ytlib/new_table_client/schemaful_writer.h>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

//! Note: only tabular format is supported.
class TSchemafulDsvConsumer
    : public virtual TFormatsConsumerBase
{
public:
    explicit TSchemafulDsvConsumer(
        TOutputStream* stream,
        TSchemafulDsvFormatConfigPtr config = New<TSchemafulDsvFormatConfig>());

    // IYsonConsumer overrides.
    virtual void OnStringScalar(const TStringBuf& value) override;
    virtual void OnInt64Scalar(i64 value) override;
    virtual void OnDoubleScalar(double value) override;
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
    TOutputStream* Stream_;
    TSchemafulDsvFormatConfigPtr Config_;

    TSchemafulDsvTable Table_;

    std::set<TStringBuf> Keys_;
    std::map<TStringBuf, TStringBuf> Values_;

    std::vector<Stroka> ValueHolder_;

    int ValueCount_;
    TStringBuf CurrentKey_;

    int TableIndex_;

    DECLARE_ENUM(EState,
        (None)
        (ExpectValue)
        (ExpectAttributeName)
        (ExpectAttributeValue)
        (ExpectEndAttributes)
        (ExpectEntity)
    );

    EState State_;

    NTableClient::EControlAttribute ControlAttribute_;

    void WriteRow();
    void EscapeAndWrite(const TStringBuf& value) const;
};

////////////////////////////////////////////////////////////////////////////////

class TSchemafulDsvWriter
    : public NVersionedTableClient::ISchemafulWriter
{
public:
    explicit TSchemafulDsvWriter(
        NConcurrency::IAsyncOutputStreamPtr stream,
        TSchemafulDsvFormatConfigPtr config = New<TSchemafulDsvFormatConfig>());

    virtual TAsyncError Open(
        const NVersionedTableClient::TTableSchema& schema,
        const TNullable<NVersionedTableClient::TKeyColumns>& keyColumns) override;

    virtual TAsyncError Close() override;

    virtual bool Write(const std::vector<NVersionedTableClient::TUnversionedRow>& rows) override;

    virtual TAsyncError GetReadyEvent() override;

private:
    void WriteValue(const NVersionedTableClient::TUnversionedValue& value);
    static char* WriteIntegerReversed(char* ptr, i64 value);

    void WriteRaw(const TStringBuf& str);
    void WriteRaw(char ch);

    NConcurrency::IAsyncOutputStreamPtr Stream_;
    TSchemafulDsvFormatConfigPtr Config_;

    std::vector<int> ColumnIdMapping_;
    TBlob Buffer_;

    TAsyncError Result_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT


#include "dsv_writer.h"

#include <yt/client/table_client/name_table.h>

#include <yt/core/misc/error.h>

#include <yt/core/yson/format.h>

namespace NYT {
namespace NFormats {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TDsvWriterBase::TDsvWriterBase(
    TDsvFormatConfigPtr config)
    : Config_(config)
{
    YCHECK(Config_);
    ConfigureEscapeTables(config, true /* addCarriageReturn */, &KeyEscapeTable_, &ValueEscapeTable_);
}

////////////////////////////////////////////////////////////////////////////////

class TSchemalessWriterForDsv
    : public TSchemalessFormatWriterBase
    , public TDsvWriterBase
{
public:
    TSchemalessWriterForDsv(
        TNameTablePtr nameTable,
        bool enableContextSaving,
        TControlAttributesConfigPtr controlAttributesConfig,
        IAsyncOutputStreamPtr output,
        TDsvFormatConfigPtr config = New<TDsvFormatConfig>())
        : TSchemalessFormatWriterBase(
             nameTable,
             std::move(output),
             enableContextSaving,
             controlAttributesConfig,
             0 /* keyColumnCount */)
        , TDsvWriterBase(config)
    { }

private:
    virtual void DoWrite(const TRange<TUnversionedRow>& rows) override
    {
        auto* output = GetOutputStream();
        for (const auto& row : rows) {
            bool firstValue = true;

            if (Config_->LinePrefix) {
                output->Write(Config_->LinePrefix.Get());
                firstValue = false;
            }

            for (const auto* value = row.Begin(); value != row.End(); ++value) {
                if (value->Type == EValueType::Null) {
                    continue;
                }

                if (IsRangeIndexColumnId(value->Id) ||
                    IsRowIndexColumnId(value->Id) ||
                    (IsTableIndexColumnId(value->Id) && !Config_->EnableTableIndex))
                {
                    continue;
                }

                if (!firstValue) {
                    output->Write(Config_->FieldSeparator);
                }
                firstValue = false;

                if (IsTableIndexColumnId(value->Id)) {
                    WriteTableIndexValue(*value);
                } else {
                    WriteValue(*value);
                }
            }

            output->Write(Config_->RecordSeparator);
            TryFlushBuffer(false);
        }
        TryFlushBuffer(true);
    }

    void WriteValue(const TUnversionedValue& value)
    {
        auto* output = GetOutputStream();
        EscapeAndWrite(NameTableReader_->GetName(value.Id), output, KeyEscapeTable_);
        output->Write(Config_->KeyValueSeparator);
        WriteUnversionedValue(value, output, ValueEscapeTable_);
    }

    void WriteTableIndexValue(const TUnversionedValue& value)
    {
        auto* output = GetOutputStream();
        EscapeAndWrite(Config_->TableIndexColumn, output, KeyEscapeTable_);
        output->Write(Config_->KeyValueSeparator);
        output->Write(::ToString(value.Data.Int64));
    }
};

////////////////////////////////////////////////////////////////////////////////

TDsvNodeConsumer::TDsvNodeConsumer(
    IOutputStream* stream,
    TDsvFormatConfigPtr config)
    : TDsvWriterBase(config)
    , Stream_(stream)
{ }

void TDsvNodeConsumer::OnStringScalar(TStringBuf value)
{
    EscapeAndWrite(value, Stream_, ValueEscapeTable_);
}

void TDsvNodeConsumer::OnInt64Scalar(i64 value)
{
    Stream_->Write(::ToString(value));
}

void TDsvNodeConsumer::OnUint64Scalar(ui64 value)
{
    Stream_->Write(::ToString(value));
}

void TDsvNodeConsumer::OnDoubleScalar(double value)
{
    Stream_->Write(::ToString(value));
}

void TDsvNodeConsumer::OnBooleanScalar(bool value)
{
    Stream_->Write(FormatBool(value));
}

void TDsvNodeConsumer::OnEntity()
{
    THROW_ERROR_EXCEPTION("Entities are not supported by DSV");
}

void TDsvNodeConsumer::OnBeginList()
{
    if (AllowBeginList_) {
        AllowBeginList_ = false;
    } else {
        THROW_ERROR_EXCEPTION("Embedded lists are not supported by DSV");
    }
}

void TDsvNodeConsumer::OnListItem()
{
    AllowBeginMap_ = true;
    if (BeforeFirstListItem_) {
        BeforeFirstListItem_ = false;
    } else {
        // Not first item.
        Stream_->Write(Config_->RecordSeparator);
    }
}

void TDsvNodeConsumer::OnEndList()
{
    Stream_->Write(Config_->RecordSeparator);
}

void TDsvNodeConsumer::OnBeginMap()
{
    if (AllowBeginMap_) {
        AllowBeginList_ = false;
        AllowBeginMap_ = false;
        BeforeFirstMapItem_ = true;
    } else {
        THROW_ERROR_EXCEPTION("Embedded maps are not supported by DSV");
    }
}

void TDsvNodeConsumer::OnKeyedItem(TStringBuf key)
{
    Y_ASSERT(!AllowBeginMap_);
    Y_ASSERT(!AllowBeginList_);

    if (BeforeFirstMapItem_) {
        BeforeFirstMapItem_ = false;
    } else {
        Stream_->Write(Config_->FieldSeparator);
    }

    EscapeAndWrite(key, Stream_, KeyEscapeTable_);
    Stream_->Write(Config_->KeyValueSeparator);
}

void TDsvNodeConsumer::OnEndMap()
{
    Y_ASSERT(!AllowBeginMap_);
    Y_ASSERT(!AllowBeginList_);
}

void TDsvNodeConsumer::OnBeginAttributes()
{
    THROW_ERROR_EXCEPTION("Embedded attributes are not supported by DSV");
}

void TDsvNodeConsumer::OnEndAttributes()
{
    Y_UNREACHABLE();
}

////////////////////////////////////////////////////////////////////////////////

ISchemalessFormatWriterPtr CreateSchemalessWriterForDsv(
    TDsvFormatConfigPtr config,
    TNameTablePtr nameTable,
    IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    TControlAttributesConfigPtr controlAttributesConfig,
    int keyColumnCount)
{
    if (controlAttributesConfig->EnableKeySwitch) {
        THROW_ERROR_EXCEPTION("Key switches are not supported in DSV format");
    }

    if (controlAttributesConfig->EnableRangeIndex) {
        THROW_ERROR_EXCEPTION("Range indices are not supported in DSV format");
    }

    if (controlAttributesConfig->EnableRowIndex) {
        THROW_ERROR_EXCEPTION("Row indices are not supported in DSV format");
    }

    return New<TSchemalessWriterForDsv>(
        nameTable,
        enableContextSaving,
        controlAttributesConfig,
        output,
        config);
}

ISchemalessFormatWriterPtr CreateSchemalessWriterForDsv(
    const IAttributeDictionary& attributes,
    TNameTablePtr nameTable,
    IAsyncOutputStreamPtr output,
    bool enableContextSaving,
    TControlAttributesConfigPtr controlAttributesConfig,
    int keyColumnCount)
{
    auto config = ConvertTo<TDsvFormatConfigPtr>(&attributes);
    return CreateSchemalessWriterForDsv(
        config,
        nameTable,
        output,
        enableContextSaving,
        controlAttributesConfig,
        keyColumnCount);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT

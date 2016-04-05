#include "dsv_writer.h"

#include <yt/ytlib/table_client/name_table.h>

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
    , Table_(config, true)
{
    YCHECK(Config_);
}

void TDsvWriterBase::EscapeAndWrite(const TStringBuf& string, bool inKey, TOutputStream* stream)
{
    if (Config_->EnableEscaping) {
        WriteEscaped(
            stream,
            string,
            inKey ? Table_.KeyStops : Table_.ValueStops,
            Table_.Escapes,
            Config_->EscapingSymbol);
    } else {
        stream->Write(string);
    }
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

    virtual void DoWrite(const std::vector<TUnversionedRow>& rows) override
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

private:
    void WriteValue(const TUnversionedValue& value)
    {
        auto* output = GetOutputStream();
        EscapeAndWrite(NameTableReader_->GetName(value.Id), true, output);
        output->Write(Config_->KeyValueSeparator);

        switch (value.Type) {
            case EValueType::Int64:
                output->Write(::ToString(value.Data.Int64));
                break;

            case EValueType::Uint64:
                output->Write(::ToString(value.Data.Uint64));
                break;

            case EValueType::Double: {
                auto str = ::ToString(value.Data.Double);
                output->Write(str);
                if (str.find('.') == Stroka::npos && str.find('e') == Stroka::npos) {
                    output->Write(".");
                }

                break;
            }

            case EValueType::Boolean:
                output->Write(FormatBool(value.Data.Boolean));
                break;

            case EValueType::String:
                EscapeAndWrite(TStringBuf(value.Data.String, value.Length), false, output);
                break;

            case EValueType::Any:
                THROW_ERROR_EXCEPTION("Values of type \"any\" are not supported by dsv format (Value: %v)", value);

            default:
                YUNREACHABLE();
        }
    }
    
    void WriteTableIndexValue(const TUnversionedValue& value)
    {
        auto* output = GetOutputStream();
        EscapeAndWrite(Config_->TableIndexColumn, true, output);
        output->Write(Config_->KeyValueSeparator);
        output->Write(::ToString(value.Data.Int64));
    }
};

////////////////////////////////////////////////////////////////////////////////

TDsvNodeConsumer::TDsvNodeConsumer(
    TOutputStream* stream,
    TDsvFormatConfigPtr config)
    : TDsvWriterBase(config)
    , AllowBeginList_(true)
    , AllowBeginMap_(true)
    , BeforeFirstMapItem_(true)
    , BeforeFirstListItem_(true)
    , Stream_(stream)
{ }

void TDsvNodeConsumer::OnStringScalar(const TStringBuf& value)
{
    EscapeAndWrite(value, false, Stream_);
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

void TDsvNodeConsumer::OnKeyedItem(const TStringBuf& key)
{
    YASSERT(!AllowBeginMap_);
    YASSERT(!AllowBeginList_);

    if (BeforeFirstMapItem_) {
        BeforeFirstMapItem_ = false;
    } else {
        Stream_->Write(Config_->FieldSeparator);
    }

    EscapeAndWrite(key, true, Stream_);
    Stream_->Write(Config_->KeyValueSeparator);
}

void TDsvNodeConsumer::OnEndMap()
{
    YASSERT(!AllowBeginMap_);
    YASSERT(!AllowBeginList_);
}

void TDsvNodeConsumer::OnBeginAttributes()
{
    THROW_ERROR_EXCEPTION("Embedded attributes are not supported by DSV");
}

void TDsvNodeConsumer::OnEndAttributes()
{
    YUNREACHABLE();
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

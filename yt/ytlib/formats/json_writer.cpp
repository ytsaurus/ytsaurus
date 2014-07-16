#include "stdafx.h"
#include "config.h"
#include "helpers.h"
#include "json_writer.h"
#include "utf8_decoder.h"

#include <core/ytree/forwarding_yson_consumer.h>
#include <core/ytree/null_yson_consumer.h>

#include <library/json/json_writer.h>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

using namespace NYTree;
using namespace NYson;

class TJsonConsumerImpl
    : public NYson::TYsonConsumerBase
{
public:
    TJsonConsumerImpl(
        TOutputStream* output,
        NYson::EYsonType type,
        TJsonFormatConfigPtr config);

    void Flush();

    virtual void OnStringScalar(const TStringBuf& value) override;
    virtual void OnInt64Scalar(i64 value) override;
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
    TJsonConsumerImpl(NJson::TJsonWriter* jsonWriter, TJsonFormatConfigPtr config);

    std::unique_ptr<NJson::TJsonWriter> UnderlyingJsonWriter;
    NJson::TJsonWriter* JsonWriter;
    TOutputStream* Output;
    TJsonFormatConfigPtr Config;
    NYson::EYsonType Type;

    void WriteStringScalar(const TStringBuf& value);

    void EnterNode();
    void LeaveNode();
    bool IsWriteAllowed();

    std::vector<bool> HasUnfoldedStructureStack;
    int InAttributesBalance;
    bool HasAttributes;
    int Depth;
    bool CheckLimit;

    TUtf8Transcoder Utf8Transcoder_;
};

class TJsonConsumer
    : public NYTree::TForwardingYsonConsumer
{
public:
    TJsonConsumer(
        TOutputStream* output,
        NYson::EYsonType type,
        TJsonFormatConfigPtr config)
            : Impl_(
                output,
                type,
                config)
    {
        Forward(&Impl_, BIND(&TJsonConsumerImpl::Flush, &Impl_), type);
    }

private:
    TJsonConsumerImpl Impl_;
};

////////////////////////////////////////////////////////////////////////////////

TJsonConsumerImpl::TJsonConsumerImpl(TOutputStream* output,
    NYson::EYsonType type,
    TJsonFormatConfigPtr config)
    : Output(output)
    , Config(config)
    , Type(type)
    , Depth(0)
    , CheckLimit(true)
    , Utf8Transcoder_(Config->EncodeUtf8)
{
    if (Type == EYsonType::MapFragment) {
        THROW_ERROR_EXCEPTION("Map fragments are not supported by Json");
    }

    UnderlyingJsonWriter.reset(new NJson::TJsonWriter(
        output,
        Config->Format == EJsonFormat::Pretty));
    JsonWriter = UnderlyingJsonWriter.get();
    HasAttributes = false;
    InAttributesBalance = 0;
}

void TJsonConsumerImpl::EnterNode()
{
    if (Config->AttributesMode == EJsonAttributesMode::Never) {
        HasAttributes = false;
    } else if (Config->AttributesMode == EJsonAttributesMode::OnDemand) {
        // Do nothing
    } else if (Config->AttributesMode == EJsonAttributesMode::Always) {
        if (!HasAttributes) {
            JsonWriter->OpenMap();
            JsonWriter->Write("$attributes");
            JsonWriter->OpenMap();
            JsonWriter->CloseMap();
        }
        HasAttributes = true;
    }
    HasUnfoldedStructureStack.push_back(HasAttributes);

    if (HasAttributes) {
        JsonWriter->Write("$value");
        HasAttributes = false;
    }

    Depth += 1;
}

void TJsonConsumerImpl::LeaveNode()
{
    YCHECK(!HasUnfoldedStructureStack.empty());
    if (HasUnfoldedStructureStack.back()) {
        // Close map of the {$attributes, $value}
        JsonWriter->CloseMap();
    }
    HasUnfoldedStructureStack.pop_back();

    Depth -= 1;

    if (Depth == 0 && Type == NYson::EYsonType::ListFragment && InAttributesBalance == 0) {
        JsonWriter->Reset();
        Output->Write("\n");
    }
}

bool TJsonConsumerImpl::IsWriteAllowed()
{
    if (Config->AttributesMode == EJsonAttributesMode::Never) {
        return InAttributesBalance == 0;
    }
    return true;
}

void TJsonConsumerImpl::OnStringScalar(const TStringBuf& value)
{
    if (IsWriteAllowed()) {
        TStringBuf writeValue = value;
        if (CheckLimit && Config->StringLengthLimit && value.Size() > *Config->StringLengthLimit) {
            // To prevent length check while writing this attribute
            CheckLimit = false;
            OnBeginAttributes();
                OnKeyedItem("incomplete");
                OnStringScalar("true");
            OnEndAttributes();
            CheckLimit = true;

            writeValue = value.substr(0, *Config->StringLengthLimit);
        }
        EnterNode();
        WriteStringScalar(writeValue);
        LeaveNode();
    }
}

void TJsonConsumerImpl::OnInt64Scalar(i64 value)
{
    if (IsWriteAllowed()) {
        EnterNode();
        JsonWriter->Write(value);
        LeaveNode();
    }
}

void TJsonConsumerImpl::OnDoubleScalar(double value)
{
    if (IsWriteAllowed()) {
        EnterNode();
        JsonWriter->Write(value);
        LeaveNode();
    }
}

void TJsonConsumerImpl::OnBooleanScalar(bool value)
{
    if (IsWriteAllowed()) {
        if (Config->BooleanAsString) {
            OnStringScalar(FormatBool(value));
        } else {
            EnterNode();
            JsonWriter->Write(value);
            LeaveNode();
        }
    }
}

void TJsonConsumerImpl::OnEntity()
{
    if (IsWriteAllowed()) {
        EnterNode();
        JsonWriter->WriteNull();
        LeaveNode();
    }
}

void TJsonConsumerImpl::OnBeginList()
{
    if (IsWriteAllowed()) {
        EnterNode();
        JsonWriter->OpenArray();
    }
}

void TJsonConsumerImpl::OnListItem()
{ }

void TJsonConsumerImpl::OnEndList()
{
    if (IsWriteAllowed()) {
        JsonWriter->CloseArray();
        LeaveNode();
    }
}

void TJsonConsumerImpl::OnBeginMap()
{
    if (IsWriteAllowed()) {
        EnterNode();
        JsonWriter->OpenMap();
    }
}

void TJsonConsumerImpl::OnKeyedItem(const TStringBuf& name)
{
    if (IsWriteAllowed()) {
        if (IsSpecialJsonKey(name)) {
            WriteStringScalar(Stroka("$") + name);
        } else {
            WriteStringScalar(name);
        }
    }
}

void TJsonConsumerImpl::OnEndMap()
{
    if (IsWriteAllowed()) {
        JsonWriter->CloseMap();
        LeaveNode();
    }
}

void TJsonConsumerImpl::OnBeginAttributes()
{
    InAttributesBalance += 1;
    if (Config->AttributesMode != EJsonAttributesMode::Never) {
        JsonWriter->OpenMap();
        JsonWriter->Write("$attributes");
        JsonWriter->OpenMap();
    }
}

void TJsonConsumerImpl::OnEndAttributes()
{
    InAttributesBalance -= 1;
    if (Config->AttributesMode != EJsonAttributesMode::Never) {
        HasAttributes = true;
        JsonWriter->CloseMap();
    }
}

TJsonConsumerImpl::TJsonConsumerImpl(NJson::TJsonWriter* jsonWriter, TJsonFormatConfigPtr config)
    : JsonWriter(jsonWriter)
    , Config(config)
    , Utf8Transcoder_(Config->EncodeUtf8)
{ }

void TJsonConsumerImpl::WriteStringScalar(const TStringBuf &value)
{
    JsonWriter->Write(Utf8Transcoder_.Encode(value));
}

void TJsonConsumerImpl::Flush()
{
    JsonWriter->Flush();
}

std::unique_ptr<IYsonConsumer> CreateJsonConsumer(
    TOutputStream* output,
    NYson::EYsonType type,
    TJsonFormatConfigPtr config)
{
    return std::unique_ptr<IYsonConsumer>(new TJsonConsumer(output, type, config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT

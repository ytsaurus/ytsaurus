#include "json_writer.h"
#include "config.h"
#include "helpers.h"
#include "utf8_decoder.h"

#include <yt/core/misc/common.h>

#include <yt/core/ytree/forwarding_yson_consumer.h>
#include <yt/core/ytree/null_yson_consumer.h>

#ifdef YT_IN_ARCADIA
#include <contrib/libs/yajl/api/yajl_gen.h>
#else
//include <yajl/yajl_gen.h>
#endif

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

using namespace NYTree;
using namespace NYson;

class TJsonWriter {
public:
    TJsonWriter(TOutputStream *out, bool formatOutput);
    ~TJsonWriter();

    void Flush();
    void Reset();

    void BeginMap();
    void EndMap();

    void BeginList();
    void EndList();

    void WriteNull();

    void Write(const TStringBuf& value);
    void Write(const char *value) {
        Write(TStringBuf(value));
    }

    void Write(double value);
    void Write(bool value);
    void Write(i64 value);
    void Write(ui64 value);

private:
    yajl_gen Handle;
    TOutputStream *Output;
};

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
    TJsonConsumerImpl(TJsonWriter* jsonWriter, TJsonFormatConfigPtr config);

    std::unique_ptr<TJsonWriter> UnderlyingJsonWriter;
    TJsonWriter* JsonWriter;
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

static void CheckYajlCode(int yajlCode)
{
    if (yajlCode == yajl_gen_status_ok) {
        return;
    }

    Stroka errorMessage;
    switch (yajlCode)
    {
        case yajl_gen_keys_must_be_strings:
            errorMessage = "JSON key must be a string";
            break;
        case yajl_max_depth_exceeded:
            errorMessage = Sprintf("JSON maximal depth exceeded %d", YAJL_MAX_DEPTH);
            break;
        case yajl_gen_in_error_state:
            errorMessage = "JSON: a generator function (yajl_gen_XXX) was called while in an error state";
            break;
        case yajl_gen_invalid_number:
            errorMessage = "Invalid floating point value in json";
            break;
        case yajl_gen_invalid_string:
            errorMessage = "Invalid UTF-8 string in json";
            break;
        default:
            errorMessage = Sprintf("Yajl writer failed with code %d", yajlCode);
    }
    THROW_ERROR_EXCEPTION(errorMessage);
}

TJsonWriter::TJsonWriter(TOutputStream *output, bool formatOutput)
    : Output(output)
{
    Handle = yajl_gen_alloc(nullptr);
    yajl_gen_config(Handle, yajl_gen_beautify, formatOutput ? 1 : 0);
    yajl_gen_config(Handle, yajl_gen_validate_utf8, 1);
}

TJsonWriter::~TJsonWriter()
{
    yajl_gen_free(Handle);
}

void TJsonWriter::Flush()
{
    size_t len = 0;
    const unsigned char *buf = nullptr;
    CheckYajlCode(yajl_gen_get_buf(Handle, &buf, &len));
    Output->Write(buf, len);
    yajl_gen_clear(Handle);
}

void TJsonWriter::Reset()
{
    Flush();
    yajl_gen_reset(Handle, nullptr);
}

void TJsonWriter::BeginMap()
{
    CheckYajlCode(yajl_gen_map_open(Handle));
}

void TJsonWriter::EndMap()
{
    CheckYajlCode(yajl_gen_map_close(Handle));
}

void TJsonWriter::BeginList()
{
    CheckYajlCode(yajl_gen_array_open(Handle));
}

void TJsonWriter::EndList()
{
    CheckYajlCode(yajl_gen_array_close(Handle));
}

void TJsonWriter::Write(const TStringBuf &value)
{
    CheckYajlCode(yajl_gen_string(Handle, (const unsigned char *)value.c_str(), value.size()));
}

void TJsonWriter::WriteNull()
{
    CheckYajlCode(yajl_gen_null(Handle));
}

void TJsonWriter::Write(double value)
{
    CheckYajlCode(yajl_gen_double(Handle, value));
}

void TJsonWriter::Write(i64 value)
{
    CheckYajlCode(yajl_gen_integer(Handle, value));
}

void TJsonWriter::Write(ui64 value)
{
    CheckYajlCode(yajl_gen_uinteger(Handle, value));
}

void TJsonWriter::Write(bool value)
{
    CheckYajlCode(yajl_gen_bool(Handle, value ? 1 : 0));
}

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

    UnderlyingJsonWriter.reset(new TJsonWriter(
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
            JsonWriter->BeginMap();
            JsonWriter->Write("$attributes");
            JsonWriter->BeginMap();
            JsonWriter->EndMap();
            HasAttributes = true;
        }
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
        JsonWriter->EndMap();
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

        if (Config->AttributesMode != EJsonAttributesMode::Never) {
            if (CheckLimit && Config->StringLengthLimit && value.Size() > *Config->StringLengthLimit ) {
                if (!HasAttributes) {
                    JsonWriter->BeginMap();
                    HasAttributes = true;
                }

                JsonWriter->Write("$incomplete");
                JsonWriter->Write(true);
                writeValue = value.substr(0, *Config->StringLengthLimit);
            }

            if (Config->AnnotateWithTypes) {
                if (!HasAttributes) {
                    JsonWriter->BeginMap();
                    HasAttributes = true;
                }

                JsonWriter->Write("$type");
                JsonWriter->Write("string");
            }
        }

        EnterNode();
        WriteStringScalar(writeValue);
        LeaveNode();
    }
}

void TJsonConsumerImpl::OnInt64Scalar(i64 value)
{
    if (IsWriteAllowed()) {
        if (Config->AnnotateWithTypes && Config->AttributesMode != EJsonAttributesMode::Never) {
            if (!HasAttributes) {
                JsonWriter->BeginMap();
                HasAttributes = true;
            }
            JsonWriter->Write("$type");
            JsonWriter->Write("int64");
        }
        EnterNode();
        if (Config->Stringify) {
            WriteStringScalar(::ToString(value));
        } else {
            JsonWriter->Write(value);
        }
        LeaveNode();
    }
}

void TJsonConsumerImpl::OnUint64Scalar(ui64 value)
{
    if (IsWriteAllowed()) {
        if (Config->AnnotateWithTypes && Config->AttributesMode != EJsonAttributesMode::Never) {
            if (!HasAttributes) {
                JsonWriter->BeginMap();
                HasAttributes = true;
            }
            JsonWriter->Write("$type");
            JsonWriter->Write("uint64");
        }
        EnterNode();
        if (Config->Stringify) {
            WriteStringScalar(::ToString(value));
        } else {
            JsonWriter->Write(value);
        }
        LeaveNode();

    }
}

void TJsonConsumerImpl::OnDoubleScalar(double value)
{
    if (IsWriteAllowed()) {
        if (Config->AnnotateWithTypes && Config->AttributesMode != EJsonAttributesMode::Never) {
            if (!HasAttributes) {
                JsonWriter->BeginMap();
                HasAttributes = true;
            }
            JsonWriter->Write("$type");
            JsonWriter->Write("double");
        }
        EnterNode();
        if (Config->Stringify) {
            WriteStringScalar(::ToString(value));
        } else {
            JsonWriter->Write(value);
        }
        LeaveNode();
    }
}

void TJsonConsumerImpl::OnBooleanScalar(bool value)
{
    if (IsWriteAllowed()) {
        if (Config->AnnotateWithTypes && Config->AttributesMode != EJsonAttributesMode::Never) {
            if (!HasAttributes) {
                JsonWriter->BeginMap();
                HasAttributes = true;
            }
            JsonWriter->Write("$type");
            JsonWriter->Write("boolean");
        }
        EnterNode();
        if (Config->Stringify || Config->BooleanAsString) {
            WriteStringScalar(FormatBool(value));
        } else {
            JsonWriter->Write(value);
        }
        LeaveNode();
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
        JsonWriter->BeginList();
    }
}

void TJsonConsumerImpl::OnListItem()
{ }

void TJsonConsumerImpl::OnEndList()
{
    if (IsWriteAllowed()) {
        JsonWriter->EndList();
        LeaveNode();
    }
}

void TJsonConsumerImpl::OnBeginMap()
{
    if (IsWriteAllowed()) {
        EnterNode();
        JsonWriter->BeginMap();
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
        JsonWriter->EndMap();
        LeaveNode();
    }
}

void TJsonConsumerImpl::OnBeginAttributes()
{
    InAttributesBalance += 1;
    if (Config->AttributesMode != EJsonAttributesMode::Never) {
        JsonWriter->BeginMap();
        JsonWriter->Write("$attributes");
        JsonWriter->BeginMap();
    }
}

void TJsonConsumerImpl::OnEndAttributes()
{
    InAttributesBalance -= 1;
    if (Config->AttributesMode != EJsonAttributesMode::Never) {
        JsonWriter->EndMap();
        HasAttributes = true;
    }
}

TJsonConsumerImpl::TJsonConsumerImpl(TJsonWriter* jsonWriter, TJsonFormatConfigPtr config)
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

std::unique_ptr<TYsonConsumerBase> CreateJsonConsumer(
    TOutputStream* output,
    NYson::EYsonType type,
    TJsonFormatConfigPtr config)
{
    return std::make_unique<TJsonConsumer>(output, type, config);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT

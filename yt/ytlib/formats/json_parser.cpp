#include "stdafx.h"
#include "json_parser.h"
#include "json_callbacks.h"
#include "helpers.h"
#include "utf8_decoder.h"

#include <core/misc/error.h>

#include <yajl/yajl_parse.h>

#include <array>

namespace NYT {
namespace NFormats {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

static int OnNull(void* ctx)
{
    static_cast<TJsonCallbacks*>(ctx)->OnEntity();
    return 1;
}

static int OnBoolean(void *ctx, int boolean)
{
    static_cast<TJsonCallbacks*>(ctx)->OnBooleanScalar(boolean);
    return 1;
}

static int OnInteger(void *ctx, long long value)
{
    static_cast<TJsonCallbacks*>(ctx)->OnInt64Scalar(value);
    return 1;
}

static int OnDouble(void *ctx, double value)
{
    static_cast<TJsonCallbacks*>(ctx)->OnDoubleScalar(value);
    return 1;
}

static int OnString(void *ctx, const unsigned char *val, size_t len)
{
    static_cast<TJsonCallbacks*>(ctx)->OnStringScalar(TStringBuf((const char *)val, len));
    return 1;
}

static int OnStartMap(void *ctx)
{
    static_cast<TJsonCallbacks*>(ctx)->OnBeginMap();
    return 1;
}

static int OnMapKey(void *ctx, const unsigned char *val, size_t len)
{
    static_cast<TJsonCallbacks*>(ctx)->OnKeyedItem(TStringBuf((const char *)val, len));
    return 1;
}

static int OnEndMap(void *ctx)
{
    static_cast<TJsonCallbacks*>(ctx)->OnEndMap();
    return 1;
}

static int OnStartArray(void *ctx)
{
    static_cast<TJsonCallbacks*>(ctx)->OnBeginList();
    return 1;
}

static int OnEndArray(void *ctx)
{
    static_cast<TJsonCallbacks*>(ctx)->OnEndList();
    return 1;
}

static yajl_callbacks YajlCallbacks = {
    OnNull,
    OnBoolean,
    OnInteger,
    OnDouble,
    nullptr,
    OnString,
    OnStartMap,
    OnMapKey,
    OnEndMap,
    OnStartArray,
    OnEndArray
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TJsonParser::TImpl
{
public:
    TImpl(IYsonConsumer* consumer, TJsonFormatConfigPtr config, EYsonType type)
        : Consumer_(consumer), Config_(config), Type_(type)
    {
        YCHECK(Type_ != EYsonType::MapFragment);
        if (!Config_) {
            Config_ = New<TJsonFormatConfig>();
        }
        if (Config_->Format == EJsonFormat::Pretty && Type_ == EYsonType::ListFragment) {
            THROW_ERROR_EXCEPTION("Pretty json format is not supported for list fragments");
        }
        Callbacks_ = TJsonCallbacks(TUtf8Transcoder(Config_->EncodeUtf8), Config_->MemoryLimit);

        YajlHandle_ = yajl_alloc(&YajlCallbacks, nullptr, static_cast<void*>(&Callbacks_));
        if (Type_ == EYsonType::ListFragment) {
            yajl_config(YajlHandle_, yajl_allow_multiple_values, 1);
            // To allow empty list fragment
            yajl_config(YajlHandle_, yajl_allow_partial_values, 1);
        }
        yajl_set_memory_limit(YajlHandle_, Config_->MemoryLimit);
    }

    void Read(const TStringBuf& data);
    void Finish();

    void Parse(TInputStream* input);

private:
    IYsonConsumer* Consumer_;
    TJsonFormatConfigPtr Config_;
    EYsonType Type_;

    yajl_handle YajlHandle_;
    TJsonCallbacks Callbacks_;

    void ConsumeNode(INodePtr node);
    void ConsumeNode(IListNodePtr node);
    void ConsumeNode(IMapNodePtr node);
    void ConsumeMapFragment(IMapNodePtr node);

    void ConsumeNodes();
    void OnError(const char* data, int len);
};

void TJsonParser::TImpl::ConsumeNodes()
{
    while (Callbacks_.HasFinishedNodes()) {
        if (Type_ == EYsonType::ListFragment) {
            Consumer_->OnListItem();
        }
        ConsumeNode(Callbacks_.ExtractFinishedNode());
    }
    if (Config_->Format == EJsonFormat::Pretty && Type_ == EYsonType::ListFragment) {
        THROW_ERROR_EXCEPTION("Pretty json format isn't supported for list fragments");
    }
}

void TJsonParser::TImpl::OnError(const char* data, int len)
{
    unsigned char* errorMessage = yajl_get_error(
        YajlHandle_,
        1,
        reinterpret_cast<const unsigned char*>(data),
        len);
    TError error("Error parsing JSON: %s", errorMessage);
    yajl_free_error(YajlHandle_, errorMessage);
    yajl_free(YajlHandle_);
    THROW_ERROR_EXCEPTION(error);
}

void TJsonParser::TImpl::Read(const TStringBuf& data)
{
    if (yajl_parse(
            YajlHandle_,
            reinterpret_cast<const unsigned char*>(data.Data()),
            data.Size()) == yajl_status_error)
    {
        OnError(data.Data(), data.Size());
    }
    ConsumeNodes();
}

void TJsonParser::TImpl::Finish()
{
    if (yajl_complete_parse(YajlHandle_) == yajl_status_error) {
        OnError(nullptr, 0);
    }
    yajl_free(YajlHandle_);
    ConsumeNodes();
}

void TJsonParser::TImpl::Parse(TInputStream* input)
{
    static const int bufferLength = 65536;
    std::array<char, bufferLength> buffer;
    while (int readLength = input->Read(buffer.data(), bufferLength))
    {
        Read(TStringBuf(buffer.data(), readLength));
    }
    Finish();
}

void TJsonParser::TImpl::ConsumeNode(INodePtr node)
{
    switch (node->GetType()) {
        case ENodeType::Int64:
            Consumer_->OnInt64Scalar(node->AsInt64()->GetValue());
            break;
        case ENodeType::Double:
            Consumer_->OnDoubleScalar(node->AsDouble()->GetValue());
            break;
        case ENodeType::Boolean:
            Consumer_->OnBooleanScalar(node->AsBoolean()->GetValue());
            break;
        case ENodeType::Entity:
            Consumer_->OnEntity();
            break;
        case ENodeType::String:
            Consumer_->OnStringScalar(node->AsString()->GetValue());
            break;
        case ENodeType::Map:
            ConsumeNode(node->AsMap());
            break;
        case ENodeType::List:
            ConsumeNode(node->AsList());
            break;
        default:
            YUNREACHABLE();
            break;
    };
}

void TJsonParser::TImpl::ConsumeMapFragment(IMapNodePtr map)
{
    for (const auto& pair : map->GetChildren()) {
        TStringBuf key = pair.first;
        INodePtr value = pair.second;
        if (IsSpecialJsonKey(key)) {
            if (key.size() < 2 || key[1] != '$') {
                THROW_ERROR_EXCEPTION(
                    "Key '%v' starts with single '$'; use '$%v'"
                    "to encode this key in JSON format", key, key);
            }
            key = key.substr(1);
        }
        Consumer_->OnKeyedItem(key);
        ConsumeNode(value);
    }
}

void TJsonParser::TImpl::ConsumeNode(IMapNodePtr map)
{
    auto node = map->FindChild("$value");
    if (node) {
        auto attributes = map->FindChild("$attributes");
        if (attributes) {
            if (attributes->GetType() != ENodeType::Map) {
                THROW_ERROR_EXCEPTION("Value of $attributes must be map");
            }
            Consumer_->OnBeginAttributes();
            ConsumeMapFragment(attributes->AsMap());
            Consumer_->OnEndAttributes();
        }
        ConsumeNode(node);
    } else {
        if (map->FindChild("$attributes")) {
            THROW_ERROR_EXCEPTION("Found key `$attributes` without key `$value`");
        }
        Consumer_->OnBeginMap();
        ConsumeMapFragment(map);
        Consumer_->OnEndMap();
    }
}

void TJsonParser::TImpl::ConsumeNode(IListNodePtr list)
{
    Consumer_->OnBeginList();
    for (int i = 0; i < list->GetChildCount(); ++i) {
        Consumer_->OnListItem();
        ConsumeNode(list->GetChild(i));
    }
    Consumer_->OnEndList();
}

////////////////////////////////////////////////////////////////////////////////

TJsonParser::TJsonParser(
    IYsonConsumer* consumer,
    TJsonFormatConfigPtr config,
    EYsonType type)
    : Impl_(new TImpl(consumer, config, type))
{ }

void TJsonParser::Read(const TStringBuf& data)
{
    Impl_->Read(data);
}

void TJsonParser::Finish()
{
    Impl_->Finish();
}

void TJsonParser::Parse(TInputStream* input)
{
    Impl_->Parse(input);
}

////////////////////////////////////////////////////////////////////////////////

void ParseJson(
    TInputStream* input,
    IYsonConsumer* consumer,
    TJsonFormatConfigPtr config,
    EYsonType type)
{
    TJsonParser jsonParser(consumer, config, type);
    jsonParser.Parse(input);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT

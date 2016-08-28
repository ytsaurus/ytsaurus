#include "json_parser.h"
#include "helpers.h"
#include "json_callbacks.h"
#include "utf8_decoder.h"

#include <yt/core/misc/error.h>

#include <array>

#include <contrib/libs/yajl/api/yajl_parse.h>

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

static int OnUnsignedInteger(void *ctx, unsigned long long value)
{
    static_cast<TJsonCallbacks*>(ctx)->OnUint64Scalar(value);
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
    OnUnsignedInteger,
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

struct TJsonParserBufferTag
{ };

class TJsonParser::TImpl
{
public:
    TImpl(IYsonConsumer* consumer, TJsonFormatConfigPtr config, EYsonType type)
        : Consumer_(consumer)
        , Config_(config ? config : New<TJsonFormatConfig>())
        , Type_(type)
        , Callbacks_(std::make_unique<TJsonCallbacks>(
            TUtf8Transcoder(Config_->EncodeUtf8),
            Config_->MemoryLimit))
    {
        YCHECK(Type_ != EYsonType::MapFragment);

        if (Config_->Format == EJsonFormat::Pretty && Type_ == EYsonType::ListFragment) {
            THROW_ERROR_EXCEPTION("Pretty JSON format is not supported for list fragments");
        }

        YajlHandle_ = yajl_alloc(&YajlCallbacks, nullptr, Callbacks_.get());
        if (Type_ == EYsonType::ListFragment) {
            yajl_config(YajlHandle_, yajl_allow_multiple_values, 1);
            // To allow empty list fragment
            yajl_config(YajlHandle_, yajl_allow_partial_values, 1);
        }
        yajl_set_memory_limit(YajlHandle_, Config_->MemoryLimit);

        Buffer_ = TSharedMutableRef::Allocate<TJsonParserBufferTag>(Config_->BufferSize);
    }

    void Read(const TStringBuf& data);
    void Finish();

    void Parse(TInputStream* input);

private:
    IYsonConsumer* const Consumer_;
    const TJsonFormatConfigPtr Config_;
    const EYsonType Type_;

    const std::unique_ptr<TJsonCallbacks> Callbacks_;

    TSharedMutableRef Buffer_;

    yajl_handle YajlHandle_;

    void ConsumeNode(INodePtr node);
    void ConsumeNode(IListNodePtr node);
    void ConsumeNode(IMapNodePtr node);
    void ConsumeMapFragment(IMapNodePtr node);

    void ConsumeNodes();
    void OnError(const char* data, int len);
};

void TJsonParser::TImpl::ConsumeNodes()
{
    while (Callbacks_->HasFinishedNodes()) {
        if (Type_ == EYsonType::ListFragment) {
            Consumer_->OnListItem();
        }
        ConsumeNode(Callbacks_->ExtractFinishedNode());
    }
    if (Config_->Format == EJsonFormat::Pretty && Type_ == EYsonType::ListFragment) {
        THROW_ERROR_EXCEPTION("Pretty JSON format is not supported for list fragments");
    }
}

void TJsonParser::TImpl::OnError(const char* data, int len)
{
    unsigned char* errorMessage = yajl_get_error(
        YajlHandle_,
        1,
        reinterpret_cast<const unsigned char*>(data),
        len);
    auto error = TError("Error parsing JSON") << TError((char*) errorMessage);
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
    while (true) {
        auto readLength = input->Read(Buffer_.Begin(), Config_->BufferSize);
        if (readLength == 0) {
            break;
        }
        Read(TStringBuf(Buffer_.begin(), readLength));
    }
    Finish();
}

void TJsonParser::TImpl::ConsumeNode(INodePtr node)
{
    switch (node->GetType()) {
        case ENodeType::Int64:
            Consumer_->OnInt64Scalar(node->AsInt64()->GetValue());
            break;
        case ENodeType::Uint64:
            Consumer_->OnUint64Scalar(node->AsUint64()->GetValue());
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
            Y_UNREACHABLE();
            break;
    };
}

void TJsonParser::TImpl::ConsumeMapFragment(IMapNodePtr map)
{
    for (const auto& pair : map->GetChildren()) {
        auto key = TStringBuf(pair.first);
        const auto& value = pair.second;
        if (IsSpecialJsonKey(key)) {
            if (key.size() < 2 || key[1] != '$') {
                THROW_ERROR_EXCEPTION(
                    "Key \"%v\" starts with single \"$\"; use \"$%v\" "
                    "to encode this key in JSON format",
                    key,
                    key);
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
                THROW_ERROR_EXCEPTION("Value of \"$attributes\" must be a map");
            }
            Consumer_->OnBeginAttributes();
            ConsumeMapFragment(attributes->AsMap());
            Consumer_->OnEndAttributes();
        }

        auto type = map->FindChild("$type");

        if (type) {
            if (type->GetType() != ENodeType::String) {
                THROW_ERROR_EXCEPTION("Value of \"$type\" must be a string");
            }
            auto typeString = type->AsString()->GetValue();
            ENodeType expectedType;
            if (typeString == "string") {
                expectedType = ENodeType::String;
            } else if (typeString == "int64") {
                expectedType = ENodeType::Int64;
            } else if (typeString == "uint64") {
                expectedType = ENodeType::Uint64;
            } else if (typeString == "double") {
                expectedType = ENodeType::Double;
            } else if (typeString == "boolean") {
                expectedType = ENodeType::Boolean;
            } else {
                THROW_ERROR_EXCEPTION("Unexpected \"$type\" value %Qv", typeString);
            }

            if (node->GetType() == expectedType) {
                ConsumeNode(node);
            } else if (node->GetType() == ENodeType::String) {
                auto nodeAsString = node->AsString()->GetValue();
                switch (expectedType) {
                    case ENodeType::Int64:
                        Consumer_->OnInt64Scalar(FromString<i64>(nodeAsString));
                        break;
                    case ENodeType::Uint64:
                        Consumer_->OnUint64Scalar(FromString<ui64>(nodeAsString));
                        break;
                    case ENodeType::Double:
                        Consumer_->OnDoubleScalar(FromString<double>(nodeAsString));
                        break;
                    case ENodeType::Boolean: {
                        if (nodeAsString == "true") {
                            Consumer_->OnBooleanScalar(true);
                        } else if (nodeAsString == "false") {
                            Consumer_->OnBooleanScalar(false);
                        } else {
                            THROW_ERROR_EXCEPTION("Invalid boolean string %Qv", nodeAsString);
                        }
                        break;
                    }
                    default:
                        Y_UNREACHABLE();
                        break;
                }
            } else if (node->GetType() == ENodeType::Int64) {
                auto nodeAsInt = node->AsInt64()->GetValue();
                switch (expectedType) {
                    case ENodeType::Int64:
                        Consumer_->OnInt64Scalar(nodeAsInt);
                        break;
                    case ENodeType::Uint64:
                        Consumer_->OnUint64Scalar(nodeAsInt);
                        break;
                    case ENodeType::Double:
                        Consumer_->OnDoubleScalar(nodeAsInt);
                        break;
                    case ENodeType::Boolean:
                    case ENodeType::String:
                        THROW_ERROR_EXCEPTION("Type mismatch in JSON")
                            << TErrorAttribute("expected_type", expectedType)
                            << TErrorAttribute("actual_type", node->GetType());
                        break;
                    default:
                        Y_UNREACHABLE();
                        break;
                }
            } else {
                THROW_ERROR_EXCEPTION("Type mismatch in JSON")
                    << TErrorAttribute("expected_type", expectedType)
                    << TErrorAttribute("actual_type", node->GetType());
            }
        } else {
            ConsumeNode(node);
        }
    } else {
        if (map->FindChild("$attributes")) {
            THROW_ERROR_EXCEPTION("Found key \"$attributes\" without key \"$value\"");
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

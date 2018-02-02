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
    {
        YCHECK(Type_ != EYsonType::MapFragment);

        // TODO(ignat): Exception in constructor -- bad idea?
        if (Config_->Format == EJsonFormat::Pretty && Type_ == EYsonType::ListFragment) {
            THROW_ERROR_EXCEPTION("Pretty JSON format is not supported for list fragments");
        }

        if (Config_->Plain) {
            Callbacks_ = std::make_unique<TJsonCallbacksForwardingImpl>(
                Consumer_,
                Type_,
                TUtf8Transcoder(Config_->EncodeUtf8));
        } else {
            Callbacks_ = std::make_unique<TJsonCallbacksBuildingNodesImpl>(
                Consumer_,
                Type_,
                TUtf8Transcoder(Config_->EncodeUtf8),
                Config_->MemoryLimit);
        }
        YajlHandle_ = yajl_alloc(&YajlCallbacks, nullptr, Callbacks_.get());

        if (Type_ == EYsonType::ListFragment) {
            yajl_config(YajlHandle_, yajl_allow_multiple_values, 1);
            // To allow empty list fragment
            yajl_config(YajlHandle_, yajl_allow_partial_values, 1);
        }
        yajl_set_memory_limit(YajlHandle_, Config_->MemoryLimit);

        Buffer_ = TSharedMutableRef::Allocate<TJsonParserBufferTag>(Config_->BufferSize, false);
    }

    void Read(const TStringBuf& data);
    void Finish();

    void Parse(IInputStream* input);

private:
    IYsonConsumer* const Consumer_;
    const TJsonFormatConfigPtr Config_;
    const EYsonType Type_;

    std::unique_ptr<TJsonCallbacks> Callbacks_;

    TSharedMutableRef Buffer_;

    yajl_handle YajlHandle_;

    void OnError(const char* data, int len);
};

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
}

void TJsonParser::TImpl::Finish()
{
    if (yajl_complete_parse(YajlHandle_) == yajl_status_error) {
        OnError(nullptr, 0);
    }
    yajl_free(YajlHandle_);
}

void TJsonParser::TImpl::Parse(IInputStream* input)
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

void TJsonParser::Parse(IInputStream* input)
{
    Impl_->Parse(input);
}

////////////////////////////////////////////////////////////////////////////////

void ParseJson(
    IInputStream* input,
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

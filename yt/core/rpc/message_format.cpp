#include "message_format.h"
//#include "client.h"
//#include "channel_detail.h"
//#include "service.h"
//

#include <yt/core/yson/writer.h>
#include <yt/core/yson/parser.h>

#include <yt/core/json/json_parser.h>
#include <yt/core/json/json_writer.h>

//#include <yt/core/ytree/helpers.h>
//
#include <yt/core/yson/protobuf_interop.h>
#include <yt/core/misc/protobuf_helpers.h>
//
//#include <contrib/libs/protobuf/io/coded_stream.h>
#include <contrib/libs/protobuf/io/zero_copy_stream_impl_lite.h>

namespace NYT {
namespace NRpc {

using namespace NYson;
using namespace NJson;
using namespace NRpc::NProto;

////////////////////////////////////////////////////////////////////////////////

struct IMessageFormat
{
    virtual ~IMessageFormat() = default;

    virtual TSharedRef ConvertFrom(
        const TSharedRef& message,
        const NYson::TProtobufMessageType* messageType) = 0;
    virtual TSharedRef ConvertTo(
        const TSharedRef& message,
        const NYson::TProtobufMessageType* messageType) = 0;
};

////////////////////////////////////////////////////////////////////////////////

yhash<EMessageFormat, IMessageFormat*>& GetMessageFormatRegistry()
{
    static yhash<EMessageFormat, IMessageFormat*> Registry;
    return Registry;
}

IMessageFormat* GetMessageFormatOrThrow(EMessageFormat format)
{
    const auto& registry = GetMessageFormatRegistry();
    auto it = registry.find(format);
    if (it == registry.end()) {
        THROW_ERROR_EXCEPTION("Unsupported message format %Qlv",
            format);
    }
    return it->second;
}

void RegisterCustomMessageFormat(EMessageFormat format, IMessageFormat* formatHandler)
{
    YCHECK(!GetMessageFormatRegistry()[format]);
    GetMessageFormatRegistry()[format] = formatHandler;
}

namespace {

class TYsonMessageFormat
    : public IMessageFormat
{
public:
    TYsonMessageFormat()
    {
        RegisterCustomMessageFormat(EMessageFormat::Yson, this);
    }

    virtual TSharedRef ConvertFrom(const TSharedRef& message, const NYson::TProtobufMessageType* messageType) override
    {
        auto ysonBuffer = PopEnvelope(message);
        TString protoBuffer;
        {
            google::protobuf::io::StringOutputStream output(&protoBuffer);
            auto converter = CreateProtobufWriter(&output, messageType);
            ParseYsonStringBuffer(TStringBuf(ysonBuffer.Begin(), ysonBuffer.End()), EYsonType::Node, converter.get());
        }
        return PushEnvelope(TSharedRef::FromString(protoBuffer));
    }

    virtual TSharedRef ConvertTo(const TSharedRef& message, const NYson::TProtobufMessageType* messageType) override
    {
        auto protoBuffer = PopEnvelope(message);
        google::protobuf::io::ArrayInputStream stream(protoBuffer.Begin(), protoBuffer.Size());
        TString ysonBuffer;
        {
            TStringOutput output(ysonBuffer);
            TYsonWriter writer{&output, EYsonFormat::Text};
            ParseProtobuf(&writer, &stream, messageType);
        }
        return PushEnvelope(TSharedRef::FromString(ysonBuffer));
    }
} YsonFormat;

class TJsonMessageFormat
    : public IMessageFormat
{
public:
    TJsonMessageFormat()
    {
        RegisterCustomMessageFormat(EMessageFormat::Json, this);
    }

    virtual TSharedRef ConvertFrom(const TSharedRef& message, const NYson::TProtobufMessageType* messageType) override
    {
        auto jsonBuffer = PopEnvelope(message);
        TString protoBuffer;
        {
            google::protobuf::io::StringOutputStream output(&protoBuffer);
            auto converter = CreateProtobufWriter(&output, messageType);
            TMemoryInput input{jsonBuffer.Begin(), jsonBuffer.Size()};
            ParseJson(&input, converter.get());
        }
        return PushEnvelope(TSharedRef::FromString(protoBuffer));
    }

    virtual TSharedRef ConvertTo(const TSharedRef& message, const NYson::TProtobufMessageType* messageType) override
    {
        auto protoBuffer = PopEnvelope(message);
        google::protobuf::io::ArrayInputStream stream(protoBuffer.Begin(), protoBuffer.Size());
        TString ysonBuffer;
        {
            TStringOutput output(ysonBuffer);
            auto writer = CreateJsonConsumer(&output);
            ParseProtobuf(writer.get(), &stream, messageType);
            writer->Flush();
        }
        return PushEnvelope(TSharedRef::FromString(ysonBuffer));
    }
} JsonFormat;

} // namespace

TSharedRef ConvertMessageToFormat(
    const TSharedRef& message,
    EMessageFormat format,
    const TProtobufMessageType* messageType)
{
    return GetMessageFormatOrThrow(format)->ConvertTo(message, messageType);
}

TSharedRef ConvertMessageFromFormat(
    const TSharedRef& message,
    EMessageFormat format,
    const TProtobufMessageType* messageType)
{
    return GetMessageFormatOrThrow(format)->ConvertFrom(message, messageType);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT

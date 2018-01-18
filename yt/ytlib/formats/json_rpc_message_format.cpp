#include "json_writer.h"
#include "json_parser.h"

#include <yt/core/yson/protobuf_interop.h>

#include <yt/core/misc/protobuf_helpers.h>

#include <yt/core/rpc/helpers.h>

#include <contrib/libs/protobuf/io/zero_copy_stream_impl_lite.h>

namespace NYT {
namespace NFormats {

using namespace NYTree;
using namespace NYson;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

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

IMessageFormat* RegisterJsonRpcMessageFormat()
{
    return &JsonFormat;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT

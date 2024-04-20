#include <yt/yt/library/program/program.h>

#include <yt/yt/core/misc/protobuf_helpers.h>
#include <yt/yt/core/rpc/message.h>

#include <yt/yt_proto/yt/core/rpc/proto/rpc.pb.h>

#include <library/cpp/yt/memory/ref.h>
#include <library/cpp/yt/string/enum.h>
#include <library/cpp/yt/string/format.h>

namespace NYT::NTools::NParseRpcHeader {

////////////////////////////////////////////////////////////////////////////////

using namespace NRpc;

using NCompression::ECodec;

using NRpc::NProto::TRequestHeader;
using NRpc::NProto::TResponseHeader;

////////////////////////////////////////////////////////////////////////////////

class TProgram
    : public NYT::TProgram
{
public:
    TProgram()
    {
        Opts_
            .AddFreeArgBinding(
                "header-message-part",
                HexMessage_,
                "hex-encoded first part of a message (as may be seen in a snapshot dump)");
    }

    void DoRun(const NLastGetopt::TOptsParseResult& /*parseResult*/) override
    {
        auto message = BuildMessage();
        auto messageType = GetMessageType(message);

        Cout << "Message type:\t" << FormatEnum(messageType) << Endl;

        switch (messageType) {
            case EMessageType::Request: {
                TRequestHeader header;
                if (ParseRequestHeader(message, &header)) {
                    Cerr << "Message type \"" << FormatEnum(messageType) << "\" is not supported yet" << Endl;
                } else {
                    Cerr << "Failed to parse request header" << Endl;
                }
                break;
            }
            case EMessageType::Response: {
                TResponseHeader header;
                if (TryParseResponseHeader(message, &header)) {
                    PrintResponseHeader(header);
                } else {
                    Cerr << "Failed to parse response header" << Endl;
                }
                break;
            }

            case EMessageType::RequestCancelation: [[fallthrough]];
            case EMessageType::StreamingPayload: [[fallthrough]];
            case EMessageType::StreamingFeedback:
                Cerr << "Message type \"" << FormatEnum(messageType) << "\" is not supported yet" << Endl;
                break;

            case EMessageType::Unknown: [[fallthrough]];
            default:
                break;
        }
    }

    TSharedRefArray BuildMessage()
    {
        if (HexMessage_.size() % 2 != 0) {
            THROW_ERROR_EXCEPTION(
                "Hex-encoded data is %v bytes long, expected an even number",
                HexMessage_.size());
        }

        TSharedRefArrayBuilder builder(1);

        TString decodedData;
        decodedData.reserve(HexMessage_.size() / 2);
        for (auto i = 0; i < std::ssize(HexMessage_); i += 2) {
            ui8 ch = (ParseHex(HexMessage_[i]) << 4) | ParseHex(HexMessage_[i + 1]);
            decodedData.push_back(ch);
        }

        builder.Add(TSharedRef::FromString(std::move(decodedData)));
        return builder.Finish();
    }

    ui8 ParseHex(char ch)
    {
        if ('a' <= ch && ch <= 'z') {
            return 10 + static_cast<ui8>(ch - 'a');
        }

        if ('A' <= ch && ch <= 'Z') {
            return 10 + static_cast<ui8>(ch - 'A');
        }

        if ('0' <= ch && ch <= '9') {
            return static_cast<ui8>(ch - '0');
        }

        THROW_ERROR_EXCEPTION("%Qv is not a valid hex digit", ch);
    }

    void PrintResponseHeader(const TResponseHeader& header)
    {
        auto requestId = FromProto<TGuid>(header.request_id());
        auto error = FromProto<TError>(header.error());
        auto format = static_cast<EMessageFormat>(header.format());
        auto codec = static_cast<ECodec>(header.codec());
        // Extensions are not supported yet.

        Cout << "RequestId:\t" << ToString(requestId) << "\n";
        Cout << "Error:\t" << ToString(error) << "\n";
        Cout << "Format:\t" << FormatEnum(format) << "\n";
        Cout << "Codec:\t" << FormatEnum(codec) << Endl;
    }

private:
    TString HexMessage_;
};

} // namespace NYT::NTools::NParseRpcHeader

int main(int argc, const char** argv)
{
    return NYT::NTools::NParseRpcHeader::TProgram().Run(argc, argv);
}

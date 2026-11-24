#include "payload_splitter.h"

#include <yt/yt/flow/extensions/framing/cpp/unpacker.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

TYdbPayloadSplitter CreateWholeYdbPayloadSplitter()
{
    return [] (const TSharedRef& data) {
        return std::vector<TYdbPayloadRecord>{{.Payload = data}};
    };
}

TYdbPayloadSplitter CreateFramingYdbPayloadSplitter(EFramingFormat framingFormat)
{
    return [framingFormat] (const TSharedRef& data) {
        auto frames = UnpackBatch<TStringBuf>(data.ToStringBuf(), framingFormat).ValueOrThrow();
        std::vector<TYdbPayloadRecord> result;
        result.reserve(frames.size());
        for (auto frame : frames) {
            result.push_back({.Payload = data.Slice(frame.data(), frame.data() + frame.size())});
        }
        return result;
    };
}

TYdbPayloadSplitter CreateMultiLineYdbPayloadSplitter(char rowDelimiter)
{
    return [rowDelimiter] (const TSharedRef& data) {
        std::vector<TYdbPayloadRecord> unpacked;
        TStringBuf rawData = data.ToStringBuf();
        while (rawData) {
            auto row = rawData.NextTok(rowDelimiter);
            unpacked.push_back({.Payload = data.Slice(row.data(), row.data() + row.size())});
        }
        return unpacked;
    };
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

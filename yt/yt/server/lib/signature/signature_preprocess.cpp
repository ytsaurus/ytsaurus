#include "signature_preprocess.h"

#include "signature_header.h"

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

using namespace NYson;
using namespace NYTree;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TPreprocessVisitor
{
    TStringBuf Header;
    TStringBuf Payload;

    std::vector<std::byte> operator()(
        const TSignatureHeaderImpl<TSignatureVersion{0, 1}>& /*header*/) const
    {
        std::vector<std::byte> result;
        auto headerBytes = std::as_bytes(std::span(Header));
        auto payloadBytes = std::as_bytes(std::span(Payload));
        result.reserve(headerBytes.size() + 1 + payloadBytes.size());
        auto it = std::copy(headerBytes.begin(), headerBytes.end(), std::back_inserter(result));
        *it = std::byte{'\0'};
        std::copy(payloadBytes.begin(), payloadBytes.end(), ++it);
        return result;
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

std::vector<std::byte> PreprocessSignature(
    const TYsonString& header,
    const std::string& payload)
{
    return std::visit(
        TPreprocessVisitor{header.AsStringBuf(), payload},
        ConvertTo<TSignatureHeader>(header));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature

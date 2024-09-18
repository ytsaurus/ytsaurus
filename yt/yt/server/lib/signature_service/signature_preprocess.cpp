#include "signature_preprocess.h"

#include "signature_header.h"

#include <yt/yt/core/yson/string.h>

#include "yt/yt/core/ytree/convert.h"

namespace NYT::NSignatureService {

////////////////////////////////////////////////////////////////////////////////

using namespace NYson;
using namespace NYTree;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct PreprocessVisitor
{
    TStringBuf Header;
    TStringBuf Payload;

    std::vector<std::byte> operator()(
        const TSignatureHeaderImpl<TSignatureVersion{0, 1}>& /*header*/) const
    {
        std::vector<std::byte> result;
        auto it = std::ranges::copy(std::as_bytes(std::span(Header)), std::back_inserter(result));
        *it.out = std::byte{'\0'};
        std::ranges::copy(std::as_bytes(std::span(Payload)), ++it.out);
        return result;
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

std::vector<std::byte> PreprocessSignature(
    const TYsonString& header,
    const TYsonString& payload)
{
    return std::visit(
        PreprocessVisitor{header.AsStringBuf(), payload.AsStringBuf()},
        ConvertTo<TSignatureHeader>(header));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignatureService

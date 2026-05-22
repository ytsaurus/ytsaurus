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

    std::string operator()(
        const TSignatureHeaderImpl<TSignatureVersion{0, 1}>& /*header*/) const
    {
        std::string result;
        result.reserve(Header.size() + 1 + Payload.size());
        result = Header;
        result += '\0';
        result += Payload;
        return result;
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

std::string PreprocessSignature(
    const TYsonString& header,
    const std::string& payload)
{
    return std::visit(
        TPreprocessVisitor{header.AsStringBuf(), payload},
        ConvertTo<TSignatureHeader>(header));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature

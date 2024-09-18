#include "signature.h"

#include <yt/yt/core/yson/consumer.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/convert.h>

namespace NYT::NSignatureService {

////////////////////////////////////////////////////////////////////////////////

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

const TYsonString& TSignature::Payload() const noexcept
{
    return Payload_;
}

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TSignature& signature, IYsonConsumer* consumer)
{
    consumer->OnBeginMap();
    BuildYsonMapFragmentFluently(consumer)
        .Item("header").Value(signature.Header_.ToString())
        .Item("payload").Value(signature.Payload().ToString())
        .Item("signature").Value(TString(
            reinterpret_cast<const char*>(signature.Signature_.data()),
            signature.Signature_.size()));
    consumer->OnEndMap();
}

////////////////////////////////////////////////////////////////////////////////

void Deserialize(TSignature& signature, INodePtr node)
{
    auto mapNode = node->AsMap();
    signature.Header_ = TYsonString(mapNode->GetChildValueOrThrow<TString>("header"));
    signature.Payload_ = TYsonString(mapNode->GetChildValueOrThrow<TString>("payload"));

    auto signatureString = mapNode->GetChildValueOrThrow<TString>("signature");
    auto signatureBytes = std::as_bytes(std::span(TStringBuf(signatureString)));
    if (signatureBytes.size() != SignatureSize) {
        THROW_ERROR_EXCEPTION("Received incorrect signature size")
            << TErrorAttribute("received", signatureString.size())
            << TErrorAttribute("expected", SignatureSize);
    }

    std::ranges::copy(
        signatureBytes.template first<SignatureSize>(),
        signature.Signature_.begin());
}

void Deserialize(TSignature& signature, TYsonPullParserCursor* cursor)
{
    Deserialize(signature, ExtractTo<INodePtr>(cursor));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignatureService

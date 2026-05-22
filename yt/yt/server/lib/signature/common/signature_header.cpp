#include "signature_header.h"

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

using namespace NYTree;
using namespace NYson;

namespace {

////////////////////////////////////////////////////////////////////////////////

void BuildHeader(const TSignatureHeaderImpl<TSignatureVersion{0, 1}>& header, TFluentMap fluent)
{
    fluent
        .Item("issuer").Value(header.Issuer)
        .Item("keypair_id").Value(header.KeypairId)
        .Item("signature_id").Value(header.SignatureId)
        .Item("issued_at").Value(header.IssuedAt)
        .Item("expires_at").Value(header.ExpiresAt)
        .Item("valid_after").Value(header.ValidAfter);
}

////////////////////////////////////////////////////////////////////////////////

struct TSerializeVisitor
{
    IYsonConsumer* const Consumer;

    template <TSignatureVersion version>
    void operator()(const TSignatureHeaderImpl<version>& header)
    {
        BuildYsonFluently(Consumer).BeginMap()
            .Item("version").Value(Format("%v.%v", version.Major, version.Minor))
            .Do([&] (TFluentMap fluent) { BuildHeader(header, fluent); })
        .EndMap();
    }
};

////////////////////////////////////////////////////////////////////////////////

TString GetVersion(NYTree::INodePtr node)
{
    return node->AsMap()->GetChildValueOrThrow<TString>("version");
}

////////////////////////////////////////////////////////////////////////////////

struct TDeserializeVisitor
{
    IMapNodePtr MapNode;

    template <TSignatureVersion version>
    void operator()(TSignatureHeaderImpl<version>& header)
    {
        header.Issuer = MapNode->GetChildValueOrThrow<TString>("issuer");
        header.KeypairId = MapNode->GetChildValueOrThrow<TGuid>("keypair_id");
        header.SignatureId = MapNode->GetChildValueOrThrow<TGuid>("signature_id");
        header.IssuedAt = MapNode->GetChildValueOrThrow<TInstant>("issued_at");
        header.ExpiresAt = MapNode->GetChildValueOrThrow<TInstant>("expires_at");
        header.ValidAfter = MapNode->GetChildValueOrThrow<TInstant>("valid_after");
        // NB(pavook) Do version-specific operations here.
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

void Serialize(const TSignatureHeader& header, IYsonConsumer* consumer)
{
    std::visit(TSerializeVisitor{consumer}, header);
}

////////////////////////////////////////////////////////////////////////////////

void Deserialize(TSignatureHeader& header, INodePtr node)
{
    TString version = GetVersion(node);
    if (version == "0.1") {
        header = TSignatureHeaderImpl<TSignatureVersion{0, 1}>();
    } else {
        THROW_ERROR_EXCEPTION("Unknown TSignature version")
            << TErrorAttribute("version", version);
    }

    std::visit(TDeserializeVisitor{node->AsMap()}, header);
}

void Deserialize(TSignatureHeader& header, TYsonPullParserCursor* cursor)
{
    Deserialize(header, ExtractTo<INodePtr>(cursor));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature

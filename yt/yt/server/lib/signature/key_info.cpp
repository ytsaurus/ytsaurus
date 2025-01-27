#include "key_info.h"

#include "private.h"

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NSignature {

using namespace NLogging;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

struct TValidateVisitor
{
    template <TKeyPairVersion version>
    bool operator()(const TKeyPairMetadataImpl<version>& meta) const
    {
        if (meta.IsDeprecated) {
            YT_LOG_WARNING(
                "Received deprecated key info (Id: %v, Version: %v.%v)",
                meta.KeyId,
                version.Major,
                version.Minor);
        }

        // TODO(pavook): separate timestamp provider into interface.
        auto currentTime = Now();

        return meta.ValidAfter <= currentTime && currentTime < meta.ExpiresAt;
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

bool IsKeyPairMetadataValid(const TKeyPairMetadata& meta)
{
    return std::visit(TValidateVisitor{}, meta);
}

////////////////////////////////////////////////////////////////////////////////

TKeyId GetKeyId(const TKeyPairMetadata& metadata)
{
    return std::visit([] (const auto& meta) { return meta.KeyId; }, metadata);
}

////////////////////////////////////////////////////////////////////////////////

namespace {

struct TSerializeVisitor
{
    IYsonConsumer* const Consumer;

    template <TKeyPairVersion version>
    void operator()(const TKeyPairMetadataImpl<version>& metadata) const
    {
        BuildYsonFluently(Consumer).BeginMap()
            .Item("version").Value(Format("%v.%v", version.Major, version.Minor))
            .Item("owner_id").Value(metadata.OwnerId)
            .Item("key_id").Value(metadata.KeyId)
            .Item("created_at").Value(metadata.CreatedAt)
            .Item("valid_after").Value(metadata.ValidAfter)
            .Item("expires_at").Value(metadata.ExpiresAt)
        .EndMap();
    }
};

} // namespace

void Serialize(const TKeyPairMetadata& metadata, NYson::IYsonConsumer* consumer)
{
    std::visit(TSerializeVisitor{consumer}, metadata);
}

////////////////////////////////////////////////////////////////////////////////

namespace {

struct TDeserializeVisitor
{
    NYTree::IMapNodePtr MapNode;

    template <TKeyPairVersion version>
    void operator()(TKeyPairMetadataImpl<version>& metadata) const
    {
        metadata.OwnerId = MapNode->GetChildValueOrThrow<TOwnerId>("owner_id");
        metadata.KeyId = MapNode->GetChildValueOrThrow<TKeyId>("key_id");
        metadata.CreatedAt = MapNode->GetChildValueOrThrow<TInstant>("created_at");
        metadata.ValidAfter = MapNode->GetChildValueOrThrow<TInstant>("valid_after");
        metadata.ExpiresAt = MapNode->GetChildValueOrThrow<TInstant>("expires_at");
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

void Deserialize(TKeyPairMetadata& metadata, INodePtr node)
{
    TString version = node->AsMap()->GetChildValueOrThrow<TString>("version");
    if (version == "0.1") {
        metadata = TKeyPairMetadataImpl<TKeyPairVersion{0, 1}>();
    } else {
        THROW_ERROR_EXCEPTION("Unknown TKeyPair version")
            << TErrorAttribute("version", version);
    }

    std::visit(TDeserializeVisitor{node->AsMap()}, metadata);
}

void Deserialize(TKeyPairMetadata& metadata, TYsonPullParserCursor* cursor)
{
    Deserialize(metadata, ExtractTo<INodePtr>(cursor));
}

////////////////////////////////////////////////////////////////////////////////

bool TKeyInfo::Verify(
    std::span<const std::byte> data,
    std::span<const std::byte, SignatureSize> signature) const
{
    return IsKeyPairMetadataValid(Meta()) && crypto_sign_verify_detached(
        reinterpret_cast<const unsigned char*>(signature.data()),
        reinterpret_cast<const unsigned char*>(data.data()),
        data.size(),
        reinterpret_cast<const unsigned char*>(Key().data())) == 0;
}

////////////////////////////////////////////////////////////////////////////////

TKeyInfo::TKeyInfo(const TPublicKey& key, const TKeyPairMetadata& meta) noexcept
    : Key_(key)
    , Meta_(meta)
{ }

////////////////////////////////////////////////////////////////////////////////

const TPublicKey& TKeyInfo::Key() const
{
    return Key_;
}

////////////////////////////////////////////////////////////////////////////////

const TKeyPairMetadata& TKeyInfo::Meta() const
{
    return Meta_;
}

////////////////////////////////////////////////////////////////////////////////

bool TKeyInfo::operator==(const TKeyInfo& other) const
{
    return Key() == other.Key() && Meta() == other.Meta();
}

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TKeyInfo& keyInfo, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer).BeginMap()
        .Item("metadata").Value(keyInfo.Meta())
        .Item("public_key").Value(TStringBuf(
            reinterpret_cast<const char*>(keyInfo.Key().data()),
            keyInfo.Key().size()))
    .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

void Deserialize(TKeyInfo& keyInfo, INodePtr node)
{
    auto mapNode = node->AsMap();
    keyInfo.Meta_ = mapNode->GetChildValueOrThrow<TKeyPairMetadata>("metadata");

    auto keyString = mapNode->GetChildValueOrThrow<std::string>("public_key");
    auto keyBytes = std::as_bytes(std::span(keyString));
    if (keyBytes.size() != PublicKeySize) {
        THROW_ERROR_EXCEPTION("Received incorrect public key size")
            << TErrorAttribute("received", keyBytes.size())
            << TErrorAttribute("expected", PublicKeySize);
    }
    std::copy(keyBytes.begin(), keyBytes.end(), keyInfo.Key_.begin());
}

void Deserialize(TKeyInfo& keyInfo, TYsonPullParserCursor* cursor)
{
    Deserialize(keyInfo, ExtractTo<INodePtr>(cursor));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature

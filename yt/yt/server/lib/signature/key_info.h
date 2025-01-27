#pragma once

#include "public.h"

#include "crypto.h"

#include <yt/yt/core/yson/public.h>

#include <yt/yt/core/ytree/public.h>

#include <span>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

struct TKeyPairVersion
{
    int Major;
    int Minor;
};

////////////////////////////////////////////////////////////////////////////////

template <TKeyPairVersion version>
struct TKeyPairMetadataImpl;

template <>
struct TKeyPairMetadataImpl<TKeyPairVersion{0, 1}>
{
    TOwnerId OwnerId;
    TKeyId KeyId;
    TInstant CreatedAt;
    TInstant ValidAfter;
    TInstant ExpiresAt;

    constexpr static bool IsDeprecated = false;

    [[nodiscard]] bool operator==(const TKeyPairMetadataImpl& other) const = default;
};

////////////////////////////////////////////////////////////////////////////////

using TKeyPairMetadata = std::variant<
    TKeyPairMetadataImpl<TKeyPairVersion{0, 1}>
>;

void Serialize(const TKeyPairMetadata& metadata, NYson::IYsonConsumer* consumer);

void Deserialize(TKeyPairMetadata& metadata, NYTree::INodePtr node);

void Deserialize(TKeyPairMetadata& metadata, NYson::TYsonPullParserCursor* cursor);

////////////////////////////////////////////////////////////////////////////////

[[nodiscard]] bool IsKeyPairMetadataValid(const TKeyPairMetadata& metadata);

////////////////////////////////////////////////////////////////////////////////

[[nodiscard]] TKeyId GetKeyId(const TKeyPairMetadata& metadata);

////////////////////////////////////////////////////////////////////////////////

using TPublicKey = std::array<std::byte, PublicKeySize>;

class TKeyInfo final
{
public:
    TKeyInfo() noexcept = default;

    TKeyInfo(const TPublicKey& key, const TKeyPairMetadata& metadata) noexcept;

    [[nodiscard]] bool Verify(
        std::span<const std::byte> data,
        std::span<const std::byte, SignatureSize> signature) const;

    [[nodiscard]] bool operator==(const TKeyInfo& other) const;

    [[nodiscard]] const TPublicKey& Key() const;

    [[nodiscard]] const TKeyPairMetadata& Meta() const;

private:
    TPublicKey Key_;
    TKeyPairMetadata Meta_;

    friend void Deserialize(TKeyInfo& keyInfo, NYTree::INodePtr node);
};

DEFINE_REFCOUNTED_TYPE(TKeyInfo)

void Serialize(const TKeyInfo& keyInfo, NYson::IYsonConsumer* consumer);

void Deserialize(TKeyInfo& keyInfo, NYson::TYsonPullParserCursor* cursor);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature

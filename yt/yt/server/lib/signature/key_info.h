#pragma once

#include "public.h"

#include "crypto.h"

#include <span>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

struct TKeyPairMetadata
{
    TOwnerId Owner;
    TKeyId Id;
    TInstant CreatedAt;
    TInstant ValidAfter;
    TInstant ExpiresAt;

    [[nodiscard]] bool IsValid() const;

    [[nodiscard]] bool operator==(const TKeyPairMetadata& other) const = default;
};

////////////////////////////////////////////////////////////////////////////////

using TPublicKey = std::array<std::byte, PublicKeySize>;

////////////////////////////////////////////////////////////////////////////////

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
    TPublicKey const Key_;
    TKeyPairMetadata const Meta_;
};

DEFINE_REFCOUNTED_TYPE(TKeyInfo)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature

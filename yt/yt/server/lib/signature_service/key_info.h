#pragma once

#include "public.h"

#include "crypto.h"

namespace NYT::NSignatureService {

////////////////////////////////////////////////////////////////////////////////

struct TKeyPairMetadata
{
    TOwnerId Owner;
    TKeyId Id;
    TInstant CreatedAt;
    TInstant ValidAfter;
    TInstant ExpiresAt;

    [[nodiscard]] bool IsValid() const noexcept;

    [[nodiscard]] bool operator==(const TKeyPairMetadata& other) const noexcept = default;
};

////////////////////////////////////////////////////////////////////////////////

using TPublicKey = std::array<std::byte, PublicKeySize>;

////////////////////////////////////////////////////////////////////////////////

class TKeyInfo
    : public TRefCounted
{
public:
    TKeyInfo() noexcept = default;

    TKeyInfo(const TPublicKey& key, const TKeyPairMetadata& metadata) noexcept;

    [[nodiscard]] bool Verify(
        std::span<const std::byte> data,
        std::span<const std::byte, SignatureSize> signature) const noexcept;

    [[nodiscard]] bool operator==(const TKeyInfo& other) const noexcept;

    [[nodiscard]] const TPublicKey& Key() const noexcept;

    [[nodiscard]] const TKeyPairMetadata& Meta() const noexcept;

private:
    TPublicKey Key_;
    TKeyPairMetadata Meta_;
};

DEFINE_REFCOUNTED_TYPE(TKeyInfo)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignatureService

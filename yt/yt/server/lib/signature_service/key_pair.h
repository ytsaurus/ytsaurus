#pragma once

#include "public.h"

#include "crypto.h"

#include <yt/yt/core/ypath/public.h>

#include <library/cpp/string_utils/secret_string/secret_string.h>

#include <util/datetime/base.h>

#include <span>

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

class TKeyPair
{
public:
    TKeyPair(const TKeyPair& other) = delete;
    TKeyPair& operator=(const TKeyPair& other) = delete;

    TKeyPair(TKeyPair&& other) = default;
    TKeyPair& operator=(TKeyPair&& other) = default;

    TKeyPair(const TKeyPairMetadata& metadata);

    [[nodiscard]] const TKeyInfo& KeyInfo() const;

    void Sign(
        std::span<const std::byte> data,
        std::span<std::byte, SignatureSize> signature) const;

    //! Checks that private key matches the public key.
    [[nodiscard]] bool SanityCheck() const;

private:
    TKeyInfoPtr KeyInfo_;
    NSecretString::TSecretString PrivateKey_;
};

////////////////////////////////////////////////////////////////////////////////

// NB(pavook) this prefix should be pruned from all logs and core dumps.
constexpr TStringBuf PrivateKeyPrefix = "!YT-PRIVATE!";

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignatureService

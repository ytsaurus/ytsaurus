#pragma once

#include "public.h"

#include "key_info.h"

#include <library/cpp/yt/memory/intrusive_ptr.h>

#include <library/cpp/string_utils/secret_string/secret_string.h>

#include <span>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

class TKeyPair final
{
public:
    explicit TKeyPair(const TKeyPairMetadata& metadata);

    TKeyPair(const TKeyPair& other) = default;
    TKeyPair& operator=(const TKeyPair& other) = default;

    TKeyPair(TKeyPair&& other) = default;
    TKeyPair& operator=(TKeyPair&& other) = default;

    [[nodiscard]] const TKeyInfoPtr& KeyInfo() const;

    void Sign(
        std::span<const std::byte> data,
        std::span<std::byte, SignatureSize> signature) const;

    //! Checks that private key matches the public key.
    [[nodiscard]] bool CheckSanity() const;

private:
    TKeyInfoPtr KeyInfo_;
    NSecretString::TSecretString PrivateKey_;
};

DEFINE_REFCOUNTED_TYPE(TKeyPair)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature

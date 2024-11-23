#pragma once

#include "public.h"

#include "key_info.h"

#include <library/cpp/yt/memory/intrusive_ptr.h>

#include <library/cpp/string_utils/secret_string/secret_string.h>

#include <span>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

class TKeyPair
{
public:
    TKeyPair(const TKeyPair& other) = delete;
    TKeyPair& operator=(const TKeyPair& other) = delete;

    TKeyPair(TKeyPair&& other) = default;
    TKeyPair& operator=(TKeyPair&& other) = default;

    TKeyPair(const TKeyPairMetadata& metadata);

    [[nodiscard]] TKeyInfoPtr KeyInfo() const;

    void Sign(
        std::span<const std::byte> data,
        std::span<std::byte, SignatureSize> signature) const;

    //! Checks that private key matches the public key.
    [[nodiscard]] bool CheckSanity() const;

private:
    TKeyInfoPtr KeyInfo_;
    NSecretString::TSecretString PrivateKey_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature

#include "key_pair.h"

#include <yt/yt/core/misc/error.h>

#include <contrib/libs/libsodium/include/sodium.h>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

TKeyPair::TKeyPair(const TKeyPairMetadata& metadata)
{
    std::array<std::byte, PrivateKeySize> privateKey;
    TPublicKey publicKey;

    // NB(pavook): until GLIBC 2.35 msan does not consider getrandom calls as initialization.
    // This is why we initialize the seed on our own and then provide it to libsodium.
    std::array<unsigned char, crypto_sign_ed25519_SEEDBYTES> seed{};
    randombytes_buf(seed.data(), seed.size());
    if (crypto_sign_ed25519_seed_keypair(
            reinterpret_cast<unsigned char*>(publicKey.data()),
            reinterpret_cast<unsigned char*>(privateKey.data()),
            seed.data()) != 0) {
        THROW_ERROR_EXCEPTION("Failed to generate keypair");
    }
    PrivateKey_ = Format(
        "%v%v",
        PrivateKeyPrefix,
        TStringBuf{reinterpret_cast<const char*>(privateKey.data()), privateKey.size()});

    KeyInfo_ = New<TKeyInfo>(std::move(publicKey), std::move(metadata));
}

////////////////////////////////////////////////////////////////////////////////

const TKeyInfo& TKeyPair::KeyInfo() const {
    YT_VERIFY(KeyInfo_);

    return *KeyInfo_;
}

////////////////////////////////////////////////////////////////////////////////

void TKeyPair::Sign(
    std::span<const std::byte> data,
    std::span<std::byte, SignatureSize> signature) const
{
    THROW_ERROR_EXCEPTION_IF(
        crypto_sign_detached(
            reinterpret_cast<unsigned char*>(signature.data()),
            nullptr,
            reinterpret_cast<const unsigned char*>(data.data()),
            data.size(),
            reinterpret_cast<const unsigned char*>(PrivateKey_.Value().Tail(PrivateKeyPrefix.size()).data())) != 0,
        "Failed to sign data");
}

////////////////////////////////////////////////////////////////////////////////

bool TKeyPair::CheckSanity() const
{
    TPublicKey extractedPublicKey;
    int res = crypto_sign_ed25519_sk_to_pk(
        reinterpret_cast<unsigned char*>(extractedPublicKey.data()),
        reinterpret_cast<const unsigned char*>(PrivateKey_.Value().Tail(PrivateKeyPrefix.size()).data()));
    return res == 0 && extractedPublicKey == KeyInfo().Key();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature

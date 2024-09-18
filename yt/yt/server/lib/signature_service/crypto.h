#pragma once

#include <contrib/libs/libsodium/include/sodium/crypto_sign.h>

namespace NYT::NSignatureService {

////////////////////////////////////////////////////////////////////////////////

constexpr static size_t PublicKeySize = crypto_sign_PUBLICKEYBYTES;
constexpr static size_t PrivateKeySize = crypto_sign_SECRETKEYBYTES;
constexpr static size_t SignatureSize = crypto_sign_BYTES;

////////////////////////////////////////////////////////////////////////////////

void InitializeCryptography();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignatureService

#pragma once

#include <yt/yt/core/actions/public.h>

#include <util/datetime/base.h>

#include <contrib/libs/libsodium/include/sodium/crypto_sign.h>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

constexpr static size_t PublicKeySize = crypto_sign_PUBLICKEYBYTES;
constexpr static size_t PrivateKeySize = crypto_sign_SECRETKEYBYTES;
constexpr static size_t SignatureSize = crypto_sign_BYTES;

////////////////////////////////////////////////////////////////////////////////

TFuture<void> InitializeCryptography(const IInvokerPtr& invoker);

/*!
 * \note Thread affinity: any
 */
void EnsureCryptographyInitialized();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature

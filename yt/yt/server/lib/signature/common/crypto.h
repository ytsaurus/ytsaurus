#pragma once

#include <yt/yt/core/actions/public.h>

#include <util/datetime/base.h>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

constexpr static size_t PublicKeySize = 32;
constexpr static size_t PrivateKeySize = 64;
constexpr static size_t SignatureSize = 64;

////////////////////////////////////////////////////////////////////////////////

TFuture<void> InitializeCryptography(const IInvokerPtr& invoker);

/*!
 * \note Thread affinity: any
 */
void EnsureCryptographyInitialized();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature

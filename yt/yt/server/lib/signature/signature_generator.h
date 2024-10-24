#pragma once

#include "public.h"

#include "key_pair.h"
#include "key_store.h"

#include <yt/yt/core/yson/public.h>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

// TODO(pavook) make this properly configurable.
constexpr auto KeyExpirationTime = TDuration::Hours(24);
constexpr auto SignatureExpirationTime = TDuration::Hours(1);
constexpr auto TimeSyncMargin = TDuration::Hours(1);

////////////////////////////////////////////////////////////////////////////////

class TSignatureGenerator
{
public:
    explicit TSignatureGenerator(IKeyStoreWriter* keyStore);

    // TODO(pavook) futurize?
    [[nodiscard]] TSignaturePtr Sign(NYson::TYsonString&& payload) const;

    [[nodiscard]] const TKeyInfo& KeyInfo() const;

    TFuture<void> Rotate();

private:
    IKeyStoreWriter* const Store_;
    const TOwnerId Owner_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, KeyPairLock_);
    std::optional<TKeyPair> KeyPair_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature

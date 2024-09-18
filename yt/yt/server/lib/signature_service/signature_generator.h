#pragma once

#include "public.h"

#include "key_pair.h"
#include "key_store.h"

#include <yt/yt/core/yson/public.h>

namespace NYT::NSignatureService {

////////////////////////////////////////////////////////////////////////////////

// TODO(pavook) make this properly configurable.
constexpr auto KeyExpirationTime = TDuration::Hours(24);
constexpr auto SignatureExpirationTime = TDuration::Hours(1);
constexpr auto TimeSyncMargin = TDuration::Hours(1);

////////////////////////////////////////////////////////////////////////////////

// TODO(pavook) futurize?

class TSignatureGenerator
{
public:
    explicit TSignatureGenerator(IKeyStoreWriter* keyStore);

    [[nodiscard]] TSignature Sign(NYson::TYsonString&& payload) const;

    [[nodiscard]] const TKeyInfo& KeyInfo() const noexcept;

    void Rotate();

private:
    IKeyStoreWriter* const Store_;
    TOwnerId Owner_;
    // TODO(pavook) lock.
    std::optional<TKeyPair> KeyPair_;
    const NLogging::TLogger Logger;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignatureService

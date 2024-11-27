#pragma once

#include "public.h"

#include "key_pair.h"

#include <yt/yt/core/yson/public.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

class TSignatureGenerator final
{
public:
    explicit TSignatureGenerator(TSignatureGeneratorConfigPtr config, IKeyStoreWriterPtr store);

    // TODO(pavook) futurize?
    [[nodiscard]] TSignaturePtr Sign(NYson::TYsonString&& payload) const;

    [[nodiscard]] TKeyInfoPtr KeyInfo() const;

    TFuture<void> Rotate();

private:
    const TSignatureGeneratorConfigPtr Config_;
    const IKeyStoreWriterPtr Store_;
    const TOwnerId Owner_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, KeyPairLock_);
    std::optional<TKeyPair> KeyPair_;
};

DEFINE_REFCOUNTED_TYPE(TSignatureGenerator)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature

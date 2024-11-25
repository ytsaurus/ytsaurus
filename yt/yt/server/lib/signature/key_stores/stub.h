#pragma once

#include "public.h"

#include <yt/yt/server/lib/signature/key_store.h>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

struct TStubKeyStore
    : public IKeyStoreReader
    , public IKeyStoreWriter
{
    THashMap<TOwnerId, std::vector<TKeyInfoPtr>> Data;

    TOwnerId GetOwner() override;

    TFuture<TKeyInfoPtr> FindKey(const TOwnerId& owner, const TKeyId& key) override;

    TFuture<void> RegisterKey(const TKeyInfoPtr& keyInfo) override;
};

DEFINE_REFCOUNTED_TYPE(TStubKeyStore)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature

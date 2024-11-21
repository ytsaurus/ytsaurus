#pragma once

#include <yt/yt/server/lib/signature/key_store.h>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TMockKeyStore)

struct TMockKeyStore
    : public IKeyStoreReader
    , public IKeyStoreWriter
{
    THashMap<TOwnerId, std::vector<TKeyInfoPtr>> Data;

    TOwnerId GetOwner() override;

    TFuture<TKeyInfoPtr> GetKey(const TOwnerId& owner, const TKeyId& keyId) override;

    TFuture<void> RegisterKey(const TKeyInfo& keyInfo) override;
};

DEFINE_REFCOUNTED_TYPE(TMockKeyStore)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature

#pragma once

#include <yt/yt/server/lib/signature/key_store.h>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TStubKeyStore)

struct TStubKeyStore
    : public IKeyStoreReader
    , public IKeyStoreWriter
{
    const TOwnerId OwnerId;

    explicit TStubKeyStore(TOwnerId ownerId = TOwnerId("TStubKeyStore"))
        : OwnerId(std::move(ownerId))
    { }

    THashMap<TOwnerId, std::vector<TKeyInfoPtr>> Data;

    const TOwnerId& GetOwner() override;

    TFuture<TKeyInfoPtr> FindKey(const TOwnerId& ownerId, const TKeyId& keyId) override;

    TFuture<void> RegisterKey(const TKeyInfoPtr& keyInfo) override;
};

DEFINE_REFCOUNTED_TYPE(TStubKeyStore)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature

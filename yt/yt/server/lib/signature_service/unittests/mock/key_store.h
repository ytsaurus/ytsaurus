#pragma once

#include <yt/yt/server/lib/signature_service/key_store.h>

namespace NYT::NSignatureService {

////////////////////////////////////////////////////////////////////////////////

struct TMockKeyStore
    : public IKeyStoreReader
    , public IKeyStoreWriter
{
    THashMap<TOwnerId, std::vector<TKeyInfoPtr>> Data;

    TOwnerId GetOwner() override;

    TKeyInfoPtr GetKey(const TOwnerId& owner, const TKeyId& keyId) override;

    bool RegisterKey(const TKeyInfo& keyInfo) override;

    ~TMockKeyStore() override = default;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignatureService

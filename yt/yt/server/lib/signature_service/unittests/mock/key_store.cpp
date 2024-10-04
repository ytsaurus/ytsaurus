#include "key_store.h"

namespace NYT::NSignatureService {

////////////////////////////////////////////////////////////////////////////////

TOwnerId TMockKeyStore::GetOwner()
{
    return TOwnerId("TMockKeyStore");
}

////////////////////////////////////////////////////////////////////////////////

TFuture<void> TMockKeyStore::RegisterKey(const TKeyInfo& key)
{
    Data[key.Meta().Owner].emplace_back(New<TKeyInfo>(key.Key(), key.Meta()));
    return VoidFuture;
}

////////////////////////////////////////////////////////////////////////////////

TFuture<TKeyInfoPtr> TMockKeyStore::GetKey(const TOwnerId& owner, const TKeyId& keyId)
{
    auto ownerIt = Data.find(owner);
    if (ownerIt == Data.end()) {
        return MakeFuture(TKeyInfoPtr());
    }
    auto it = std::ranges::find(
        ownerIt->second,
        keyId,
        [ ](TKeyInfoPtr keyInfo) {
            return keyInfo->Meta().Id;
        });
    return MakeFuture(it != ownerIt->second.end() ? *it : TKeyInfoPtr());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignatureService

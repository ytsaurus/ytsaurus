#include "key_store.h"

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

TOwnerId TMockKeyStore::GetOwner()
{
    return TOwnerId("TMockKeyStore");
}

////////////////////////////////////////////////////////////////////////////////

TFuture<void> TMockKeyStore::RegisterKey(const TKeyInfo& key)
{
    auto owner = std::visit([](const auto& meta) { return meta.Owner; }, key.Meta());

    Data[owner].emplace_back(New<TKeyInfo>(key.Key(), key.Meta()));
    return VoidFuture;
}

////////////////////////////////////////////////////////////////////////////////

TFuture<TKeyInfoPtr> TMockKeyStore::GetKey(const TOwnerId& owner, const TKeyId& keyId)
{
    auto ownerIt = Data.find(owner);
    if (ownerIt == Data.end()) {
        return MakeFuture(TKeyInfoPtr());
    }
    auto it = std::find_if(
        ownerIt->second.begin(),
        ownerIt->second.end(),
        [&keyId](TKeyInfoPtr keyInfo) {
            return GetKeyId(keyInfo->Meta()) == keyId;
        });
    return MakeFuture(it != ownerIt->second.end() ? *it : TKeyInfoPtr());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature

#include "key_store.h"

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

TOwnerId TMockKeyStore::GetOwner()
{
    return TOwnerId("TMockKeyStore");
}

////////////////////////////////////////////////////////////////////////////////

TFuture<void> TMockKeyStore::RegisterKey(const TKeyInfoPtr& keyInfo)
{
    auto owner = std::visit([](const auto& meta) { return meta.Owner; }, keyInfo->Meta());

    Data[owner].emplace_back(New<TKeyInfo>(*keyInfo));
    return VoidFuture;
}

////////////////////////////////////////////////////////////////////////////////

TFuture<TKeyInfoPtr> TMockKeyStore::FindKey(const TOwnerId& owner, const TKeyId& key)
{
    auto ownerIt = Data.find(owner);
    if (ownerIt == Data.end()) {
        return MakeFuture(TKeyInfoPtr());
    }
    auto it = std::find_if(
        ownerIt->second.begin(),
        ownerIt->second.end(),
        [&key](TKeyInfoPtr keyInfo) {
            return GetKeyId(keyInfo->Meta()) == key;
        });
    return MakeFuture(it != ownerIt->second.end() ? *it : TKeyInfoPtr());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature

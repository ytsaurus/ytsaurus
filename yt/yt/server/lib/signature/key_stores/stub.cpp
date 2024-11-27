#include "stub.h"

#include <yt/yt/server/lib/signature/key_info.h>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

TOwnerId TStubKeyStore::GetOwner()
{
    return Owner;
}

////////////////////////////////////////////////////////////////////////////////

TFuture<void> TStubKeyStore::RegisterKey(const TKeyInfoPtr& keyInfo)
{
    auto owner = std::visit([](const auto& meta) { return meta.Owner; }, keyInfo->Meta());

    YT_ASSERT(owner == Owner);

    Data[owner].emplace_back(New<TKeyInfo>(*keyInfo));
    return VoidFuture;
}

////////////////////////////////////////////////////////////////////////////////

TFuture<TKeyInfoPtr> TStubKeyStore::FindKey(const TOwnerId& owner, const TKeyId& key)
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

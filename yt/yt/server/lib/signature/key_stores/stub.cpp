#include "stub.h"

#include <yt/yt/server/lib/signature/key_info.h>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

const TOwnerId& TStubKeyStore::GetOwner()
{
    return OwnerId;
}

////////////////////////////////////////////////////////////////////////////////

TFuture<void> TStubKeyStore::RegisterKey(const TKeyInfoPtr& keyInfo)
{
    auto ownerId = std::visit([] (const auto& meta) { return meta.OwnerId; }, keyInfo->Meta());

    YT_VERIFY(ownerId == OwnerId);

    Data[ownerId].push_back(New<TKeyInfo>(*keyInfo));
    return VoidFuture;
}

////////////////////////////////////////////////////////////////////////////////

TFuture<TKeyInfoPtr> TStubKeyStore::FindKey(const TOwnerId& ownerId, const TKeyId& keyId)
{
    auto ownerIt = Data.find(ownerId);
    if (ownerIt == Data.end()) {
        return MakeFuture(TKeyInfoPtr());
    }
    auto it = std::find_if(
        ownerIt->second.begin(),
        ownerIt->second.end(),
        [&keyId] (TKeyInfoPtr keyInfo) {
            return GetKeyId(keyInfo->Meta()) == keyId;
        });
    return MakeFuture(it != ownerIt->second.end() ? *it : TKeyInfoPtr());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature

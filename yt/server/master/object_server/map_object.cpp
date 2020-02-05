#include "map_object.h"

#include <yt/server/master/security_server/account.h>

#include <yt/server/master/scheduler_pool_server/scheduler_pool.h>

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

template <class TSelf>
TNonversionedMapObjectBase<TSelf>::TNonversionedMapObjectBase(TObjectId id, bool isRoot)
    : TNonversionedObjectBase(id)
    , Acd_(this)
    , IsRoot_(isRoot)
{ }

template <class TSelf>
TSelf* TNonversionedMapObjectBase<TSelf>::GetSelf()
{
    return static_cast<TSelf*>(this);
}

template <class TSelf>
const TSelf* TNonversionedMapObjectBase<TSelf>::GetSelf() const
{
    return static_cast<const TSelf*>(this);
}

template <class TSelf>
void TNonversionedMapObjectBase<TSelf>::AttachChild(const TString& key, TSelf* child) noexcept
{
    YT_VERIFY(child);
    YT_VERIFY(!child->GetParent());
    YT_VERIFY(!child->IsRoot());
    child->SetParent(GetSelf());
    YT_VERIFY(KeyToChild().emplace(key, child).second);
    YT_VERIFY(ChildToKey().emplace(child, key).second);
}

template <class TSelf>
void TNonversionedMapObjectBase<TSelf>::DetachChild(TSelf* child) noexcept
{
    YT_VERIFY(child);
    YT_VERIFY(child->GetParent() == this);
    child->ResetParent();
    auto key = GetChildKey(child);
    YT_VERIFY(KeyToChild().erase(key) == 1);
    YT_VERIFY(ChildToKey().erase(child) == 1);
}

template <class TSelf>
void TNonversionedMapObjectBase<TSelf>::RenameChild(TSelf* child, const TString& newKey) noexcept
{
    YT_VERIFY(child->GetParent() == this);
    auto key = GetChildKey(child);
    if (key == newKey) {
        return;
    }
    YT_VERIFY(KeyToChild().erase(key) == 1);
    YT_VERIFY(KeyToChild().emplace(newKey, child).second);

    auto it = ChildToKey().find(child);
    YT_VERIFY(it != ChildToKey().end());
    YT_VERIFY(it->second == key);
    it->second = newKey;
}

template <class TSelf>
TSelf* TNonversionedMapObjectBase<TSelf>::FindChild(const TString& key) const
{
    auto it = KeyToChild().find(key);
    return it == KeyToChild().end()
        ? nullptr
        : it->second;
}

template <class TSelf>
TSelf* TNonversionedMapObjectBase<TSelf>::GetChild(const TString& key) const noexcept
{
    auto* child = FindChild(key);
    YT_VERIFY(child);
    return child;
}

template <class TSelf>
std::optional<TString> TNonversionedMapObjectBase<TSelf>::FindChildKey(const TSelf* child) const noexcept
{
    auto it = ChildToKey().find(child);
    return it == ChildToKey().end()
        ? std::nullopt
        : std::make_optional(it->second);
}

template <class TSelf>
TString TNonversionedMapObjectBase<TSelf>::GetChildKey(const TSelf* child) const noexcept
{
    auto optionalKey = FindChildKey(child);
    YT_VERIFY(optionalKey);
    return *optionalKey;
}

template <class TSelf>
TString TNonversionedMapObjectBase<TSelf>::GetName() const
{
    if (Parent_) {
        return Parent_->GetChildKey(GetSelf());
    }
    return IsRoot() ? GetRootName() : NObjectClient::FromObjectId(GetId());
}

template <class TSelf>
TString TNonversionedMapObjectBase<TSelf>::GetRootName() const
{
    YT_VERIFY(IsRoot());
    return NObjectClient::FromObjectId(GetId());
}

template <class TSelf>
void TNonversionedMapObjectBase<TSelf>::ResetParent()
{
    SetParent(nullptr);
}

template <class TSelf>
bool TNonversionedMapObjectBase<TSelf>::IsRoot() const
{
    return IsRoot_;
}

template <class TSelf>
void TNonversionedMapObjectBase<TSelf>::Save(NCellMaster::TSaveContext& context) const
{
    TNonversionedObjectBase::Save(context);

    using NYT::Save;
    Save(context, Acd_);
    Save(context, IsRoot_);
    Save(context, Parent_);
    Save(context, KeyToChild_);
}

template <class TSelf>
void TNonversionedMapObjectBase<TSelf>::Load(NCellMaster::TLoadContext& context)
{
    TNonversionedObjectBase::Load(context);

    using NYT::Load;
    Load(context, Acd_);
    Load(context, IsRoot_);
    Load(context, Parent_);
    Load(context, KeyToChild_);

    // Reconstruct ChildToKey_ map.
    for (const auto& [key, child] : KeyToChild_) {
        if (child) {
            YT_VERIFY(ChildToKey_.emplace(child, key).second);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

template class TNonversionedMapObjectBase<NSecurityServer::TAccount>;
template class TNonversionedMapObjectBase<NSchedulerPoolServer::TSchedulerPool>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer

#include "attribute_set.h"
#include "yson_intern_registry.h"

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/core/misc/serialize.h>

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

using TAttributeSetSerializer = TMapSerializer<
    TDefaultSerializer,
    NCellMaster::TInternedYsonStringSerializer
>;

void TAttributeSet::Save(NCellMaster::TSaveContext& context) const
{
    TAttributeSetSerializer::Save(context, Attributes_);
}

void TAttributeSet::Load(NCellMaster::TLoadContext& context)
{
    using NYT::Load;

    TAttributeSetSerializer::Load(context, Attributes_);

    for (const auto& [key, value] : Attributes_) {
        MasterMemoryUsage_ += key.size();
        if (value) {
            MasterMemoryUsage_ += value.AsStringBuf().size();
        }
    }
}

void TAttributeSet::Set(const TString& key, const NYson::TYsonString& value)
{
    if (auto it = Attributes_.find(key); it != Attributes_.end()) {
        MasterMemoryUsage_ -= static_cast<i64>(it->first.size());
        if (it->second) {
            MasterMemoryUsage_ -= static_cast<i64>(it->second.AsStringBuf().size());
        }
    }
    Attributes_[key] = value;
    MasterMemoryUsage_ += key.size();
    if (value) {
        MasterMemoryUsage_ += value.AsStringBuf().size();
    }
}

bool TAttributeSet::TryInsert(const TString& key, const NYson::TYsonString& value)
{
    if (Attributes_.find(key) != Attributes_.end()) {
        return false;
    }
    YT_VERIFY(Attributes_.emplace(key, value).second);
    MasterMemoryUsage_ += key.size();
    if (value) {
        MasterMemoryUsage_ += value.AsStringBuf().size();
    }
    return true;
}

bool TAttributeSet::Remove(const TString& key)
{
    auto it = Attributes_.find(key);
    if (it == Attributes_.end()) {
        return false;
    }
    MasterMemoryUsage_ -= static_cast<i64>(it->first.size());
    if (it->second) {
        MasterMemoryUsage_ -= static_cast<i64>(it->second.AsStringBuf().size());
    }
    Attributes_.erase(it);
    return true;
}

NYson::TYsonString TAttributeSet::Find(TStringBuf key) const {
    auto it = Attributes_.find(key);
    if (it == Attributes_.end()) {
        return NYson::TYsonString();
    } else {
        return it->second;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer

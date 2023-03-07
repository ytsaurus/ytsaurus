#include "attribute_set.h"

#include <yt/server/master/cell_master/serialize.h>

#include <yt/core/misc/serialize.h>

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

void TAttributeSet::Save(NCellMaster::TSaveContext& context) const
{
    using NYT::Save;
    Save(context, Attributes_);
}

void TAttributeSet::Load(NCellMaster::TLoadContext& context)
{
    using NYT::Load;
    Load(context, Attributes_);

    for (const auto& [key, value] : Attributes_) {
        MasterMemoryUsage_ += key.size() + value.GetData().size();
    }
}

void TAttributeSet::Set(const TString& key, const NYson::TYsonString& value)
{
    if (auto it = Attributes_.find(key); it != Attributes_.end()) {
        MasterMemoryUsage_ -= static_cast<i64>(it->first.size());
        if (it->second) {
            MasterMemoryUsage_ -= static_cast<i64>(it->second.GetData().size());
        }
    }
    Attributes_[key] = value;
    MasterMemoryUsage_ += key.size();
    if (value) {
        MasterMemoryUsage_ += value.GetData().size();
    }
}

bool TAttributeSet::TryInsert(const TString& key, const NYson::TYsonString& value)
{
    if (Attributes_.find(key) != Attributes_.end()) {
        return false;
    }
    YT_VERIFY(Attributes_.emplace(key, value).second);
    MasterMemoryUsage_ += key.size() + value.GetData().size();
    return true;
}

bool TAttributeSet::Remove(const TString& key)
{
    auto it = Attributes_.find(key);
    if (it == Attributes_.end()) {
        return false;
    }
    MasterMemoryUsage_ -= static_cast<i64>(it->first.size() + it->second.GetData().size());
    Attributes_.erase(it);
    return true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer

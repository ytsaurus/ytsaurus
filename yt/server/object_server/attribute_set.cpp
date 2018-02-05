#include "attribute_set.h"

#include <yt/server/cell_master/serialize.h>

#include <yt/core/misc/serialize.h>

namespace NYT {
namespace NObjectServer {

////////////////////////////////////////////////////////////////////////////////

void TAttributeSet::Save(NCellMaster::TSaveContext& context) const
{
    using NYT::Save;
    Save(context, Attributes_);
}

void TAttributeSet::Load(NCellMaster::TLoadContext& context)
{
    using NYT::Load;
    // COMPAT(babenko)
    if (context.GetVersion() < 501) {
        THashMap<TString, TNullable<NYson::TYsonString>> attributes;
        Load(context, attributes);
        Attributes_.clear();
        for (const auto& pair : attributes) {
            YCHECK(Attributes_.emplace(pair.first, pair.second ? *pair.second : NYson::TYsonString()).second);
        }
    } else {
        Load(context, Attributes_);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT

#include "area.h"
#include "cell_bundle.h"

#include <yt/yt/server/master/cell_master/serialize.h>

namespace NYT::NCellServer {

////////////////////////////////////////////////////////////////////////////////

TString TArea::GetLowercaseObjectName() const
{
    return Format("area %Qv", GetName());
}

TString TArea::GetCapitalizedObjectName() const
{
    return Format("Area %Qv", GetName());
}

void TArea::Save(NCellMaster::TSaveContext& context) const
{
    TObject::Save(context);

    using NYT::Save;
    Save(context, Name_);
    Save(context, CellBundle_);
    Save(context, NodeTagFilter_);
}

void TArea::Load(NCellMaster::TLoadContext& context)
{
    TObject::Load(context);

    using NYT::Load;
    Load(context, Name_);
    Load(context, CellBundle_);
    Load(context, NodeTagFilter_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer

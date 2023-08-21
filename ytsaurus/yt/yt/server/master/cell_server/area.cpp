#include "area.h"
#include "cell_bundle.h"

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/lib/chaos_server/config.h>

namespace NYT::NCellServer {

using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

TString TArea::GetLowercaseObjectName() const
{
    return Format("area %Qv of bundle %Qv", GetName(), CellBundle_->GetName());
}

TString TArea::GetCapitalizedObjectName() const
{
    return Format("Area %Qv of Bundle %Qv", GetName(), CellBundle_->GetName());
}

TString TArea::GetObjectPath() const
{
    return Format("//sys/areas/%v", GetName());
}

void TArea::Save(NCellMaster::TSaveContext& context) const
{
    TObject::Save(context);

    using NYT::Save;
    Save(context, Name_);
    Save(context, CellBundle_);
    Save(context, NodeTagFilter_);
    TNullableIntrusivePtrSerializer<>::Save(context, ChaosOptions_);
}

void TArea::Load(NCellMaster::TLoadContext& context)
{
    TObject::Load(context);

    using NYT::Load;
    Load(context, Name_);
    Load(context, CellBundle_);
    Load(context, NodeTagFilter_);
    TNullableIntrusivePtrSerializer<>::Load(context, ChaosOptions_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer

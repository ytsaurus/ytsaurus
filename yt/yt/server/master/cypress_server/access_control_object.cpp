#include "access_control_object.h"
#include "access_control_object_namespace.h"

#include <yt/yt/server/master/cell_master/serialize.h>

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

TAccessControlObject::TAccessControlObject(TAccessControlObjectId id)
    : TObject(id)
    , Acd_(this)
    , PrincipalAcd_(this)
{ }

void TAccessControlObject::Save(NCellMaster::TSaveContext& context) const
{
    using NYT::Save;

    TObject::Save(context);

    Save(context, Name_);
    Save(context, Namespace_);
    Save(context, Acd_);
    Save(context, PrincipalAcd_);
}

void TAccessControlObject::Load(NCellMaster::TLoadContext& context)
{
    using NYT::Load;

    TObject::Load(context);

    Load(context, Name_);
    Load(context, Namespace_);
    Load(context, Acd_);
    Load(context, PrincipalAcd_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer

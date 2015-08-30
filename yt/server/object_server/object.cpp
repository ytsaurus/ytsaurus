#include "stdafx.h"
#include "object.h"

#include <ytlib/object_client/helpers.h>

#include <server/cypress_server/node.h>

#include <server/cell_master/serialize.h>

namespace NYT {
namespace NObjectServer {

using namespace NObjectClient;
using namespace NCypressServer;

////////////////////////////////////////////////////////////////////////////////

EObjectType TObjectBase::GetType() const
{
    return TypeFromId(Id_);
}

bool TObjectBase::IsBuiltin() const
{
    return IsWellKnownId(Id_);
}

bool TObjectBase::IsTrunk() const
{
    if (!IsVersionedType(TypeFromId(Id_))) {
        return true;
    }

    auto* node = static_cast<const TCypressNodeBase*>(this);
    return node->GetTrunkNode() == node;
}

const TAttributeSet* TObjectBase::GetAttributes() const
{
    return Attributes_.get();
}

TAttributeSet* TObjectBase::GetMutableAttributes()
{
    if (!Attributes_) {
        Attributes_ = std::make_unique<TAttributeSet>();
    }
    return Attributes_.get();
}

void TObjectBase::ClearAttributes()
{
    Attributes_.reset();
}

void TObjectBase::Save(NCellMaster::TSaveContext& context) const
{
    using NYT::Save;
    Save(context, RefCounter_);
    Save(context, ImportRefCounter_);
    if (Attributes_) {
        Save(context, true);
        Save(context, *Attributes_);
    } else {
        Save(context, false);
    }
}

void TObjectBase::Load(NCellMaster::TLoadContext& context)
{
    using NYT::Load;
    Load(context, RefCounter_);
    // COMPAT(babenko)
    if (context.GetVersion() >= 200) {
        Load(context, ImportRefCounter_);
    }
    if (Load<bool>(context)) {
        Attributes_ = std::make_unique<TAttributeSet>();
        Load(context, *Attributes_);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT

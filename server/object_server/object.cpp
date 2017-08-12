#include "object.h"

#include <yt/server/cell_master/serialize.h>
#include <yt/server/cell_master/bootstrap.h>

#include <yt/server/cypress_server/node.h>

#include <yt/ytlib/object_client/helpers.h>

namespace NYT {
namespace NObjectServer {

using namespace NObjectClient;
using namespace NCypressServer;

////////////////////////////////////////////////////////////////////////////////

EObjectType IObjectBase::GetType() const
{
    return TypeFromId(Id_);
}

bool IObjectBase::IsBuiltin() const
{
    return IsWellKnownId(Id_);
}

const TAttributeSet* IObjectBase::GetAttributes() const
{
    return Attributes_.get();
}

TAttributeSet* IObjectBase::GetMutableAttributes()
{
    if (!Attributes_) {
        Attributes_ = std::make_unique<TAttributeSet>();
    }
    return Attributes_.get();
}

void IObjectBase::ClearAttributes()
{
    Attributes_.reset();
}

int IObjectBase::GetGCWeight() const
{
    return 10;
}

void IObjectBase::Save(NCellMaster::TSaveContext& context) const
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
    Save(context, IsForeign());
}

void IObjectBase::Load(NCellMaster::TLoadContext& context)
{
    using NYT::Load;
    Load(context, RefCounter_);
    Load(context, ImportRefCounter_);
    if (Load<bool>(context)) {
        Attributes_ = std::make_unique<TAttributeSet>();
        Load(context, *Attributes_);
    }
    if (Load<bool>(context)) {
        SetForeign();
    }
}

////////////////////////////////////////////////////////////////////////////////

void TObjectIdFormatter::operator()(TStringBuilder* builder, const IObjectBase* object) const
{
    FormatValue(builder, object->GetId(), TStringBuf());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT

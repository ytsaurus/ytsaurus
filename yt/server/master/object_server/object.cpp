#include "object.h"

#include <yt/server/master/cell_master/serialize.h>
#include <yt/server/master/cell_master/bootstrap.h>

#include <yt/server/master/cypress_server/node.h>

#include <yt/client/object_client/helpers.h>

namespace NYT::NObjectServer {

using namespace NObjectClient;
using namespace NCypressServer;

////////////////////////////////////////////////////////////////////////////////

EObjectLifeStage GetNextLifeStage(EObjectLifeStage lifeStage)
{
    switch (lifeStage) {
        case EObjectLifeStage::CreationStarted:
            return EObjectLifeStage::CreationPreCommitted;
        case EObjectLifeStage::CreationPreCommitted:
            return EObjectLifeStage::CreationCommitted;
        case EObjectLifeStage::RemovalStarted:
            return EObjectLifeStage::RemovalPreCommitted;
        case EObjectLifeStage::RemovalPreCommitted:
            return EObjectLifeStage::RemovalCommitted;
        default:
            YT_ABORT();
    }
}

bool IsStableLifeStage(EObjectLifeStage lifeStage)
{
    switch (lifeStage) {
        case EObjectLifeStage::CreationCommitted:
        case EObjectLifeStage::RemovalCommitted:
            return true;
        default:
            return false;
    }
}

////////////////////////////////////////////////////////////////////////////////

TCellTag TObject::GetNativeCellTag() const
{
    return CellTagFromId(Id_);
}

EObjectType TObject::GetType() const
{
    return TypeFromId(Id_);
}

bool TObject::IsBuiltin() const
{
    return IsWellKnownId(Id_);
}

int TObject::GetLifeStageVoteCount() const
{
    return LifeStageVoteCount_;
}

void TObject::ResetLifeStageVoteCount()
{
    LifeStageVoteCount_ = 0;
}

int TObject::IncrementLifeStageVoteCount()
{
    return ++LifeStageVoteCount_;
}

const TAttributeSet* TObject::GetAttributes() const
{
    return Attributes_.get();
}

TAttributeSet* TObject::GetMutableAttributes()
{
    if (!Attributes_) {
        Attributes_ = std::make_unique<TAttributeSet>();
    }
    return Attributes_.get();
}

void TObject::ClearAttributes()
{
    Attributes_.reset();
}

int TObject::GetGCWeight() const
{
    return 10;
}

void TObject::Save(NCellMaster::TSaveContext& context) const
{
    using NYT::Save;
    Save(context, RefCounter_);
    Save(context, WeakRefCounter_);
    Save(context, ImportRefCounter_);
    Save(context, LifeStageVoteCount_);
    Save(context, LifeStage_);
    if (Attributes_) {
        Save(context, true);
        Save(context, *Attributes_);
    } else {
        Save(context, false);
    }
    Save(context, IsForeign());
}

void TObject::Load(NCellMaster::TLoadContext& context)
{
    using NYT::Load;
    Load(context, RefCounter_);
    Load(context, WeakRefCounter_);
    Load(context, ImportRefCounter_);
    Load(context, LifeStageVoteCount_);
    Load(context, LifeStage_);
    if (Load<bool>(context)) {
        Attributes_ = std::make_unique<TAttributeSet>();
        Load(context, *Attributes_);
    }
    if (Load<bool>(context)) {
        SetForeign();
    }
}

////////////////////////////////////////////////////////////////////////////////

TString TNonversionedObjectBase::GetObjectName() const
{
    return Format("Object %v", Id_);
}

void TNonversionedObjectBase::ValidateCreationCommitted() const
{
    if (LifeStage_ != EObjectLifeStage::CreationCommitted) {
        THROW_ERROR_EXCEPTION(
            NObjectClient::EErrorCode::InvalidObjectLifeStage,
            "%v cannot be used since it is in %Qlv life stage",
            GetObjectName(),
            LifeStage_);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TObjectIdFormatter::operator()(TStringBuilderBase* builder, const TObject* object) const
{
    FormatValue(builder, object->GetId(), TStringBuf());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer

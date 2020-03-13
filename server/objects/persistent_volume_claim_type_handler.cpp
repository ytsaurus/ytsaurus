#include "persistent_volume_claim_type_handler.h"
#include "persistent_volume_claim.h"
#include "persistent_volume.h"
#include "type_handler_detail.h"
#include "db_schema.h"

namespace NYP::NServer::NObjects {

using std::placeholders::_1;
using std::placeholders::_2;

////////////////////////////////////////////////////////////////////////////////

class TPersistentVolumeClaimTypeHandler
    : public TObjectTypeHandlerBase
{
public:
    TPersistentVolumeClaimTypeHandler(NMaster::TBootstrap* bootstrap)
        : TObjectTypeHandlerBase(bootstrap, EObjectType::PersistentVolumeClaim)
    { }

    virtual void Initialize() override
    {
        TObjectTypeHandlerBase::Initialize();

        SpecAttributeSchema_
            ->AddChildren({
                MakeEtcAttributeSchema()
                    ->SetAttribute(TPersistentVolumeClaim::TSpec::EtcSchema)
                    ->SetUpdatable(),
            })
            ->SetValidator<TPersistentVolumeClaim>(ValidateSpec);

        StatusAttributeSchema_
            ->AddChildren({
                MakeAttributeSchema("bound_volume_id")
                    ->SetAttribute(TPersistentVolumeClaim::TStatus::BoundVolumeSchema),
            });
    }

    virtual const NYson::TProtobufMessageType* GetRootProtobufType() override
    {
        return NYson::ReflectProtobufMessageType<NClient::NApi::NProto::TPersistentVolumeClaim>();
    }

    virtual const TDBTable* GetTable() override
    {
        return &PersistentVolumeClaimsTable;
    }

    virtual const TDBField* GetIdField() override
    {
        return &PersistentVolumeClaimsTable.Fields.Meta_Id;
    }

    virtual std::unique_ptr<TObject> InstantiateObject(
        const TObjectId& id,
        const TObjectId& parentId,
        ISession* session) override
    {
        YT_VERIFY(!parentId);
        return std::unique_ptr<TObject>(new TPersistentVolumeClaim(id, this, session));
    }

private:
    virtual void AfterObjectCreated(
        TTransaction* transaction,
        TObject* object) override
    {
        TObjectTypeHandlerBase::AfterObjectCreated(transaction, object);

        auto* claim = object->As<TPersistentVolumeClaim>();
        const auto& specEtc = claim->Spec().Etc().Load();
        if (specEtc.has_existing_volume_policy()) {
            const auto& policy = specEtc.existing_volume_policy();
            const auto& volumeId = policy.volume_id();
            auto* volume = transaction->GetPersistentVolume(volumeId);
            if (!volume->DoesExist()) {
                THROW_ERROR_EXCEPTION("Persistent volume claim %Qv refers to non-existing volume %Qv",
                    claim->GetId(),
                    volumeId);
            }
            claim->Status().BoundVolume().Store(volume);
        }
    }

    static EPersistentVolumeClaimKind DeriveKind(TPersistentVolumeClaim* claim)
    {
        std::optional<EPersistentVolumeClaimKind> optionalKind;
        auto setKind = [&] (EPersistentVolumeClaimKind kind) {
            if (optionalKind) {
                THROW_ERROR_EXCEPTION("Persistent volume claim %Qv has multiple kinds",
                    claim->GetId());
            }
            optionalKind = kind;
        };
        const auto& specEtc = claim->Spec().Etc().Load();
        if (specEtc.has_quota_policy()) {
            setKind(EPersistentVolumeClaimKind::Quota);
        }
        if (specEtc.has_exclusive_policy()) {
            setKind(EPersistentVolumeClaimKind::Exclusive);
        }
        if (specEtc.has_existing_volume_policy()) {
            setKind(EPersistentVolumeClaimKind::ExistingVolume);
        }
        if (!optionalKind) {
            THROW_ERROR_EXCEPTION("Persistent volume claim %Qv is of an unrecognized kind",
                claim->GetId());
        }
        return *optionalKind;
    }

    static void ValidateSpec(
        TTransaction* /*transaction*/,
        TPersistentVolumeClaim* claim)
    {
        if (auto* volume = claim->Status().BoundVolume().Load()) {
            THROW_ERROR_EXCEPTION("Cannot update spec of persistent volume claim %Qv since it is bound to volume %Qv",
                claim->GetId(),
                volume->GetId());
        }

        auto oldKind = claim->Kind().Load();
        auto newKind = DeriveKind(claim);
        if (oldKind != EPersistentVolumeClaimKind::Unknown && oldKind != newKind) {
            THROW_ERROR_EXCEPTION("Changing kind of persistent volume claim %Qv from %Qlv to %Qlv is forbidden",
                claim->GetId(),
                oldKind,
                newKind);
        }
    }
};

std::unique_ptr<IObjectTypeHandler> CreatePersistentVolumeClaimTypeHandler(NMaster::TBootstrap* bootstrap)
{
    return std::unique_ptr<IObjectTypeHandler>(new TPersistentVolumeClaimTypeHandler(bootstrap));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects

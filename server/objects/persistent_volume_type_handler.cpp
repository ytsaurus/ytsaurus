#include "persistent_volume_type_handler.h"
#include "persistent_volume.h"
#include "persistent_disk.h"
#include "persistent_volume_claim.h"
#include "pod.h"
#include "type_handler_detail.h"
#include "db_schema.h"

namespace NYP::NServer::NObjects {

using std::placeholders::_1;
using std::placeholders::_2;

////////////////////////////////////////////////////////////////////////////////

class TPersistentVolumeTypeHandler
    : public TObjectTypeHandlerBase
{
public:
    explicit TPersistentVolumeTypeHandler(NMaster::TBootstrap* bootstrap)
        : TObjectTypeHandlerBase(bootstrap, EObjectType::PersistentVolume)
    { }

    virtual void Initialize() override
    {
        TObjectTypeHandlerBase::Initialize();

        MetaAttributeSchema_
            ->AddChildren({
                ParentIdAttributeSchema_ = MakeAttributeSchema("disk_id")
                    ->SetParentIdAttribute()
                    ->SetMandatory(),
                MakeAttributeSchema("kind")
                    ->SetAttribute(TPersistentVolume::KindSchema)
            });

        SpecAttributeSchema_
            ->AddChildren({
                MakeEtcAttributeSchema()
                    ->SetAttribute(TPersistentVolume::TSpec::EtcSchema)
                    ->SetUpdatable(),
            })
            ->SetValidator<TPersistentVolume>(std::bind(&TPersistentVolumeTypeHandler::ValidateSpec, _1, _2));

        StatusAttributeSchema_
            ->AddChildren({
                MakeAttributeSchema("bound_claim_id")
                    ->SetAttribute(TPersistentVolume::TStatus::BoundClaimSchema),

                MakeEtcAttributeSchema()
                    ->SetAttribute(TPersistentVolume::TStatus::EtcSchema)
                    ->SetUpdatable(),
            });
    }

    virtual const NYson::TProtobufMessageType* GetRootProtobufType() override
    {
        return NYson::ReflectProtobufMessageType<NClient::NApi::NProto::TPersistentVolume>();
    }

    virtual EObjectType GetParentType() override
    {
        return EObjectType::PersistentDisk;
    }

    virtual TObject* GetParent(TObject* object) override
    {
        return object->As<TPersistentVolume>()->Disk().Load();
    }

    virtual const TDBTable* GetTable() override
    {
        return &PersistentVolumesTable;
    }

    virtual const TDBField* GetIdField() override
    {
        return &PersistentVolumesTable.Fields.Meta_Id;
    }

    virtual const TDBField* GetParentIdField() override
    {
        return &PersistentVolumesTable.Fields.Meta_DiskId;
    }

    virtual TChildrenAttributeBase* GetParentChildrenAttribute(TObject* parent) override
    {
        return &parent->As<TPersistentDisk>()->Volumes();
    }

    virtual std::unique_ptr<TObject> InstantiateObject(
        const TObjectId& id,
        const TObjectId& parentId,
        ISession* session) override
    {
        return std::unique_ptr<TObject>(new TPersistentVolume(id, parentId, this, session));
    }

private:
    static EPersistentVolumeKind DeriveKind(TPersistentVolume* volume)
    {
        std::optional<EPersistentVolumeKind> optionalKind;
        auto setKind = [&] (EPersistentVolumeKind kind) {
            if (optionalKind) {
                THROW_ERROR_EXCEPTION("Persistent volume %Qv has multiple kinds",
                    volume->GetId());
            }
            optionalKind = kind;
        };
        const auto& specEtc = volume->Spec().Etc().Load();
        if (specEtc.has_rbind_policy()) {
            setKind(EPersistentVolumeKind::Rbind);
        }
        if (specEtc.has_managed_policy()) {
            setKind(EPersistentVolumeKind::Managed);
        }
        if (!optionalKind) {
            THROW_ERROR_EXCEPTION("Persistent volume %Qv is of an unrecognized kind",
                volume->GetId());
        }
        return *optionalKind;
    }

    static bool CheckKind(EPersistentDiskKind diskKind, EPersistentVolumeKind volumeKind)
    {
        return
            diskKind == EPersistentDiskKind::Rbind && volumeKind == EPersistentVolumeKind::Rbind ||
            diskKind == EPersistentDiskKind::Managed && volumeKind == EPersistentVolumeKind::Managed;
    }

    static void ValidateSpec(
        TTransaction* /*transaction*/,
        TPersistentVolume* volume)
    {
        if (auto* claim = volume->Status().BoundClaim().Load()) {
            THROW_ERROR_EXCEPTION("Cannot update spec of persistent volume %Qv since it is bound to claim %Qv",
                volume->GetId(),
                claim->GetId());
        }

        if (auto* pod = volume->Status().MountedToPod().Load()) {
            THROW_ERROR_EXCEPTION("Cannot update spec of persistent volume %Qv since it is mounted to pod %Qv",
                volume->GetId(),
                pod->GetId());
        }

        auto oldKind = volume->Kind().Load();
        auto newKind = DeriveKind(volume);
        if (oldKind != EPersistentVolumeKind::Unknown && oldKind != newKind) {
            THROW_ERROR_EXCEPTION("Changing kind of persistent volume %Qv from %Qlv to %Qlv is forbidden",
                volume->GetId(),
                oldKind,
                newKind);
        }
    }

    virtual void BeforeObjectCreated(
        TTransaction* transaction,
        TObject* object) override
    {
        TObjectTypeHandlerBase::BeforeObjectCreated(transaction, object);

        auto* volume = object->As<TPersistentVolume>();
        volume->Kind() = EPersistentVolumeKind::Unknown;
    }

    virtual void AfterObjectCreated(
        TTransaction* transaction,
        TObject* object) override
    {
        TObjectTypeHandlerBase::AfterObjectCreated(transaction, object);

        auto* volume = object->As<TPersistentVolume>();
        auto volumeKind = DeriveKind(volume);
        auto* disk = volume->Disk().Load();
        auto diskKind = disk->Kind().Load();
        if (!CheckKind(diskKind, volumeKind)) {
            THROW_ERROR_EXCEPTION("Persistent disk %Qv and persistent volume %Qv has mismatching kinds:%Qlv and %Qlv",
                disk->GetId(),
                volume->GetId(),
                diskKind,
                volumeKind);
        }
        volume->Kind() = volumeKind;
    }

    virtual void BeforeObjectRemoved(TTransaction* transaction, TObject* object) override
    {
        TObjectTypeHandlerBase::BeforeObjectRemoved(transaction, object);

        auto* volume = object->As<TPersistentVolume>();
        if (auto* claim = volume->Status().BoundClaim().Load()) {
            THROW_ERROR_EXCEPTION("Cannot remove persistent volume %Qv since it is bound to claim %Qv",
                volume->GetId(),
                claim->GetId());
        }
        if (auto* pod = volume->Status().MountedToPod().Load()) {
            THROW_ERROR_EXCEPTION("Cannot remove persistent volume %Qv since it is mounted to pod %Qv",
                volume->GetId(),
                pod->GetId());
        }
    }
};

std::unique_ptr<IObjectTypeHandler> CreatePersistentVolumeTypeHandler(NMaster::TBootstrap* bootstrap)
{
    return std::unique_ptr<IObjectTypeHandler>(new TPersistentVolumeTypeHandler(bootstrap));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects

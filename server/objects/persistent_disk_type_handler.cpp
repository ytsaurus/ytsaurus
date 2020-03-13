#include "persistent_disk_type_handler.h"
#include "persistent_disk.h"
#include "persistent_volume.h"
#include "node.h"
#include "resource.h"
#include "type_handler_detail.h"
#include "db_schema.h"

#include <yp/server/master/bootstrap.h>

#include <yp/server/scheduler/resource_manager.h>

namespace NYP::NServer::NObjects {

using std::placeholders::_1;
using std::placeholders::_2;

////////////////////////////////////////////////////////////////////////////////

class TPersistentDiskTypeHandler
    : public TObjectTypeHandlerBase
{
public:
    explicit TPersistentDiskTypeHandler(NMaster::TBootstrap* bootstrap)
        : TObjectTypeHandlerBase(bootstrap, EObjectType::PersistentDisk)
    { }

    virtual void Initialize() override
    {
        TObjectTypeHandlerBase::Initialize();

        MetaAttributeSchema_
            ->AddChildren({
                MakeAttributeSchema("kind")
                    ->SetAttribute(TPersistentDisk::KindSchema)
            });

        SpecAttributeSchema_
            ->AddChildren({
                MakeEtcAttributeSchema()
                    ->SetAttribute(TPersistentDisk::TSpec::EtcSchema)
                    ->SetUpdatable(),
            })
            ->SetValidator<TPersistentDisk>(std::bind(&TPersistentDiskTypeHandler::ValidateSpec, _1, _2));

        StatusAttributeSchema_
            ->AddChildren({
                MakeAttributeSchema("attached_to_node_id")
                    ->SetAttribute(TPersistentDisk::TStatus::AttachedToNodeSchema)
            });
    }

    virtual const NYson::TProtobufMessageType* GetRootProtobufType() override
    {
        return NYson::ReflectProtobufMessageType<NClient::NApi::NProto::TPersistentDisk>();
    }

    virtual const TDBTable* GetTable() override
    {
        return &PersistentDisksTable;
    }

    virtual const TDBField* GetIdField() override
    {
        return &PersistentDisksTable.Fields.Meta_Id;
    }

    virtual std::unique_ptr<TObject> InstantiateObject(
        const TObjectId& id,
        const TObjectId& parentId,
        ISession* session) override
    {
        YT_VERIFY(!parentId);
        return std::unique_ptr<TObject>(new TPersistentDisk(id, this, session));
    }

private:
    static EPersistentDiskKind DeriveKind(TPersistentDisk* disk)
    {
        std::optional<EPersistentDiskKind> optionalKind;
        auto setKind = [&] (EPersistentDiskKind kind) {
            if (optionalKind) {
                THROW_ERROR_EXCEPTION("Persistent disk %Qv has multiple kinds",
                    disk->GetId());
            }
            optionalKind = kind;
        };
        const auto& specEtc = disk->Spec().Etc().Load();
        if (specEtc.has_rbind_policy()) {
            setKind(EPersistentDiskKind::Rbind);
        }
        if (specEtc.has_managed_policy()) {
            setKind(EPersistentDiskKind::Managed);
        }
        if (!optionalKind) {
            THROW_ERROR_EXCEPTION("Persistent disk %Qv is of an unrecognized kind",
                disk->GetId());
        }
        return *optionalKind;
    }

    static void ValidateSpec(
        TTransaction* /*transaction*/,
        TPersistentDisk* disk)
    {
        auto oldKind = disk->Kind().Load();
        auto newKind = DeriveKind(disk);
        if (oldKind != EPersistentDiskKind::Unknown && oldKind != newKind) {
            THROW_ERROR_EXCEPTION("Changing kind of persistent disk %Qv from %Qlv to %Qlv is forbidden",
                disk->GetId(),
                oldKind,
                newKind);
        }
    }

    virtual void BeforeObjectCreated(
        TTransaction* transaction,
        TObject* object) override
    {
        TObjectTypeHandlerBase::BeforeObjectCreated(transaction, object);

        auto* disk = object->As<TPersistentDisk>();
        disk->Kind() = EPersistentDiskKind::Unknown;
    }

    virtual void AfterObjectCreated(
        TTransaction* transaction,
        TObject* object) override
    {
        TObjectTypeHandlerBase::AfterObjectCreated(transaction, object);

        auto* disk = object->As<TPersistentDisk>();
        disk->Kind() = DeriveKind(disk);
    }

    virtual void BeforeObjectRemoved(TTransaction* transaction, TObject* object) override
    {
        TObjectTypeHandlerBase::BeforeObjectRemoved(transaction, object);

        auto* disk = object->As<TPersistentDisk>();
        auto volumes = disk->Volumes().Load();
        if (!volumes.empty()) {
            THROW_ERROR_EXCEPTION("Cannot remove persistent disk %Qv since it contains %v volume(s)",
                disk->GetId(),
                volumes.size());
        }
    }
};

std::unique_ptr<IObjectTypeHandler> CreatePersistentDiskTypeHandler(NMaster::TBootstrap* bootstrap)
{
    return std::unique_ptr<IObjectTypeHandler>(new TPersistentDiskTypeHandler(bootstrap));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects


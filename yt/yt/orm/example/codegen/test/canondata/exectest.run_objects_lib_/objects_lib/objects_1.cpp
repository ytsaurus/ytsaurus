// AUTOMATICALLY GENERATED. DO NOT EDIT!

#include "objects.h"

#include <yt/yt/orm/server/objects/helpers.h>
#include <yt/yt/orm/example/plugins/server/library/mother_ship.h>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

NYT::NOrm::NExample::NServer::NPlugins::TMotherShip*
TObjectPluginTraits<NYT::NOrm::NExample::NServer::NLibrary::TMotherShip>::Downcast(
    NYT::NOrm::NExample::NServer::NLibrary::TMotherShip* object)
{
    auto* castedObject = dynamic_cast<NYT::NOrm::NExample::NServer::NPlugins::TMotherShip*>(object);
    YT_VERIFY(castedObject);
    return castedObject;
};

NYT::NOrm::NExample::NServer::NLibrary::TMotherShip*
TObjectPluginTraits<NYT::NOrm::NExample::NServer::NPlugins::TMotherShip>::Upcast(
    NYT::NOrm::NExample::NServer::NPlugins::TMotherShip* object)
{
    return object;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects;

namespace NYT::NOrm::NExample::NServer::NLibrary {

////////////////////////////////////////////////////////////////////////////////

using NYT::NOrm::NServer::NObjects::IObjectTypeHandler;
using NYT::NOrm::NServer::NObjects::ISession;

////////////////////////////////////////////////////////////////////////////////

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TMotherShip, ui64>
TMotherShip::RevisionDescriptor{
    &MotherShipsTable.Fields.MetaRevision,
    [] (TMotherShip* obj) { return &obj->Revision(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TMotherShip, i64>
TMotherShip::ReleaseYearDescriptor{
    &MotherShipsTable.Fields.MetaReleaseYear,
    [] (TMotherShip* obj) { return &obj->ReleaseYear(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TMotherShip, ui64>
TMotherShip::FinalizationStartTimeDescriptor{
    &MotherShipsTable.Fields.MetaFinalizationStartTime,
    [] (TMotherShip* obj) { return &obj->FinalizationStartTime(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TMotherShip, THashMap<TString, NYT::NOrm::NDataModel::TFinalizer>>
TMotherShip::FinalizersDescriptor{
    &MotherShipsTable.Fields.MetaFinalizers,
    [] (TMotherShip* obj) { return &obj->Finalizers(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TMotherShip, TMotherShip::TMetaEtc>
TMotherShip::MetaEtcDescriptor{
    &MotherShipsTable.Fields.MetaEtc,
    [] (TMotherShip* object) { return &object->MetaEtc(); }
};

////////////////////////////////////////////////////////////////////////////////

TMotherShip::TSpec::TSpec(TMotherShip* object)
    : ExecutorId_(object, &ExecutorIdDescriptor)
    , Revision_(object, &RevisionDescriptor)
    , SectorNames_(object, &SectorNamesDescriptor)
    , Price_(object, &PriceDescriptor)
    , Etc_(object, &EtcDescriptor)
{ }

const NYT::NOrm::NServer::NObjects::TManyToOneAttributeDescriptor<NYT::NOrm::NExample::NServer::NLibrary::TMotherShip, NYT::NOrm::NExample::NServer::NLibrary::TExecutor> TMotherShip::TSpec::ExecutorIdDescriptor {
    &MotherShipsTable.Fields.SpecExecutorId,
    [] (NYT::NOrm::NExample::NServer::NLibrary::TMotherShip* obj) { return &obj->Spec().ExecutorId(); },
    [] (NYT::NOrm::NExample::NServer::NLibrary::TExecutor* obj) { return &obj->Status().MotherShips(); },
    /*forbidNonEmptyRemoval*/ false
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TMotherShip, ui64>
TMotherShip::TSpec::RevisionDescriptor{
    &MotherShipsTable.Fields.SpecRevision,
    [] (TMotherShip* obj) { return &obj->Spec().Revision(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TMotherShip, std::vector<TString>>
TMotherShip::TSpec::SectorNamesDescriptor{
    &MotherShipsTable.Fields.SpecSectorNames,
    [] (TMotherShip* obj) { return &obj->Spec().SectorNames(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TMotherShip, i64>
TMotherShip::TSpec::PriceDescriptor{
    &MotherShipsTable.Fields.SpecPrice,
    [] (TMotherShip* obj) { return &obj->Spec().Price(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TMotherShip, TMotherShip::TSpec::TEtc>
TMotherShip::TSpec::EtcDescriptor{
    &MotherShipsTable.Fields.SpecEtc,
    [] (TMotherShip* obj) { return &obj->Spec().Etc(); }
};

TMotherShip::TStatus::TStatus(TMotherShip* object)
    : Etc_(object, &EtcDescriptor)
{ }

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TMotherShip, TMotherShip::TStatus::TEtc>
TMotherShip::TStatus::EtcDescriptor{
    &MotherShipsTable.Fields.StatusEtc,
    [] (TMotherShip* obj) { return &obj->Status().Etc(); }
};

////////////////////////////////////////////////////////////////////////////////

TMotherShip::TMotherShip(
    const i64& id,
    const NYT::NOrm::NClient::NObjects::TObjectKey& parentKey,
    IObjectTypeHandler* typeHandler,
    ISession* session)
    : TDataModelObject(typeHandler, session)
    , Id_(id)
    , ParentKeyAttribute_(this, parentKey)
    , Nexus_(this)
    , Interceptors_(this)
    , Revision_(this, &RevisionDescriptor)
    , ReleaseYear_(this, &ReleaseYearDescriptor)
    , FinalizationStartTime_(this, &FinalizationStartTimeDescriptor)
    , Finalizers_(this, &FinalizersDescriptor)
    , MetaEtc_(this, &MetaEtcDescriptor)
    , Spec_(this)
    , Status_(this)
{
}

TMotherShip::~TMotherShip()
{ }

////////////////////////////////////////////////////////////////////////////////

NYT::NOrm::NClient::NObjects::TObjectKey TMotherShip::GetKey() const
{
    return NYT::NOrm::NServer::NObjects::TObjectKey(
        GetId());
}

NYT::NOrm::NClient::NObjects::TObjectKey TMotherShip::GetParentKey(
    std::source_location location) const
{
    return ParentKeyAttribute_.GetKey(location);
}

NYT::NOrm::NServer::NObjects::TParentKeyAttribute* TMotherShip::GetParentKeyAttribute()
{
    return &ParentKeyAttribute_;
}

i64 TMotherShip::NexusId(std::source_location location) const
{
    return GetParentKey(location).GetWithDefault<i64>(0);
}

void TMotherShip::ScheduleUuidLoad() const
{
    MetaEtc().ScheduleLoad();
}

NYT::NOrm::NClient::NObjects::TObjectId TMotherShip::GetUuid(
    std::source_location location) const
{
    return MetaEtc().Load(location).uuid();
}

TString TMotherShip::GetName(
    std::source_location location) const
{
    return MetaEtc().Load(location).name();
}

////////////////////////////////////////////////////////////////////////////////

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TMultipolicyId, ui64>
TMultipolicyId::FinalizationStartTimeDescriptor{
    &MultipolicyIdsTable.Fields.MetaFinalizationStartTime,
    [] (TMultipolicyId* obj) { return &obj->FinalizationStartTime(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TMultipolicyId, THashMap<TString, NYT::NOrm::NDataModel::TFinalizer>>
TMultipolicyId::FinalizersDescriptor{
    &MultipolicyIdsTable.Fields.MetaFinalizers,
    [] (TMultipolicyId* obj) { return &obj->Finalizers(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TMultipolicyId, TMultipolicyId::TMetaEtc>
TMultipolicyId::MetaEtcDescriptor{
    &MultipolicyIdsTable.Fields.MetaEtc,
    [] (TMultipolicyId* object) { return &object->MetaEtc(); }
};

////////////////////////////////////////////////////////////////////////////////

TMultipolicyId::TSpec::TSpec(TMultipolicyId* object)
    : StrValue_(object, &StrValueDescriptor)
    , I64Value_(object, &I64ValueDescriptor)
    , Ui64Value_(object, &Ui64ValueDescriptor)
    , AnotherUi64Value_(object, &AnotherUi64ValueDescriptor)
    , Etc_(object, &EtcDescriptor)
{ }

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TMultipolicyId, TString>
TMultipolicyId::TSpec::StrValueDescriptor{
    &MultipolicyIdsTable.Fields.SpecStrValue,
    [] (TMultipolicyId* obj) { return &obj->Spec().StrValue(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TMultipolicyId, i64>
TMultipolicyId::TSpec::I64ValueDescriptor{
    &MultipolicyIdsTable.Fields.SpecI64Value,
    [] (TMultipolicyId* obj) { return &obj->Spec().I64Value(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TMultipolicyId, ui64>
TMultipolicyId::TSpec::Ui64ValueDescriptor{
    &MultipolicyIdsTable.Fields.SpecUi64Value,
    [] (TMultipolicyId* obj) { return &obj->Spec().Ui64Value(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TMultipolicyId, ui64>
TMultipolicyId::TSpec::AnotherUi64ValueDescriptor{
    &MultipolicyIdsTable.Fields.SpecAnotherUi64Value,
    [] (TMultipolicyId* obj) { return &obj->Spec().AnotherUi64Value(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TMultipolicyId, TMultipolicyId::TSpec::TEtc>
TMultipolicyId::TSpec::EtcDescriptor{
    &MultipolicyIdsTable.Fields.SpecEtc,
    [] (TMultipolicyId* obj) { return &obj->Spec().Etc(); }
};

TMultipolicyId::TStatus::TStatus(TMultipolicyId* object)
    : Etc_(object, &EtcDescriptor)
{ }

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TMultipolicyId, TMultipolicyId::TStatus::TEtc>
TMultipolicyId::TStatus::EtcDescriptor{
    &MultipolicyIdsTable.Fields.StatusEtc,
    [] (TMultipolicyId* obj) { return &obj->Status().Etc(); }
};

////////////////////////////////////////////////////////////////////////////////

TMultipolicyId::TMultipolicyId(
    const TString& strId,
    const i64& i64Id,
    const ui64& ui64Id,
    const ui64& anotherUi64Id,
    IObjectTypeHandler* typeHandler,
    ISession* session)
    : TDataModelObject(typeHandler, session)
    , StrId_(strId)
    , I64Id_(i64Id)
    , Ui64Id_(ui64Id)
    , AnotherUi64Id_(anotherUi64Id)
    , FinalizationStartTime_(this, &FinalizationStartTimeDescriptor)
    , Finalizers_(this, &FinalizersDescriptor)
    , MetaEtc_(this, &MetaEtcDescriptor)
    , Spec_(this)
    , Status_(this)
{
}

////////////////////////////////////////////////////////////////////////////////

NYT::NOrm::NClient::NObjects::TObjectKey TMultipolicyId::GetKey() const
{
    return NYT::NOrm::NServer::NObjects::TObjectKey(
        GetStrId(),
        GetI64Id(),
        GetUi64Id(),
        GetAnotherUi64Id());
}

void TMultipolicyId::ScheduleUuidLoad() const
{
    MetaEtc().ScheduleLoad();
}

NYT::NOrm::NClient::NObjects::TObjectId TMultipolicyId::GetUuid(
    std::source_location location) const
{
    return MetaEtc().Load(location).uuid();
}

TString TMultipolicyId::GetName(
    std::source_location location) const
{
    return MetaEtc().Load(location).name();
}

////////////////////////////////////////////////////////////////////////////////

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TNestedColumns, ui64>
TNestedColumns::FinalizationStartTimeDescriptor{
    &NestedColumnsTable.Fields.MetaFinalizationStartTime,
    [] (TNestedColumns* obj) { return &obj->FinalizationStartTime(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TNestedColumns, THashMap<TString, NYT::NOrm::NDataModel::TFinalizer>>
TNestedColumns::FinalizersDescriptor{
    &NestedColumnsTable.Fields.MetaFinalizers,
    [] (TNestedColumns* obj) { return &obj->Finalizers(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TNestedColumns, TNestedColumns::TMetaEtc>
TNestedColumns::MetaEtcDescriptor{
    &NestedColumnsTable.Fields.MetaEtc,
    [] (TNestedColumns* object) { return &object->MetaEtc(); }
};

////////////////////////////////////////////////////////////////////////////////

TNestedColumns::TSpec::TSpec(TNestedColumns* object)
    : CompositeSingular_(object)
    , ColumnSingular_(object, &ColumnSingularDescriptor)
    , ColumnRepeated_(object, &ColumnRepeatedDescriptor)
    , ColumnMap_(object, &ColumnMapDescriptor)
    , Etc_(object, &EtcDescriptor)
{ }

TNestedColumns::TSpec::TCompositeSingular::TCompositeSingular(TNestedColumns* object)
    : ColumnSingular_(object, &ColumnSingularDescriptor)
    , ColumnRepeated_(object, &ColumnRepeatedDescriptor)
    , ColumnMap_(object, &ColumnMapDescriptor)
    , Etc_(object, &EtcDescriptor)
{ }

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TNestedColumns, NYT::NOrm::NExample::NClient::NProto::NDataModel::TNestedColumnsSpec::TSimple>
TNestedColumns::TSpec::TCompositeSingular::ColumnSingularDescriptor{
    &NestedColumnsTable.Fields.SpecCompositeSingularColumnSingular,
    [] (TNestedColumns* obj) { return &obj->Spec().CompositeSingular().ColumnSingular(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TNestedColumns, std::vector<NYT::NOrm::NExample::NClient::NProto::NDataModel::TNestedColumnsSpec::TSimple>>
TNestedColumns::TSpec::TCompositeSingular::ColumnRepeatedDescriptor{
    &NestedColumnsTable.Fields.SpecCompositeSingularColumnRepeated,
    [] (TNestedColumns* obj) { return &obj->Spec().CompositeSingular().ColumnRepeated(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TNestedColumns, THashMap<TString, NYT::NOrm::NExample::NClient::NProto::NDataModel::TNestedColumnsSpec::TSimple>>
TNestedColumns::TSpec::TCompositeSingular::ColumnMapDescriptor{
    &NestedColumnsTable.Fields.SpecCompositeSingularColumnMap,
    [] (TNestedColumns* obj) { return &obj->Spec().CompositeSingular().ColumnMap(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TNestedColumns, TNestedColumns::TSpec::TCompositeSingular::TEtc>
TNestedColumns::TSpec::TCompositeSingular::EtcDescriptor{
    &NestedColumnsTable.Fields.SpecCompositeSingular,
    [] (TNestedColumns* obj) { return &obj->Spec().CompositeSingular().Etc(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TNestedColumns, NYT::NOrm::NExample::NClient::NProto::NDataModel::TNestedColumnsSpec::TSimple>
TNestedColumns::TSpec::ColumnSingularDescriptor{
    &NestedColumnsTable.Fields.SpecColumnSingular,
    [] (TNestedColumns* obj) { return &obj->Spec().ColumnSingular(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TNestedColumns, std::vector<NYT::NOrm::NExample::NClient::NProto::NDataModel::TNestedColumnsSpec::TSimple>>
TNestedColumns::TSpec::ColumnRepeatedDescriptor{
    &NestedColumnsTable.Fields.SpecColumnRepeated,
    [] (TNestedColumns* obj) { return &obj->Spec().ColumnRepeated(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TNestedColumns, THashMap<TString, NYT::NOrm::NExample::NClient::NProto::NDataModel::TNestedColumnsSpec::TSimple>>
TNestedColumns::TSpec::ColumnMapDescriptor{
    &NestedColumnsTable.Fields.SpecColumnMap,
    [] (TNestedColumns* obj) { return &obj->Spec().ColumnMap(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TNestedColumns, TNestedColumns::TSpec::TEtc>
TNestedColumns::TSpec::EtcDescriptor{
    &NestedColumnsTable.Fields.SpecEtc,
    [] (TNestedColumns* obj) { return &obj->Spec().Etc(); }
};

TNestedColumns::TStatus::TStatus(TNestedColumns* object)
    : Etc_(object, &EtcDescriptor)
{ }

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TNestedColumns, TNestedColumns::TStatus::TEtc>
TNestedColumns::TStatus::EtcDescriptor{
    &NestedColumnsTable.Fields.StatusEtc,
    [] (TNestedColumns* obj) { return &obj->Status().Etc(); }
};

////////////////////////////////////////////////////////////////////////////////

TNestedColumns::TNestedColumns(
    const i64& id,
    IObjectTypeHandler* typeHandler,
    ISession* session)
    : TDataModelObject(typeHandler, session)
    , Id_(id)
    , FinalizationStartTime_(this, &FinalizationStartTimeDescriptor)
    , Finalizers_(this, &FinalizersDescriptor)
    , MetaEtc_(this, &MetaEtcDescriptor)
    , Spec_(this)
    , Status_(this)
{
}

////////////////////////////////////////////////////////////////////////////////

NYT::NOrm::NClient::NObjects::TObjectKey TNestedColumns::GetKey() const
{
    return NYT::NOrm::NServer::NObjects::TObjectKey(
        GetId());
}

void TNestedColumns::ScheduleUuidLoad() const
{
    MetaEtc().ScheduleLoad();
}

NYT::NOrm::NClient::NObjects::TObjectId TNestedColumns::GetUuid(
    std::source_location location) const
{
    return MetaEtc().Load(location).uuid();
}

TString TNestedColumns::GetName(
    std::source_location location) const
{
    return MetaEtc().Load(location).name();
}

////////////////////////////////////////////////////////////////////////////////

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TNexus, ui64>
TNexus::FinalizationStartTimeDescriptor{
    &NexusTable.Fields.MetaFinalizationStartTime,
    [] (TNexus* obj) { return &obj->FinalizationStartTime(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TNexus, THashMap<TString, NYT::NOrm::NDataModel::TFinalizer>>
TNexus::FinalizersDescriptor{
    &NexusTable.Fields.MetaFinalizers,
    [] (TNexus* obj) { return &obj->Finalizers(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TNexus, TNexus::TMetaEtc>
TNexus::MetaEtcDescriptor{
    &NexusTable.Fields.MetaEtc,
    [] (TNexus* object) { return &object->MetaEtc(); }
};

////////////////////////////////////////////////////////////////////////////////

TNexus::TSpec::TSpec(TNexus* object)
    : SomeMapToMessageColumn_(object, &SomeMapToMessageColumnDescriptor)
    , Etc_(object, &EtcDescriptor)
{ }

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TNexus, THashMap<ui64, NYT::NOrm::NExample::NClient::NProto::NDataModel::TNexusSpec::NestedMessage>>
TNexus::TSpec::SomeMapToMessageColumnDescriptor{
    &NexusTable.Fields.SpecSomeMapToMessageColumn,
    [] (TNexus* obj) { return &obj->Spec().SomeMapToMessageColumn(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TNexus, TNexus::TSpec::TEtc>
TNexus::TSpec::EtcDescriptor{
    &NexusTable.Fields.SpecEtc,
    [] (TNexus* obj) { return &obj->Spec().Etc(); }
};

TNexus::TStatus::TStatus(TNexus* object)
    : ColumnSemaphore_(object, &ColumnSemaphoreDescriptor)
    , Etc_(object, &EtcDescriptor)
{ }

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TNexus, NYT::NOrm::NDataModel::TEmbeddedSemaphore>
TNexus::TStatus::ColumnSemaphoreDescriptor{
    &NexusTable.Fields.StatusColumnSemaphore,
    [] (TNexus* obj) { return &obj->Status().ColumnSemaphore(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TNexus, TNexus::TStatus::TEtc>
TNexus::TStatus::EtcDescriptor{
    &NexusTable.Fields.StatusEtc,
    [] (TNexus* obj) { return &obj->Status().Etc(); }
};

////////////////////////////////////////////////////////////////////////////////

TNexus::TNexus(
    const i64& id,
    IObjectTypeHandler* typeHandler,
    ISession* session)
    : TDataModelObject(typeHandler, session)
    , Id_(id)
    , MotherShips_(this)
    , FinalizationStartTime_(this, &FinalizationStartTimeDescriptor)
    , Finalizers_(this, &FinalizersDescriptor)
    , MetaEtc_(this, &MetaEtcDescriptor)
    , Spec_(this)
    , Status_(this)
{
}

////////////////////////////////////////////////////////////////////////////////

NYT::NOrm::NClient::NObjects::TObjectKey TNexus::GetKey() const
{
    return NYT::NOrm::NServer::NObjects::TObjectKey(
        GetId());
}

void TNexus::ScheduleUuidLoad() const
{
    MetaEtc().ScheduleLoad();
}

NYT::NOrm::NClient::NObjects::TObjectId TNexus::GetUuid(
    std::source_location location) const
{
    return MetaEtc().Load(location).uuid();
}

TString TNexus::GetName(
    std::source_location location) const
{
    return MetaEtc().Load(location).name();
}

////////////////////////////////////////////////////////////////////////////////

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TNirvanaDMProcessInstance, ui64>
TNirvanaDMProcessInstance::FinalizationStartTimeDescriptor{
    &NirvanaDMProcessInstancesTable.Fields.MetaFinalizationStartTime,
    [] (TNirvanaDMProcessInstance* obj) { return &obj->FinalizationStartTime(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TNirvanaDMProcessInstance, THashMap<TString, NYT::NOrm::NDataModel::TFinalizer>>
TNirvanaDMProcessInstance::FinalizersDescriptor{
    &NirvanaDMProcessInstancesTable.Fields.MetaFinalizers,
    [] (TNirvanaDMProcessInstance* obj) { return &obj->Finalizers(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TNirvanaDMProcessInstance, TNirvanaDMProcessInstance::TMetaEtc>
TNirvanaDMProcessInstance::MetaEtcDescriptor{
    &NirvanaDMProcessInstancesTable.Fields.MetaEtc,
    [] (TNirvanaDMProcessInstance* object) { return &object->MetaEtc(); }
};

////////////////////////////////////////////////////////////////////////////////

TNirvanaDMProcessInstance::TSpec::TSpec(TNirvanaDMProcessInstance* object)
    : Etc_(object, &EtcDescriptor)
{ }

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TNirvanaDMProcessInstance, TNirvanaDMProcessInstance::TSpec::TEtc>
TNirvanaDMProcessInstance::TSpec::EtcDescriptor{
    &NirvanaDMProcessInstancesTable.Fields.SpecEtc,
    [] (TNirvanaDMProcessInstance* obj) { return &obj->Spec().Etc(); }
};

TNirvanaDMProcessInstance::TStatus::TStatus(TNirvanaDMProcessInstance* object)
    : Etc_(object, &EtcDescriptor)
{ }

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TNirvanaDMProcessInstance, TNirvanaDMProcessInstance::TStatus::TEtc>
TNirvanaDMProcessInstance::TStatus::EtcDescriptor{
    &NirvanaDMProcessInstancesTable.Fields.StatusEtc,
    [] (TNirvanaDMProcessInstance* obj) { return &obj->Status().Etc(); }
};

////////////////////////////////////////////////////////////////////////////////

TNirvanaDMProcessInstance::TNirvanaDMProcessInstance(
    const TString& id,
    IObjectTypeHandler* typeHandler,
    ISession* session)
    : TDataModelObject(typeHandler, session)
    , Id_(id)
    , FinalizationStartTime_(this, &FinalizationStartTimeDescriptor)
    , Finalizers_(this, &FinalizersDescriptor)
    , MetaEtc_(this, &MetaEtcDescriptor)
    , Spec_(this)
    , Status_(this)
{
    RegisterScalarAttributeIndex(
        "nirvana_dm_process_instances_by_created",
        std::make_unique<NYT::NOrm::NServer::NObjects::TScalarAttributeIndex>(
            typeHandler->GetIndexDescriptorOrThrow("nirvana_dm_process_instances_by_created"),
            this,
            std::vector<NYT::NOrm::NServer::NObjects::TIndexAttribute>{
                NYT::NOrm::NServer::NObjects::TIndexAttribute(
                    &Spec().Etc(),
                    "/created",
                    /*repeated*/ false),
            },
            std::vector<NYT::NOrm::NServer::NObjects::TIndexAttribute>{
            },
            /*unique*/ false));
    RegisterScalarAttributeIndex(
        "nirvana_dm_process_instances_by_definition_id",
        std::make_unique<NYT::NOrm::NServer::NObjects::TScalarAttributeIndex>(
            typeHandler->GetIndexDescriptorOrThrow("nirvana_dm_process_instances_by_definition_id"),
            this,
            std::vector<NYT::NOrm::NServer::NObjects::TIndexAttribute>{
                NYT::NOrm::NServer::NObjects::TIndexAttribute(
                    &Spec().Etc(),
                    "/definition_id",
                    /*repeated*/ false),
            },
            std::vector<NYT::NOrm::NServer::NObjects::TIndexAttribute>{
            },
            /*unique*/ false));
}

////////////////////////////////////////////////////////////////////////////////

NYT::NOrm::NClient::NObjects::TObjectKey TNirvanaDMProcessInstance::GetKey() const
{
    return NYT::NOrm::NServer::NObjects::TObjectKey(
        GetId());
}

void TNirvanaDMProcessInstance::ScheduleUuidLoad() const
{
    MetaEtc().ScheduleLoad();
}

NYT::NOrm::NClient::NObjects::TObjectId TNirvanaDMProcessInstance::GetUuid(
    std::source_location location) const
{
    return MetaEtc().Load(location).uuid();
}

TString TNirvanaDMProcessInstance::GetName(
    std::source_location location) const
{
    return MetaEtc().Load(location).name();
}

////////////////////////////////////////////////////////////////////////////////

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TPublisher, ui64>
TPublisher::FinalizationStartTimeDescriptor{
    &PublishersTable.Fields.MetaFinalizationStartTime,
    [] (TPublisher* obj) { return &obj->FinalizationStartTime(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TPublisher, THashMap<TString, NYT::NOrm::NDataModel::TFinalizer>>
TPublisher::FinalizersDescriptor{
    &PublishersTable.Fields.MetaFinalizers,
    [] (TPublisher* obj) { return &obj->Finalizers(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TPublisher, TPublisher::TMetaEtc>
TPublisher::MetaEtcDescriptor{
    &PublishersTable.Fields.MetaEtc,
    [] (TPublisher* object) { return &object->MetaEtc(); }
};

////////////////////////////////////////////////////////////////////////////////

TPublisher::TSpec::TSpec(TPublisher* object)
    : Name_(object, &NameDescriptor)
    , EditorInChief_(object, &EditorInChiefDescriptor)
    , IllustratorInChief_(object, &IllustratorInChiefDescriptor)
    , PublisherGroup_(object, &PublisherGroupDescriptor)
    , ColumnField_(object, &ColumnFieldDescriptor)
    , FeaturedIllustrators_(object, &FeaturedIllustratorsDescriptor)
    , EditorInChiefView_(object)
    , IllustratorInChiefView_(object)
    , PublisherGroupView_(object)
    , FeaturedIllustratorsView_(object)
    , Etc_(object, &EtcDescriptor)
{ }

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TPublisher, TString>
TPublisher::TSpec::NameDescriptor{
    &PublishersTable.Fields.SpecName,
    [] (TPublisher* obj) { return &obj->Spec().Name(); }
};

const NYT::NOrm::NServer::NObjects::TManyToOneAttributeDescriptor<NYT::NOrm::NExample::NServer::NLibrary::TPublisher, NYT::NOrm::NExample::NServer::NLibrary::TEditor> TPublisher::TSpec::EditorInChiefDescriptor {
    &PublishersTable.Fields.SpecEditorInChief,
    [] (NYT::NOrm::NExample::NServer::NLibrary::TPublisher* obj) { return &obj->Spec().EditorInChief(); },
    [] (NYT::NOrm::NExample::NServer::NLibrary::TEditor* obj) { return &obj->Status().Publishers(); },
    /*forbidNonEmptyRemoval*/ false
};

const NYT::NOrm::NServer::NObjects::TManyToOneAttributeDescriptor<NYT::NOrm::NExample::NServer::NLibrary::TPublisher, NYT::NOrm::NExample::NServer::NLibrary::TIllustrator> TPublisher::TSpec::IllustratorInChiefDescriptor {
    &PublishersTable.Fields.SpecIllustratorInChief,
    [] (NYT::NOrm::NExample::NServer::NLibrary::TPublisher* obj) { return &obj->Spec().IllustratorInChief(); },
    [] (NYT::NOrm::NExample::NServer::NLibrary::TIllustrator* obj) { return &obj->Status().Publishers(); },
    /*forbidNonEmptyRemoval*/ false
};

const NYT::NOrm::NServer::NObjects::TManyToOneAttributeDescriptor<NYT::NOrm::NExample::NServer::NLibrary::TPublisher, NYT::NOrm::NExample::NServer::NLibrary::TPublisher> TPublisher::TSpec::PublisherGroupDescriptor {
    &PublishersTable.Fields.SpecPublisherGroup,
    [] (NYT::NOrm::NExample::NServer::NLibrary::TPublisher* obj) { return &obj->Spec().PublisherGroup(); },
    [] (NYT::NOrm::NExample::NServer::NLibrary::TPublisher* obj) { return &obj->Status().Publishers(); },
    /*forbidNonEmptyRemoval*/ true
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TPublisher, i64>
TPublisher::TSpec::ColumnFieldDescriptor{
    &PublishersTable.Fields.SpecColumnField,
    [] (TPublisher* obj) { return &obj->Spec().ColumnField(); }
};

const NYT::NOrm::NServer::NObjects::TManyToManyInlineAttributeDescriptor<NYT::NOrm::NExample::NServer::NLibrary::TPublisher, NYT::NOrm::NExample::NServer::NLibrary::TIllustrator> TPublisher::TSpec::FeaturedIllustratorsDescriptor {
    &PublishersTable.Fields.SpecFeaturedIllustrators,
    [] (NYT::NOrm::NExample::NServer::NLibrary::TPublisher* obj) { return &obj->Spec().FeaturedIllustrators(); },
    [] (NYT::NOrm::NExample::NServer::NLibrary::TIllustrator* obj) { return &obj->Status().PublishersForOldFeaturedIllustrators(); },
    /*forbidNonEmptyRemoval*/ true
};

const NYT::NOrm::NServer::NObjects::TManyToOneViewAttributeDescriptor<NYT::NOrm::NExample::NServer::NLibrary::TPublisher, NYT::NOrm::NExample::NServer::NLibrary::TEditor> TPublisher::TSpec::EditorInChiefViewDescriptor {
    TPublisher::TSpec::EditorInChiefDescriptor,
    [] (const TPublisher* obj) { return &obj->Spec().EditorInChiefView(); }
};

const NYT::NOrm::NServer::NObjects::TManyToOneViewAttributeDescriptor<NYT::NOrm::NExample::NServer::NLibrary::TPublisher, NYT::NOrm::NExample::NServer::NLibrary::TIllustrator> TPublisher::TSpec::IllustratorInChiefViewDescriptor {
    TPublisher::TSpec::IllustratorInChiefDescriptor,
    [] (const TPublisher* obj) { return &obj->Spec().IllustratorInChiefView(); }
};

const NYT::NOrm::NServer::NObjects::TManyToOneViewAttributeDescriptor<NYT::NOrm::NExample::NServer::NLibrary::TPublisher, NYT::NOrm::NExample::NServer::NLibrary::TPublisher> TPublisher::TSpec::PublisherGroupViewDescriptor {
    TPublisher::TSpec::PublisherGroupDescriptor,
    [] (const TPublisher* obj) { return &obj->Spec().PublisherGroupView(); }
};

const NYT::NOrm::NServer::NObjects::TManyToManyInlineViewAttributeDescriptor<NYT::NOrm::NExample::NServer::NLibrary::TPublisher, NYT::NOrm::NExample::NServer::NLibrary::TIllustrator> TPublisher::TSpec::FeaturedIllustratorsViewDescriptor {
    TPublisher::TSpec::FeaturedIllustratorsDescriptor,
    [] (const TPublisher* obj) { return &obj->Spec().FeaturedIllustratorsView(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TPublisher, TPublisher::TSpec::TEtc>
TPublisher::TSpec::EtcDescriptor{
    &PublishersTable.Fields.SpecEtc,
    [] (TPublisher* obj) { return &obj->Spec().Etc(); }
};

TPublisher::TStatus::TStatus(TPublisher* object)
    : ColumnField_(object, &ColumnFieldDescriptor)
    , ColumnList_(object, &ColumnListDescriptor)
    , FeaturedIllustrators_(object, &FeaturedIllustratorsDescriptor)
    , Books_(object, &BooksDescriptor)
    , Illustrators_(object, &IllustratorsDescriptor)
    , Publishers_(object, &PublishersDescriptor)
    , FeaturedIllustratorsView_(object)
    , Etc_(object, &EtcDescriptor)
{ }

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TPublisher, i64>
TPublisher::TStatus::ColumnFieldDescriptor{
    &PublishersTable.Fields.StatusColumnField,
    [] (TPublisher* obj) { return &obj->Status().ColumnField(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TPublisher, std::vector<i64>>
TPublisher::TStatus::ColumnListDescriptor{
    &PublishersTable.Fields.StatusColumnList,
    [] (TPublisher* obj) { return &obj->Status().ColumnList(); }
};

const NYT::NOrm::NServer::NObjects::TManyToManyInlineAttributeDescriptor<NYT::NOrm::NExample::NServer::NLibrary::TPublisher, NYT::NOrm::NExample::NServer::NLibrary::TIllustrator> TPublisher::TStatus::FeaturedIllustratorsDescriptor {
    &PublishersTable.Fields.StatusFeaturedIllustrators,
    [] (NYT::NOrm::NExample::NServer::NLibrary::TPublisher* obj) { return &obj->Status().FeaturedIllustrators(); },
    [] (NYT::NOrm::NExample::NServer::NLibrary::TIllustrator* obj) { return &obj->Status().PublishersForNewFeaturedIllustrators(); },
    /*forbidNonEmptyRemoval*/ true
};

const NYT::NOrm::NServer::NObjects::TManyToManyTabularAttributeDescriptor<NYT::NOrm::NExample::NServer::NLibrary::TPublisher, NYT::NOrm::NExample::NServer::NLibrary::TBook> TPublisher::TStatus::BooksDescriptor {
    &PublishersToBooksTable,
    { &PublishersToBooksTable.Fields.PublisherId },
    { &PublishersToBooksTable.Fields.BookId, &PublishersToBooksTable.Fields.BookId2 },
    [] (NYT::NOrm::NExample::NServer::NLibrary::TPublisher* obj) { return &obj->Status().Books(); },
    [] (NYT::NOrm::NExample::NServer::NLibrary::TBook* obj) { return &obj->Spec().AlternativePublisherIds(); },
    /*foreignObjectTableKey*/ false,
    /*forbidNonEmptyRemoval*/ true
};

const NYT::NOrm::NServer::NObjects::TOneToManyAttributeDescriptor<NYT::NOrm::NExample::NServer::NLibrary::TPublisher, NYT::NOrm::NExample::NServer::NLibrary::TIllustrator> TPublisher::TStatus::IllustratorsDescriptor {
    &PublisherToIllustratorsTable,
    { &PublisherToIllustratorsTable.Fields.TargetPublisherId },
    { &PublisherToIllustratorsTable.Fields.SourceIllustratorUid },
    [] (NYT::NOrm::NExample::NServer::NLibrary::TPublisher* obj) { return &obj->Status().Illustrators(); },
    [] (NYT::NOrm::NExample::NServer::NLibrary::TIllustrator* obj) { return &obj->PartTimeJob(); },
    /*foreignObjectTableKey*/ true,
    /*forbidNonEmptyRemoval*/ false
};

const NYT::NOrm::NServer::NObjects::TOneToManyAttributeDescriptor<NYT::NOrm::NExample::NServer::NLibrary::TPublisher, NYT::NOrm::NExample::NServer::NLibrary::TPublisher> TPublisher::TStatus::PublishersDescriptor {
    &PublisherToPublishersTable,
    { &PublisherToPublishersTable.Fields.TargetPublisherId },
    { &PublisherToPublishersTable.Fields.SourcePublisherId },
    [] (NYT::NOrm::NExample::NServer::NLibrary::TPublisher* obj) { return &obj->Status().Publishers(); },
    [] (NYT::NOrm::NExample::NServer::NLibrary::TPublisher* obj) { return &obj->Spec().PublisherGroup(); },
    /*foreignObjectTableKey*/ false,
    /*forbidNonEmptyRemoval*/ false
};

const NYT::NOrm::NServer::NObjects::TManyToManyInlineViewAttributeDescriptor<NYT::NOrm::NExample::NServer::NLibrary::TPublisher, NYT::NOrm::NExample::NServer::NLibrary::TIllustrator> TPublisher::TStatus::FeaturedIllustratorsViewDescriptor {
    TPublisher::TStatus::FeaturedIllustratorsDescriptor,
    [] (const TPublisher* obj) { return &obj->Status().FeaturedIllustratorsView(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TPublisher, TPublisher::TStatus::TEtc>
TPublisher::TStatus::EtcDescriptor{
    &PublishersTable.Fields.StatusEtc,
    [] (TPublisher* obj) { return &obj->Status().Etc(); }
};

////////////////////////////////////////////////////////////////////////////////

TPublisher::TPublisher(
    const i64& id,
    IObjectTypeHandler* typeHandler,
    ISession* session)
    : TDataModelObject(typeHandler, session)
    , Id_(id)
    , Books_(this)
    , Illustrators_(this)
    , Typographers_(this)
    , FinalizationStartTime_(this, &FinalizationStartTimeDescriptor)
    , Finalizers_(this, &FinalizersDescriptor)
    , MetaEtc_(this, &MetaEtcDescriptor)
    , Spec_(this)
    , Status_(this)
{
    RegisterScalarAttributeIndex(
        "publishers_by_column_field",
        std::make_unique<NYT::NOrm::NServer::NObjects::TScalarAttributeIndex>(
            typeHandler->GetIndexDescriptorOrThrow("publishers_by_column_field"),
            this,
            std::vector<NYT::NOrm::NServer::NObjects::TIndexAttribute>{
                NYT::NOrm::NServer::NObjects::TIndexAttribute(
                    &Spec().ColumnField(),
                    "",
                    /*repeated*/ false),
            },
            std::vector<NYT::NOrm::NServer::NObjects::TIndexAttribute>{
            },
            /*unique*/ false));
    RegisterScalarAttributeIndex(
        "publishers_by_number_of_awards",
        std::make_unique<NYT::NOrm::NServer::NObjects::TScalarAttributeIndex>(
            typeHandler->GetIndexDescriptorOrThrow("publishers_by_number_of_awards"),
            this,
            std::vector<NYT::NOrm::NServer::NObjects::TIndexAttribute>{
                NYT::NOrm::NServer::NObjects::TIndexAttribute(
                    &Status().Etc(),
                    "/number_of_awards",
                    /*repeated*/ false),
            },
            std::vector<NYT::NOrm::NServer::NObjects::TIndexAttribute>{
            },
            /*unique*/ false));
}

////////////////////////////////////////////////////////////////////////////////

NYT::NOrm::NClient::NObjects::TObjectKey TPublisher::GetKey() const
{
    return NYT::NOrm::NServer::NObjects::TObjectKey(
        GetId());
}

void TPublisher::ScheduleUuidLoad() const
{
    MetaEtc().ScheduleLoad();
}

NYT::NOrm::NClient::NObjects::TObjectId TPublisher::GetUuid(
    std::source_location location) const
{
    return MetaEtc().Load(location).uuid();
}

TString TPublisher::GetName(
    std::source_location location) const
{
    return MetaEtc().Load(location).name();
}

////////////////////////////////////////////////////////////////////////////////

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TRandomId, ui64>
TRandomId::FinalizationStartTimeDescriptor{
    &RandomIdsTable.Fields.MetaFinalizationStartTime,
    [] (TRandomId* obj) { return &obj->FinalizationStartTime(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TRandomId, THashMap<TString, NYT::NOrm::NDataModel::TFinalizer>>
TRandomId::FinalizersDescriptor{
    &RandomIdsTable.Fields.MetaFinalizers,
    [] (TRandomId* obj) { return &obj->Finalizers(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TRandomId, TRandomId::TMetaEtc>
TRandomId::MetaEtcDescriptor{
    &RandomIdsTable.Fields.MetaEtc,
    [] (TRandomId* object) { return &object->MetaEtc(); }
};

////////////////////////////////////////////////////////////////////////////////

TRandomId::TSpec::TSpec(TRandomId* object)
    : StrValue_(object, &StrValueDescriptor)
    , Etc_(object, &EtcDescriptor)
{ }

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TRandomId, TString>
TRandomId::TSpec::StrValueDescriptor{
    &RandomIdsTable.Fields.SpecStrValue,
    [] (TRandomId* obj) { return &obj->Spec().StrValue(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TRandomId, TRandomId::TSpec::TEtc>
TRandomId::TSpec::EtcDescriptor{
    &RandomIdsTable.Fields.SpecEtc,
    [] (TRandomId* obj) { return &obj->Spec().Etc(); }
};

TRandomId::TStatus::TStatus(TRandomId* object)
    : Etc_(object, &EtcDescriptor)
{ }

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TRandomId, TRandomId::TStatus::TEtc>
TRandomId::TStatus::EtcDescriptor{
    &RandomIdsTable.Fields.StatusEtc,
    [] (TRandomId* obj) { return &obj->Status().Etc(); }
};

////////////////////////////////////////////////////////////////////////////////

TRandomId::TRandomId(
    const TString& strId,
    const i64& i64Id,
    const ui64& ui64Id,
    IObjectTypeHandler* typeHandler,
    ISession* session)
    : TDataModelObject(typeHandler, session)
    , StrId_(strId)
    , I64Id_(i64Id)
    , Ui64Id_(ui64Id)
    , FinalizationStartTime_(this, &FinalizationStartTimeDescriptor)
    , Finalizers_(this, &FinalizersDescriptor)
    , MetaEtc_(this, &MetaEtcDescriptor)
    , Spec_(this)
    , Status_(this)
{
}

////////////////////////////////////////////////////////////////////////////////

NYT::NOrm::NClient::NObjects::TObjectKey TRandomId::GetKey() const
{
    return NYT::NOrm::NServer::NObjects::TObjectKey(
        GetStrId(),
        GetI64Id(),
        GetUi64Id());
}

void TRandomId::ScheduleUuidLoad() const
{
    MetaEtc().ScheduleLoad();
}

NYT::NOrm::NClient::NObjects::TObjectId TRandomId::GetUuid(
    std::source_location location) const
{
    return MetaEtc().Load(location).uuid();
}

TString TRandomId::GetName(
    std::source_location location) const
{
    return MetaEtc().Load(location).name();
}

////////////////////////////////////////////////////////////////////////////////

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TSchema, ui64>
TSchema::FinalizationStartTimeDescriptor{
    &SchemasTable.Fields.MetaFinalizationStartTime,
    [] (TSchema* obj) { return &obj->FinalizationStartTime(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TSchema, THashMap<TString, NYT::NOrm::NDataModel::TFinalizer>>
TSchema::FinalizersDescriptor{
    &SchemasTable.Fields.MetaFinalizers,
    [] (TSchema* obj) { return &obj->Finalizers(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TSchema, TSchema::TMetaEtc>
TSchema::MetaEtcDescriptor{
    &SchemasTable.Fields.MetaEtc,
    [] (TSchema* object) { return &object->MetaEtc(); }
};

////////////////////////////////////////////////////////////////////////////////

TSchema::TSpec::TSpec(TSchema* object)
    : Etc_(object, &EtcDescriptor)
{ }

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TSchema, TSchema::TSpec::TEtc>
TSchema::TSpec::EtcDescriptor{
    &SchemasTable.Fields.SpecEtc,
    [] (TSchema* obj) { return &obj->Spec().Etc(); }
};

TSchema::TStatus::TStatus(TSchema* object)
    : Etc_(object, &EtcDescriptor)
{ }

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TSchema, TSchema::TStatus::TEtc>
TSchema::TStatus::EtcDescriptor{
    &SchemasTable.Fields.StatusEtc,
    [] (TSchema* obj) { return &obj->Status().Etc(); }
};

////////////////////////////////////////////////////////////////////////////////

TSchema::TSchema(
    const TString& id,
    IObjectTypeHandler* typeHandler,
    ISession* session)
    : TDataModelObject(typeHandler, session)
    , Id_(id)
    , FinalizationStartTime_(this, &FinalizationStartTimeDescriptor)
    , Finalizers_(this, &FinalizersDescriptor)
    , MetaEtc_(this, &MetaEtcDescriptor)
    , Spec_(this)
    , Status_(this)
{
}

////////////////////////////////////////////////////////////////////////////////

NYT::NOrm::NClient::NObjects::TObjectKey TSchema::GetKey() const
{
    return NYT::NOrm::NServer::NObjects::TObjectKey(
        GetId());
}

void TSchema::ScheduleUuidLoad() const
{
    MetaEtc().ScheduleLoad();
}

NYT::NOrm::NClient::NObjects::TObjectId TSchema::GetUuid(
    std::source_location location) const
{
    return MetaEtc().Load(location).uuid();
}

TString TSchema::GetName(
    std::source_location location) const
{
    return MetaEtc().Load(location).name();
}

////////////////////////////////////////////////////////////////////////////////

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TSemaphore, ui64>
TSemaphore::FinalizationStartTimeDescriptor{
    &SemaphoresTable.Fields.MetaFinalizationStartTime,
    [] (TSemaphore* obj) { return &obj->FinalizationStartTime(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TSemaphore, THashMap<TString, NYT::NOrm::NDataModel::TFinalizer>>
TSemaphore::FinalizersDescriptor{
    &SemaphoresTable.Fields.MetaFinalizers,
    [] (TSemaphore* obj) { return &obj->Finalizers(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TSemaphore, TSemaphore::TMetaEtc>
TSemaphore::MetaEtcDescriptor{
    &SemaphoresTable.Fields.MetaEtc,
    [] (TSemaphore* object) { return &object->MetaEtc(); }
};

////////////////////////////////////////////////////////////////////////////////

TSemaphore::TSpec::TSpec(TSemaphore* object)
    : Etc_(object, &EtcDescriptor)
{ }

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TSemaphore, TSemaphore::TSpec::TEtc>
TSemaphore::TSpec::EtcDescriptor{
    &SemaphoresTable.Fields.Spec,
    [] (TSemaphore* obj) { return &obj->Spec().Etc(); }
};

TSemaphore::TStatus::TStatus(TSemaphore* object)
    : Etc_(object, &EtcDescriptor)
{ }

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TSemaphore, TSemaphore::TStatus::TEtc>
TSemaphore::TStatus::EtcDescriptor{
    &SemaphoresTable.Fields.StatusEtc,
    [] (TSemaphore* obj) { return &obj->Status().Etc(); }
};

////////////////////////////////////////////////////////////////////////////////

TSemaphore::TSemaphore(
    const TString& id,
    const NYT::NOrm::NClient::NObjects::TObjectKey& parentKey,
    IObjectTypeHandler* typeHandler,
    ISession* session)
    : TDataModelObject(typeHandler, session)
    , Id_(id)
    , ParentKeyAttribute_(this, parentKey)
    , SemaphoreSet_(this)
    , FinalizationStartTime_(this, &FinalizationStartTimeDescriptor)
    , Finalizers_(this, &FinalizersDescriptor)
    , MetaEtc_(this, &MetaEtcDescriptor)
    , Spec_(this)
    , Status_(this)
{
}

////////////////////////////////////////////////////////////////////////////////

NYT::NOrm::NClient::NObjects::TObjectKey TSemaphore::GetKey() const
{
    return NYT::NOrm::NServer::NObjects::TObjectKey(
        GetId());
}

NYT::NOrm::NClient::NObjects::TObjectKey TSemaphore::GetParentKey(
    std::source_location location) const
{
    return ParentKeyAttribute_.GetKey(location);
}

NYT::NOrm::NServer::NObjects::TParentKeyAttribute* TSemaphore::GetParentKeyAttribute()
{
    return &ParentKeyAttribute_;
}

TString TSemaphore::SemaphoreSetId(std::source_location location) const
{
    return GetParentKey(location).GetWithDefault<TString>(0);
}

void TSemaphore::ScheduleUuidLoad() const
{
    MetaEtc().ScheduleLoad();
}

NYT::NOrm::NClient::NObjects::TObjectId TSemaphore::GetUuid(
    std::source_location location) const
{
    return MetaEtc().Load(location).uuid();
}

TString TSemaphore::GetName(
    std::source_location location) const
{
    return MetaEtc().Load(location).name();
}

////////////////////////////////////////////////////////////////////////////////

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TSemaphoreSet, ui64>
TSemaphoreSet::FinalizationStartTimeDescriptor{
    &SemaphoreSetsTable.Fields.MetaFinalizationStartTime,
    [] (TSemaphoreSet* obj) { return &obj->FinalizationStartTime(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TSemaphoreSet, THashMap<TString, NYT::NOrm::NDataModel::TFinalizer>>
TSemaphoreSet::FinalizersDescriptor{
    &SemaphoreSetsTable.Fields.MetaFinalizers,
    [] (TSemaphoreSet* obj) { return &obj->Finalizers(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TSemaphoreSet, TSemaphoreSet::TMetaEtc>
TSemaphoreSet::MetaEtcDescriptor{
    &SemaphoreSetsTable.Fields.MetaEtc,
    [] (TSemaphoreSet* object) { return &object->MetaEtc(); }
};

////////////////////////////////////////////////////////////////////////////////

TSemaphoreSet::TSpec::TSpec(TSemaphoreSet* object)
    : Etc_(object, &EtcDescriptor)
{ }

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TSemaphoreSet, TSemaphoreSet::TSpec::TEtc>
TSemaphoreSet::TSpec::EtcDescriptor{
    &SemaphoreSetsTable.Fields.Spec,
    [] (TSemaphoreSet* obj) { return &obj->Spec().Etc(); }
};

TSemaphoreSet::TStatus::TStatus(TSemaphoreSet* object)
    : Etc_(object, &EtcDescriptor)
{ }

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TSemaphoreSet, TSemaphoreSet::TStatus::TEtc>
TSemaphoreSet::TStatus::EtcDescriptor{
    &SemaphoreSetsTable.Fields.Status,
    [] (TSemaphoreSet* obj) { return &obj->Status().Etc(); }
};

////////////////////////////////////////////////////////////////////////////////

TSemaphoreSet::TSemaphoreSet(
    const TString& id,
    IObjectTypeHandler* typeHandler,
    ISession* session)
    : TDataModelObject(typeHandler, session)
    , Id_(id)
    , Semaphores_(this)
    , FinalizationStartTime_(this, &FinalizationStartTimeDescriptor)
    , Finalizers_(this, &FinalizersDescriptor)
    , MetaEtc_(this, &MetaEtcDescriptor)
    , Spec_(this)
    , Status_(this)
{
}

////////////////////////////////////////////////////////////////////////////////

NYT::NOrm::NClient::NObjects::TObjectKey TSemaphoreSet::GetKey() const
{
    return NYT::NOrm::NServer::NObjects::TObjectKey(
        GetId());
}

void TSemaphoreSet::ScheduleUuidLoad() const
{
    MetaEtc().ScheduleLoad();
}

NYT::NOrm::NClient::NObjects::TObjectId TSemaphoreSet::GetUuid(
    std::source_location location) const
{
    return MetaEtc().Load(location).uuid();
}

TString TSemaphoreSet::GetName(
    std::source_location location) const
{
    return MetaEtc().Load(location).name();
}

////////////////////////////////////////////////////////////////////////////////

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TTimestampId, ui64>
TTimestampId::FinalizationStartTimeDescriptor{
    &TimestampIdsTable.Fields.MetaFinalizationStartTime,
    [] (TTimestampId* obj) { return &obj->FinalizationStartTime(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TTimestampId, THashMap<TString, NYT::NOrm::NDataModel::TFinalizer>>
TTimestampId::FinalizersDescriptor{
    &TimestampIdsTable.Fields.MetaFinalizers,
    [] (TTimestampId* obj) { return &obj->Finalizers(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TTimestampId, TTimestampId::TMetaEtc>
TTimestampId::MetaEtcDescriptor{
    &TimestampIdsTable.Fields.MetaEtc,
    [] (TTimestampId* object) { return &object->MetaEtc(); }
};

////////////////////////////////////////////////////////////////////////////////

TTimestampId::TSpec::TSpec(TTimestampId* object)
    : Ui64Value_(object, &Ui64ValueDescriptor)
    , Etc_(object, &EtcDescriptor)
{ }

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TTimestampId, ui64>
TTimestampId::TSpec::Ui64ValueDescriptor{
    &TimestampIdsTable.Fields.SpecUi64Value,
    [] (TTimestampId* obj) { return &obj->Spec().Ui64Value(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TTimestampId, TTimestampId::TSpec::TEtc>
TTimestampId::TSpec::EtcDescriptor{
    &TimestampIdsTable.Fields.SpecEtc,
    [] (TTimestampId* obj) { return &obj->Spec().Etc(); }
};

TTimestampId::TStatus::TStatus(TTimestampId* object)
    : Etc_(object, &EtcDescriptor)
{ }

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TTimestampId, TTimestampId::TStatus::TEtc>
TTimestampId::TStatus::EtcDescriptor{
    &TimestampIdsTable.Fields.StatusEtc,
    [] (TTimestampId* obj) { return &obj->Status().Etc(); }
};

////////////////////////////////////////////////////////////////////////////////

TTimestampId::TTimestampId(
    const i64& i64Id,
    const ui64& ui64Id,
    IObjectTypeHandler* typeHandler,
    ISession* session)
    : TDataModelObject(typeHandler, session)
    , I64Id_(i64Id)
    , Ui64Id_(ui64Id)
    , FinalizationStartTime_(this, &FinalizationStartTimeDescriptor)
    , Finalizers_(this, &FinalizersDescriptor)
    , MetaEtc_(this, &MetaEtcDescriptor)
    , Spec_(this)
    , Status_(this)
{
}

////////////////////////////////////////////////////////////////////////////////

NYT::NOrm::NClient::NObjects::TObjectKey TTimestampId::GetKey() const
{
    return NYT::NOrm::NServer::NObjects::TObjectKey(
        GetI64Id(),
        GetUi64Id());
}

void TTimestampId::ScheduleUuidLoad() const
{
    MetaEtc().ScheduleLoad();
}

NYT::NOrm::NClient::NObjects::TObjectId TTimestampId::GetUuid(
    std::source_location location) const
{
    return MetaEtc().Load(location).uuid();
}

TString TTimestampId::GetName(
    std::source_location location) const
{
    return MetaEtc().Load(location).name();
}

////////////////////////////////////////////////////////////////////////////////

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TTypographer, TString>
TTypographer::LoginDescriptor{
    &TypographersTable.Fields.MetaLogin,
    [] (TTypographer* obj) { return &obj->Login(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TTypographer, ui64>
TTypographer::FinalizationStartTimeDescriptor{
    &TypographersTable.Fields.MetaFinalizationStartTime,
    [] (TTypographer* obj) { return &obj->FinalizationStartTime(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TTypographer, THashMap<TString, NYT::NOrm::NDataModel::TFinalizer>>
TTypographer::FinalizersDescriptor{
    &TypographersTable.Fields.MetaFinalizers,
    [] (TTypographer* obj) { return &obj->Finalizers(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TTypographer, TTypographer::TMetaEtc>
TTypographer::MetaEtcDescriptor{
    &TypographersTable.Fields.MetaEtc,
    [] (TTypographer* object) { return &object->MetaEtc(); }
};

////////////////////////////////////////////////////////////////////////////////

TTypographer::TSpec::TSpec(TTypographer* object)
    : TestMandatoryColumnField_(object, &TestMandatoryColumnFieldDescriptor)
    , Etc_(object, &EtcDescriptor)
{ }

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TTypographer, TString>
TTypographer::TSpec::TestMandatoryColumnFieldDescriptor{
    &TypographersTable.Fields.SpecTestMandatoryColumnField,
    [] (TTypographer* obj) { return &obj->Spec().TestMandatoryColumnField(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TTypographer, TTypographer::TSpec::TEtc>
TTypographer::TSpec::EtcDescriptor{
    &TypographersTable.Fields.SpecEtc,
    [] (TTypographer* obj) { return &obj->Spec().Etc(); }
};

TTypographer::TStatus::TStatus(TTypographer* object)
    : Etc_(object, &EtcDescriptor)
{ }

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TTypographer, TTypographer::TStatus::TEtc>
TTypographer::TStatus::EtcDescriptor{
    &TypographersTable.Fields.StatusEtc,
    [] (TTypographer* obj) { return &obj->Status().Etc(); }
};

////////////////////////////////////////////////////////////////////////////////

TTypographer::TTypographer(
    const i64& id,
    const NYT::NOrm::NClient::NObjects::TObjectKey& parentKey,
    IObjectTypeHandler* typeHandler,
    ISession* session)
    : TDataModelObject(typeHandler, session)
    , Id_(id)
    , ParentKeyAttribute_(this, parentKey)
    , Publisher_(this)
    , Login_(this, &LoginDescriptor)
    , FinalizationStartTime_(this, &FinalizationStartTimeDescriptor)
    , Finalizers_(this, &FinalizersDescriptor)
    , MetaEtc_(this, &MetaEtcDescriptor)
    , Spec_(this)
    , Status_(this)
{
    RegisterScalarAttributeIndex(
        "typographers_by_login",
        std::make_unique<NYT::NOrm::NServer::NObjects::TScalarAttributeIndex>(
            typeHandler->GetIndexDescriptorOrThrow("typographers_by_login"),
            this,
            std::vector<NYT::NOrm::NServer::NObjects::TIndexAttribute>{
                NYT::NOrm::NServer::NObjects::TIndexAttribute(
                    &Login(),
                    "",
                    /*repeated*/ false),
            },
            std::vector<NYT::NOrm::NServer::NObjects::TIndexAttribute>{
            },
            /*unique*/ false));
}

////////////////////////////////////////////////////////////////////////////////

NYT::NOrm::NClient::NObjects::TObjectKey TTypographer::GetKey() const
{
    return NYT::NOrm::NServer::NObjects::TObjectKey(
        GetId());
}

NYT::NOrm::NClient::NObjects::TObjectKey TTypographer::GetParentKey(
    std::source_location location) const
{
    return ParentKeyAttribute_.GetKey(location);
}

NYT::NOrm::NServer::NObjects::TParentKeyAttribute* TTypographer::GetParentKeyAttribute()
{
    return &ParentKeyAttribute_;
}

i64 TTypographer::PublisherId(std::source_location location) const
{
    return GetParentKey(location).GetWithDefault<i64>(0);
}

void TTypographer::ScheduleUuidLoad() const
{
    MetaEtc().ScheduleLoad();
}

NYT::NOrm::NClient::NObjects::TObjectId TTypographer::GetUuid(
    std::source_location location) const
{
    return MetaEtc().Load(location).uuid();
}

TString TTypographer::GetName(
    std::source_location location) const
{
    return MetaEtc().Load(location).name();
}

////////////////////////////////////////////////////////////////////////////////

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TUser, ui64>
TUser::FinalizationStartTimeDescriptor{
    &UsersTable.Fields.MetaFinalizationStartTime,
    [] (TUser* obj) { return &obj->FinalizationStartTime(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TUser, THashMap<TString, NYT::NOrm::NDataModel::TFinalizer>>
TUser::FinalizersDescriptor{
    &UsersTable.Fields.MetaFinalizers,
    [] (TUser* obj) { return &obj->Finalizers(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TUser, TUser::TMetaEtc>
TUser::MetaEtcDescriptor{
    &UsersTable.Fields.MetaEtc,
    [] (TUser* object) { return &object->MetaEtc(); }
};

////////////////////////////////////////////////////////////////////////////////

TUser::TSpec::TSpec(TUser* object)
    : Etc_(object, &EtcDescriptor)
{ }

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TUser, TUser::TSpec::TEtc>
TUser::TSpec::EtcDescriptor{
    &UsersTable.Fields.Spec,
    [] (TUser* obj) { return &obj->Spec().Etc(); }
};

TUser::TStatus::TStatus(TUser* object)
    : Etc_(object, &EtcDescriptor)
{ }

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TUser, TUser::TStatus::TEtc>
TUser::TStatus::EtcDescriptor{
    &UsersTable.Fields.StatusEtc,
    [] (TUser* obj) { return &obj->Status().Etc(); }
};

////////////////////////////////////////////////////////////////////////////////

TUser::TUser(
    const TString& id,
    IObjectTypeHandler* typeHandler,
    ISession* session)
    : TDataModelObject(typeHandler, session)
    , Id_(id)
    , FinalizationStartTime_(this, &FinalizationStartTimeDescriptor)
    , Finalizers_(this, &FinalizersDescriptor)
    , MetaEtc_(this, &MetaEtcDescriptor)
    , Spec_(this)
    , Status_(this)
{
}

////////////////////////////////////////////////////////////////////////////////

NYT::NOrm::NClient::NObjects::TObjectKey TUser::GetKey() const
{
    return NYT::NOrm::NServer::NObjects::TObjectKey(
        GetId());
}

void TUser::ScheduleUuidLoad() const
{
    MetaEtc().ScheduleLoad();
}

NYT::NOrm::NClient::NObjects::TObjectId TUser::GetUuid(
    std::source_location location) const
{
    return MetaEtc().Load(location).uuid();
}

TString TUser::GetName(
    std::source_location location) const
{
    return MetaEtc().Load(location).name();
}

////////////////////////////////////////////////////////////////////////////////

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TWatchLogConsumer, ui64>
TWatchLogConsumer::FinalizationStartTimeDescriptor{
    &WatchLogConsumersTable.Fields.MetaFinalizationStartTime,
    [] (TWatchLogConsumer* obj) { return &obj->FinalizationStartTime(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TWatchLogConsumer, THashMap<TString, NYT::NOrm::NDataModel::TFinalizer>>
TWatchLogConsumer::FinalizersDescriptor{
    &WatchLogConsumersTable.Fields.MetaFinalizers,
    [] (TWatchLogConsumer* obj) { return &obj->Finalizers(); }
};

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TWatchLogConsumer, TWatchLogConsumer::TMetaEtc>
TWatchLogConsumer::MetaEtcDescriptor{
    &WatchLogConsumersTable.Fields.MetaEtc,
    [] (TWatchLogConsumer* object) { return &object->MetaEtc(); }
};

////////////////////////////////////////////////////////////////////////////////

TWatchLogConsumer::TSpec::TSpec(TWatchLogConsumer* object)
    : Etc_(object, &EtcDescriptor)
{ }

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TWatchLogConsumer, TWatchLogConsumer::TSpec::TEtc>
TWatchLogConsumer::TSpec::EtcDescriptor{
    &WatchLogConsumersTable.Fields.Spec,
    [] (TWatchLogConsumer* obj) { return &obj->Spec().Etc(); }
};

TWatchLogConsumer::TStatus::TStatus(TWatchLogConsumer* object)
    : Etc_(object, &EtcDescriptor)
{ }

const NYT::NOrm::NServer::NObjects::TScalarAttributeDescriptor<TWatchLogConsumer, TWatchLogConsumer::TStatus::TEtc>
TWatchLogConsumer::TStatus::EtcDescriptor{
    &WatchLogConsumersTable.Fields.Status,
    [] (TWatchLogConsumer* obj) { return &obj->Status().Etc(); }
};

////////////////////////////////////////////////////////////////////////////////

TWatchLogConsumer::TWatchLogConsumer(
    const TString& id,
    IObjectTypeHandler* typeHandler,
    ISession* session)
    : TDataModelObject(typeHandler, session)
    , Id_(id)
    , FinalizationStartTime_(this, &FinalizationStartTimeDescriptor)
    , Finalizers_(this, &FinalizersDescriptor)
    , MetaEtc_(this, &MetaEtcDescriptor)
    , Spec_(this)
    , Status_(this)
{
}

////////////////////////////////////////////////////////////////////////////////

NYT::NOrm::NClient::NObjects::TObjectKey TWatchLogConsumer::GetKey() const
{
    return NYT::NOrm::NServer::NObjects::TObjectKey(
        GetId());
}

void TWatchLogConsumer::ScheduleUuidLoad() const
{
    MetaEtc().ScheduleLoad();
}

NYT::NOrm::NClient::NObjects::TObjectId TWatchLogConsumer::GetUuid(
    std::source_location location) const
{
    return MetaEtc().Load(location).uuid();
}

TString TWatchLogConsumer::GetName(
    std::source_location location) const
{
    return MetaEtc().Load(location).name();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NExample::NServer::NLibrary

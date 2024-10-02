// AUTOMATICALLY GENERATED. DO NOT EDIT!

#include "type_handler_impls.h"

#include <yt/yt/orm/server/objects/attribute_schema.h>
#include <yt/yt/orm/server/objects/build_tags.h>
#include <yt/yt/orm/server/objects/watch_log.h>
#include <yt/yt/orm/example/plugins/server/library/mother_ship.h>


#include <yt/yt/orm/client/objects/registry.h>

namespace NYT::NOrm::NExample::NServer::NLibrary {

////////////////////////////////////////////////////////////////////////////////

using NYT::NOrm::NServer::NMaster::IBootstrap;
using NYT::NOrm::NServer::NObjects::IObjectTypeHandler;
using NYT::NOrm::NServer::NObjects::ISession;
using NYT::NOrm::NServer::NObjects::TTagSet;
using NYT::NOrm::NServer::NObjects::TObjectFilter;
using NYT::NOrm::NServer::NObjects::ESetUpdateObjectMode;

using TDBFields = std::vector<const NYT::NOrm::NServer::NObjects::TDBField*>;

TMotherShipTypeHandler::TMotherShipTypeHandler(
    IBootstrap* bootstrap,
    NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config)
    : TBase(
        bootstrap,
        static_cast<NYT::NOrm::NClient::NObjects::TObjectTypeValue>(EObjectType::MotherShip))
    , Config_(std::move(config))
    , KeyFields_({
        &MotherShipsTable.Fields.MetaId})
    , ParentKeyFields_({
        &MotherShipsTable.Fields.MetaNexusId})
    , NullKey_(NYT::NOrm::NServer::NObjects::GenerateZeroKey(KeyFields_))
{ }

TMotherShipTypeHandler::~TMotherShipTypeHandler()
{ }

void TMotherShipTypeHandler::Initialize()
{
    TBase::Initialize();

    MetaAttributeSchema_
        ->AddChildren({
        MakeScalarAttributeSchema("acl", TDataModelObject::AclDescriptor)
            ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::Updatable)
            ->AddValidator<NYT::NOrm::NServer::NObjects::TObject>(std::bind_front(
                &TMotherShipTypeHandler::ValidateAcl, this))
    });
    MetaAttributeSchema_
        ->AddChildren({
            MakeParentIdAttributeSchema(
                "nexus_id",
                &MotherShipsTable.Fields.MetaNexusId,
                MakeValueGetter(&TMotherShip::NexusId)),
            MakeIdAttributeSchema(
                "id",
                &MotherShipsTable.Fields.MetaId,
                MakeValueGetter(&TMotherShip::GetId))
                ->SetKeyFieldPolicy(
                    NYT::NOrm::NServer::NObjects::CreateIntegerAttributePolicy<i64>(
                        NYT::NOrm::NServer::NObjects::EAttributeGenerationPolicy::Random,
                        GetBootstrap(),
                        NYT::NOrm::NServer::NObjects::DefaultMinIntegerAttributeValue<i64>,
                        NYT::NOrm::NServer::NObjects::DefaultMaxIntegerAttributeValue<i64>,
                        NYT::NOrm::NServer::NObjects::TAttributePolicyOptions{
                            .Type = GetType(),
                            .AttributePath = "/meta/id",
                            .IndexForIncrement = ""
                        })),
            MakeScalarAttributeSchema("revision", TMotherShip::RevisionDescriptor)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueReadOnly),
            MakeScalarAttributeSchema("release_year", TMotherShip::ReleaseYearDescriptor),
            MakeScalarAttributeSchema("finalization_start_time", TMotherShip::FinalizationStartTimeDescriptor)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueReadOnly),
            MakeScalarAttributeSchema("finalizers", TMotherShip::FinalizersDescriptor)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueReadOnly),
            MakeScalarAttributeSchema("ultimate_question_of_life")
                ->SetComputed()
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::ReadOnly)
                ->SetOpaque(/*emitEntity*/ false),
            TBase::MakeEtcAttributeSchema(TMotherShip::MetaEtcDescriptor.AddValidator(
                std::bind_front(&TMotherShipTypeHandler::ValidateMetaEtcOnValueUpdate, this)))
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::Updatable)
                ->template AddValidator<TMotherShip>(std::bind_front(
                    &TMotherShipTypeHandler::ValidateMetaEtcOnTransactionCommit, this))
            });

    SpecAttributeSchema_
        ->AddChildren({
            MakeScalarAttributeSchema("executor_id", TMotherShip::TSpec::ExecutorIdDescriptor),
            MakeScalarAttributeSchema("revision", TMotherShip::TSpec::RevisionDescriptor)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueReadOnly),
            MakeScalarAttributeSchema("sector_names", TMotherShip::TSpec::SectorNamesDescriptor),
            MakeScalarAttributeSchema("price", TMotherShip::TSpec::PriceDescriptor),
            MakeEtcAttributeSchema(TMotherShip::TSpec::EtcDescriptor),
        });

    StatusAttributeSchema_
        ->AddChildren({
            MakeEtcAttributeSchema(TMotherShip::TStatus::EtcDescriptor),
        });
    PrepareRevisionTracker(
        /*trackedPaths*/ {
            "/meta",
        },
        /*trackerPath*/ "/meta/revision",
        /*lockGroupRestrictionEnabled*/ false,
        /*excludedPaths*/ {
            "/meta/release_year",
        });
    PrepareRevisionTracker(
        /*trackedPaths*/ {
            "/spec",
        },
        /*trackerPath*/ "/spec/revision",
        /*lockGroupRestrictionEnabled*/ false,
        /*excludedPaths*/ {
            "/spec/capacity",
            "/spec/sector_names",
        });

    ControlAttributeSchema_
        ->AddChildren({
            MakeScalarAttributeSchema("touch")
                ->SetControl<TMotherShip, NYT::NOrm::NExample::NClient::NProto::NDataModel::TObjectTouch>(
                    [] (NYT::NOrm::NServer::NObjects::TTransaction* transaction,
                        TMotherShip* object,
                        const auto& touch)
                    {
                        if (touch.lock_removal()) {
                            auto* session = transaction->GetSession();
                            session->ScheduleStore(
                                [object] (NYT::NOrm::NServer::NObjects::IStoreContext* context) {
                                    auto* typeHandler = object->GetTypeHandler();
                                    // Lock is taken to prevent success of concurrent RemoveObject query.
                                    // Without it, no write set would be added and
                                    // `RemoveObject` may be committed before `/control/touch` update.
                                    context->LockRow(
                                        typeHandler->GetTable(),
                                        typeHandler->GetObjectTableKey(object),
                                        std::array{&NYT::NOrm::NServer::NObjects::ObjectsTable.Fields.ExistenceLock},
                                        NYT::NTableClient::ELockType::SharedStrong);
                                });
                        }
                        if (touch.store_event_to_history()) {
                            THROW_ERROR_EXCEPTION("History is disabled for %v",
                                object->GetDisplayName());
                            object->ControlTouchEventsToSave().insert("touch");
                        }
                    }),
            MakeScalarAttributeSchema("touch_revision")
                ->SetControlWithTransactionCallContext<TMotherShip, NYT::NOrm::NExample::NClient::NProto::NDataModel::TObjectTouchRevision>(
                    [&] (NYT::NOrm::NServer::NObjects::TTransaction* transaction,
                        TMotherShip* object,
                        const NYT::NOrm::NExample::NClient::NProto::NDataModel::TObjectTouchRevision& touchRevision,
                        const NYT::NOrm::NServer::NObjects::TTransactionCallContext& transactionCallContext)
                    {
                        if (touchRevision.has_path() && !touchRevision.path().empty()) {
                            if (!RevisionTrackerOptionsByPath_.contains(touchRevision.path())) {
                                THROW_ERROR_EXCEPTION("Error running /control/touch_revision: %Qv is not a valid revision path",
                                    touchRevision.path());
                            }
                            auto resolveResult = NYT::NOrm::NServer::NObjects::ResolveAttribute(
                                object->GetTypeHandler(),
                                touchRevision.path(),
                                /*callback*/ {},
                                /*validateProtoSchemaCompliance*/ false);
                            auto* schema = resolveResult.Attribute->AsScalar();
                            THROW_ERROR_EXCEPTION_UNLESS(schema->HasValueSetter(),
                                "Error running /control/touch_revision: Resolved schema does not have a value setter")
                            schema->RunValueSetter(
                                transaction,
                                object,
                                /*path*/ "",
                                NYT::NYTree::ConvertToNode(transaction->GetStartTimestamp()),
                                /*recursive*/ false,
                                /*sharedWrite*/ std::nullopt,
                                /*aggregateMode*/ NYT::NOrm::NServer::NObjects::EAggregateMode::Unspecified,
                                transactionCallContext);
                        } else {
                            THROW_ERROR_EXCEPTION("Error running /control/touch_revision: empty path is given");
                        }
                    }),
        });
    AddAttributeSensor("/spec/templar_count", NYT::NOrm::NClient::NProto::EAttributeSensorPolicy(1));
    RegisterChildrenAttribute(
        static_cast<NYT::NOrm::NClient::NObjects::TObjectTypeValue>(EObjectType::Interceptor),
        [] (const NYT::NOrm::NServer::NObjects::TObject* object) {
            return &object->As<TMotherShip>()->Interceptors();
        });
}

void TMotherShipTypeHandler::PostInitialize()
{
    // History is configured in PostInitialize stage, as users may add some evaluated fields via plugins in Initialize stage.
    // NB: Called before TBase::PostInitialize as base type handler prepares cache.
    ConfigureHistory(
        /*attributes*/ {},
        /*filter*/ std::nullopt);

    TBase::PostInitialize();

    FillUpdatePolicies();
}

bool TMotherShipTypeHandler::IsObjectNameSupported() const
{
    return false;
}

bool TMotherShipTypeHandler::SkipStoreWithoutChanges() const
{
    return true;
}

bool TMotherShipTypeHandler::ForceZeroKeyEvaluation() const
{
    return false;
}

std::vector<NYT::NOrm::NServer::NObjects::TWatchLog>
TMotherShipTypeHandler::GetWatchLogs() const
{
    return std::vector<NYT::NOrm::NServer::NObjects::TWatchLog>{
        NYT::NOrm::NServer::NObjects::TWatchLog{
            .ObjectName = "mother_ship",
            .Name = "watch_log",
            .Filter{
                .Query{
                    ""
                }
            },
            .Selector{
            },
        },
    };
}

const NYT::NYson::TProtobufMessageType*
TMotherShipTypeHandler::GetRootProtobufType() const
{
    static const auto* type = NYT::NYson::ReflectProtobufMessageType<NYT::NOrm::NExample::NClient::NProto::NDataModel::TMotherShip>();
    return type;
}

const TDBFields& TMotherShipTypeHandler::GetKeyFields() const
{
    return KeyFields_;
}

NYT::NOrm::NClient::NObjects::TObjectKey TMotherShipTypeHandler::GetNullKey() const
{
    return NullKey_;
}

const NYT::NOrm::NServer::NObjects::TDBTable* TMotherShipTypeHandler::GetTable() const
{
    return &MotherShipsTable;
}

const NYT::NOrm::NServer::NObjects::TDBTable* TMotherShipTypeHandler::GetParentsTable() const
{
    return &MotherShipsToNexusTable;
}

NYT::NOrm::NClient::NObjects::TObjectTypeValue
TMotherShipTypeHandler::GetParentType() const
{
    return static_cast<NYT::NOrm::NClient::NObjects::TObjectTypeValue>(EObjectType::Nexus);
}

const TDBFields& TMotherShipTypeHandler::GetParentKeyFields() const
{
    return ParentKeyFields_;
}

NYT::NOrm::NServer::NObjects::TObject*
TMotherShipTypeHandler::GetParent(
    const NYT::NOrm::NServer::NObjects::TObject* object,
    std::source_location location)
{
    return object->As<TMotherShip>()
        ->Nexus().Load(location);
}

std::unique_ptr<NYT::NOrm::NServer::NObjects::TObject>
TMotherShipTypeHandler::InstantiateObject(
    const NYT::NOrm::NClient::NObjects::TObjectKey& key,
    const NYT::NOrm::NClient::NObjects::TObjectKey& parentKey,
    ISession* session)
{
    return std::make_unique<NYT::NOrm::NExample::NServer::NPlugins::TMotherShip>(
        std::get<i64>(key[0]),
        parentKey,
        this,
        session);
}

void TMotherShipTypeHandler::InitializeCreatedObject(
    NYT::NOrm::NServer::NObjects::TTransaction* transaction,
    NYT::NOrm::NServer::NObjects::TObject* object)
{
    TBase::InitializeCreatedObject(transaction, object);
    auto* typedObject = object->As<TMotherShip>();
    YT_VERIFY(typedObject);
    typedObject->MetaEtc().MutableLoad()->set_uuid(NYT::NOrm::NClient::NObjects::GenerateUuid());
}

void TMotherShipTypeHandler::ValidateCreatedObject(
    NYT::NOrm::NServer::NObjects::TTransaction* transaction,
    NYT::NOrm::NServer::NObjects::TObject* object)
{
    TBase::ValidateCreatedObject(transaction, object);
    auto* typedObject = object->As<TMotherShip>();
    YT_VERIFY(typedObject);
    ResolveAttribute("/meta/revision", /*callback*/ {}, /*validateProtoSchemaCompliance*/ false).Attribute
        ->AsScalar()
        ->GetAttribute<NYT::NOrm::NServer::NObjects::TScalarAttribute<ui64>>(object)
        ->Store(transaction->GetStartTimestamp());
    ResolveAttribute("/spec/revision", /*callback*/ {}, /*validateProtoSchemaCompliance*/ false).Attribute
        ->AsScalar()
        ->GetAttribute<NYT::NOrm::NServer::NObjects::TScalarAttribute<ui64>>(object)
        ->Store(transaction->GetStartTimestamp());
}

void TMotherShipTypeHandler::ValidateMetaEtcOnTransactionCommit(
    NYT::NOrm::NServer::NObjects::TTransaction*,
    const TMotherShip* object)
{
    const auto& metaEtcOld = object->MetaEtc().LoadOld();
    const auto& metaEtcNew = object->MetaEtc().Load();

    // Sanity check.
    ValidateMetaUuid(metaEtcOld, metaEtcNew);

    if (metaEtcNew.name() && !this->IsObjectNameSupported()) {
        THROW_ERROR_EXCEPTION("Cannot set /meta/name for %v objects",
            NYT::NOrm::NClient::NObjects::GetGlobalObjectTypeRegistry()
                ->GetHumanReadableTypeNameOrCrash(this->GetType()));
    }
}

void TMotherShipTypeHandler::ValidateMetaEtcOnValueUpdate(
    NYT::NOrm::NServer::NObjects::TTransaction*,
    const TMotherShip* object,
    const TMotherShip::TMetaEtc& metaEtcOld,
    const TMotherShip::TMetaEtc& metaEtcNew)
{
    ValidateMetaUuid(metaEtcOld, metaEtcNew);

    // All user-defined attributes are considered updatable by any user
    // having corresponding write permission.
    Y_UNUSED(object);
}

void TMotherShipTypeHandler::ValidateMetaUuid(
    const TMotherShip::TMetaEtc& metaEtcOld,
    const TMotherShip::TMetaEtc& metaEtcNew)
{
    // Uuid protects, for example, YP secrets from object identity theft, i.e. from ABA race conditions,
    // so we try hard to make sure uuid doesn't change.
    if (!metaEtcOld.uuid().empty() && metaEtcOld.uuid() != metaEtcNew.uuid()) {
        this->GetBootstrap()->GetAccessControlManager()->ValidateSuperuser("change /meta/uuid");
    }
}

////////////////////////////////////////////////////////////////////////////////

TMultipolicyIdTypeHandler::TMultipolicyIdTypeHandler(
    IBootstrap* bootstrap,
    NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config)
    : TBase(
        bootstrap,
        static_cast<NYT::NOrm::NClient::NObjects::TObjectTypeValue>(EObjectType::MultipolicyId))
    , Config_(std::move(config))
    , KeyFields_({
        &MultipolicyIdsTable.Fields.MetaStrId,
        &MultipolicyIdsTable.Fields.MetaI64Id,
        &MultipolicyIdsTable.Fields.MetaUi64Id,
        &MultipolicyIdsTable.Fields.MetaAnotherUi64Id})
    , NullKey_(NYT::NOrm::NServer::NObjects::GenerateZeroKey(KeyFields_))
{ }

void TMultipolicyIdTypeHandler::Initialize()
{
    TBase::Initialize();

    MetaAttributeSchema_
        ->AddChildren({
        MakeScalarAttributeSchema("acl", TDataModelObject::AclDescriptor)
            ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::Updatable)
            ->AddValidator<NYT::NOrm::NServer::NObjects::TObject>(std::bind_front(
                &TMultipolicyIdTypeHandler::ValidateAcl, this))
    });
    MetaAttributeSchema_
        ->AddChildren({
            MakeIdAttributeSchema(
                "str_id",
                &MultipolicyIdsTable.Fields.MetaStrId,
                MakeValueGetter(&TMultipolicyId::GetStrId))
                ->SetKeyFieldPolicy(
                    NYT::NOrm::NServer::NObjects::CreateStringAttributePolicy(
                        NYT::NOrm::NServer::NObjects::EAttributeGenerationPolicy::Manual,
                        NYT::NOrm::NServer::NObjects::DefaultMinStringAttributeLength,
                        NYT::NOrm::NServer::NObjects::DefaultMaxStringAttributeLength,
                        NYT::NOrm::NServer::NObjects::DefaultStringAttributeValidChars)),
            MakeIdAttributeSchema(
                "i64_id",
                &MultipolicyIdsTable.Fields.MetaI64Id,
                MakeValueGetter(&TMultipolicyId::GetI64Id))
                ->SetKeyFieldPolicy(
                    NYT::NOrm::NServer::NObjects::CreateIntegerAttributePolicy<i64>(
                        NYT::NOrm::NServer::NObjects::EAttributeGenerationPolicy::Random,
                        GetBootstrap(),
                        NYT::NOrm::NServer::NObjects::DefaultMinIntegerAttributeValue<i64>,
                        NYT::NOrm::NServer::NObjects::DefaultMaxIntegerAttributeValue<i64>,
                        NYT::NOrm::NServer::NObjects::TAttributePolicyOptions{
                            .Type = GetType(),
                            .AttributePath = "/meta/i64_id",
                            .IndexForIncrement = ""
                        })),
            MakeIdAttributeSchema(
                "ui64_id",
                &MultipolicyIdsTable.Fields.MetaUi64Id,
                MakeValueGetter(&TMultipolicyId::GetUi64Id))
                ->SetKeyFieldPolicy(
                    NYT::NOrm::NServer::NObjects::CreateIntegerAttributePolicy<ui64>(
                        NYT::NOrm::NServer::NObjects::EAttributeGenerationPolicy::Timestamp,
                        GetBootstrap(),
                        NYT::NOrm::NServer::NObjects::DefaultMinIntegerAttributeValue<ui64>,
                        NYT::NOrm::NServer::NObjects::DefaultMaxIntegerAttributeValue<ui64>,
                        NYT::NOrm::NServer::NObjects::TAttributePolicyOptions{
                            .Type = GetType(),
                            .AttributePath = "/meta/ui64_id",
                            .IndexForIncrement = ""
                        })),
            MakeIdAttributeSchema(
                "another_ui64_id",
                &MultipolicyIdsTable.Fields.MetaAnotherUi64Id,
                MakeValueGetter(&TMultipolicyId::GetAnotherUi64Id))
                ->SetKeyFieldPolicy(
                    NYT::NOrm::NServer::NObjects::CreateIntegerAttributePolicy<ui64>(
                        NYT::NOrm::NServer::NObjects::EAttributeGenerationPolicy::BufferedTimestamp,
                        GetBootstrap(),
                        NYT::NOrm::NServer::NObjects::DefaultMinIntegerAttributeValue<ui64>,
                        NYT::NOrm::NServer::NObjects::DefaultMaxIntegerAttributeValue<ui64>,
                        NYT::NOrm::NServer::NObjects::TAttributePolicyOptions{
                            .Type = GetType(),
                            .AttributePath = "/meta/another_ui64_id",
                            .IndexForIncrement = ""
                        })),
            MakeScalarAttributeSchema("finalization_start_time", TMultipolicyId::FinalizationStartTimeDescriptor)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueReadOnly),
            MakeScalarAttributeSchema("finalizers", TMultipolicyId::FinalizersDescriptor)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueReadOnly),
            MakeScalarAttributeSchema("ultimate_question_of_life")
                ->SetComputed()
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::ReadOnly)
                ->SetOpaque(/*emitEntity*/ false),
            TBase::MakeEtcAttributeSchema(TMultipolicyId::MetaEtcDescriptor.AddValidator(
                std::bind_front(&TMultipolicyIdTypeHandler::ValidateMetaEtcOnValueUpdate, this)))
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::Updatable)
                ->template AddValidator<TMultipolicyId>(std::bind_front(
                    &TMultipolicyIdTypeHandler::ValidateMetaEtcOnTransactionCommit, this))
            });

    SpecAttributeSchema_
        ->AddChildren({
            MakeScalarAttributeSchema("str_value", TMultipolicyId::TSpec::StrValueDescriptor)
                ->SetPolicy(TMultipolicyId::TSpec::StrValueDescriptor,
                            NYT::NOrm::NServer::NObjects::CreateStringAttributePolicy(
                                NYT::NOrm::NServer::NObjects::EAttributeGenerationPolicy::Manual,
                                NYT::NOrm::NServer::NObjects::DefaultMinStringAttributeLength,
                                NYT::NOrm::NServer::NObjects::DefaultMaxStringAttributeLength,
                                NYT::NOrm::NServer::NObjects::DefaultStringAttributeValidChars)),
            MakeScalarAttributeSchema("i64_value", TMultipolicyId::TSpec::I64ValueDescriptor)
                ->SetPolicy(TMultipolicyId::TSpec::I64ValueDescriptor,
                            NYT::NOrm::NServer::NObjects::CreateIntegerAttributePolicy<i64>(
                                NYT::NOrm::NServer::NObjects::EAttributeGenerationPolicy::Random,
                                GetBootstrap(),
                                NYT::NOrm::NServer::NObjects::DefaultMinIntegerAttributeValue<i64>,
                                NYT::NOrm::NServer::NObjects::DefaultMaxIntegerAttributeValue<i64>,
                                NYT::NOrm::NServer::NObjects::TAttributePolicyOptions{
                                    .Type = GetType(),
                                    .AttributePath = "/spec/i64_value",
                                    .IndexForIncrement = ""
                                })),
            MakeScalarAttributeSchema("ui64_value", TMultipolicyId::TSpec::Ui64ValueDescriptor)
                ->SetPolicy(TMultipolicyId::TSpec::Ui64ValueDescriptor,
                            NYT::NOrm::NServer::NObjects::CreateIntegerAttributePolicy<ui64>(
                                NYT::NOrm::NServer::NObjects::EAttributeGenerationPolicy::Timestamp,
                                GetBootstrap(),
                                NYT::NOrm::NServer::NObjects::DefaultMinIntegerAttributeValue<ui64>,
                                NYT::NOrm::NServer::NObjects::DefaultMaxIntegerAttributeValue<ui64>,
                                NYT::NOrm::NServer::NObjects::TAttributePolicyOptions{
                                    .Type = GetType(),
                                    .AttributePath = "/spec/ui64_value",
                                    .IndexForIncrement = ""
                                })),
            MakeScalarAttributeSchema("another_ui64_value", TMultipolicyId::TSpec::AnotherUi64ValueDescriptor)
                ->SetPolicy(TMultipolicyId::TSpec::AnotherUi64ValueDescriptor,
                            NYT::NOrm::NServer::NObjects::CreateIntegerAttributePolicy<ui64>(
                                NYT::NOrm::NServer::NObjects::EAttributeGenerationPolicy::BufferedTimestamp,
                                GetBootstrap(),
                                NYT::NOrm::NServer::NObjects::DefaultMinIntegerAttributeValue<ui64>,
                                NYT::NOrm::NServer::NObjects::DefaultMaxIntegerAttributeValue<ui64>,
                                NYT::NOrm::NServer::NObjects::TAttributePolicyOptions{
                                    .Type = GetType(),
                                    .AttributePath = "/spec/another_ui64_value",
                                    .IndexForIncrement = ""
                                })),
            MakeEtcAttributeSchema(TMultipolicyId::TSpec::EtcDescriptor),
        });

    StatusAttributeSchema_
        ->AddChildren({
            MakeEtcAttributeSchema(TMultipolicyId::TStatus::EtcDescriptor),
        });

    ControlAttributeSchema_
        ->AddChildren({
            MakeScalarAttributeSchema("touch")
                ->SetControl<TMultipolicyId, NYT::NOrm::NExample::NClient::NProto::NDataModel::TObjectTouch>(
                    [] (NYT::NOrm::NServer::NObjects::TTransaction* transaction,
                        TMultipolicyId* object,
                        const auto& touch)
                    {
                        if (touch.lock_removal()) {
                            auto* session = transaction->GetSession();
                            session->ScheduleStore(
                                [object] (NYT::NOrm::NServer::NObjects::IStoreContext* context) {
                                    auto* typeHandler = object->GetTypeHandler();
                                    // Lock is taken to prevent success of concurrent RemoveObject query.
                                    // Without it, no write set would be added and
                                    // `RemoveObject` may be committed before `/control/touch` update.
                                    context->LockRow(
                                        typeHandler->GetTable(),
                                        typeHandler->GetObjectTableKey(object),
                                        std::array{&NYT::NOrm::NServer::NObjects::ObjectsTable.Fields.ExistenceLock},
                                        NYT::NTableClient::ELockType::SharedStrong);
                                });
                        }
                        if (touch.store_event_to_history()) {
                            THROW_ERROR_EXCEPTION("History is disabled for %v",
                                object->GetDisplayName());
                            object->ControlTouchEventsToSave().insert("touch");
                        }
                    }),
        });
}

void TMultipolicyIdTypeHandler::PostInitialize()
{
    // History is configured in PostInitialize stage, as users may add some evaluated fields via plugins in Initialize stage.
    // NB: Called before TBase::PostInitialize as base type handler prepares cache.
    ConfigureHistory(
        /*attributes*/ {},
        /*filter*/ std::nullopt);

    TBase::PostInitialize();

    FillUpdatePolicies();
}

bool TMultipolicyIdTypeHandler::IsObjectNameSupported() const
{
    return false;
}

bool TMultipolicyIdTypeHandler::SkipStoreWithoutChanges() const
{
    return false;
}

bool TMultipolicyIdTypeHandler::ForceZeroKeyEvaluation() const
{
    return false;
}

std::vector<NYT::NOrm::NServer::NObjects::TWatchLog>
TMultipolicyIdTypeHandler::GetWatchLogs() const
{
    return std::vector<NYT::NOrm::NServer::NObjects::TWatchLog>{
    };
}

const NYT::NYson::TProtobufMessageType*
TMultipolicyIdTypeHandler::GetRootProtobufType() const
{
    static const auto* type = NYT::NYson::ReflectProtobufMessageType<NYT::NOrm::NExample::NClient::NProto::NDataModel::TMultipolicyId>();
    return type;
}

const TDBFields& TMultipolicyIdTypeHandler::GetKeyFields() const
{
    return KeyFields_;
}

NYT::NOrm::NClient::NObjects::TObjectKey TMultipolicyIdTypeHandler::GetNullKey() const
{
    return NullKey_;
}

const NYT::NOrm::NServer::NObjects::TDBTable* TMultipolicyIdTypeHandler::GetTable() const
{
    return &MultipolicyIdsTable;
}

const NYT::NOrm::NServer::NObjects::TDBTable* TMultipolicyIdTypeHandler::GetParentsTable() const
{
    return nullptr;
}

std::unique_ptr<NYT::NOrm::NServer::NObjects::TObject>
TMultipolicyIdTypeHandler::InstantiateObject(
    const NYT::NOrm::NClient::NObjects::TObjectKey& key,
    const NYT::NOrm::NClient::NObjects::TObjectKey& parentKey,
    ISession* session)
{
    YT_VERIFY(!parentKey);

    return std::make_unique<TMultipolicyId>(
        std::get<TString>(key[0]),
        std::get<i64>(key[1]),
        std::get<ui64>(key[2]),
        std::get<ui64>(key[3]),
        this,
        session);
}

void TMultipolicyIdTypeHandler::InitializeCreatedObject(
    NYT::NOrm::NServer::NObjects::TTransaction* transaction,
    NYT::NOrm::NServer::NObjects::TObject* object)
{
    TBase::InitializeCreatedObject(transaction, object);
    auto* typedObject = object->As<TMultipolicyId>();
    YT_VERIFY(typedObject);
    typedObject->MetaEtc().MutableLoad()->set_uuid(NYT::NOrm::NClient::NObjects::GenerateUuid());
}

void TMultipolicyIdTypeHandler::ValidateCreatedObject(
    NYT::NOrm::NServer::NObjects::TTransaction* transaction,
    NYT::NOrm::NServer::NObjects::TObject* object)
{
    TBase::ValidateCreatedObject(transaction, object);
    auto* typedObject = object->As<TMultipolicyId>();
    YT_VERIFY(typedObject);
}

void TMultipolicyIdTypeHandler::ValidateMetaEtcOnTransactionCommit(
    NYT::NOrm::NServer::NObjects::TTransaction*,
    const TMultipolicyId* object)
{
    const auto& metaEtcOld = object->MetaEtc().LoadOld();
    const auto& metaEtcNew = object->MetaEtc().Load();

    // Sanity check.
    ValidateMetaUuid(metaEtcOld, metaEtcNew);

    if (metaEtcNew.name() && !this->IsObjectNameSupported()) {
        THROW_ERROR_EXCEPTION("Cannot set /meta/name for %v objects",
            NYT::NOrm::NClient::NObjects::GetGlobalObjectTypeRegistry()
                ->GetHumanReadableTypeNameOrCrash(this->GetType()));
    }
}

void TMultipolicyIdTypeHandler::ValidateMetaEtcOnValueUpdate(
    NYT::NOrm::NServer::NObjects::TTransaction*,
    const TMultipolicyId* object,
    const TMultipolicyId::TMetaEtc& metaEtcOld,
    const TMultipolicyId::TMetaEtc& metaEtcNew)
{
    ValidateMetaUuid(metaEtcOld, metaEtcNew);

    // All user-defined attributes are considered updatable by any user
    // having corresponding write permission.
    Y_UNUSED(object);
}

void TMultipolicyIdTypeHandler::ValidateMetaUuid(
    const TMultipolicyId::TMetaEtc& metaEtcOld,
    const TMultipolicyId::TMetaEtc& metaEtcNew)
{
    // Uuid protects, for example, YP secrets from object identity theft, i.e. from ABA race conditions,
    // so we try hard to make sure uuid doesn't change.
    if (!metaEtcOld.uuid().empty() && metaEtcOld.uuid() != metaEtcNew.uuid()) {
        this->GetBootstrap()->GetAccessControlManager()->ValidateSuperuser("change /meta/uuid");
    }
}

////////////////////////////////////////////////////////////////////////////////

TNestedColumnsTypeHandler::TNestedColumnsTypeHandler(
    IBootstrap* bootstrap,
    NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config)
    : TBase(
        bootstrap,
        static_cast<NYT::NOrm::NClient::NObjects::TObjectTypeValue>(EObjectType::NestedColumns))
    , Config_(std::move(config))
    , KeyFields_({
        &NestedColumnsTable.Fields.MetaId})
    , NullKey_(NYT::NOrm::NServer::NObjects::GenerateZeroKey(KeyFields_))
{ }

void TNestedColumnsTypeHandler::Initialize()
{
    TBase::Initialize();

    MetaAttributeSchema_
        ->AddChildren({
        MakeScalarAttributeSchema("acl", TDataModelObject::AclDescriptor)
            ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::Updatable)
            ->AddValidator<NYT::NOrm::NServer::NObjects::TObject>(std::bind_front(
                &TNestedColumnsTypeHandler::ValidateAcl, this))
    });
    MetaAttributeSchema_
        ->AddChildren({
            MakeIdAttributeSchema(
                "id",
                &NestedColumnsTable.Fields.MetaId,
                MakeValueGetter(&TNestedColumns::GetId))
                ->SetKeyFieldPolicy(
                    NYT::NOrm::NServer::NObjects::CreateIntegerAttributePolicy<i64>(
                        NYT::NOrm::NServer::NObjects::EAttributeGenerationPolicy::Random,
                        GetBootstrap(),
                        NYT::NOrm::NServer::NObjects::DefaultMinIntegerAttributeValue<i64>,
                        NYT::NOrm::NServer::NObjects::DefaultMaxIntegerAttributeValue<i64>,
                        NYT::NOrm::NServer::NObjects::TAttributePolicyOptions{
                            .Type = GetType(),
                            .AttributePath = "/meta/id",
                            .IndexForIncrement = ""
                        })),
            MakeScalarAttributeSchema("finalization_start_time", TNestedColumns::FinalizationStartTimeDescriptor)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueReadOnly),
            MakeScalarAttributeSchema("finalizers", TNestedColumns::FinalizersDescriptor)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueReadOnly),
            MakeScalarAttributeSchema("ultimate_question_of_life")
                ->SetComputed()
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::ReadOnly)
                ->SetOpaque(/*emitEntity*/ false),
            TBase::MakeEtcAttributeSchema(TNestedColumns::MetaEtcDescriptor.AddValidator(
                std::bind_front(&TNestedColumnsTypeHandler::ValidateMetaEtcOnValueUpdate, this)))
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::Updatable)
                ->template AddValidator<TNestedColumns>(std::bind_front(
                    &TNestedColumnsTypeHandler::ValidateMetaEtcOnTransactionCommit, this))
            });

    SpecAttributeSchema_
        ->AddChildren({
            MakeCompositeAttributeSchema("composite_singular")
                ->AddChildren({
                    MakeScalarAttributeSchema("column_singular", TNestedColumns::TSpec::TCompositeSingular::ColumnSingularDescriptor),
                    MakeScalarAttributeSchema("column_repeated", TNestedColumns::TSpec::TCompositeSingular::ColumnRepeatedDescriptor),
                    MakeScalarAttributeSchema("column_map", TNestedColumns::TSpec::TCompositeSingular::ColumnMapDescriptor),
                    MakeEtcAttributeSchema(TNestedColumns::TSpec::TCompositeSingular::EtcDescriptor),
                }),
            MakeScalarAttributeSchema("column_singular", TNestedColumns::TSpec::ColumnSingularDescriptor),
            MakeScalarAttributeSchema("column_repeated", TNestedColumns::TSpec::ColumnRepeatedDescriptor),
            MakeScalarAttributeSchema("column_map", TNestedColumns::TSpec::ColumnMapDescriptor),
            MakeEtcAttributeSchema(TNestedColumns::TSpec::EtcDescriptor),
        });

    StatusAttributeSchema_
        ->AddChildren({
            MakeEtcAttributeSchema(TNestedColumns::TStatus::EtcDescriptor),
        });

    ControlAttributeSchema_
        ->AddChildren({
            MakeScalarAttributeSchema("touch")
                ->SetControl<TNestedColumns, NYT::NOrm::NExample::NClient::NProto::NDataModel::TObjectTouch>(
                    [] (NYT::NOrm::NServer::NObjects::TTransaction* transaction,
                        TNestedColumns* object,
                        const auto& touch)
                    {
                        if (touch.lock_removal()) {
                            auto* session = transaction->GetSession();
                            session->ScheduleStore(
                                [object] (NYT::NOrm::NServer::NObjects::IStoreContext* context) {
                                    auto* typeHandler = object->GetTypeHandler();
                                    // Lock is taken to prevent success of concurrent RemoveObject query.
                                    // Without it, no write set would be added and
                                    // `RemoveObject` may be committed before `/control/touch` update.
                                    context->LockRow(
                                        typeHandler->GetTable(),
                                        typeHandler->GetObjectTableKey(object),
                                        std::array{&NYT::NOrm::NServer::NObjects::ObjectsTable.Fields.ExistenceLock},
                                        NYT::NTableClient::ELockType::SharedStrong);
                                });
                        }
                        if (touch.store_event_to_history()) {
                            THROW_ERROR_EXCEPTION("History is disabled for %v",
                                object->GetDisplayName());
                            object->ControlTouchEventsToSave().insert("touch");
                        }
                    }),
        });
}

void TNestedColumnsTypeHandler::PostInitialize()
{
    // History is configured in PostInitialize stage, as users may add some evaluated fields via plugins in Initialize stage.
    // NB: Called before TBase::PostInitialize as base type handler prepares cache.
    ConfigureHistory(
        /*attributes*/ {},
        /*filter*/ std::nullopt);

    TBase::PostInitialize();

    FillUpdatePolicies();
}

bool TNestedColumnsTypeHandler::IsObjectNameSupported() const
{
    return false;
}

bool TNestedColumnsTypeHandler::SkipStoreWithoutChanges() const
{
    return false;
}

bool TNestedColumnsTypeHandler::ForceZeroKeyEvaluation() const
{
    return false;
}

std::vector<NYT::NOrm::NServer::NObjects::TWatchLog>
TNestedColumnsTypeHandler::GetWatchLogs() const
{
    return std::vector<NYT::NOrm::NServer::NObjects::TWatchLog>{
    };
}

const NYT::NYson::TProtobufMessageType*
TNestedColumnsTypeHandler::GetRootProtobufType() const
{
    static const auto* type = NYT::NYson::ReflectProtobufMessageType<NYT::NOrm::NExample::NClient::NProto::NDataModel::TNestedColumns>();
    return type;
}

const TDBFields& TNestedColumnsTypeHandler::GetKeyFields() const
{
    return KeyFields_;
}

NYT::NOrm::NClient::NObjects::TObjectKey TNestedColumnsTypeHandler::GetNullKey() const
{
    return NullKey_;
}

const NYT::NOrm::NServer::NObjects::TDBTable* TNestedColumnsTypeHandler::GetTable() const
{
    return &NestedColumnsTable;
}

const NYT::NOrm::NServer::NObjects::TDBTable* TNestedColumnsTypeHandler::GetParentsTable() const
{
    return nullptr;
}

std::unique_ptr<NYT::NOrm::NServer::NObjects::TObject>
TNestedColumnsTypeHandler::InstantiateObject(
    const NYT::NOrm::NClient::NObjects::TObjectKey& key,
    const NYT::NOrm::NClient::NObjects::TObjectKey& parentKey,
    ISession* session)
{
    YT_VERIFY(!parentKey);

    return std::make_unique<TNestedColumns>(
        std::get<i64>(key[0]),
        this,
        session);
}

void TNestedColumnsTypeHandler::InitializeCreatedObject(
    NYT::NOrm::NServer::NObjects::TTransaction* transaction,
    NYT::NOrm::NServer::NObjects::TObject* object)
{
    TBase::InitializeCreatedObject(transaction, object);
    auto* typedObject = object->As<TNestedColumns>();
    YT_VERIFY(typedObject);
    typedObject->MetaEtc().MutableLoad()->set_uuid(NYT::NOrm::NClient::NObjects::GenerateUuid());
}

void TNestedColumnsTypeHandler::ValidateCreatedObject(
    NYT::NOrm::NServer::NObjects::TTransaction* transaction,
    NYT::NOrm::NServer::NObjects::TObject* object)
{
    TBase::ValidateCreatedObject(transaction, object);
    auto* typedObject = object->As<TNestedColumns>();
    YT_VERIFY(typedObject);
}

void TNestedColumnsTypeHandler::ValidateMetaEtcOnTransactionCommit(
    NYT::NOrm::NServer::NObjects::TTransaction*,
    const TNestedColumns* object)
{
    const auto& metaEtcOld = object->MetaEtc().LoadOld();
    const auto& metaEtcNew = object->MetaEtc().Load();

    // Sanity check.
    ValidateMetaUuid(metaEtcOld, metaEtcNew);

    if (metaEtcNew.name() && !this->IsObjectNameSupported()) {
        THROW_ERROR_EXCEPTION("Cannot set /meta/name for %v objects",
            NYT::NOrm::NClient::NObjects::GetGlobalObjectTypeRegistry()
                ->GetHumanReadableTypeNameOrCrash(this->GetType()));
    }
}

void TNestedColumnsTypeHandler::ValidateMetaEtcOnValueUpdate(
    NYT::NOrm::NServer::NObjects::TTransaction*,
    const TNestedColumns* object,
    const TNestedColumns::TMetaEtc& metaEtcOld,
    const TNestedColumns::TMetaEtc& metaEtcNew)
{
    ValidateMetaUuid(metaEtcOld, metaEtcNew);

    // All user-defined attributes are considered updatable by any user
    // having corresponding write permission.
    Y_UNUSED(object);
}

void TNestedColumnsTypeHandler::ValidateMetaUuid(
    const TNestedColumns::TMetaEtc& metaEtcOld,
    const TNestedColumns::TMetaEtc& metaEtcNew)
{
    // Uuid protects, for example, YP secrets from object identity theft, i.e. from ABA race conditions,
    // so we try hard to make sure uuid doesn't change.
    if (!metaEtcOld.uuid().empty() && metaEtcOld.uuid() != metaEtcNew.uuid()) {
        this->GetBootstrap()->GetAccessControlManager()->ValidateSuperuser("change /meta/uuid");
    }
}

////////////////////////////////////////////////////////////////////////////////

TNexusTypeHandler::TNexusTypeHandler(
    IBootstrap* bootstrap,
    NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config)
    : TBase(
        bootstrap,
        static_cast<NYT::NOrm::NClient::NObjects::TObjectTypeValue>(EObjectType::Nexus))
    , Config_(std::move(config))
    , KeyFields_({
        &NexusTable.Fields.MetaId})
    , NullKey_(NYT::NOrm::NServer::NObjects::GenerateZeroKey(KeyFields_))
{ }

void TNexusTypeHandler::Initialize()
{
    TBase::Initialize();

    MetaAttributeSchema_
        ->AddChildren({
        MakeScalarAttributeSchema("acl", TDataModelObject::AclDescriptor)
            ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::Updatable)
            ->AddValidator<NYT::NOrm::NServer::NObjects::TObject>(std::bind_front(
                &TNexusTypeHandler::ValidateAcl, this))
    });
    MetaAttributeSchema_
        ->AddChildren({
            MakeIdAttributeSchema(
                "id",
                &NexusTable.Fields.MetaId,
                MakeValueGetter(&TNexus::GetId))
                ->SetKeyFieldPolicy(
                    NYT::NOrm::NServer::NObjects::CreateIntegerAttributePolicy<i64>(
                        NYT::NOrm::NServer::NObjects::EAttributeGenerationPolicy::Random,
                        GetBootstrap(),
                        NYT::NOrm::NServer::NObjects::DefaultMinIntegerAttributeValue<i64>,
                        NYT::NOrm::NServer::NObjects::DefaultMaxIntegerAttributeValue<i64>,
                        NYT::NOrm::NServer::NObjects::TAttributePolicyOptions{
                            .Type = GetType(),
                            .AttributePath = "/meta/id",
                            .IndexForIncrement = ""
                        })),
            MakeScalarAttributeSchema("finalization_start_time", TNexus::FinalizationStartTimeDescriptor)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueReadOnly),
            MakeScalarAttributeSchema("finalizers", TNexus::FinalizersDescriptor)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueReadOnly),
            MakeScalarAttributeSchema("ultimate_question_of_life")
                ->SetComputed()
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::ReadOnly)
                ->SetOpaque(/*emitEntity*/ false),
            TBase::MakeEtcAttributeSchema(TNexus::MetaEtcDescriptor.AddValidator(
                std::bind_front(&TNexusTypeHandler::ValidateMetaEtcOnValueUpdate, this)))
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::Updatable)
                ->template AddValidator<TNexus>(std::bind_front(
                    &TNexusTypeHandler::ValidateMetaEtcOnTransactionCommit, this))
            });

    SpecAttributeSchema_
        ->AddChildren({
            MakeScalarAttributeSchema("some_map_to_message_column", TNexus::TSpec::SomeMapToMessageColumnDescriptor),
            MakeEtcAttributeSchema(TNexus::TSpec::EtcDescriptor),
        });

    StatusAttributeSchema_
        ->AddChildren({
            MakeScalarAttributeSchema("column_semaphore", TNexus::TStatus::ColumnSemaphoreDescriptor),
            MakeEtcAttributeSchema(TNexus::TStatus::EtcDescriptor),
        });

    ControlAttributeSchema_
        ->AddChildren({
            MakeScalarAttributeSchema("touch")
                ->SetControl<TNexus, NYT::NOrm::NExample::NClient::NProto::NDataModel::TObjectTouch>(
                    [] (NYT::NOrm::NServer::NObjects::TTransaction* transaction,
                        TNexus* object,
                        const auto& touch)
                    {
                        if (touch.lock_removal()) {
                            auto* session = transaction->GetSession();
                            session->ScheduleStore(
                                [object] (NYT::NOrm::NServer::NObjects::IStoreContext* context) {
                                    auto* typeHandler = object->GetTypeHandler();
                                    // Lock is taken to prevent success of concurrent RemoveObject query.
                                    // Without it, no write set would be added and
                                    // `RemoveObject` may be committed before `/control/touch` update.
                                    context->LockRow(
                                        typeHandler->GetTable(),
                                        typeHandler->GetObjectTableKey(object),
                                        std::array{&NYT::NOrm::NServer::NObjects::ObjectsTable.Fields.ExistenceLock},
                                        NYT::NTableClient::ELockType::SharedStrong);
                                });
                        }
                        if (touch.store_event_to_history()) {
                            THROW_ERROR_EXCEPTION("History is disabled for %v",
                                object->GetDisplayName());
                            object->ControlTouchEventsToSave().insert("touch");
                        }
                    }),
            MakeCompositeAttributeSchema("embedded_semaphore")
                ->AddChildren({
                    MakeScalarAttributeSchema("acquire")
                        ->template SetControlWithTransactionCallContext<
                            NYT::NOrm::NServer::NObjects::TObject, NDataModel::TSemaphoreAcquire>(
                            &NYT::NOrm::NServer::NObjects::NSemaphores::EmbeddedAcquire<
                                NDataModel::TEmbeddedSemaphore, NDataModel::TSemaphoreAcquire>),
                    MakeScalarAttributeSchema("ping")
                        ->template SetControlWithTransactionCallContext<
                            NYT::NOrm::NServer::NObjects::TObject, NDataModel::TSemaphorePing>(
                            &NYT::NOrm::NServer::NObjects::NSemaphores::EmbeddedPing<
                                NDataModel::TEmbeddedSemaphore, NDataModel::TSemaphorePing>),
                    MakeScalarAttributeSchema("release")
                        ->template SetControlWithTransactionCallContext<
                            NYT::NOrm::NServer::NObjects::TObject, NDataModel::TSemaphoreRelease>(
                            &NYT::NOrm::NServer::NObjects::NSemaphores::EmbeddedRelease<
                                NDataModel::TEmbeddedSemaphore, NDataModel::TSemaphoreRelease>),
                }),
        });
    RegisterChildrenAttribute(
        static_cast<NYT::NOrm::NClient::NObjects::TObjectTypeValue>(EObjectType::MotherShip),
        [] (const NYT::NOrm::NServer::NObjects::TObject* object) {
            return &object->As<TNexus>()->MotherShips();
        });
}

void TNexusTypeHandler::PostInitialize()
{
    // History is configured in PostInitialize stage, as users may add some evaluated fields via plugins in Initialize stage.
    // NB: Called before TBase::PostInitialize as base type handler prepares cache.
    ConfigureHistory(
        /*attributes*/ {},
        /*filter*/ std::nullopt);

    TBase::PostInitialize();

    FillUpdatePolicies();
}

bool TNexusTypeHandler::IsObjectNameSupported() const
{
    return false;
}

bool TNexusTypeHandler::SkipStoreWithoutChanges() const
{
    return false;
}

bool TNexusTypeHandler::ForceZeroKeyEvaluation() const
{
    return false;
}

std::vector<NYT::NOrm::NServer::NObjects::TWatchLog>
TNexusTypeHandler::GetWatchLogs() const
{
    return std::vector<NYT::NOrm::NServer::NObjects::TWatchLog>{
        NYT::NOrm::NServer::NObjects::TWatchLog{
            .ObjectName = "nexus",
            .Name = "watch_log",
            .Filter{
                .Query{
                    ""
                }
            },
            .Selector{
            },
        },
    };
}

const NYT::NYson::TProtobufMessageType*
TNexusTypeHandler::GetRootProtobufType() const
{
    static const auto* type = NYT::NYson::ReflectProtobufMessageType<NYT::NOrm::NExample::NClient::NProto::NDataModel::TNexus>();
    return type;
}

const TDBFields& TNexusTypeHandler::GetKeyFields() const
{
    return KeyFields_;
}

NYT::NOrm::NClient::NObjects::TObjectKey TNexusTypeHandler::GetNullKey() const
{
    return NullKey_;
}

const NYT::NOrm::NServer::NObjects::TDBTable* TNexusTypeHandler::GetTable() const
{
    return &NexusTable;
}

const NYT::NOrm::NServer::NObjects::TDBTable* TNexusTypeHandler::GetParentsTable() const
{
    return nullptr;
}

std::unique_ptr<NYT::NOrm::NServer::NObjects::TObject>
TNexusTypeHandler::InstantiateObject(
    const NYT::NOrm::NClient::NObjects::TObjectKey& key,
    const NYT::NOrm::NClient::NObjects::TObjectKey& parentKey,
    ISession* session)
{
    YT_VERIFY(!parentKey);

    return std::make_unique<TNexus>(
        std::get<i64>(key[0]),
        this,
        session);
}

void TNexusTypeHandler::InitializeCreatedObject(
    NYT::NOrm::NServer::NObjects::TTransaction* transaction,
    NYT::NOrm::NServer::NObjects::TObject* object)
{
    TBase::InitializeCreatedObject(transaction, object);
    auto* typedObject = object->As<TNexus>();
    YT_VERIFY(typedObject);
    typedObject->MetaEtc().MutableLoad()->set_uuid(NYT::NOrm::NClient::NObjects::GenerateUuid());
}

void TNexusTypeHandler::ValidateCreatedObject(
    NYT::NOrm::NServer::NObjects::TTransaction* transaction,
    NYT::NOrm::NServer::NObjects::TObject* object)
{
    TBase::ValidateCreatedObject(transaction, object);
    auto* typedObject = object->As<TNexus>();
    YT_VERIFY(typedObject);
}

void TNexusTypeHandler::ValidateMetaEtcOnTransactionCommit(
    NYT::NOrm::NServer::NObjects::TTransaction*,
    const TNexus* object)
{
    const auto& metaEtcOld = object->MetaEtc().LoadOld();
    const auto& metaEtcNew = object->MetaEtc().Load();

    // Sanity check.
    ValidateMetaUuid(metaEtcOld, metaEtcNew);

    if (metaEtcNew.name() && !this->IsObjectNameSupported()) {
        THROW_ERROR_EXCEPTION("Cannot set /meta/name for %v objects",
            NYT::NOrm::NClient::NObjects::GetGlobalObjectTypeRegistry()
                ->GetHumanReadableTypeNameOrCrash(this->GetType()));
    }
}

void TNexusTypeHandler::ValidateMetaEtcOnValueUpdate(
    NYT::NOrm::NServer::NObjects::TTransaction*,
    const TNexus* object,
    const TNexus::TMetaEtc& metaEtcOld,
    const TNexus::TMetaEtc& metaEtcNew)
{
    ValidateMetaUuid(metaEtcOld, metaEtcNew);

    // All user-defined attributes are considered updatable by any user
    // having corresponding write permission.
    Y_UNUSED(object);
}

void TNexusTypeHandler::ValidateMetaUuid(
    const TNexus::TMetaEtc& metaEtcOld,
    const TNexus::TMetaEtc& metaEtcNew)
{
    // Uuid protects, for example, YP secrets from object identity theft, i.e. from ABA race conditions,
    // so we try hard to make sure uuid doesn't change.
    if (!metaEtcOld.uuid().empty() && metaEtcOld.uuid() != metaEtcNew.uuid()) {
        this->GetBootstrap()->GetAccessControlManager()->ValidateSuperuser("change /meta/uuid");
    }
}

////////////////////////////////////////////////////////////////////////////////

TNirvanaDMProcessInstanceTypeHandler::TNirvanaDMProcessInstanceTypeHandler(
    IBootstrap* bootstrap,
    NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config)
    : TBase(
        bootstrap,
        static_cast<NYT::NOrm::NClient::NObjects::TObjectTypeValue>(EObjectType::NirvanaDMProcessInstance))
    , Config_(std::move(config))
    , KeyFields_({
        &NirvanaDMProcessInstancesTable.Fields.MetaId})
    , NullKey_(NYT::NOrm::NServer::NObjects::GenerateZeroKey(KeyFields_))
{ }

void TNirvanaDMProcessInstanceTypeHandler::Initialize()
{
    TBase::Initialize();

    MetaAttributeSchema_
        ->AddChildren({
        MakeScalarAttributeSchema("acl", TDataModelObject::AclDescriptor)
            ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::Updatable)
            ->AddValidator<NYT::NOrm::NServer::NObjects::TObject>(std::bind_front(
                &TNirvanaDMProcessInstanceTypeHandler::ValidateAcl, this))
    });
    MetaAttributeSchema_
        ->AddChildren({
            MakeIdAttributeSchema(
                "id",
                &NirvanaDMProcessInstancesTable.Fields.MetaId,
                MakeValueGetter(&TNirvanaDMProcessInstance::GetId))
                ->SetKeyFieldPolicy(
                    NYT::NOrm::NServer::NObjects::CreateStringAttributePolicy(
                        NYT::NOrm::NServer::NObjects::EAttributeGenerationPolicy::Random,
                        NYT::NOrm::NServer::NObjects::DefaultMinStringAttributeLength,
                        NYT::NOrm::NServer::NObjects::DefaultMaxStringAttributeLength,
                        NYT::NOrm::NServer::NObjects::DefaultStringAttributeValidChars)),
            MakeScalarAttributeSchema("finalization_start_time", TNirvanaDMProcessInstance::FinalizationStartTimeDescriptor)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueReadOnly),
            MakeScalarAttributeSchema("finalizers", TNirvanaDMProcessInstance::FinalizersDescriptor)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueReadOnly),
            MakeScalarAttributeSchema("ultimate_question_of_life")
                ->SetComputed()
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::ReadOnly)
                ->SetOpaque(/*emitEntity*/ false),
            TBase::MakeEtcAttributeSchema(TNirvanaDMProcessInstance::MetaEtcDescriptor.AddValidator(
                std::bind_front(&TNirvanaDMProcessInstanceTypeHandler::ValidateMetaEtcOnValueUpdate, this)))
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::Updatable)
                ->template AddValidator<TNirvanaDMProcessInstance>(std::bind_front(
                    &TNirvanaDMProcessInstanceTypeHandler::ValidateMetaEtcOnTransactionCommit, this))
            });

    SpecAttributeSchema_
        ->AddChildren({
            MakeEtcAttributeSchema(TNirvanaDMProcessInstance::TSpec::EtcDescriptor),
        });

    StatusAttributeSchema_
        ->AddChildren({
            MakeEtcAttributeSchema(TNirvanaDMProcessInstance::TStatus::EtcDescriptor),
        });

    ControlAttributeSchema_
        ->AddChildren({
            MakeScalarAttributeSchema("touch")
                ->SetControl<TNirvanaDMProcessInstance, NYT::NOrm::NExample::NClient::NProto::NDataModel::TObjectTouch>(
                    [] (NYT::NOrm::NServer::NObjects::TTransaction* transaction,
                        TNirvanaDMProcessInstance* object,
                        const auto& touch)
                    {
                        if (touch.lock_removal()) {
                            auto* session = transaction->GetSession();
                            session->ScheduleStore(
                                [object] (NYT::NOrm::NServer::NObjects::IStoreContext* context) {
                                    auto* typeHandler = object->GetTypeHandler();
                                    // Lock is taken to prevent success of concurrent RemoveObject query.
                                    // Without it, no write set would be added and
                                    // `RemoveObject` may be committed before `/control/touch` update.
                                    context->LockRow(
                                        typeHandler->GetTable(),
                                        typeHandler->GetObjectTableKey(object),
                                        std::array{&NYT::NOrm::NServer::NObjects::ObjectsTable.Fields.ExistenceLock},
                                        NYT::NTableClient::ELockType::SharedStrong);
                                });
                        }
                        if (touch.store_event_to_history()) {
                            THROW_ERROR_EXCEPTION("History is disabled for %v",
                                object->GetDisplayName());
                            object->ControlTouchEventsToSave().insert("touch");
                        }
                    }),
            MakeScalarAttributeSchema("touch_index")
                ->SetControl<TNirvanaDMProcessInstance, NYT::NOrm::NExample::NClient::NProto::NDataModel::TObjectTouchIndex>(
                [] (NYT::NOrm::NServer::NObjects::TTransaction* /*transaction*/,
                    TNirvanaDMProcessInstance* object,
                    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TObjectTouchIndex& touchIndex)
                {
                    for (const auto& indexName : touchIndex.index_names()) {
                        object->GetIndexOrThrow(indexName)->TouchIndex();
                    }
                }),
        });
    RegisterScalarAttributeIndex(
        "nirvana_dm_process_instances_by_created",
        std::make_unique<NYT::NOrm::NServer::NObjects::TScalarAttributeIndexDescriptor>(
            "nirvana_dm_process_instances_by_created",
            &NirvanaDMProcessInstancesByCreatedTable,
            std::vector<NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor>{
                NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor(
                    &TNirvanaDMProcessInstance::TSpec::EtcDescriptor,
                    "/created"),
            },
            std::vector<NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor>{
            },
            /*repeated*/ false,
            Config_->TryGetIndexMode("nirvana_dm_process_instances_by_created")
                .value_or(NYT::NOrm::NServer::NObjects::EIndexMode::Enabled)));
    RegisterScalarAttributeIndex(
        "nirvana_dm_process_instances_by_definition_id",
        std::make_unique<NYT::NOrm::NServer::NObjects::TScalarAttributeIndexDescriptor>(
            "nirvana_dm_process_instances_by_definition_id",
            &NirvanaDMProcessInstancesByDefinitionIdTable,
            std::vector<NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor>{
                NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor(
                    &TNirvanaDMProcessInstance::TSpec::EtcDescriptor,
                    "/definition_id"),
            },
            std::vector<NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor>{
            },
            /*repeated*/ false,
            Config_->TryGetIndexMode("nirvana_dm_process_instances_by_definition_id")
                .value_or(NYT::NOrm::NServer::NObjects::EIndexMode::Enabled)));
}

void TNirvanaDMProcessInstanceTypeHandler::PostInitialize()
{
    // History is configured in PostInitialize stage, as users may add some evaluated fields via plugins in Initialize stage.
    // NB: Called before TBase::PostInitialize as base type handler prepares cache.
    ConfigureHistory(
        /*attributes*/ {},
        /*filter*/ std::nullopt);

    TBase::PostInitialize();

    FillUpdatePolicies();
}

bool TNirvanaDMProcessInstanceTypeHandler::IsObjectNameSupported() const
{
    return false;
}

bool TNirvanaDMProcessInstanceTypeHandler::SkipStoreWithoutChanges() const
{
    return false;
}

bool TNirvanaDMProcessInstanceTypeHandler::ForceZeroKeyEvaluation() const
{
    return false;
}

std::vector<NYT::NOrm::NServer::NObjects::TWatchLog>
TNirvanaDMProcessInstanceTypeHandler::GetWatchLogs() const
{
    return std::vector<NYT::NOrm::NServer::NObjects::TWatchLog>{
        NYT::NOrm::NServer::NObjects::TWatchLog{
            .ObjectName = "nirvana_dm_process_instance",
            .Name = "watch_log",
            .Filter{
                .Query{
                    ""
                }
            },
            .Selector{
            },
        },
    };
}

const NYT::NYson::TProtobufMessageType*
TNirvanaDMProcessInstanceTypeHandler::GetRootProtobufType() const
{
    static const auto* type = NYT::NYson::ReflectProtobufMessageType<NYT::NOrm::NExample::NClient::NProto::NDataModel::TNirvanaDMProcessInstance>();
    return type;
}

const TDBFields& TNirvanaDMProcessInstanceTypeHandler::GetKeyFields() const
{
    return KeyFields_;
}

NYT::NOrm::NClient::NObjects::TObjectKey TNirvanaDMProcessInstanceTypeHandler::GetNullKey() const
{
    return NullKey_;
}

const NYT::NOrm::NServer::NObjects::TDBTable* TNirvanaDMProcessInstanceTypeHandler::GetTable() const
{
    return &NirvanaDMProcessInstancesTable;
}

const NYT::NOrm::NServer::NObjects::TDBTable* TNirvanaDMProcessInstanceTypeHandler::GetParentsTable() const
{
    return nullptr;
}

std::unique_ptr<NYT::NOrm::NServer::NObjects::TObject>
TNirvanaDMProcessInstanceTypeHandler::InstantiateObject(
    const NYT::NOrm::NClient::NObjects::TObjectKey& key,
    const NYT::NOrm::NClient::NObjects::TObjectKey& parentKey,
    ISession* session)
{
    YT_VERIFY(!parentKey);

    return std::make_unique<TNirvanaDMProcessInstance>(
        std::get<TString>(key[0]),
        this,
        session);
}

void TNirvanaDMProcessInstanceTypeHandler::InitializeCreatedObject(
    NYT::NOrm::NServer::NObjects::TTransaction* transaction,
    NYT::NOrm::NServer::NObjects::TObject* object)
{
    TBase::InitializeCreatedObject(transaction, object);
    auto* typedObject = object->As<TNirvanaDMProcessInstance>();
    YT_VERIFY(typedObject);
    typedObject->MetaEtc().MutableLoad()->set_uuid(NYT::NOrm::NClient::NObjects::GenerateUuid());
}

void TNirvanaDMProcessInstanceTypeHandler::ValidateCreatedObject(
    NYT::NOrm::NServer::NObjects::TTransaction* transaction,
    NYT::NOrm::NServer::NObjects::TObject* object)
{
    TBase::ValidateCreatedObject(transaction, object);
    auto* typedObject = object->As<TNirvanaDMProcessInstance>();
    YT_VERIFY(typedObject);
}

void TNirvanaDMProcessInstanceTypeHandler::ValidateMetaEtcOnTransactionCommit(
    NYT::NOrm::NServer::NObjects::TTransaction*,
    const TNirvanaDMProcessInstance* object)
{
    const auto& metaEtcOld = object->MetaEtc().LoadOld();
    const auto& metaEtcNew = object->MetaEtc().Load();

    // Sanity check.
    ValidateMetaUuid(metaEtcOld, metaEtcNew);

    if (metaEtcNew.name() && !this->IsObjectNameSupported()) {
        THROW_ERROR_EXCEPTION("Cannot set /meta/name for %v objects",
            NYT::NOrm::NClient::NObjects::GetGlobalObjectTypeRegistry()
                ->GetHumanReadableTypeNameOrCrash(this->GetType()));
    }
}

void TNirvanaDMProcessInstanceTypeHandler::ValidateMetaEtcOnValueUpdate(
    NYT::NOrm::NServer::NObjects::TTransaction*,
    const TNirvanaDMProcessInstance* object,
    const TNirvanaDMProcessInstance::TMetaEtc& metaEtcOld,
    const TNirvanaDMProcessInstance::TMetaEtc& metaEtcNew)
{
    ValidateMetaUuid(metaEtcOld, metaEtcNew);

    // All user-defined attributes are considered updatable by any user
    // having corresponding write permission.
    Y_UNUSED(object);
}

void TNirvanaDMProcessInstanceTypeHandler::ValidateMetaUuid(
    const TNirvanaDMProcessInstance::TMetaEtc& metaEtcOld,
    const TNirvanaDMProcessInstance::TMetaEtc& metaEtcNew)
{
    // Uuid protects, for example, YP secrets from object identity theft, i.e. from ABA race conditions,
    // so we try hard to make sure uuid doesn't change.
    if (!metaEtcOld.uuid().empty() && metaEtcOld.uuid() != metaEtcNew.uuid()) {
        this->GetBootstrap()->GetAccessControlManager()->ValidateSuperuser("change /meta/uuid");
    }
}

////////////////////////////////////////////////////////////////////////////////

TPublisherTypeHandler::TPublisherTypeHandler(
    IBootstrap* bootstrap,
    NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config)
    : TBase(
        bootstrap,
        static_cast<NYT::NOrm::NClient::NObjects::TObjectTypeValue>(EObjectType::Publisher))
    , Config_(std::move(config))
    , KeyFields_({
        &PublishersTable.Fields.MetaId})
    , NullKey_(NYT::NOrm::NServer::NObjects::GenerateZeroKey(KeyFields_))
{ }

TPublisherTypeHandler::~TPublisherTypeHandler()
{ }

void TPublisherTypeHandler::Initialize()
{
    TBase::Initialize();

    MetaAttributeSchema_
        ->AddChildren({
        MakeScalarAttributeSchema("acl", TDataModelObject::AclDescriptor)
            ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::Updatable)
            ->AddValidator<NYT::NOrm::NServer::NObjects::TObject>(std::bind_front(
                &TPublisherTypeHandler::ValidateAcl, this))
    });
    MetaAttributeSchema_
        ->AddChildren({
            MakeIdAttributeSchema(
                "id",
                &PublishersTable.Fields.MetaId,
                MakeValueGetter(&TPublisher::GetId))
                ->SetKeyFieldPolicy(
                    NYT::NOrm::NServer::NObjects::CreateIntegerAttributePolicy<i64>(
                        NYT::NOrm::NServer::NObjects::EAttributeGenerationPolicy::Random,
                        GetBootstrap(),
                        NYT::NOrm::NServer::NObjects::DefaultMinIntegerAttributeValue<i64>,
                        NYT::NOrm::NServer::NObjects::DefaultMaxIntegerAttributeValue<i64>,
                        NYT::NOrm::NServer::NObjects::TAttributePolicyOptions{
                            .Type = GetType(),
                            .AttributePath = "/meta/id",
                            .IndexForIncrement = ""
                        })),
            MakeScalarAttributeSchema("finalization_start_time", TPublisher::FinalizationStartTimeDescriptor)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueReadOnly),
            MakeScalarAttributeSchema("finalizers", TPublisher::FinalizersDescriptor)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueReadOnly),
            MakeScalarAttributeSchema("ultimate_question_of_life")
                ->SetComputed()
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::ReadOnly)
                ->SetOpaque(/*emitEntity*/ false),
            TBase::MakeEtcAttributeSchema(TPublisher::MetaEtcDescriptor.AddValidator(
                std::bind_front(&TPublisherTypeHandler::ValidateMetaEtcOnValueUpdate, this)))
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::Updatable)
                ->template AddValidator<TPublisher>(std::bind_front(
                    &TPublisherTypeHandler::ValidateMetaEtcOnTransactionCommit, this))
            });

    SpecAttributeSchema_
        ->AddChildren({
            MakeScalarAttributeSchema("name", TPublisher::TSpec::NameDescriptor),
            MakeScalarAttributeSchema("editor_in_chief", TPublisher::TSpec::EditorInChiefDescriptor),
            MakeScalarAttributeSchema("illustrator_in_chief", TPublisher::TSpec::IllustratorInChiefDescriptor),
            MakeScalarAttributeSchema("publisher_group", TPublisher::TSpec::PublisherGroupDescriptor),
            MakeScalarAttributeSchema("column_field", TPublisher::TSpec::ColumnFieldDescriptor)
                ->SetOpaque(),
            MakeScalarAttributeSchema("featured_illustrators", TPublisher::TSpec::FeaturedIllustratorsDescriptor),
            MakeEtcAttributeSchema(TPublisher::TSpec::EtcDescriptor),
            MakeScalarAttributeSchema("editor_in_chief_view", TPublisher::TSpec::EditorInChiefViewDescriptor),
            MakeScalarAttributeSchema("illustrator_in_chief_view", TPublisher::TSpec::IllustratorInChiefViewDescriptor),
            MakeScalarAttributeSchema("publisher_group_view", TPublisher::TSpec::PublisherGroupViewDescriptor),
            MakeScalarAttributeSchema("featured_illustrators_view", TPublisher::TSpec::FeaturedIllustratorsViewDescriptor),
        });

    StatusAttributeSchema_
        ->AddChildren({
            MakeScalarAttributeSchema("column_field", TPublisher::TStatus::ColumnFieldDescriptor)
                ->SetOpaque(),
            MakeScalarAttributeSchema("column_list", TPublisher::TStatus::ColumnListDescriptor)
                ->SetOpaque(),
            MakeScalarAttributeSchema("featured_illustrators", TPublisher::TStatus::FeaturedIllustratorsDescriptor),
            MakeEtcAttributeSchema(TPublisher::TStatus::EtcDescriptor),
            MakeScalarAttributeSchema("featured_illustrators_view", TPublisher::TStatus::FeaturedIllustratorsViewDescriptor),
        });

    ControlAttributeSchema_
        ->AddChildren({
            MakeScalarAttributeSchema("touch")
                ->SetControl<TPublisher, NYT::NOrm::NExample::NClient::NProto::NDataModel::TObjectTouch>(
                    [] (NYT::NOrm::NServer::NObjects::TTransaction* transaction,
                        TPublisher* object,
                        const auto& touch)
                    {
                        if (touch.lock_removal()) {
                            auto* session = transaction->GetSession();
                            session->ScheduleStore(
                                [object] (NYT::NOrm::NServer::NObjects::IStoreContext* context) {
                                    auto* typeHandler = object->GetTypeHandler();
                                    // Lock is taken to prevent success of concurrent RemoveObject query.
                                    // Without it, no write set would be added and
                                    // `RemoveObject` may be committed before `/control/touch` update.
                                    context->LockRow(
                                        typeHandler->GetTable(),
                                        typeHandler->GetObjectTableKey(object),
                                        std::array{&NYT::NOrm::NServer::NObjects::ObjectsTable.Fields.ExistenceLock},
                                        NYT::NTableClient::ELockType::SharedStrong);
                                });
                        }
                        if (touch.store_event_to_history()) {
                            object->ControlTouchEventsToSave().insert("touch");
                        }
                    }),
            MakeScalarAttributeSchema("touch_index")
                ->SetControl<TPublisher, NYT::NOrm::NExample::NClient::NProto::NDataModel::TObjectTouchIndex>(
                [] (NYT::NOrm::NServer::NObjects::TTransaction* /*transaction*/,
                    TPublisher* object,
                    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TObjectTouchIndex& touchIndex)
                {
                    for (const auto& indexName : touchIndex.index_names()) {
                        object->GetIndexOrThrow(indexName)->TouchIndex();
                    }
                }),
            MakeScalarAttributeSchema("migrate_attributes")
                ->SetControl<TPublisher, NYT::NOrm::NExample::NClient::NProto::NDataModel::TObjectMigrateAttributes>(
                [] ([[maybe_unused]] NYT::NOrm::NServer::NObjects::TTransaction* transaction,
                    TPublisher* object,
                    const auto& migrateAttributes)
                {
                    static const THashMap<NYT::NYPath::TYPath, int> AttributeMigrationByPath {
                        {
                            "/status/address",
                            static_cast<int>(TPublisher::EAttributeMigrations::SpecAddressToStatusAddress)
                        },
                        {
                            "/status/number_of_awards",
                            static_cast<int>(TPublisher::EAttributeMigrations::SpecNumberOfAwardsToStatusNumberOfAwards)
                        },
                        {
                            "/spec/column_field",
                            static_cast<int>(TPublisher::EAttributeMigrations::SpecNonColumnFieldToSpecColumnField)
                        },
                        {
                            "/status/non_column_field",
                            static_cast<int>(TPublisher::EAttributeMigrations::StatusColumnFieldToStatusNonColumnField)
                        },
                        {
                            "/status/featured_illustrators",
                            static_cast<int>(TPublisher::EAttributeMigrations::SpecFeaturedIllustratorsToStatusFeaturedIllustrators)
                        },
                        {
                            "/status/non_column_list",
                            static_cast<int>(TPublisher::EAttributeMigrations::StatusColumnListToStatusNonColumnList)
                        },
                    };

                    for (const auto& path : migrateAttributes.target_paths()) {
                        auto it = AttributeMigrationByPath.find(path);
                        THROW_ERROR_EXCEPTION_IF(it == AttributeMigrationByPath.end(),
                            NYT::NOrm::NClient::EErrorCode::InvalidRequestArguments,
                            "Unknown migration target attribute %Qv",
                            path);
                        object->ForceMigrateAttribute(it->second);
                        object->GetSession()->GetOwner()->ScheduleCommitAction(
                            NYT::NOrm::NServer::NObjects::TCommitActionTypes::HandleAttributeMigrations,
                            object);
                    }
                }),
        });
    RegisterScalarAttributeIndex(
        "publishers_by_column_field",
        std::make_unique<NYT::NOrm::NServer::NObjects::TScalarAttributeIndexDescriptor>(
            "publishers_by_column_field",
            &PublishersByColumnFieldTable,
            std::vector<NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor>{
                NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor(
                    &TPublisher::TSpec::ColumnFieldDescriptor,
                    ""),
            },
            std::vector<NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor>{
            },
            /*repeated*/ false,
            Config_->TryGetIndexMode("publishers_by_column_field")
                .value_or(NYT::NOrm::NServer::NObjects::EIndexMode::Enabled)));
    RegisterScalarAttributeIndex(
        "publishers_by_number_of_awards",
        std::make_unique<NYT::NOrm::NServer::NObjects::TScalarAttributeIndexDescriptor>(
            "publishers_by_number_of_awards",
            &PublishersByNumberOfAwardsTable,
            std::vector<NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor>{
                NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor(
                    &TPublisher::TStatus::EtcDescriptor,
                    "/number_of_awards"),
            },
            std::vector<NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor>{
            },
            /*repeated*/ false,
            Config_->TryGetIndexMode("publishers_by_number_of_awards")
                .value_or(NYT::NOrm::NServer::NObjects::EIndexMode::Enabled)));
    RegisterChildrenAttribute(
        static_cast<NYT::NOrm::NClient::NObjects::TObjectTypeValue>(EObjectType::Book),
        [] (const NYT::NOrm::NServer::NObjects::TObject* object) {
            return &object->As<TPublisher>()->Books();
        });
    RegisterChildrenAttribute(
        static_cast<NYT::NOrm::NClient::NObjects::TObjectTypeValue>(EObjectType::Illustrator),
        [] (const NYT::NOrm::NServer::NObjects::TObject* object) {
            return &object->As<TPublisher>()->Illustrators();
        });
    RegisterChildrenAttribute(
        static_cast<NYT::NOrm::NClient::NObjects::TObjectTypeValue>(EObjectType::Typographer),
        [] (const NYT::NOrm::NServer::NObjects::TObject* object) {
            return &object->As<TPublisher>()->Typographers();
        });
    ResolveAttribute("/spec/address", {}, true).Attribute
        ->AsScalar()
        ->AddUpdateHandler<TPublisher>(
            [] (NYT::NOrm::NServer::NObjects::TTransaction* /*transaction*/, TPublisher* object) {
                object->MigrateAttribute(static_cast<int>(TPublisher::EAttributeMigrations::SpecAddressToStatusAddress));
                object->GetSession()->GetOwner()->ScheduleCommitAction(
                    NYT::NOrm::NServer::NObjects::TCommitActionTypes::HandleAttributeMigrations,
                    object);
            });
    ResolveAttribute("/spec/number_of_awards", {}, true).Attribute
        ->AsScalar()
        ->AddUpdateHandler<TPublisher>(
            [] (NYT::NOrm::NServer::NObjects::TTransaction* /*transaction*/, TPublisher* object) {
                object->MigrateAttribute(static_cast<int>(TPublisher::EAttributeMigrations::SpecNumberOfAwardsToStatusNumberOfAwards));
                object->GetSession()->GetOwner()->ScheduleCommitAction(
                    NYT::NOrm::NServer::NObjects::TCommitActionTypes::HandleAttributeMigrations,
                    object);
            });
    ResolveAttribute("/status/number_of_awards", {}, true).Attribute
        ->AsScalar()
        ->AddUpdateHandler<TPublisher>(
            [] (NYT::NOrm::NServer::NObjects::TTransaction* /*transaction*/, TPublisher* object) {
                object->MigrateAttribute(static_cast<int>(TPublisher::EAttributeMigrations::SpecNumberOfAwardsToStatusNumberOfAwards));
                object->GetSession()->GetOwner()->ScheduleCommitAction(
                    NYT::NOrm::NServer::NObjects::TCommitActionTypes::HandleAttributeMigrations,
                    object);
            });
    ResolveAttribute("/spec/non_column_field", {}, true).Attribute
        ->AsScalar()
        ->AddUpdateHandler<TPublisher>(
            [] (NYT::NOrm::NServer::NObjects::TTransaction* /*transaction*/, TPublisher* object) {
                object->MigrateAttribute(static_cast<int>(TPublisher::EAttributeMigrations::SpecNonColumnFieldToSpecColumnField));
                object->GetSession()->GetOwner()->ScheduleCommitAction(
                    NYT::NOrm::NServer::NObjects::TCommitActionTypes::HandleAttributeMigrations,
                    object);
            });
    ResolveAttribute("/spec/column_field", {}, true).Attribute
        ->AsScalar()
        ->AddUpdateHandler<TPublisher>(
            [] (NYT::NOrm::NServer::NObjects::TTransaction* /*transaction*/, TPublisher* object) {
                object->MigrateAttribute(static_cast<int>(TPublisher::EAttributeMigrations::SpecNonColumnFieldToSpecColumnField));
                object->GetSession()->GetOwner()->ScheduleCommitAction(
                    NYT::NOrm::NServer::NObjects::TCommitActionTypes::HandleAttributeMigrations,
                    object);
            });
    ResolveAttribute("/status/column_field", {}, true).Attribute
        ->AsScalar()
        ->AddUpdateHandler<TPublisher>(
            [] (NYT::NOrm::NServer::NObjects::TTransaction* /*transaction*/, TPublisher* object) {
                object->MigrateAttribute(static_cast<int>(TPublisher::EAttributeMigrations::StatusColumnFieldToStatusNonColumnField));
                object->GetSession()->GetOwner()->ScheduleCommitAction(
                    NYT::NOrm::NServer::NObjects::TCommitActionTypes::HandleAttributeMigrations,
                    object);
            });
    ResolveAttribute("/spec/featured_illustrators", {}, true).Attribute
        ->AsScalar()
        ->AddUpdateHandler<TPublisher>(
            [] (NYT::NOrm::NServer::NObjects::TTransaction* /*transaction*/, TPublisher* object) {
                object->MigrateAttribute(static_cast<int>(TPublisher::EAttributeMigrations::SpecFeaturedIllustratorsToStatusFeaturedIllustrators));
                object->GetSession()->GetOwner()->ScheduleCommitAction(
                    NYT::NOrm::NServer::NObjects::TCommitActionTypes::HandleAttributeMigrations,
                    object);
            });
    ResolveAttribute("/status/featured_illustrators", {}, true).Attribute
        ->AsScalar()
        ->AddUpdateHandler<TPublisher>(
            [] (NYT::NOrm::NServer::NObjects::TTransaction* /*transaction*/, TPublisher* object) {
                object->MigrateAttribute(static_cast<int>(TPublisher::EAttributeMigrations::SpecFeaturedIllustratorsToStatusFeaturedIllustrators));
                object->GetSession()->GetOwner()->ScheduleCommitAction(
                    NYT::NOrm::NServer::NObjects::TCommitActionTypes::HandleAttributeMigrations,
                    object);
            });
    ResolveAttribute("/status/column_list", {}, true).Attribute
        ->AsScalar()
        ->AddUpdateHandler<TPublisher>(
            [] (NYT::NOrm::NServer::NObjects::TTransaction* /*transaction*/, TPublisher* object) {
                object->MigrateAttribute(static_cast<int>(TPublisher::EAttributeMigrations::StatusColumnListToStatusNonColumnList));
                object->GetSession()->GetOwner()->ScheduleCommitAction(
                    NYT::NOrm::NServer::NObjects::TCommitActionTypes::HandleAttributeMigrations,
                    object);
            });
    ResolveAttribute("/status/non_column_list", {}, true).Attribute
        ->AsScalar()
        ->AddUpdateHandler<TPublisher>(
            [] (NYT::NOrm::NServer::NObjects::TTransaction* /*transaction*/, TPublisher* object) {
                object->MigrateAttribute(static_cast<int>(TPublisher::EAttributeMigrations::StatusColumnListToStatusNonColumnList));
                object->GetSession()->GetOwner()->ScheduleCommitAction(
                    NYT::NOrm::NServer::NObjects::TCommitActionTypes::HandleAttributeMigrations,
                    object);
            });
}

void TPublisherTypeHandler::PostInitialize()
{
    // History is configured in PostInitialize stage, as users may add some evaluated fields via plugins in Initialize stage.
    // NB: Called before TBase::PostInitialize as base type handler prepares cache.
    ConfigureHistory(
        /*attributes*/ {
            THistoryAttribute{
                .Path = "/spec/illustrator_in_chief",
                .Indexed = false,
            },
            THistoryAttribute{
                .Path = "/spec/name",
                .Indexed = false,
            },
            THistoryAttribute{
                .Path = "/meta",
                .Indexed = false,
            },},
        /*filter*/ TObjectFilter{.Query = "[/spec/name]!='Secret Publisher'"});

    TBase::PostInitialize();

    FillUpdatePolicies();
}

bool TPublisherTypeHandler::IsObjectNameSupported() const
{
    return false;
}

bool TPublisherTypeHandler::SkipStoreWithoutChanges() const
{
    return false;
}

bool TPublisherTypeHandler::ForceZeroKeyEvaluation() const
{
    return false;
}

std::vector<NYT::NOrm::NServer::NObjects::TWatchLog>
TPublisherTypeHandler::GetWatchLogs() const
{
    return std::vector<NYT::NOrm::NServer::NObjects::TWatchLog>{
        NYT::NOrm::NServer::NObjects::TWatchLog{
            .ObjectName = "publisher",
            .Name = "watch_log",
            .Filter{
                .Query{
                    ""
                }
            },
            .Selector{
            },
        },
    };
}

const NYT::NYson::TProtobufMessageType*
TPublisherTypeHandler::GetRootProtobufType() const
{
    static const auto* type = NYT::NYson::ReflectProtobufMessageType<NYT::NOrm::NExample::NClient::NProto::NDataModel::TPublisher>();
    return type;
}

const TDBFields& TPublisherTypeHandler::GetKeyFields() const
{
    return KeyFields_;
}

NYT::NOrm::NClient::NObjects::TObjectKey TPublisherTypeHandler::GetNullKey() const
{
    return NullKey_;
}

const NYT::NOrm::NServer::NObjects::TDBTable* TPublisherTypeHandler::GetTable() const
{
    return &PublishersTable;
}

const NYT::NOrm::NServer::NObjects::TDBTable* TPublisherTypeHandler::GetParentsTable() const
{
    return nullptr;
}

std::unique_ptr<NYT::NOrm::NServer::NObjects::TObject>
TPublisherTypeHandler::InstantiateObject(
    const NYT::NOrm::NClient::NObjects::TObjectKey& key,
    const NYT::NOrm::NClient::NObjects::TObjectKey& parentKey,
    ISession* session)
{
    YT_VERIFY(!parentKey);

    return std::make_unique<TPublisher>(
        std::get<i64>(key[0]),
        this,
        session);
}

void TPublisherTypeHandler::InitializeCreatedObject(
    NYT::NOrm::NServer::NObjects::TTransaction* transaction,
    NYT::NOrm::NServer::NObjects::TObject* object)
{
    TBase::InitializeCreatedObject(transaction, object);
    auto* typedObject = object->As<TPublisher>();
    YT_VERIFY(typedObject);
    typedObject->MetaEtc().MutableLoad()->set_uuid(NYT::NOrm::NClient::NObjects::GenerateUuid());
}

void TPublisherTypeHandler::ValidateCreatedObject(
    NYT::NOrm::NServer::NObjects::TTransaction* transaction,
    NYT::NOrm::NServer::NObjects::TObject* object)
{
    TBase::ValidateCreatedObject(transaction, object);
    auto* typedObject = object->As<TPublisher>();
    YT_VERIFY(typedObject);
}

void TPublisherTypeHandler::ValidateMetaEtcOnTransactionCommit(
    NYT::NOrm::NServer::NObjects::TTransaction*,
    const TPublisher* object)
{
    const auto& metaEtcOld = object->MetaEtc().LoadOld();
    const auto& metaEtcNew = object->MetaEtc().Load();

    // Sanity check.
    ValidateMetaUuid(metaEtcOld, metaEtcNew);

    if (metaEtcNew.name() && !this->IsObjectNameSupported()) {
        THROW_ERROR_EXCEPTION("Cannot set /meta/name for %v objects",
            NYT::NOrm::NClient::NObjects::GetGlobalObjectTypeRegistry()
                ->GetHumanReadableTypeNameOrCrash(this->GetType()));
    }
}

void TPublisherTypeHandler::ValidateMetaEtcOnValueUpdate(
    NYT::NOrm::NServer::NObjects::TTransaction*,
    const TPublisher* object,
    const TPublisher::TMetaEtc& metaEtcOld,
    const TPublisher::TMetaEtc& metaEtcNew)
{
    ValidateMetaUuid(metaEtcOld, metaEtcNew);

    // All user-defined attributes are considered updatable by any user
    // having corresponding write permission.
    Y_UNUSED(object);
}

void TPublisherTypeHandler::ValidateMetaUuid(
    const TPublisher::TMetaEtc& metaEtcOld,
    const TPublisher::TMetaEtc& metaEtcNew)
{
    // Uuid protects, for example, YP secrets from object identity theft, i.e. from ABA race conditions,
    // so we try hard to make sure uuid doesn't change.
    if (!metaEtcOld.uuid().empty() && metaEtcOld.uuid() != metaEtcNew.uuid()) {
        this->GetBootstrap()->GetAccessControlManager()->ValidateSuperuser("change /meta/uuid");
    }
}

void TPublisherTypeHandler::DoPrepareAttributeMigrations(
    NYT::NOrm::NServer::NObjects::TObject* object,
    const TBitSet<int>& attributeMigrations,
    const TBitSet<int>& /*forcedAttributeMigrations*/)
{
    auto* typedObject = static_cast<TPublisher*>(object);
    if (attributeMigrations.contains(static_cast<int>(TPublisher::EAttributeMigrations::SpecAddressToStatusAddress))) {
        typedObject->Spec().Etc().ScheduleLoad();
        typedObject->Status().Etc().ScheduleLoad();
    }
    if (attributeMigrations.contains(static_cast<int>(TPublisher::EAttributeMigrations::SpecNumberOfAwardsToStatusNumberOfAwards))) {
        typedObject->Spec().Etc().ScheduleLoad();
        typedObject->Status().Etc().ScheduleLoad();
        DoPrepareSpecNumberOfAwardsToStatusNumberOfAwardsMigration(typedObject);
    }
    if (attributeMigrations.contains(static_cast<int>(TPublisher::EAttributeMigrations::SpecNonColumnFieldToSpecColumnField))) {
        typedObject->Spec().Etc().ScheduleLoad();
        typedObject->Spec().ColumnField().ScheduleLoad();
    }
    if (attributeMigrations.contains(static_cast<int>(TPublisher::EAttributeMigrations::StatusColumnFieldToStatusNonColumnField))) {
        typedObject->Status().ColumnField().ScheduleLoad();
        typedObject->Status().Etc().ScheduleLoad();
    }
    if (attributeMigrations.contains(static_cast<int>(TPublisher::EAttributeMigrations::SpecFeaturedIllustratorsToStatusFeaturedIllustrators))) {
        typedObject->Spec().FeaturedIllustrators().ScheduleLoad();
        typedObject->Status().FeaturedIllustrators().ScheduleLoad();
        DoPrepareSpecFeaturedIllustratorsToStatusFeaturedIllustratorsMigration(typedObject);
    }
    if (attributeMigrations.contains(static_cast<int>(TPublisher::EAttributeMigrations::StatusColumnListToStatusNonColumnList))) {
        typedObject->Status().ColumnList().ScheduleLoad();
        typedObject->Status().Etc().ScheduleLoad();
    }
}

void TPublisherTypeHandler::DoFinalizeAttributeMigrations(
    NYT::NOrm::NServer::NObjects::TObject* object,
    const TBitSet<int>& /*attributeMigrations*/,
    const TBitSet<int>& forcedAttributeMigrations)
{
    auto state = object->GetState();
    if (state == NYT::NOrm::NServer::NObjects::EObjectState::Removed ||
        state == NYT::NOrm::NServer::NObjects::EObjectState::CreatedRemoved)
    {
        return;
    }

    auto* typedObject = static_cast<TPublisher*>(object);
    {
        bool forceMigrated = forcedAttributeMigrations.contains(static_cast<int>(TPublisher::EAttributeMigrations::SpecAddressToStatusAddress));
        bool sourceChanged = forceMigrated || typedObject->Spec().Etc().IsChanged("/address");
        bool targetChanged = typedObject->Status().Etc().IsChanged("/address");
        // On conflict action: error.
        bool newObject = state == NYT::NOrm::NServer::NObjects::EObjectState::Created;
        THROW_ERROR_EXCEPTION_IF(!newObject && sourceChanged && targetChanged,
            NYT::NOrm::NClient::EErrorCode::InvalidRequestArguments,
            "Both migration attributes are changed, migration is ambiguous "
            "(Source: /spec/address, Target: /status/address)");
        bool shouldMigrate = sourceChanged && !targetChanged;
        if (shouldMigrate) {
            const auto& sourceValue = typedObject->Spec().Etc().Load().address();
            typedObject->Status().Etc().MutableLoad()->set_address(sourceValue);
        }
    }
    {
        bool forceMigrated = forcedAttributeMigrations.contains(static_cast<int>(TPublisher::EAttributeMigrations::SpecNumberOfAwardsToStatusNumberOfAwards));
        bool sourceChanged = forceMigrated || typedObject->Spec().Etc().IsChanged("/number_of_awards");
        bool targetChanged = typedObject->Status().Etc().IsChanged("/number_of_awards");
        // On conflict action: target.
        bool shouldMigrate = sourceChanged && !targetChanged;
        bool shouldWriteReverse = targetChanged;
        if (shouldMigrate) {
            DoFinalizeSpecNumberOfAwardsToStatusNumberOfAwardsMigration(typedObject);
        }
        if (shouldWriteReverse) {
            DoReverseSpecNumberOfAwardsToStatusNumberOfAwardsMigration(typedObject);
        }
    }
    {
        bool forceMigrated = forcedAttributeMigrations.contains(static_cast<int>(TPublisher::EAttributeMigrations::SpecNonColumnFieldToSpecColumnField));
        bool sourceChanged = forceMigrated || typedObject->Spec().Etc().IsChanged("/non_column_field");
        bool targetChanged = typedObject->Spec().ColumnField().IsChanged();
        // On conflict action: error.
        bool newObject = state == NYT::NOrm::NServer::NObjects::EObjectState::Created;
        THROW_ERROR_EXCEPTION_IF(!newObject && sourceChanged && targetChanged,
            NYT::NOrm::NClient::EErrorCode::InvalidRequestArguments,
            "Both migration attributes are changed, migration is ambiguous "
            "(Source: /spec/non_column_field, Target: /spec/column_field)");
        bool shouldMigrate = sourceChanged && !targetChanged;
        bool shouldWriteReverse = !sourceChanged && targetChanged;
        if (shouldMigrate) {
            const auto& sourceValue = typedObject->Spec().Etc().Load().non_column_field();
            (*typedObject->Spec().ColumnField().MutableLoad()) = sourceValue;
        }
        if (shouldWriteReverse) {
            const auto& targetValue = typedObject->Spec().ColumnField().Load();
            typedObject->Spec().Etc().MutableLoad()->set_non_column_field(targetValue);
        }
    }
    {
        bool forceMigrated = forcedAttributeMigrations.contains(static_cast<int>(TPublisher::EAttributeMigrations::StatusColumnFieldToStatusNonColumnField));
        bool sourceChanged = forceMigrated || typedObject->Status().ColumnField().IsChanged();
        bool targetChanged = typedObject->Status().Etc().IsChanged("/non_column_field");
        // On conflict action: error.
        bool newObject = state == NYT::NOrm::NServer::NObjects::EObjectState::Created;
        THROW_ERROR_EXCEPTION_IF(!newObject && sourceChanged && targetChanged,
            NYT::NOrm::NClient::EErrorCode::InvalidRequestArguments,
            "Both migration attributes are changed, migration is ambiguous "
            "(Source: /status/column_field, Target: /status/non_column_field)");
        bool shouldMigrate = sourceChanged && !targetChanged;
        if (shouldMigrate) {
            const auto& sourceValue = typedObject->Status().ColumnField().Load();
            typedObject->Status().Etc().MutableLoad()->set_non_column_field(sourceValue);
        }
    }
    {
        bool forceMigrated = forcedAttributeMigrations.contains(static_cast<int>(TPublisher::EAttributeMigrations::SpecFeaturedIllustratorsToStatusFeaturedIllustrators));
        bool sourceChanged = forceMigrated || typedObject->Spec().FeaturedIllustrators().IsChanged();
        bool targetChanged = typedObject->Status().FeaturedIllustrators().IsChanged();
        // On conflict action: error.
        bool newObject = state == NYT::NOrm::NServer::NObjects::EObjectState::Created;
        THROW_ERROR_EXCEPTION_IF(!newObject && sourceChanged && targetChanged,
            NYT::NOrm::NClient::EErrorCode::InvalidRequestArguments,
            "Both migration attributes are changed, migration is ambiguous "
            "(Source: /spec/featured_illustrators, Target: /status/featured_illustrators)");
        bool shouldMigrate = sourceChanged && !targetChanged;
        bool shouldWriteReverse = !sourceChanged && targetChanged;
        if (shouldMigrate) {
            DoFinalizeSpecFeaturedIllustratorsToStatusFeaturedIllustratorsMigration(typedObject);
        }
        if (shouldWriteReverse) {
            DoReverseSpecFeaturedIllustratorsToStatusFeaturedIllustratorsMigration(typedObject);
        }
    }
    {
        bool forceMigrated = forcedAttributeMigrations.contains(static_cast<int>(TPublisher::EAttributeMigrations::StatusColumnListToStatusNonColumnList));
        bool sourceChanged = forceMigrated || typedObject->Status().ColumnList().IsChanged();
        bool targetChanged = typedObject->Status().Etc().IsChanged("/non_column_list");
        // On conflict action: error.
        bool newObject = state == NYT::NOrm::NServer::NObjects::EObjectState::Created;
        THROW_ERROR_EXCEPTION_IF(!newObject && sourceChanged && targetChanged,
            NYT::NOrm::NClient::EErrorCode::InvalidRequestArguments,
            "Both migration attributes are changed, migration is ambiguous "
            "(Source: /status/column_list, Target: /status/non_column_list)");
        bool shouldMigrate = sourceChanged && !targetChanged;
        bool shouldWriteReverse = !sourceChanged && targetChanged;
        if (shouldMigrate) {
            const auto& sourceValue = typedObject->Status().ColumnList().Load();
            typedObject->Status().Etc().MutableLoad()->mutable_non_column_list()->Assign(sourceValue.begin(), sourceValue.end());
        }
        if (shouldWriteReverse) {
            const auto& targetValue = typedObject->Status().Etc().Load().non_column_list();
            typedObject->Status().ColumnList().MutableLoad()->assign(targetValue.begin(), targetValue.end());
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TRandomIdTypeHandler::TRandomIdTypeHandler(
    IBootstrap* bootstrap,
    NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config)
    : TBase(
        bootstrap,
        static_cast<NYT::NOrm::NClient::NObjects::TObjectTypeValue>(EObjectType::RandomId))
    , Config_(std::move(config))
    , KeyFields_({
        &RandomIdsTable.Fields.MetaStrId,
        &RandomIdsTable.Fields.MetaI64Id,
        &RandomIdsTable.Fields.MetaUi64Id})
    , NullKey_(NYT::NOrm::NServer::NObjects::GenerateZeroKey(KeyFields_))
{ }

void TRandomIdTypeHandler::Initialize()
{
    TBase::Initialize();

    MetaAttributeSchema_
        ->AddChildren({
        MakeScalarAttributeSchema("acl", TDataModelObject::AclDescriptor)
            ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::Updatable)
            ->AddValidator<NYT::NOrm::NServer::NObjects::TObject>(std::bind_front(
                &TRandomIdTypeHandler::ValidateAcl, this))
    });
    MetaAttributeSchema_
        ->AddChildren({
            MakeIdAttributeSchema(
                "str_id",
                &RandomIdsTable.Fields.MetaStrId,
                MakeValueGetter(&TRandomId::GetStrId))
                ->SetKeyFieldPolicy(
                    NYT::NOrm::NServer::NObjects::CreateStringAttributePolicy(
                        NYT::NOrm::NServer::NObjects::EAttributeGenerationPolicy::Random,
                        NYT::NOrm::NServer::NObjects::DefaultMinStringAttributeLength,
                        NYT::NOrm::NServer::NObjects::DefaultMaxStringAttributeLength,
                        NYT::NOrm::NServer::NObjects::DefaultStringAttributeValidChars)),
            MakeIdAttributeSchema(
                "i64_id",
                &RandomIdsTable.Fields.MetaI64Id,
                MakeValueGetter(&TRandomId::GetI64Id))
                ->SetKeyFieldPolicy(
                    NYT::NOrm::NServer::NObjects::CreateIntegerAttributePolicy<i64>(
                        NYT::NOrm::NServer::NObjects::EAttributeGenerationPolicy::Random,
                        GetBootstrap(),
                        NYT::NOrm::NServer::NObjects::DefaultMinIntegerAttributeValue<i64>,
                        NYT::NOrm::NServer::NObjects::DefaultMaxIntegerAttributeValue<i64>,
                        NYT::NOrm::NServer::NObjects::TAttributePolicyOptions{
                            .Type = GetType(),
                            .AttributePath = "/meta/i64_id",
                            .IndexForIncrement = ""
                        })),
            MakeIdAttributeSchema(
                "ui64_id",
                &RandomIdsTable.Fields.MetaUi64Id,
                MakeValueGetter(&TRandomId::GetUi64Id))
                ->SetKeyFieldPolicy(
                    NYT::NOrm::NServer::NObjects::CreateIntegerAttributePolicy<ui64>(
                        NYT::NOrm::NServer::NObjects::EAttributeGenerationPolicy::Random,
                        GetBootstrap(),
                        NYT::NOrm::NServer::NObjects::DefaultMinIntegerAttributeValue<ui64>,
                        NYT::NOrm::NServer::NObjects::DefaultMaxIntegerAttributeValue<ui64>,
                        NYT::NOrm::NServer::NObjects::TAttributePolicyOptions{
                            .Type = GetType(),
                            .AttributePath = "/meta/ui64_id",
                            .IndexForIncrement = ""
                        })),
            MakeScalarAttributeSchema("finalization_start_time", TRandomId::FinalizationStartTimeDescriptor)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueReadOnly),
            MakeScalarAttributeSchema("finalizers", TRandomId::FinalizersDescriptor)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueReadOnly),
            MakeScalarAttributeSchema("ultimate_question_of_life")
                ->SetComputed()
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::ReadOnly)
                ->SetOpaque(/*emitEntity*/ false),
            TBase::MakeEtcAttributeSchema(TRandomId::MetaEtcDescriptor.AddValidator(
                std::bind_front(&TRandomIdTypeHandler::ValidateMetaEtcOnValueUpdate, this)))
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::Updatable)
                ->template AddValidator<TRandomId>(std::bind_front(
                    &TRandomIdTypeHandler::ValidateMetaEtcOnTransactionCommit, this))
            });

    SpecAttributeSchema_
        ->AddChildren({
            MakeScalarAttributeSchema("str_value", TRandomId::TSpec::StrValueDescriptor)
                ->SetPolicy(TRandomId::TSpec::StrValueDescriptor,
                            NYT::NOrm::NServer::NObjects::CreateStringAttributePolicy(
                                NYT::NOrm::NServer::NObjects::EAttributeGenerationPolicy::Random,
                                NYT::NOrm::NServer::NObjects::DefaultMinStringAttributeLength,
                                NYT::NOrm::NServer::NObjects::DefaultMaxStringAttributeLength,
                                NYT::NOrm::NServer::NObjects::DefaultStringAttributeValidChars)),
            MakeEtcAttributeSchema(TRandomId::TSpec::EtcDescriptor),
        });

    StatusAttributeSchema_
        ->AddChildren({
            MakeEtcAttributeSchema(TRandomId::TStatus::EtcDescriptor),
        });

    ControlAttributeSchema_
        ->AddChildren({
            MakeScalarAttributeSchema("touch")
                ->SetControl<TRandomId, NYT::NOrm::NExample::NClient::NProto::NDataModel::TObjectTouch>(
                    [] (NYT::NOrm::NServer::NObjects::TTransaction* transaction,
                        TRandomId* object,
                        const auto& touch)
                    {
                        if (touch.lock_removal()) {
                            auto* session = transaction->GetSession();
                            session->ScheduleStore(
                                [object] (NYT::NOrm::NServer::NObjects::IStoreContext* context) {
                                    auto* typeHandler = object->GetTypeHandler();
                                    // Lock is taken to prevent success of concurrent RemoveObject query.
                                    // Without it, no write set would be added and
                                    // `RemoveObject` may be committed before `/control/touch` update.
                                    context->LockRow(
                                        typeHandler->GetTable(),
                                        typeHandler->GetObjectTableKey(object),
                                        std::array{&NYT::NOrm::NServer::NObjects::ObjectsTable.Fields.ExistenceLock},
                                        NYT::NTableClient::ELockType::SharedStrong);
                                });
                        }
                        if (touch.store_event_to_history()) {
                            THROW_ERROR_EXCEPTION("History is disabled for %v",
                                object->GetDisplayName());
                            object->ControlTouchEventsToSave().insert("touch");
                        }
                    }),
        });
}

void TRandomIdTypeHandler::PostInitialize()
{
    // History is configured in PostInitialize stage, as users may add some evaluated fields via plugins in Initialize stage.
    // NB: Called before TBase::PostInitialize as base type handler prepares cache.
    ConfigureHistory(
        /*attributes*/ {},
        /*filter*/ std::nullopt);

    TBase::PostInitialize();

    FillUpdatePolicies();
}

bool TRandomIdTypeHandler::IsObjectNameSupported() const
{
    return false;
}

bool TRandomIdTypeHandler::SkipStoreWithoutChanges() const
{
    return false;
}

bool TRandomIdTypeHandler::ForceZeroKeyEvaluation() const
{
    return false;
}

std::vector<NYT::NOrm::NServer::NObjects::TWatchLog>
TRandomIdTypeHandler::GetWatchLogs() const
{
    return std::vector<NYT::NOrm::NServer::NObjects::TWatchLog>{
    };
}

const NYT::NYson::TProtobufMessageType*
TRandomIdTypeHandler::GetRootProtobufType() const
{
    static const auto* type = NYT::NYson::ReflectProtobufMessageType<NYT::NOrm::NExample::NClient::NProto::NDataModel::TRandomId>();
    return type;
}

const TDBFields& TRandomIdTypeHandler::GetKeyFields() const
{
    return KeyFields_;
}

NYT::NOrm::NClient::NObjects::TObjectKey TRandomIdTypeHandler::GetNullKey() const
{
    return NullKey_;
}

const NYT::NOrm::NServer::NObjects::TDBTable* TRandomIdTypeHandler::GetTable() const
{
    return &RandomIdsTable;
}

const NYT::NOrm::NServer::NObjects::TDBTable* TRandomIdTypeHandler::GetParentsTable() const
{
    return nullptr;
}

std::unique_ptr<NYT::NOrm::NServer::NObjects::TObject>
TRandomIdTypeHandler::InstantiateObject(
    const NYT::NOrm::NClient::NObjects::TObjectKey& key,
    const NYT::NOrm::NClient::NObjects::TObjectKey& parentKey,
    ISession* session)
{
    YT_VERIFY(!parentKey);

    return std::make_unique<TRandomId>(
        std::get<TString>(key[0]),
        std::get<i64>(key[1]),
        std::get<ui64>(key[2]),
        this,
        session);
}

void TRandomIdTypeHandler::InitializeCreatedObject(
    NYT::NOrm::NServer::NObjects::TTransaction* transaction,
    NYT::NOrm::NServer::NObjects::TObject* object)
{
    TBase::InitializeCreatedObject(transaction, object);
    auto* typedObject = object->As<TRandomId>();
    YT_VERIFY(typedObject);
    typedObject->MetaEtc().MutableLoad()->set_uuid(NYT::NOrm::NClient::NObjects::GenerateUuid());
}

void TRandomIdTypeHandler::ValidateCreatedObject(
    NYT::NOrm::NServer::NObjects::TTransaction* transaction,
    NYT::NOrm::NServer::NObjects::TObject* object)
{
    TBase::ValidateCreatedObject(transaction, object);
    auto* typedObject = object->As<TRandomId>();
    YT_VERIFY(typedObject);
}

void TRandomIdTypeHandler::ValidateMetaEtcOnTransactionCommit(
    NYT::NOrm::NServer::NObjects::TTransaction*,
    const TRandomId* object)
{
    const auto& metaEtcOld = object->MetaEtc().LoadOld();
    const auto& metaEtcNew = object->MetaEtc().Load();

    // Sanity check.
    ValidateMetaUuid(metaEtcOld, metaEtcNew);

    if (metaEtcNew.name() && !this->IsObjectNameSupported()) {
        THROW_ERROR_EXCEPTION("Cannot set /meta/name for %v objects",
            NYT::NOrm::NClient::NObjects::GetGlobalObjectTypeRegistry()
                ->GetHumanReadableTypeNameOrCrash(this->GetType()));
    }
}

void TRandomIdTypeHandler::ValidateMetaEtcOnValueUpdate(
    NYT::NOrm::NServer::NObjects::TTransaction*,
    const TRandomId* object,
    const TRandomId::TMetaEtc& metaEtcOld,
    const TRandomId::TMetaEtc& metaEtcNew)
{
    ValidateMetaUuid(metaEtcOld, metaEtcNew);

    // All user-defined attributes are considered updatable by any user
    // having corresponding write permission.
    Y_UNUSED(object);
}

void TRandomIdTypeHandler::ValidateMetaUuid(
    const TRandomId::TMetaEtc& metaEtcOld,
    const TRandomId::TMetaEtc& metaEtcNew)
{
    // Uuid protects, for example, YP secrets from object identity theft, i.e. from ABA race conditions,
    // so we try hard to make sure uuid doesn't change.
    if (!metaEtcOld.uuid().empty() && metaEtcOld.uuid() != metaEtcNew.uuid()) {
        this->GetBootstrap()->GetAccessControlManager()->ValidateSuperuser("change /meta/uuid");
    }
}

////////////////////////////////////////////////////////////////////////////////

TSchemaTypeHandler::TSchemaTypeHandler(
    IBootstrap* bootstrap,
    NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config)
    : TBase(
        bootstrap,
        static_cast<NYT::NOrm::NClient::NObjects::TObjectTypeValue>(EObjectType::Schema))
    , Config_(std::move(config))
    , KeyFields_({
        &SchemasTable.Fields.MetaId})
    , NullKey_(NYT::NOrm::NServer::NObjects::GenerateZeroKey(KeyFields_))
{ }

void TSchemaTypeHandler::Initialize()
{
    TBase::Initialize();

    MetaAttributeSchema_
        ->AddChildren({
        MakeScalarAttributeSchema("acl", TDataModelObject::AclDescriptor)
            ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::Updatable)
            ->AddValidator<NYT::NOrm::NServer::NObjects::TObject>(std::bind_front(
                &TSchemaTypeHandler::ValidateAcl, this))
    });
    MetaAttributeSchema_
        ->AddChildren({
            MakeIdAttributeSchema(
                "id",
                &SchemasTable.Fields.MetaId,
                MakeValueGetter(&TSchema::GetId))
                ->SetKeyFieldPolicy(
                    NYT::NOrm::NServer::NObjects::CreateStringAttributePolicy(
                        NYT::NOrm::NServer::NObjects::EAttributeGenerationPolicy::Random,
                        NYT::NOrm::NServer::NObjects::DefaultMinStringAttributeLength,
                        NYT::NOrm::NServer::NObjects::DefaultMaxStringAttributeLength,
                        NYT::NOrm::NServer::NObjects::DefaultStringAttributeValidChars)),
            MakeScalarAttributeSchema("finalization_start_time", TSchema::FinalizationStartTimeDescriptor)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueReadOnly),
            MakeScalarAttributeSchema("finalizers", TSchema::FinalizersDescriptor)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueReadOnly),
            MakeScalarAttributeSchema("ultimate_question_of_life")
                ->SetComputed()
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::ReadOnly)
                ->SetOpaque(/*emitEntity*/ false),
            TBase::MakeEtcAttributeSchema(TSchema::MetaEtcDescriptor.AddValidator(
                std::bind_front(&TSchemaTypeHandler::ValidateMetaEtcOnValueUpdate, this)))
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::Updatable)
                ->template AddValidator<TSchema>(std::bind_front(
                    &TSchemaTypeHandler::ValidateMetaEtcOnTransactionCommit, this))
            });

    SpecAttributeSchema_
        ->AddChildren({
            MakeEtcAttributeSchema(TSchema::TSpec::EtcDescriptor),
        });

    StatusAttributeSchema_
        ->AddChildren({
            MakeEtcAttributeSchema(TSchema::TStatus::EtcDescriptor),
        });

    ControlAttributeSchema_
        ->AddChildren({
            MakeScalarAttributeSchema("touch")
                ->SetControl<TSchema, NYT::NOrm::NExample::NClient::NProto::NDataModel::TObjectTouch>(
                    [] (NYT::NOrm::NServer::NObjects::TTransaction* transaction,
                        TSchema* object,
                        const auto& touch)
                    {
                        if (touch.lock_removal()) {
                            auto* session = transaction->GetSession();
                            session->ScheduleStore(
                                [object] (NYT::NOrm::NServer::NObjects::IStoreContext* context) {
                                    auto* typeHandler = object->GetTypeHandler();
                                    // Lock is taken to prevent success of concurrent RemoveObject query.
                                    // Without it, no write set would be added and
                                    // `RemoveObject` may be committed before `/control/touch` update.
                                    context->LockRow(
                                        typeHandler->GetTable(),
                                        typeHandler->GetObjectTableKey(object),
                                        std::array{&NYT::NOrm::NServer::NObjects::ObjectsTable.Fields.ExistenceLock},
                                        NYT::NTableClient::ELockType::SharedStrong);
                                });
                        }
                        if (touch.store_event_to_history()) {
                            object->ControlTouchEventsToSave().insert("touch");
                        }
                    }),
        });
}

void TSchemaTypeHandler::PostInitialize()
{
    // History is configured in PostInitialize stage, as users may add some evaluated fields via plugins in Initialize stage.
    // NB: Called before TBase::PostInitialize as base type handler prepares cache.
    ConfigureHistory(
        /*attributes*/ {
            THistoryAttribute{
                .Path = "/meta/acl",
                .Indexed = false,
            },},
        /*filter*/ std::nullopt);

    TBase::PostInitialize();

    FillUpdatePolicies();
}

bool TSchemaTypeHandler::IsObjectNameSupported() const
{
    return false;
}

bool TSchemaTypeHandler::SkipStoreWithoutChanges() const
{
    return false;
}

bool TSchemaTypeHandler::ForceZeroKeyEvaluation() const
{
    return false;
}

std::vector<NYT::NOrm::NServer::NObjects::TWatchLog>
TSchemaTypeHandler::GetWatchLogs() const
{
    return std::vector<NYT::NOrm::NServer::NObjects::TWatchLog>{
        NYT::NOrm::NServer::NObjects::TWatchLog{
            .ObjectName = "schema",
            .Name = "watch_log",
            .Filter{
                .Query{
                    ""
                }
            },
            .Selector{
            },
        },
    };
}

const NYT::NYson::TProtobufMessageType*
TSchemaTypeHandler::GetRootProtobufType() const
{
    static const auto* type = NYT::NYson::ReflectProtobufMessageType<NYT::NOrm::NExample::NClient::NProto::NDataModel::TSchema>();
    return type;
}

const TDBFields& TSchemaTypeHandler::GetKeyFields() const
{
    return KeyFields_;
}

NYT::NOrm::NClient::NObjects::TObjectKey TSchemaTypeHandler::GetNullKey() const
{
    return NullKey_;
}

const NYT::NOrm::NServer::NObjects::TDBTable* TSchemaTypeHandler::GetTable() const
{
    return &SchemasTable;
}

const NYT::NOrm::NServer::NObjects::TDBTable* TSchemaTypeHandler::GetParentsTable() const
{
    return nullptr;
}

std::unique_ptr<NYT::NOrm::NServer::NObjects::TObject>
TSchemaTypeHandler::InstantiateObject(
    const NYT::NOrm::NClient::NObjects::TObjectKey& key,
    const NYT::NOrm::NClient::NObjects::TObjectKey& parentKey,
    ISession* session)
{
    YT_VERIFY(!parentKey);

    return std::make_unique<TSchema>(
        std::get<TString>(key[0]),
        this,
        session);
}

void TSchemaTypeHandler::InitializeCreatedObject(
    NYT::NOrm::NServer::NObjects::TTransaction* transaction,
    NYT::NOrm::NServer::NObjects::TObject* object)
{
    TBase::InitializeCreatedObject(transaction, object);
    auto* typedObject = object->As<TSchema>();
    YT_VERIFY(typedObject);
    typedObject->MetaEtc().MutableLoad()->set_uuid(NYT::NOrm::NClient::NObjects::GenerateUuid());
}

void TSchemaTypeHandler::ValidateCreatedObject(
    NYT::NOrm::NServer::NObjects::TTransaction* transaction,
    NYT::NOrm::NServer::NObjects::TObject* object)
{
    TBase::ValidateCreatedObject(transaction, object);
    auto* typedObject = object->As<TSchema>();
    YT_VERIFY(typedObject);
}

void TSchemaTypeHandler::ValidateMetaEtcOnTransactionCommit(
    NYT::NOrm::NServer::NObjects::TTransaction*,
    const TSchema* object)
{
    const auto& metaEtcOld = object->MetaEtc().LoadOld();
    const auto& metaEtcNew = object->MetaEtc().Load();

    // Sanity check.
    ValidateMetaUuid(metaEtcOld, metaEtcNew);

    if (metaEtcNew.name() && !this->IsObjectNameSupported()) {
        THROW_ERROR_EXCEPTION("Cannot set /meta/name for %v objects",
            NYT::NOrm::NClient::NObjects::GetGlobalObjectTypeRegistry()
                ->GetHumanReadableTypeNameOrCrash(this->GetType()));
    }
}

void TSchemaTypeHandler::ValidateMetaEtcOnValueUpdate(
    NYT::NOrm::NServer::NObjects::TTransaction*,
    const TSchema* object,
    const TSchema::TMetaEtc& metaEtcOld,
    const TSchema::TMetaEtc& metaEtcNew)
{
    ValidateMetaUuid(metaEtcOld, metaEtcNew);

    // All user-defined attributes are considered updatable by any user
    // having corresponding write permission.
    Y_UNUSED(object);
}

void TSchemaTypeHandler::ValidateMetaUuid(
    const TSchema::TMetaEtc& metaEtcOld,
    const TSchema::TMetaEtc& metaEtcNew)
{
    // Uuid protects, for example, YP secrets from object identity theft, i.e. from ABA race conditions,
    // so we try hard to make sure uuid doesn't change.
    if (!metaEtcOld.uuid().empty() && metaEtcOld.uuid() != metaEtcNew.uuid()) {
        this->GetBootstrap()->GetAccessControlManager()->ValidateSuperuser("change /meta/uuid");
    }
}

////////////////////////////////////////////////////////////////////////////////

TSemaphoreTypeHandler::TSemaphoreTypeHandler(
    IBootstrap* bootstrap,
    NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config)
    : TBase(
        bootstrap,
        static_cast<NYT::NOrm::NClient::NObjects::TObjectTypeValue>(EObjectType::Semaphore))
    , Config_(std::move(config))
    , KeyFields_({
        &SemaphoresTable.Fields.MetaId})
    , ParentKeyFields_({
        &SemaphoresTable.Fields.MetaSemaphoreSetId})
    , NullKey_(NYT::NOrm::NServer::NObjects::GenerateZeroKey(KeyFields_))
{ }

void TSemaphoreTypeHandler::Initialize()
{
    TBase::Initialize();

    MetaAttributeSchema_
        ->AddChildren({
        MakeScalarAttributeSchema("acl", TDataModelObject::AclDescriptor)
            ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::Updatable)
            ->AddValidator<NYT::NOrm::NServer::NObjects::TObject>(std::bind_front(
                &TSemaphoreTypeHandler::ValidateAcl, this))
    });
    MetaAttributeSchema_
        ->AddChildren({
            MakeParentIdAttributeSchema(
                "semaphore_set_id",
                &SemaphoresTable.Fields.MetaSemaphoreSetId,
                MakeValueGetter(&TSemaphore::SemaphoreSetId)),
            MakeIdAttributeSchema(
                "id",
                &SemaphoresTable.Fields.MetaId,
                MakeValueGetter(&TSemaphore::GetId))
                ->SetKeyFieldPolicy(
                    NYT::NOrm::NServer::NObjects::CreateStringAttributePolicy(
                        NYT::NOrm::NServer::NObjects::EAttributeGenerationPolicy::Random,
                        NYT::NOrm::NServer::NObjects::DefaultMinStringAttributeLength,
                        NYT::NOrm::NServer::NObjects::DefaultMaxStringAttributeLength,
                        NYT::NOrm::NServer::NObjects::DefaultStringAttributeValidChars)),
            MakeScalarAttributeSchema("finalization_start_time", TSemaphore::FinalizationStartTimeDescriptor)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueReadOnly),
            MakeScalarAttributeSchema("finalizers", TSemaphore::FinalizersDescriptor)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueReadOnly),
            MakeScalarAttributeSchema("ultimate_question_of_life")
                ->SetComputed()
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::ReadOnly)
                ->SetOpaque(/*emitEntity*/ false),
            TBase::MakeEtcAttributeSchema(TSemaphore::MetaEtcDescriptor.AddValidator(
                std::bind_front(&TSemaphoreTypeHandler::ValidateMetaEtcOnValueUpdate, this)))
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::Updatable)
                ->template AddValidator<TSemaphore>(std::bind_front(
                    &TSemaphoreTypeHandler::ValidateMetaEtcOnTransactionCommit, this))
            });

    SpecAttributeSchema_
        ->AddChildren({
            MakeEtcAttributeSchema(TSemaphore::TSpec::EtcDescriptor),
        });

    StatusAttributeSchema_
        ->AddChildren({
            MakeEtcAttributeSchema(TSemaphore::TStatus::EtcDescriptor),
            MakeScalarAttributeSchema("fresh_leases")
                ->SetComputed()
                ->SetOpaque(),
        });

    ControlAttributeSchema_
        ->AddChildren({
            MakeScalarAttributeSchema("touch")
                ->SetControl<TSemaphore, NYT::NOrm::NExample::NClient::NProto::NDataModel::TObjectTouch>(
                    [] (NYT::NOrm::NServer::NObjects::TTransaction* transaction,
                        TSemaphore* object,
                        const auto& touch)
                    {
                        if (touch.lock_removal()) {
                            auto* session = transaction->GetSession();
                            session->ScheduleStore(
                                [object] (NYT::NOrm::NServer::NObjects::IStoreContext* context) {
                                    auto* typeHandler = object->GetTypeHandler();
                                    // Lock is taken to prevent success of concurrent RemoveObject query.
                                    // Without it, no write set would be added and
                                    // `RemoveObject` may be committed before `/control/touch` update.
                                    context->LockRow(
                                        typeHandler->GetTable(),
                                        typeHandler->GetObjectTableKey(object),
                                        std::array{&NYT::NOrm::NServer::NObjects::ObjectsTable.Fields.ExistenceLock},
                                        NYT::NTableClient::ELockType::SharedStrong);
                                });
                        }
                        if (touch.store_event_to_history()) {
                            THROW_ERROR_EXCEPTION("History is disabled for %v",
                                object->GetDisplayName());
                            object->ControlTouchEventsToSave().insert("touch");
                        }
                    }),
        });
}

void TSemaphoreTypeHandler::PostInitialize()
{
    // History is configured in PostInitialize stage, as users may add some evaluated fields via plugins in Initialize stage.
    // NB: Called before TBase::PostInitialize as base type handler prepares cache.
    ConfigureHistory(
        /*attributes*/ {},
        /*filter*/ std::nullopt);

    TBase::PostInitialize();

    FillUpdatePolicies();
}

bool TSemaphoreTypeHandler::IsObjectNameSupported() const
{
    return false;
}

bool TSemaphoreTypeHandler::SkipStoreWithoutChanges() const
{
    return false;
}

bool TSemaphoreTypeHandler::ForceZeroKeyEvaluation() const
{
    return false;
}

std::vector<NYT::NOrm::NServer::NObjects::TWatchLog>
TSemaphoreTypeHandler::GetWatchLogs() const
{
    return std::vector<NYT::NOrm::NServer::NObjects::TWatchLog>{
    };
}

const NYT::NYson::TProtobufMessageType*
TSemaphoreTypeHandler::GetRootProtobufType() const
{
    static const auto* type = NYT::NYson::ReflectProtobufMessageType<NYT::NOrm::NExample::NClient::NProto::NDataModel::TSemaphore>();
    return type;
}

const TDBFields& TSemaphoreTypeHandler::GetKeyFields() const
{
    return KeyFields_;
}

NYT::NOrm::NClient::NObjects::TObjectKey TSemaphoreTypeHandler::GetNullKey() const
{
    return NullKey_;
}

const NYT::NOrm::NServer::NObjects::TDBTable* TSemaphoreTypeHandler::GetTable() const
{
    return &SemaphoresTable;
}

const NYT::NOrm::NServer::NObjects::TDBTable* TSemaphoreTypeHandler::GetParentsTable() const
{
    return &SemaphoresToSemaphoreSetsTable;
}

NYT::NOrm::NClient::NObjects::TObjectTypeValue
TSemaphoreTypeHandler::GetParentType() const
{
    return static_cast<NYT::NOrm::NClient::NObjects::TObjectTypeValue>(EObjectType::SemaphoreSet);
}

const TDBFields& TSemaphoreTypeHandler::GetParentKeyFields() const
{
    return ParentKeyFields_;
}

NYT::NOrm::NServer::NObjects::TObject*
TSemaphoreTypeHandler::GetParent(
    const NYT::NOrm::NServer::NObjects::TObject* object,
    std::source_location location)
{
    return object->As<TSemaphore>()
        ->SemaphoreSet().Load(location);
}

std::unique_ptr<NYT::NOrm::NServer::NObjects::TObject>
TSemaphoreTypeHandler::InstantiateObject(
    const NYT::NOrm::NClient::NObjects::TObjectKey& key,
    const NYT::NOrm::NClient::NObjects::TObjectKey& parentKey,
    ISession* session)
{
    return std::make_unique<TSemaphore>(
        std::get<TString>(key[0]),
        parentKey,
        this,
        session);
}

void TSemaphoreTypeHandler::InitializeCreatedObject(
    NYT::NOrm::NServer::NObjects::TTransaction* transaction,
    NYT::NOrm::NServer::NObjects::TObject* object)
{
    TBase::InitializeCreatedObject(transaction, object);
    auto* typedObject = object->As<TSemaphore>();
    YT_VERIFY(typedObject);
    typedObject->MetaEtc().MutableLoad()->set_uuid(NYT::NOrm::NClient::NObjects::GenerateUuid());
}

void TSemaphoreTypeHandler::ValidateCreatedObject(
    NYT::NOrm::NServer::NObjects::TTransaction* transaction,
    NYT::NOrm::NServer::NObjects::TObject* object)
{
    TBase::ValidateCreatedObject(transaction, object);
    auto* typedObject = object->As<TSemaphore>();
    YT_VERIFY(typedObject);
}

void TSemaphoreTypeHandler::ValidateMetaEtcOnTransactionCommit(
    NYT::NOrm::NServer::NObjects::TTransaction*,
    const TSemaphore* object)
{
    const auto& metaEtcOld = object->MetaEtc().LoadOld();
    const auto& metaEtcNew = object->MetaEtc().Load();

    // Sanity check.
    ValidateMetaUuid(metaEtcOld, metaEtcNew);

    if (metaEtcNew.name() && !this->IsObjectNameSupported()) {
        THROW_ERROR_EXCEPTION("Cannot set /meta/name for %v objects",
            NYT::NOrm::NClient::NObjects::GetGlobalObjectTypeRegistry()
                ->GetHumanReadableTypeNameOrCrash(this->GetType()));
    }
}

void TSemaphoreTypeHandler::ValidateMetaEtcOnValueUpdate(
    NYT::NOrm::NServer::NObjects::TTransaction*,
    const TSemaphore* object,
    const TSemaphore::TMetaEtc& metaEtcOld,
    const TSemaphore::TMetaEtc& metaEtcNew)
{
    ValidateMetaUuid(metaEtcOld, metaEtcNew);

    // All user-defined attributes are considered updatable by any user
    // having corresponding write permission.
    Y_UNUSED(object);
}

void TSemaphoreTypeHandler::ValidateMetaUuid(
    const TSemaphore::TMetaEtc& metaEtcOld,
    const TSemaphore::TMetaEtc& metaEtcNew)
{
    // Uuid protects, for example, YP secrets from object identity theft, i.e. from ABA race conditions,
    // so we try hard to make sure uuid doesn't change.
    if (!metaEtcOld.uuid().empty() && metaEtcOld.uuid() != metaEtcNew.uuid()) {
        this->GetBootstrap()->GetAccessControlManager()->ValidateSuperuser("change /meta/uuid");
    }
}

////////////////////////////////////////////////////////////////////////////////

TSemaphoreSetTypeHandler::TSemaphoreSetTypeHandler(
    IBootstrap* bootstrap,
    NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config)
    : TBase(
        bootstrap,
        static_cast<NYT::NOrm::NClient::NObjects::TObjectTypeValue>(EObjectType::SemaphoreSet))
    , Config_(std::move(config))
    , KeyFields_({
        &SemaphoreSetsTable.Fields.MetaId})
    , NullKey_(NYT::NOrm::NServer::NObjects::GenerateZeroKey(KeyFields_))
{ }

void TSemaphoreSetTypeHandler::Initialize()
{
    TBase::Initialize();

    MetaAttributeSchema_
        ->AddChildren({
        MakeScalarAttributeSchema("acl", TDataModelObject::AclDescriptor)
            ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::Updatable)
            ->AddValidator<NYT::NOrm::NServer::NObjects::TObject>(std::bind_front(
                &TSemaphoreSetTypeHandler::ValidateAcl, this))
    });
    MetaAttributeSchema_
        ->AddChildren({
            MakeIdAttributeSchema(
                "id",
                &SemaphoreSetsTable.Fields.MetaId,
                MakeValueGetter(&TSemaphoreSet::GetId))
                ->SetKeyFieldPolicy(
                    NYT::NOrm::NServer::NObjects::CreateStringAttributePolicy(
                        NYT::NOrm::NServer::NObjects::EAttributeGenerationPolicy::Random,
                        NYT::NOrm::NServer::NObjects::DefaultMinStringAttributeLength,
                        NYT::NOrm::NServer::NObjects::DefaultMaxStringAttributeLength,
                        NYT::NOrm::NServer::NObjects::DefaultStringAttributeValidChars)),
            MakeScalarAttributeSchema("finalization_start_time", TSemaphoreSet::FinalizationStartTimeDescriptor)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueReadOnly),
            MakeScalarAttributeSchema("finalizers", TSemaphoreSet::FinalizersDescriptor)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueReadOnly),
            MakeScalarAttributeSchema("ultimate_question_of_life")
                ->SetComputed()
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::ReadOnly)
                ->SetOpaque(/*emitEntity*/ false),
            TBase::MakeEtcAttributeSchema(TSemaphoreSet::MetaEtcDescriptor.AddValidator(
                std::bind_front(&TSemaphoreSetTypeHandler::ValidateMetaEtcOnValueUpdate, this)))
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::Updatable)
                ->template AddValidator<TSemaphoreSet>(std::bind_front(
                    &TSemaphoreSetTypeHandler::ValidateMetaEtcOnTransactionCommit, this))
            });

    SpecAttributeSchema_
        ->AddChildren({
            MakeEtcAttributeSchema(TSemaphoreSet::TSpec::EtcDescriptor),
        });

    StatusAttributeSchema_
        ->AddChildren({
            MakeEtcAttributeSchema(TSemaphoreSet::TStatus::EtcDescriptor),
            MakeScalarAttributeSchema("fresh_leases_by_semaphore")
                ->SetComputed()
                ->SetOpaque(),
        });

    ControlAttributeSchema_
        ->AddChildren({
            MakeScalarAttributeSchema("touch")
                ->SetControl<TSemaphoreSet, NYT::NOrm::NExample::NClient::NProto::NDataModel::TObjectTouch>(
                    [] (NYT::NOrm::NServer::NObjects::TTransaction* transaction,
                        TSemaphoreSet* object,
                        const auto& touch)
                    {
                        if (touch.lock_removal()) {
                            auto* session = transaction->GetSession();
                            session->ScheduleStore(
                                [object] (NYT::NOrm::NServer::NObjects::IStoreContext* context) {
                                    auto* typeHandler = object->GetTypeHandler();
                                    // Lock is taken to prevent success of concurrent RemoveObject query.
                                    // Without it, no write set would be added and
                                    // `RemoveObject` may be committed before `/control/touch` update.
                                    context->LockRow(
                                        typeHandler->GetTable(),
                                        typeHandler->GetObjectTableKey(object),
                                        std::array{&NYT::NOrm::NServer::NObjects::ObjectsTable.Fields.ExistenceLock},
                                        NYT::NTableClient::ELockType::SharedStrong);
                                });
                        }
                        if (touch.store_event_to_history()) {
                            THROW_ERROR_EXCEPTION("History is disabled for %v",
                                object->GetDisplayName());
                            object->ControlTouchEventsToSave().insert("touch");
                        }
                    }),
        });
    RegisterChildrenAttribute(
        static_cast<NYT::NOrm::NClient::NObjects::TObjectTypeValue>(EObjectType::Semaphore),
        [] (const NYT::NOrm::NServer::NObjects::TObject* object) {
            return &object->As<TSemaphoreSet>()->Semaphores();
        });
}

void TSemaphoreSetTypeHandler::PostInitialize()
{
    // History is configured in PostInitialize stage, as users may add some evaluated fields via plugins in Initialize stage.
    // NB: Called before TBase::PostInitialize as base type handler prepares cache.
    ConfigureHistory(
        /*attributes*/ {},
        /*filter*/ std::nullopt);

    TBase::PostInitialize();

    FillUpdatePolicies();
}

bool TSemaphoreSetTypeHandler::IsObjectNameSupported() const
{
    return false;
}

bool TSemaphoreSetTypeHandler::SkipStoreWithoutChanges() const
{
    return false;
}

bool TSemaphoreSetTypeHandler::ForceZeroKeyEvaluation() const
{
    return false;
}

std::vector<NYT::NOrm::NServer::NObjects::TWatchLog>
TSemaphoreSetTypeHandler::GetWatchLogs() const
{
    return std::vector<NYT::NOrm::NServer::NObjects::TWatchLog>{
    };
}

const NYT::NYson::TProtobufMessageType*
TSemaphoreSetTypeHandler::GetRootProtobufType() const
{
    static const auto* type = NYT::NYson::ReflectProtobufMessageType<NYT::NOrm::NExample::NClient::NProto::NDataModel::TSemaphoreSet>();
    return type;
}

const TDBFields& TSemaphoreSetTypeHandler::GetKeyFields() const
{
    return KeyFields_;
}

NYT::NOrm::NClient::NObjects::TObjectKey TSemaphoreSetTypeHandler::GetNullKey() const
{
    return NullKey_;
}

const NYT::NOrm::NServer::NObjects::TDBTable* TSemaphoreSetTypeHandler::GetTable() const
{
    return &SemaphoreSetsTable;
}

const NYT::NOrm::NServer::NObjects::TDBTable* TSemaphoreSetTypeHandler::GetParentsTable() const
{
    return nullptr;
}

std::unique_ptr<NYT::NOrm::NServer::NObjects::TObject>
TSemaphoreSetTypeHandler::InstantiateObject(
    const NYT::NOrm::NClient::NObjects::TObjectKey& key,
    const NYT::NOrm::NClient::NObjects::TObjectKey& parentKey,
    ISession* session)
{
    YT_VERIFY(!parentKey);

    return std::make_unique<TSemaphoreSet>(
        std::get<TString>(key[0]),
        this,
        session);
}

void TSemaphoreSetTypeHandler::InitializeCreatedObject(
    NYT::NOrm::NServer::NObjects::TTransaction* transaction,
    NYT::NOrm::NServer::NObjects::TObject* object)
{
    TBase::InitializeCreatedObject(transaction, object);
    auto* typedObject = object->As<TSemaphoreSet>();
    YT_VERIFY(typedObject);
    typedObject->MetaEtc().MutableLoad()->set_uuid(NYT::NOrm::NClient::NObjects::GenerateUuid());
}

void TSemaphoreSetTypeHandler::ValidateCreatedObject(
    NYT::NOrm::NServer::NObjects::TTransaction* transaction,
    NYT::NOrm::NServer::NObjects::TObject* object)
{
    TBase::ValidateCreatedObject(transaction, object);
    auto* typedObject = object->As<TSemaphoreSet>();
    YT_VERIFY(typedObject);
}

void TSemaphoreSetTypeHandler::ValidateMetaEtcOnTransactionCommit(
    NYT::NOrm::NServer::NObjects::TTransaction*,
    const TSemaphoreSet* object)
{
    const auto& metaEtcOld = object->MetaEtc().LoadOld();
    const auto& metaEtcNew = object->MetaEtc().Load();

    // Sanity check.
    ValidateMetaUuid(metaEtcOld, metaEtcNew);

    if (metaEtcNew.name() && !this->IsObjectNameSupported()) {
        THROW_ERROR_EXCEPTION("Cannot set /meta/name for %v objects",
            NYT::NOrm::NClient::NObjects::GetGlobalObjectTypeRegistry()
                ->GetHumanReadableTypeNameOrCrash(this->GetType()));
    }
}

void TSemaphoreSetTypeHandler::ValidateMetaEtcOnValueUpdate(
    NYT::NOrm::NServer::NObjects::TTransaction*,
    const TSemaphoreSet* object,
    const TSemaphoreSet::TMetaEtc& metaEtcOld,
    const TSemaphoreSet::TMetaEtc& metaEtcNew)
{
    ValidateMetaUuid(metaEtcOld, metaEtcNew);

    // All user-defined attributes are considered updatable by any user
    // having corresponding write permission.
    Y_UNUSED(object);
}

void TSemaphoreSetTypeHandler::ValidateMetaUuid(
    const TSemaphoreSet::TMetaEtc& metaEtcOld,
    const TSemaphoreSet::TMetaEtc& metaEtcNew)
{
    // Uuid protects, for example, YP secrets from object identity theft, i.e. from ABA race conditions,
    // so we try hard to make sure uuid doesn't change.
    if (!metaEtcOld.uuid().empty() && metaEtcOld.uuid() != metaEtcNew.uuid()) {
        this->GetBootstrap()->GetAccessControlManager()->ValidateSuperuser("change /meta/uuid");
    }
}

////////////////////////////////////////////////////////////////////////////////

TTimestampIdTypeHandler::TTimestampIdTypeHandler(
    IBootstrap* bootstrap,
    NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config)
    : TBase(
        bootstrap,
        static_cast<NYT::NOrm::NClient::NObjects::TObjectTypeValue>(EObjectType::TimestampId))
    , Config_(std::move(config))
    , KeyFields_({
        &TimestampIdsTable.Fields.MetaI64Id,
        &TimestampIdsTable.Fields.MetaUi64Id})
    , NullKey_(NYT::NOrm::NServer::NObjects::GenerateZeroKey(KeyFields_))
{ }

void TTimestampIdTypeHandler::Initialize()
{
    TBase::Initialize();

    MetaAttributeSchema_
        ->AddChildren({
        MakeScalarAttributeSchema("acl", TDataModelObject::AclDescriptor)
            ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::Updatable)
            ->AddValidator<NYT::NOrm::NServer::NObjects::TObject>(std::bind_front(
                &TTimestampIdTypeHandler::ValidateAcl, this))
    });
    MetaAttributeSchema_
        ->AddChildren({
            MakeIdAttributeSchema(
                "i64_id",
                &TimestampIdsTable.Fields.MetaI64Id,
                MakeValueGetter(&TTimestampId::GetI64Id))
                ->SetKeyFieldPolicy(
                    NYT::NOrm::NServer::NObjects::CreateIntegerAttributePolicy<i64>(
                        NYT::NOrm::NServer::NObjects::EAttributeGenerationPolicy::Timestamp,
                        GetBootstrap(),
                        NYT::NOrm::NServer::NObjects::DefaultMinIntegerAttributeValue<i64>,
                        NYT::NOrm::NServer::NObjects::DefaultMaxIntegerAttributeValue<i64>,
                        NYT::NOrm::NServer::NObjects::TAttributePolicyOptions{
                            .Type = GetType(),
                            .AttributePath = "/meta/i64_id",
                            .IndexForIncrement = ""
                        })),
            MakeIdAttributeSchema(
                "ui64_id",
                &TimestampIdsTable.Fields.MetaUi64Id,
                MakeValueGetter(&TTimestampId::GetUi64Id))
                ->SetKeyFieldPolicy(
                    NYT::NOrm::NServer::NObjects::CreateIntegerAttributePolicy<ui64>(
                        NYT::NOrm::NServer::NObjects::EAttributeGenerationPolicy::Timestamp,
                        GetBootstrap(),
                        NYT::NOrm::NServer::NObjects::DefaultMinIntegerAttributeValue<ui64>,
                        NYT::NOrm::NServer::NObjects::DefaultMaxIntegerAttributeValue<ui64>,
                        NYT::NOrm::NServer::NObjects::TAttributePolicyOptions{
                            .Type = GetType(),
                            .AttributePath = "/meta/ui64_id",
                            .IndexForIncrement = ""
                        })),
            MakeScalarAttributeSchema("finalization_start_time", TTimestampId::FinalizationStartTimeDescriptor)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueReadOnly),
            MakeScalarAttributeSchema("finalizers", TTimestampId::FinalizersDescriptor)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueReadOnly),
            MakeScalarAttributeSchema("ultimate_question_of_life")
                ->SetComputed()
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::ReadOnly)
                ->SetOpaque(/*emitEntity*/ false),
            TBase::MakeEtcAttributeSchema(TTimestampId::MetaEtcDescriptor.AddValidator(
                std::bind_front(&TTimestampIdTypeHandler::ValidateMetaEtcOnValueUpdate, this)))
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::Updatable)
                ->template AddValidator<TTimestampId>(std::bind_front(
                    &TTimestampIdTypeHandler::ValidateMetaEtcOnTransactionCommit, this))
            });

    SpecAttributeSchema_
        ->AddChildren({
            MakeScalarAttributeSchema("ui64_value", TTimestampId::TSpec::Ui64ValueDescriptor)
                ->SetPolicy(TTimestampId::TSpec::Ui64ValueDescriptor,
                            NYT::NOrm::NServer::NObjects::CreateIntegerAttributePolicy<ui64>(
                                NYT::NOrm::NServer::NObjects::EAttributeGenerationPolicy::Timestamp,
                                GetBootstrap(),
                                NYT::NOrm::NServer::NObjects::DefaultMinIntegerAttributeValue<ui64>,
                                NYT::NOrm::NServer::NObjects::DefaultMaxIntegerAttributeValue<ui64>,
                                NYT::NOrm::NServer::NObjects::TAttributePolicyOptions{
                                    .Type = GetType(),
                                    .AttributePath = "/spec/ui64_value",
                                    .IndexForIncrement = ""
                                })),
            MakeEtcAttributeSchema(TTimestampId::TSpec::EtcDescriptor),
        });

    StatusAttributeSchema_
        ->AddChildren({
            MakeEtcAttributeSchema(TTimestampId::TStatus::EtcDescriptor),
        });

    ControlAttributeSchema_
        ->AddChildren({
            MakeScalarAttributeSchema("touch")
                ->SetControl<TTimestampId, NYT::NOrm::NExample::NClient::NProto::NDataModel::TObjectTouch>(
                    [] (NYT::NOrm::NServer::NObjects::TTransaction* transaction,
                        TTimestampId* object,
                        const auto& touch)
                    {
                        if (touch.lock_removal()) {
                            auto* session = transaction->GetSession();
                            session->ScheduleStore(
                                [object] (NYT::NOrm::NServer::NObjects::IStoreContext* context) {
                                    auto* typeHandler = object->GetTypeHandler();
                                    // Lock is taken to prevent success of concurrent RemoveObject query.
                                    // Without it, no write set would be added and
                                    // `RemoveObject` may be committed before `/control/touch` update.
                                    context->LockRow(
                                        typeHandler->GetTable(),
                                        typeHandler->GetObjectTableKey(object),
                                        std::array{&NYT::NOrm::NServer::NObjects::ObjectsTable.Fields.ExistenceLock},
                                        NYT::NTableClient::ELockType::SharedStrong);
                                });
                        }
                        if (touch.store_event_to_history()) {
                            THROW_ERROR_EXCEPTION("History is disabled for %v",
                                object->GetDisplayName());
                            object->ControlTouchEventsToSave().insert("touch");
                        }
                    }),
        });
}

void TTimestampIdTypeHandler::PostInitialize()
{
    // History is configured in PostInitialize stage, as users may add some evaluated fields via plugins in Initialize stage.
    // NB: Called before TBase::PostInitialize as base type handler prepares cache.
    ConfigureHistory(
        /*attributes*/ {},
        /*filter*/ std::nullopt);

    TBase::PostInitialize();

    FillUpdatePolicies();
}

bool TTimestampIdTypeHandler::IsObjectNameSupported() const
{
    return false;
}

bool TTimestampIdTypeHandler::SkipStoreWithoutChanges() const
{
    return false;
}

bool TTimestampIdTypeHandler::ForceZeroKeyEvaluation() const
{
    return false;
}

std::vector<NYT::NOrm::NServer::NObjects::TWatchLog>
TTimestampIdTypeHandler::GetWatchLogs() const
{
    return std::vector<NYT::NOrm::NServer::NObjects::TWatchLog>{
    };
}

const NYT::NYson::TProtobufMessageType*
TTimestampIdTypeHandler::GetRootProtobufType() const
{
    static const auto* type = NYT::NYson::ReflectProtobufMessageType<NYT::NOrm::NExample::NClient::NProto::NDataModel::TTimestampId>();
    return type;
}

const TDBFields& TTimestampIdTypeHandler::GetKeyFields() const
{
    return KeyFields_;
}

NYT::NOrm::NClient::NObjects::TObjectKey TTimestampIdTypeHandler::GetNullKey() const
{
    return NullKey_;
}

const NYT::NOrm::NServer::NObjects::TDBTable* TTimestampIdTypeHandler::GetTable() const
{
    return &TimestampIdsTable;
}

const NYT::NOrm::NServer::NObjects::TDBTable* TTimestampIdTypeHandler::GetParentsTable() const
{
    return nullptr;
}

std::unique_ptr<NYT::NOrm::NServer::NObjects::TObject>
TTimestampIdTypeHandler::InstantiateObject(
    const NYT::NOrm::NClient::NObjects::TObjectKey& key,
    const NYT::NOrm::NClient::NObjects::TObjectKey& parentKey,
    ISession* session)
{
    YT_VERIFY(!parentKey);

    return std::make_unique<TTimestampId>(
        std::get<i64>(key[0]),
        std::get<ui64>(key[1]),
        this,
        session);
}

void TTimestampIdTypeHandler::InitializeCreatedObject(
    NYT::NOrm::NServer::NObjects::TTransaction* transaction,
    NYT::NOrm::NServer::NObjects::TObject* object)
{
    TBase::InitializeCreatedObject(transaction, object);
    auto* typedObject = object->As<TTimestampId>();
    YT_VERIFY(typedObject);
    typedObject->MetaEtc().MutableLoad()->set_uuid(NYT::NOrm::NClient::NObjects::GenerateUuid());
}

void TTimestampIdTypeHandler::ValidateCreatedObject(
    NYT::NOrm::NServer::NObjects::TTransaction* transaction,
    NYT::NOrm::NServer::NObjects::TObject* object)
{
    TBase::ValidateCreatedObject(transaction, object);
    auto* typedObject = object->As<TTimestampId>();
    YT_VERIFY(typedObject);
}

void TTimestampIdTypeHandler::ValidateMetaEtcOnTransactionCommit(
    NYT::NOrm::NServer::NObjects::TTransaction*,
    const TTimestampId* object)
{
    const auto& metaEtcOld = object->MetaEtc().LoadOld();
    const auto& metaEtcNew = object->MetaEtc().Load();

    // Sanity check.
    ValidateMetaUuid(metaEtcOld, metaEtcNew);

    if (metaEtcNew.name() && !this->IsObjectNameSupported()) {
        THROW_ERROR_EXCEPTION("Cannot set /meta/name for %v objects",
            NYT::NOrm::NClient::NObjects::GetGlobalObjectTypeRegistry()
                ->GetHumanReadableTypeNameOrCrash(this->GetType()));
    }
}

void TTimestampIdTypeHandler::ValidateMetaEtcOnValueUpdate(
    NYT::NOrm::NServer::NObjects::TTransaction*,
    const TTimestampId* object,
    const TTimestampId::TMetaEtc& metaEtcOld,
    const TTimestampId::TMetaEtc& metaEtcNew)
{
    ValidateMetaUuid(metaEtcOld, metaEtcNew);

    // All user-defined attributes are considered updatable by any user
    // having corresponding write permission.
    Y_UNUSED(object);
}

void TTimestampIdTypeHandler::ValidateMetaUuid(
    const TTimestampId::TMetaEtc& metaEtcOld,
    const TTimestampId::TMetaEtc& metaEtcNew)
{
    // Uuid protects, for example, YP secrets from object identity theft, i.e. from ABA race conditions,
    // so we try hard to make sure uuid doesn't change.
    if (!metaEtcOld.uuid().empty() && metaEtcOld.uuid() != metaEtcNew.uuid()) {
        this->GetBootstrap()->GetAccessControlManager()->ValidateSuperuser("change /meta/uuid");
    }
}

////////////////////////////////////////////////////////////////////////////////

TTypographerTypeHandler::TTypographerTypeHandler(
    IBootstrap* bootstrap,
    NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config)
    : TBase(
        bootstrap,
        static_cast<NYT::NOrm::NClient::NObjects::TObjectTypeValue>(EObjectType::Typographer))
    , Config_(std::move(config))
    , KeyFields_({
        &TypographersTable.Fields.MetaId})
    , ParentKeyFields_({
        &TypographersTable.Fields.MetaPublisherId})
    , NullKey_(NYT::NOrm::NServer::NObjects::GenerateZeroKey(KeyFields_))
{ }

void TTypographerTypeHandler::Initialize()
{
    TBase::Initialize();

    MetaAttributeSchema_
        ->AddChildren({
        MakeScalarAttributeSchema("acl", TDataModelObject::AclDescriptor)
            ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::Updatable)
            ->AddValidator<NYT::NOrm::NServer::NObjects::TObject>(std::bind_front(
                &TTypographerTypeHandler::ValidateAcl, this))
    });
    MetaAttributeSchema_
        ->AddChildren({
            MakeParentIdAttributeSchema(
                "publisher_id",
                &TypographersTable.Fields.MetaPublisherId,
                MakeValueGetter(&TTypographer::PublisherId)),
            MakeIdAttributeSchema(
                "id",
                &TypographersTable.Fields.MetaId,
                MakeValueGetter(&TTypographer::GetId))
                ->SetKeyFieldPolicy(
                    NYT::NOrm::NServer::NObjects::CreateIntegerAttributePolicy<i64>(
                        NYT::NOrm::NServer::NObjects::EAttributeGenerationPolicy::Random,
                        GetBootstrap(),
                        NYT::NOrm::NServer::NObjects::DefaultMinIntegerAttributeValue<i64>,
                        NYT::NOrm::NServer::NObjects::DefaultMaxIntegerAttributeValue<i64>,
                        NYT::NOrm::NServer::NObjects::TAttributePolicyOptions{
                            .Type = GetType(),
                            .AttributePath = "/meta/id",
                            .IndexForIncrement = ""
                        })),
            MakeScalarAttributeSchema("login", TTypographer::LoginDescriptor)
                ->SetMandatory(),
            MakeScalarAttributeSchema("finalization_start_time", TTypographer::FinalizationStartTimeDescriptor)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueReadOnly),
            MakeScalarAttributeSchema("finalizers", TTypographer::FinalizersDescriptor)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueReadOnly),
            MakeScalarAttributeSchema("ultimate_question_of_life")
                ->SetComputed()
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::ReadOnly)
                ->SetOpaque(/*emitEntity*/ false),
            TBase::MakeEtcAttributeSchema(TTypographer::MetaEtcDescriptor.AddValidator(
                std::bind_front(&TTypographerTypeHandler::ValidateMetaEtcOnValueUpdate, this)))
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::Updatable)
                ->template AddValidator<TTypographer>(std::bind_front(
                    &TTypographerTypeHandler::ValidateMetaEtcOnTransactionCommit, this))
                ->SetMandatory("inn")
            });

    SpecAttributeSchema_
        ->AddChildren({
            MakeScalarAttributeSchema("test_mandatory_column_field", TTypographer::TSpec::TestMandatoryColumnFieldDescriptor)
                ->SetMandatory(),
            MakeEtcAttributeSchema(TTypographer::TSpec::EtcDescriptor)
                ->SetMandatory("test_mandatory_etc_field"),
        });

    StatusAttributeSchema_
        ->AddChildren({
            MakeEtcAttributeSchema(TTypographer::TStatus::EtcDescriptor),
        });

    ControlAttributeSchema_
        ->AddChildren({
            MakeScalarAttributeSchema("touch")
                ->SetControl<TTypographer, NYT::NOrm::NExample::NClient::NProto::NDataModel::TObjectTouch>(
                    [] (NYT::NOrm::NServer::NObjects::TTransaction* transaction,
                        TTypographer* object,
                        const auto& touch)
                    {
                        if (touch.lock_removal()) {
                            auto* session = transaction->GetSession();
                            session->ScheduleStore(
                                [object] (NYT::NOrm::NServer::NObjects::IStoreContext* context) {
                                    auto* typeHandler = object->GetTypeHandler();
                                    // Lock is taken to prevent success of concurrent RemoveObject query.
                                    // Without it, no write set would be added and
                                    // `RemoveObject` may be committed before `/control/touch` update.
                                    context->LockRow(
                                        typeHandler->GetTable(),
                                        typeHandler->GetObjectTableKey(object),
                                        std::array{&NYT::NOrm::NServer::NObjects::ObjectsTable.Fields.ExistenceLock},
                                        NYT::NTableClient::ELockType::SharedStrong);
                                });
                        }
                        if (touch.store_event_to_history()) {
                            THROW_ERROR_EXCEPTION("History is disabled for %v",
                                object->GetDisplayName());
                            object->ControlTouchEventsToSave().insert("touch");
                        }
                    }),
            MakeScalarAttributeSchema("touch_index")
                ->SetControl<TTypographer, NYT::NOrm::NExample::NClient::NProto::NDataModel::TObjectTouchIndex>(
                [] (NYT::NOrm::NServer::NObjects::TTransaction* /*transaction*/,
                    TTypographer* object,
                    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TObjectTouchIndex& touchIndex)
                {
                    for (const auto& indexName : touchIndex.index_names()) {
                        object->GetIndexOrThrow(indexName)->TouchIndex();
                    }
                }),
        });
    RegisterScalarAttributeIndex(
        "typographers_by_login",
        std::make_unique<NYT::NOrm::NServer::NObjects::TScalarAttributeIndexDescriptor>(
            "typographers_by_login",
            &TypographersByLoginTable,
            std::vector<NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor>{
                NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor(
                    &TTypographer::LoginDescriptor,
                    ""),
            },
            std::vector<NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor>{
            },
            /*repeated*/ false,
            Config_->TryGetIndexMode("typographers_by_login")
                .value_or(NYT::NOrm::NServer::NObjects::EIndexMode::Enabled)));
}

void TTypographerTypeHandler::PostInitialize()
{
    // History is configured in PostInitialize stage, as users may add some evaluated fields via plugins in Initialize stage.
    // NB: Called before TBase::PostInitialize as base type handler prepares cache.
    ConfigureHistory(
        /*attributes*/ {},
        /*filter*/ std::nullopt);

    TBase::PostInitialize();

    FillUpdatePolicies();
}

bool TTypographerTypeHandler::IsObjectNameSupported() const
{
    return false;
}

bool TTypographerTypeHandler::SkipStoreWithoutChanges() const
{
    return false;
}

bool TTypographerTypeHandler::ForceZeroKeyEvaluation() const
{
    return false;
}

std::vector<NYT::NOrm::NServer::NObjects::TWatchLog>
TTypographerTypeHandler::GetWatchLogs() const
{
    return std::vector<NYT::NOrm::NServer::NObjects::TWatchLog>{
    };
}

const NYT::NYson::TProtobufMessageType*
TTypographerTypeHandler::GetRootProtobufType() const
{
    static const auto* type = NYT::NYson::ReflectProtobufMessageType<NYT::NOrm::NExample::NClient::NProto::NDataModel::TTypographer>();
    return type;
}

const TDBFields& TTypographerTypeHandler::GetKeyFields() const
{
    return KeyFields_;
}

NYT::NOrm::NClient::NObjects::TObjectKey TTypographerTypeHandler::GetNullKey() const
{
    return NullKey_;
}

const NYT::NOrm::NServer::NObjects::TDBTable* TTypographerTypeHandler::GetTable() const
{
    return &TypographersTable;
}

const NYT::NOrm::NServer::NObjects::TDBTable* TTypographerTypeHandler::GetParentsTable() const
{
    return &TypographersToPublishersTable;
}

NYT::NOrm::NClient::NObjects::TObjectTypeValue
TTypographerTypeHandler::GetParentType() const
{
    return static_cast<NYT::NOrm::NClient::NObjects::TObjectTypeValue>(EObjectType::Publisher);
}

const TDBFields& TTypographerTypeHandler::GetParentKeyFields() const
{
    return ParentKeyFields_;
}

NYT::NOrm::NServer::NObjects::TObject*
TTypographerTypeHandler::GetParent(
    const NYT::NOrm::NServer::NObjects::TObject* object,
    std::source_location location)
{
    return object->As<TTypographer>()
        ->Publisher().Load(location);
}

std::unique_ptr<NYT::NOrm::NServer::NObjects::TObject>
TTypographerTypeHandler::InstantiateObject(
    const NYT::NOrm::NClient::NObjects::TObjectKey& key,
    const NYT::NOrm::NClient::NObjects::TObjectKey& parentKey,
    ISession* session)
{
    return std::make_unique<TTypographer>(
        std::get<i64>(key[0]),
        parentKey,
        this,
        session);
}

void TTypographerTypeHandler::InitializeCreatedObject(
    NYT::NOrm::NServer::NObjects::TTransaction* transaction,
    NYT::NOrm::NServer::NObjects::TObject* object)
{
    TBase::InitializeCreatedObject(transaction, object);
    auto* typedObject = object->As<TTypographer>();
    YT_VERIFY(typedObject);
    typedObject->MetaEtc().MutableLoad()->set_uuid(NYT::NOrm::NClient::NObjects::GenerateUuid());
}

void TTypographerTypeHandler::ValidateCreatedObject(
    NYT::NOrm::NServer::NObjects::TTransaction* transaction,
    NYT::NOrm::NServer::NObjects::TObject* object)
{
    TBase::ValidateCreatedObject(transaction, object);
    auto* typedObject = object->As<TTypographer>();
    YT_VERIFY(typedObject);
}

void TTypographerTypeHandler::ValidateMetaEtcOnTransactionCommit(
    NYT::NOrm::NServer::NObjects::TTransaction*,
    const TTypographer* object)
{
    const auto& metaEtcOld = object->MetaEtc().LoadOld();
    const auto& metaEtcNew = object->MetaEtc().Load();

    // Sanity check.
    ValidateMetaUuid(metaEtcOld, metaEtcNew);

    if (metaEtcNew.name() && !this->IsObjectNameSupported()) {
        THROW_ERROR_EXCEPTION("Cannot set /meta/name for %v objects",
            NYT::NOrm::NClient::NObjects::GetGlobalObjectTypeRegistry()
                ->GetHumanReadableTypeNameOrCrash(this->GetType()));
    }
}

void TTypographerTypeHandler::ValidateMetaEtcOnValueUpdate(
    NYT::NOrm::NServer::NObjects::TTransaction*,
    const TTypographer* object,
    const TTypographer::TMetaEtc& metaEtcOld,
    const TTypographer::TMetaEtc& metaEtcNew)
{
    ValidateMetaUuid(metaEtcOld, metaEtcNew);

    // All user-defined attributes are considered updatable by any user
    // having corresponding write permission.
    Y_UNUSED(object);
}

void TTypographerTypeHandler::ValidateMetaUuid(
    const TTypographer::TMetaEtc& metaEtcOld,
    const TTypographer::TMetaEtc& metaEtcNew)
{
    // Uuid protects, for example, YP secrets from object identity theft, i.e. from ABA race conditions,
    // so we try hard to make sure uuid doesn't change.
    if (!metaEtcOld.uuid().empty() && metaEtcOld.uuid() != metaEtcNew.uuid()) {
        this->GetBootstrap()->GetAccessControlManager()->ValidateSuperuser("change /meta/uuid");
    }
}

////////////////////////////////////////////////////////////////////////////////

TUserTypeHandler::TUserTypeHandler(
    IBootstrap* bootstrap,
    NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config)
    : TBase(
        bootstrap,
        static_cast<NYT::NOrm::NClient::NObjects::TObjectTypeValue>(EObjectType::User))
    , Config_(std::move(config))
    , KeyFields_({
        &UsersTable.Fields.MetaId})
    , NullKey_(NYT::NOrm::NServer::NObjects::GenerateZeroKey(KeyFields_))
{ }

void TUserTypeHandler::Initialize()
{
    TBase::Initialize();

    MetaAttributeSchema_
        ->AddChildren({
        MakeScalarAttributeSchema("acl", TDataModelObject::AclDescriptor)
            ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::Updatable)
            ->AddValidator<NYT::NOrm::NServer::NObjects::TObject>(std::bind_front(
                &TUserTypeHandler::ValidateAcl, this))
    });
    MetaAttributeSchema_
        ->AddChildren({
            MakeIdAttributeSchema(
                "id",
                &UsersTable.Fields.MetaId,
                MakeValueGetter(&TUser::GetId))
                ->SetKeyFieldPolicy(
                    NYT::NOrm::NServer::NObjects::CreateStringAttributePolicy(
                        NYT::NOrm::NServer::NObjects::EAttributeGenerationPolicy::Random,
                        NYT::NOrm::NServer::NObjects::DefaultMinStringAttributeLength,
                        NYT::NOrm::NServer::NObjects::DefaultMaxStringAttributeLength,
                        NYT::NOrm::NServer::NObjects::DefaultStringAttributeValidChars)),
            MakeScalarAttributeSchema("finalization_start_time", TUser::FinalizationStartTimeDescriptor)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueReadOnly),
            MakeScalarAttributeSchema("finalizers", TUser::FinalizersDescriptor)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueReadOnly),
            MakeScalarAttributeSchema("ultimate_question_of_life")
                ->SetComputed()
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::ReadOnly)
                ->SetOpaque(/*emitEntity*/ false),
            TBase::MakeEtcAttributeSchema(TUser::MetaEtcDescriptor.AddValidator(
                std::bind_front(&TUserTypeHandler::ValidateMetaEtcOnValueUpdate, this)))
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::Updatable)
                ->template AddValidator<TUser>(std::bind_front(
                    &TUserTypeHandler::ValidateMetaEtcOnTransactionCommit, this))
            });

    SpecAttributeSchema_
        ->AddChildren({
            MakeEtcAttributeSchema(TUser::TSpec::EtcDescriptor),
        });

    StatusAttributeSchema_
        ->AddChildren({
            MakeEtcAttributeSchema(TUser::TStatus::EtcDescriptor),
        });

    ControlAttributeSchema_
        ->AddChildren({
            MakeScalarAttributeSchema("touch")
                ->SetControl<TUser, NYT::NOrm::NExample::NClient::NProto::NDataModel::TObjectTouch>(
                    [] (NYT::NOrm::NServer::NObjects::TTransaction* transaction,
                        TUser* object,
                        const auto& touch)
                    {
                        if (touch.lock_removal()) {
                            auto* session = transaction->GetSession();
                            session->ScheduleStore(
                                [object] (NYT::NOrm::NServer::NObjects::IStoreContext* context) {
                                    auto* typeHandler = object->GetTypeHandler();
                                    // Lock is taken to prevent success of concurrent RemoveObject query.
                                    // Without it, no write set would be added and
                                    // `RemoveObject` may be committed before `/control/touch` update.
                                    context->LockRow(
                                        typeHandler->GetTable(),
                                        typeHandler->GetObjectTableKey(object),
                                        std::array{&NYT::NOrm::NServer::NObjects::ObjectsTable.Fields.ExistenceLock},
                                        NYT::NTableClient::ELockType::SharedStrong);
                                });
                        }
                        if (touch.store_event_to_history()) {
                            THROW_ERROR_EXCEPTION("History is disabled for %v",
                                object->GetDisplayName());
                            object->ControlTouchEventsToSave().insert("touch");
                        }
                    }),
        });
}

void TUserTypeHandler::PostInitialize()
{
    // History is configured in PostInitialize stage, as users may add some evaluated fields via plugins in Initialize stage.
    // NB: Called before TBase::PostInitialize as base type handler prepares cache.
    ConfigureHistory(
        /*attributes*/ {},
        /*filter*/ std::nullopt);

    TBase::PostInitialize();

    FillUpdatePolicies();
}

bool TUserTypeHandler::IsObjectNameSupported() const
{
    return false;
}

bool TUserTypeHandler::SkipStoreWithoutChanges() const
{
    return false;
}

bool TUserTypeHandler::ForceZeroKeyEvaluation() const
{
    return false;
}

std::vector<NYT::NOrm::NServer::NObjects::TWatchLog>
TUserTypeHandler::GetWatchLogs() const
{
    return std::vector<NYT::NOrm::NServer::NObjects::TWatchLog>{
        NYT::NOrm::NServer::NObjects::TWatchLog{
            .ObjectName = "user",
            .Name = "watch_log",
            .Filter{
                .Query{
                    ""
                }
            },
            .Selector{
            },
        },
    };
}

const NYT::NYson::TProtobufMessageType*
TUserTypeHandler::GetRootProtobufType() const
{
    static const auto* type = NYT::NYson::ReflectProtobufMessageType<NYT::NOrm::NExample::NClient::NProto::NDataModel::TUser>();
    return type;
}

const TDBFields& TUserTypeHandler::GetKeyFields() const
{
    return KeyFields_;
}

NYT::NOrm::NClient::NObjects::TObjectKey TUserTypeHandler::GetNullKey() const
{
    return NullKey_;
}

const NYT::NOrm::NServer::NObjects::TDBTable* TUserTypeHandler::GetTable() const
{
    return &UsersTable;
}

const NYT::NOrm::NServer::NObjects::TDBTable* TUserTypeHandler::GetParentsTable() const
{
    return nullptr;
}

std::unique_ptr<NYT::NOrm::NServer::NObjects::TObject>
TUserTypeHandler::InstantiateObject(
    const NYT::NOrm::NClient::NObjects::TObjectKey& key,
    const NYT::NOrm::NClient::NObjects::TObjectKey& parentKey,
    ISession* session)
{
    YT_VERIFY(!parentKey);

    return std::make_unique<TUser>(
        std::get<TString>(key[0]),
        this,
        session);
}

void TUserTypeHandler::InitializeCreatedObject(
    NYT::NOrm::NServer::NObjects::TTransaction* transaction,
    NYT::NOrm::NServer::NObjects::TObject* object)
{
    TBase::InitializeCreatedObject(transaction, object);
    auto* typedObject = object->As<TUser>();
    YT_VERIFY(typedObject);
    typedObject->MetaEtc().MutableLoad()->set_uuid(NYT::NOrm::NClient::NObjects::GenerateUuid());
}

void TUserTypeHandler::ValidateCreatedObject(
    NYT::NOrm::NServer::NObjects::TTransaction* transaction,
    NYT::NOrm::NServer::NObjects::TObject* object)
{
    TBase::ValidateCreatedObject(transaction, object);
    auto* typedObject = object->As<TUser>();
    YT_VERIFY(typedObject);
}

void TUserTypeHandler::ValidateMetaEtcOnTransactionCommit(
    NYT::NOrm::NServer::NObjects::TTransaction*,
    const TUser* object)
{
    const auto& metaEtcOld = object->MetaEtc().LoadOld();
    const auto& metaEtcNew = object->MetaEtc().Load();

    // Sanity check.
    ValidateMetaUuid(metaEtcOld, metaEtcNew);

    if (metaEtcNew.name() && !this->IsObjectNameSupported()) {
        THROW_ERROR_EXCEPTION("Cannot set /meta/name for %v objects",
            NYT::NOrm::NClient::NObjects::GetGlobalObjectTypeRegistry()
                ->GetHumanReadableTypeNameOrCrash(this->GetType()));
    }
}

void TUserTypeHandler::ValidateMetaEtcOnValueUpdate(
    NYT::NOrm::NServer::NObjects::TTransaction*,
    const TUser* object,
    const TUser::TMetaEtc& metaEtcOld,
    const TUser::TMetaEtc& metaEtcNew)
{
    ValidateMetaUuid(metaEtcOld, metaEtcNew);

    // All user-defined attributes are considered updatable by any user
    // having corresponding write permission.
    Y_UNUSED(object);
}

void TUserTypeHandler::ValidateMetaUuid(
    const TUser::TMetaEtc& metaEtcOld,
    const TUser::TMetaEtc& metaEtcNew)
{
    // Uuid protects, for example, YP secrets from object identity theft, i.e. from ABA race conditions,
    // so we try hard to make sure uuid doesn't change.
    if (!metaEtcOld.uuid().empty() && metaEtcOld.uuid() != metaEtcNew.uuid()) {
        this->GetBootstrap()->GetAccessControlManager()->ValidateSuperuser("change /meta/uuid");
    }
}

////////////////////////////////////////////////////////////////////////////////

TWatchLogConsumerTypeHandler::TWatchLogConsumerTypeHandler(
    IBootstrap* bootstrap,
    NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config)
    : TBase(
        bootstrap,
        static_cast<NYT::NOrm::NClient::NObjects::TObjectTypeValue>(EObjectType::WatchLogConsumer))
    , Config_(std::move(config))
    , KeyFields_({
        &WatchLogConsumersTable.Fields.MetaId})
    , NullKey_(NYT::NOrm::NServer::NObjects::GenerateZeroKey(KeyFields_))
{ }

void TWatchLogConsumerTypeHandler::Initialize()
{
    TBase::Initialize();

    MetaAttributeSchema_
        ->AddChildren({
        MakeScalarAttributeSchema("acl", TDataModelObject::AclDescriptor)
            ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::Updatable)
            ->AddValidator<NYT::NOrm::NServer::NObjects::TObject>(std::bind_front(
                &TWatchLogConsumerTypeHandler::ValidateAcl, this))
    });
    MetaAttributeSchema_
        ->AddChildren({
            MakeIdAttributeSchema(
                "id",
                &WatchLogConsumersTable.Fields.MetaId,
                MakeValueGetter(&TWatchLogConsumer::GetId))
                ->SetKeyFieldPolicy(
                    NYT::NOrm::NServer::NObjects::CreateStringAttributePolicy(
                        NYT::NOrm::NServer::NObjects::EAttributeGenerationPolicy::Random,
                        NYT::NOrm::NServer::NObjects::DefaultMinStringAttributeLength,
                        NYT::NOrm::NServer::NObjects::DefaultMaxStringAttributeLength,
                        NYT::NOrm::NServer::NObjects::DefaultStringAttributeValidChars)),
            MakeScalarAttributeSchema("finalization_start_time", TWatchLogConsumer::FinalizationStartTimeDescriptor)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueReadOnly),
            MakeScalarAttributeSchema("finalizers", TWatchLogConsumer::FinalizersDescriptor)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueReadOnly),
            MakeScalarAttributeSchema("ultimate_question_of_life")
                ->SetComputed()
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::ReadOnly)
                ->SetOpaque(/*emitEntity*/ false),
            TBase::MakeEtcAttributeSchema(TWatchLogConsumer::MetaEtcDescriptor.AddValidator(
                std::bind_front(&TWatchLogConsumerTypeHandler::ValidateMetaEtcOnValueUpdate, this)))
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::Updatable)
                ->template AddValidator<TWatchLogConsumer>(std::bind_front(
                    &TWatchLogConsumerTypeHandler::ValidateMetaEtcOnTransactionCommit, this))
            });

    SpecAttributeSchema_
        ->AddChildren({
            MakeEtcAttributeSchema(TWatchLogConsumer::TSpec::EtcDescriptor),
        });

    StatusAttributeSchema_
        ->AddChildren({
            MakeEtcAttributeSchema(TWatchLogConsumer::TStatus::EtcDescriptor),
        });

    ControlAttributeSchema_
        ->AddChildren({
            MakeScalarAttributeSchema("touch")
                ->SetControl<TWatchLogConsumer, NYT::NOrm::NExample::NClient::NProto::NDataModel::TObjectTouch>(
                    [] (NYT::NOrm::NServer::NObjects::TTransaction* transaction,
                        TWatchLogConsumer* object,
                        const auto& touch)
                    {
                        if (touch.lock_removal()) {
                            auto* session = transaction->GetSession();
                            session->ScheduleStore(
                                [object] (NYT::NOrm::NServer::NObjects::IStoreContext* context) {
                                    auto* typeHandler = object->GetTypeHandler();
                                    // Lock is taken to prevent success of concurrent RemoveObject query.
                                    // Without it, no write set would be added and
                                    // `RemoveObject` may be committed before `/control/touch` update.
                                    context->LockRow(
                                        typeHandler->GetTable(),
                                        typeHandler->GetObjectTableKey(object),
                                        std::array{&NYT::NOrm::NServer::NObjects::ObjectsTable.Fields.ExistenceLock},
                                        NYT::NTableClient::ELockType::SharedStrong);
                                });
                        }
                        if (touch.store_event_to_history()) {
                            THROW_ERROR_EXCEPTION("History is disabled for %v",
                                object->GetDisplayName());
                            object->ControlTouchEventsToSave().insert("touch");
                        }
                    }),
        });
}

void TWatchLogConsumerTypeHandler::PostInitialize()
{
    // History is configured in PostInitialize stage, as users may add some evaluated fields via plugins in Initialize stage.
    // NB: Called before TBase::PostInitialize as base type handler prepares cache.
    ConfigureHistory(
        /*attributes*/ {},
        /*filter*/ std::nullopt);

    TBase::PostInitialize();

    FillUpdatePolicies();
}

bool TWatchLogConsumerTypeHandler::IsObjectNameSupported() const
{
    return false;
}

bool TWatchLogConsumerTypeHandler::SkipStoreWithoutChanges() const
{
    return false;
}

bool TWatchLogConsumerTypeHandler::ForceZeroKeyEvaluation() const
{
    return false;
}

std::vector<NYT::NOrm::NServer::NObjects::TWatchLog>
TWatchLogConsumerTypeHandler::GetWatchLogs() const
{
    return std::vector<NYT::NOrm::NServer::NObjects::TWatchLog>{
    };
}

const NYT::NYson::TProtobufMessageType*
TWatchLogConsumerTypeHandler::GetRootProtobufType() const
{
    static const auto* type = NYT::NYson::ReflectProtobufMessageType<NYT::NOrm::NExample::NClient::NProto::NDataModel::TWatchLogConsumer>();
    return type;
}

const TDBFields& TWatchLogConsumerTypeHandler::GetKeyFields() const
{
    return KeyFields_;
}

NYT::NOrm::NClient::NObjects::TObjectKey TWatchLogConsumerTypeHandler::GetNullKey() const
{
    return NullKey_;
}

const NYT::NOrm::NServer::NObjects::TDBTable* TWatchLogConsumerTypeHandler::GetTable() const
{
    return &WatchLogConsumersTable;
}

const NYT::NOrm::NServer::NObjects::TDBTable* TWatchLogConsumerTypeHandler::GetParentsTable() const
{
    return nullptr;
}

std::unique_ptr<NYT::NOrm::NServer::NObjects::TObject>
TWatchLogConsumerTypeHandler::InstantiateObject(
    const NYT::NOrm::NClient::NObjects::TObjectKey& key,
    const NYT::NOrm::NClient::NObjects::TObjectKey& parentKey,
    ISession* session)
{
    YT_VERIFY(!parentKey);

    return std::make_unique<TWatchLogConsumer>(
        std::get<TString>(key[0]),
        this,
        session);
}

void TWatchLogConsumerTypeHandler::InitializeCreatedObject(
    NYT::NOrm::NServer::NObjects::TTransaction* transaction,
    NYT::NOrm::NServer::NObjects::TObject* object)
{
    TBase::InitializeCreatedObject(transaction, object);
    auto* typedObject = object->As<TWatchLogConsumer>();
    YT_VERIFY(typedObject);
    typedObject->MetaEtc().MutableLoad()->set_uuid(NYT::NOrm::NClient::NObjects::GenerateUuid());
}

void TWatchLogConsumerTypeHandler::ValidateCreatedObject(
    NYT::NOrm::NServer::NObjects::TTransaction* transaction,
    NYT::NOrm::NServer::NObjects::TObject* object)
{
    TBase::ValidateCreatedObject(transaction, object);
    auto* typedObject = object->As<TWatchLogConsumer>();
    YT_VERIFY(typedObject);
}

void TWatchLogConsumerTypeHandler::ValidateMetaEtcOnTransactionCommit(
    NYT::NOrm::NServer::NObjects::TTransaction*,
    const TWatchLogConsumer* object)
{
    const auto& metaEtcOld = object->MetaEtc().LoadOld();
    const auto& metaEtcNew = object->MetaEtc().Load();

    // Sanity check.
    ValidateMetaUuid(metaEtcOld, metaEtcNew);

    if (metaEtcNew.name() && !this->IsObjectNameSupported()) {
        THROW_ERROR_EXCEPTION("Cannot set /meta/name for %v objects",
            NYT::NOrm::NClient::NObjects::GetGlobalObjectTypeRegistry()
                ->GetHumanReadableTypeNameOrCrash(this->GetType()));
    }
}

void TWatchLogConsumerTypeHandler::ValidateMetaEtcOnValueUpdate(
    NYT::NOrm::NServer::NObjects::TTransaction*,
    const TWatchLogConsumer* object,
    const TWatchLogConsumer::TMetaEtc& metaEtcOld,
    const TWatchLogConsumer::TMetaEtc& metaEtcNew)
{
    ValidateMetaUuid(metaEtcOld, metaEtcNew);

    // All user-defined attributes are considered updatable by any user
    // having corresponding write permission.
    Y_UNUSED(object);
}

void TWatchLogConsumerTypeHandler::ValidateMetaUuid(
    const TWatchLogConsumer::TMetaEtc& metaEtcOld,
    const TWatchLogConsumer::TMetaEtc& metaEtcNew)
{
    // Uuid protects, for example, YP secrets from object identity theft, i.e. from ABA race conditions,
    // so we try hard to make sure uuid doesn't change.
    if (!metaEtcOld.uuid().empty() && metaEtcOld.uuid() != metaEtcNew.uuid()) {
        this->GetBootstrap()->GetAccessControlManager()->ValidateSuperuser("change /meta/uuid");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NExample::NServer::NLibrary

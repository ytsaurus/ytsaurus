// AUTOMATICALLY GENERATED. DO NOT EDIT!

#include "type_handler_impls.h"

#include <yt/yt/orm/server/objects/attribute_schema.h>
#include <yt/yt/orm/server/objects/build_tags.h>
#include <yt/yt/orm/server/objects/watch_log.h>

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

TAuthorTypeHandler::TAuthorTypeHandler(
    IBootstrap* bootstrap,
    NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config)
    : TBase(
        bootstrap,
        static_cast<NYT::NOrm::NClient::NObjects::TObjectTypeValue>(EObjectType::Author))
    , Config_(std::move(config))
    , KeyFields_({
        &AuthorsTable.Fields.MetaId})
    , NullKey_(NYT::NOrm::NServer::NObjects::GenerateZeroKey(KeyFields_))
{ }

void TAuthorTypeHandler::Initialize()
{
    TBase::Initialize();

    MetaAttributeSchema_
        ->AddChildren({
        MakeScalarAttributeSchema("acl", TDataModelObject::AclDescriptor)
            ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::Updatable)
            ->AddValidator<NYT::NOrm::NServer::NObjects::TObject>(std::bind_front(
                &TAuthorTypeHandler::ValidateAcl, this))
    });
    MetaAttributeSchema_
        ->AddChildren({
            MakeIdAttributeSchema(
                "id",
                &AuthorsTable.Fields.MetaId,
                MakeValueGetter(&TAuthor::GetId))
                ->SetKeyFieldPolicy(
                    NYT::NOrm::NServer::NObjects::CreateStringAttributePolicy(
                        NYT::NOrm::NServer::NObjects::EAttributeGenerationPolicy::Random,
                        NYT::NOrm::NServer::NObjects::DefaultMinStringAttributeLength,
                        NYT::NOrm::NServer::NObjects::DefaultMaxStringAttributeLength,
                        NYT::NOrm::NServer::NObjects::DefaultStringAttributeValidChars)),
            MakeScalarAttributeSchema("finalization_start_time", TAuthor::FinalizationStartTimeDescriptor)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueReadOnly),
            MakeScalarAttributeSchema("finalizers", TAuthor::FinalizersDescriptor)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueReadOnly),
            MakeScalarAttributeSchema("ultimate_question_of_life")
                ->SetComputed()
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::ReadOnly)
                ->SetOpaque(/*emitEntity*/ false),
            TBase::MakeEtcAttributeSchema(TAuthor::MetaEtcDescriptor.AddValidator(
                std::bind_front(&TAuthorTypeHandler::ValidateMetaEtcOnValueUpdate, this)))
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::Updatable)
                ->template AddValidator<TAuthor>(std::bind_front(
                    &TAuthorTypeHandler::ValidateMetaEtcOnTransactionCommit, this))
            });

    SpecAttributeSchema_
        ->AddChildren({
            MakeScalarAttributeSchema("name", TAuthor::TSpec::NameDescriptor),
            MakeScalarAttributeSchema("age", TAuthor::TSpec::AgeDescriptor),
            MakeEtcAttributeSchema(TAuthor::TSpec::EtcDescriptor),
        });

    StatusAttributeSchema_
        ->AddChildren({
            MakeEtcAttributeSchema(TAuthor::TStatus::EtcDescriptor),
            MakeScalarAttributeSchema("book_refs", TAuthor::TStatus::BookRefsDescriptor),
        });

    ControlAttributeSchema_
        ->AddChildren({
            MakeScalarAttributeSchema("touch")
                ->SetControl<TAuthor, NYT::NOrm::NExample::NClient::NProto::NDataModel::TObjectTouch>(
                    [] (NYT::NOrm::NServer::NObjects::TTransaction* transaction,
                        TAuthor* object,
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

void TAuthorTypeHandler::PostInitialize()
{
    // History is configured in PostInitialize stage, as users may add some evaluated fields via plugins in Initialize stage.
    // NB: Called before TBase::PostInitialize as base type handler prepares cache.
    ConfigureHistory(
        /*attributes*/ {
            THistoryAttribute{
                .Path = "/spec/age",
                .Indexed = false,
                .AllowedInFilter = true,
            },
            THistoryAttribute{
                .Path = "/spec/name",
                .Indexed = false,
            },},
        /*filter*/ std::nullopt);

    TBase::PostInitialize();

    FillUpdatePolicies();
}

bool TAuthorTypeHandler::IsObjectNameSupported() const
{
    return false;
}

bool TAuthorTypeHandler::SkipStoreWithoutChanges() const
{
    return false;
}

bool TAuthorTypeHandler::ForceZeroKeyEvaluation() const
{
    return false;
}

std::vector<NYT::NOrm::NServer::NObjects::TWatchLog>
TAuthorTypeHandler::GetWatchLogs() const
{
    return std::vector<NYT::NOrm::NServer::NObjects::TWatchLog>{
        NYT::NOrm::NServer::NObjects::TWatchLog{
            .ObjectName = "author",
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
TAuthorTypeHandler::GetRootProtobufType() const
{
    static const auto* type = NYT::NYson::ReflectProtobufMessageType<NYT::NOrm::NExample::NClient::NProto::NDataModel::TAuthor>();
    return type;
}

const TDBFields& TAuthorTypeHandler::GetKeyFields() const
{
    return KeyFields_;
}

NYT::NOrm::NClient::NObjects::TObjectKey TAuthorTypeHandler::GetNullKey() const
{
    return NullKey_;
}

const NYT::NOrm::NServer::NObjects::TDBTable* TAuthorTypeHandler::GetTable() const
{
    return &AuthorsTable;
}

const NYT::NOrm::NServer::NObjects::TDBTable* TAuthorTypeHandler::GetParentsTable() const
{
    return nullptr;
}

std::vector<NYT::NOrm::NServer::NAccessControl::TAccessControlEntry>
TAuthorTypeHandler::GetDefaultAcl()
{
    return {
        {
            .Action = NYT::NOrm::NServer::NAccessControl::EAccessControlAction::Allow,
            .Permissions = {
                NYT::NOrm::NServer::NAccessControl::TAccessControlPermissionValues::Write,
            },
            .Attributes = {
                "/spec",
            },
        },
        {
            .Action = NYT::NOrm::NServer::NAccessControl::EAccessControlAction::Allow,
            .Permissions = {
                NYT::NOrm::NServer::NAccessControl::TAccessControlPermissionValues::Read,
            },
        },
    };
}

std::unique_ptr<NYT::NOrm::NServer::NObjects::TObject>
TAuthorTypeHandler::InstantiateObject(
    const NYT::NOrm::NClient::NObjects::TObjectKey& key,
    const NYT::NOrm::NClient::NObjects::TObjectKey& parentKey,
    ISession* session)
{
    YT_VERIFY(!parentKey);

    return std::make_unique<TAuthor>(
        std::get<TString>(key[0]),
        this,
        session);
}

void TAuthorTypeHandler::InitializeCreatedObject(
    NYT::NOrm::NServer::NObjects::TTransaction* transaction,
    NYT::NOrm::NServer::NObjects::TObject* object)
{
    TBase::InitializeCreatedObject(transaction, object);
    auto* typedObject = object->As<TAuthor>();
    YT_VERIFY(typedObject);
    typedObject->MetaEtc().MutableLoad()->set_uuid(NYT::NOrm::NClient::NObjects::GenerateUuid());
}

void TAuthorTypeHandler::ValidateCreatedObject(
    NYT::NOrm::NServer::NObjects::TTransaction* transaction,
    NYT::NOrm::NServer::NObjects::TObject* object)
{
    TBase::ValidateCreatedObject(transaction, object);
    auto* typedObject = object->As<TAuthor>();
    YT_VERIFY(typedObject);
}

void TAuthorTypeHandler::ValidateMetaEtcOnTransactionCommit(
    NYT::NOrm::NServer::NObjects::TTransaction*,
    const TAuthor* object)
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

void TAuthorTypeHandler::ValidateMetaEtcOnValueUpdate(
    NYT::NOrm::NServer::NObjects::TTransaction*,
    const TAuthor* object,
    const TAuthor::TMetaEtc& metaEtcOld,
    const TAuthor::TMetaEtc& metaEtcNew)
{
    ValidateMetaUuid(metaEtcOld, metaEtcNew);

    // All user-defined attributes are considered updatable by any user
    // having corresponding write permission.
    Y_UNUSED(object);
}

void TAuthorTypeHandler::ValidateMetaUuid(
    const TAuthor::TMetaEtc& metaEtcOld,
    const TAuthor::TMetaEtc& metaEtcNew)
{
    // Uuid protects, for example, YP secrets from object identity theft, i.e. from ABA race conditions,
    // so we try hard to make sure uuid doesn't change.
    if (!metaEtcOld.uuid().empty() && metaEtcOld.uuid() != metaEtcNew.uuid()) {
        this->GetBootstrap()->GetAccessControlManager()->ValidateSuperuser("change /meta/uuid");
    }
}

////////////////////////////////////////////////////////////////////////////////

TBookTypeHandler::TBookTypeHandler(
    IBootstrap* bootstrap,
    NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config)
    : TBase(
        bootstrap,
        static_cast<NYT::NOrm::NClient::NObjects::TObjectTypeValue>(EObjectType::Book))
    , Config_(std::move(config))
    , KeyFields_({
        &BooksTable.Fields.MetaId,
        &BooksTable.Fields.MetaId2})
    , ParentKeyFields_({
        &BooksTable.Fields.MetaPublisherId})
    , NullKey_(NYT::NOrm::NServer::NObjects::ParseObjectKey("0;0", KeyFields_))
{ }

void TBookTypeHandler::Initialize()
{
    TBase::Initialize();

    MetaAttributeSchema_
        ->AddChildren({
        MakeScalarAttributeSchema("acl", TDataModelObject::AclDescriptor)
            ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::Updatable)
            ->AddValidator<NYT::NOrm::NServer::NObjects::TObject>(std::bind_front(
                &TBookTypeHandler::ValidateAcl, this))
    });
    MetaAttributeSchema_
        ->AddChildren({
            MakeParentIdAttributeSchema(
                "publisher_id",
                &BooksTable.Fields.MetaPublisherId,
                MakeValueGetter(&TBook::PublisherId)),
            MakeIdAttributeSchema(
                "id",
                &BooksTable.Fields.MetaId,
                MakeValueGetter(&TBook::GetId))
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
            MakeIdAttributeSchema(
                "id2",
                &BooksTable.Fields.MetaId2,
                MakeValueGetter(&TBook::GetId2))
                ->SetKeyFieldPolicy(
                    NYT::NOrm::NServer::NObjects::CreateIntegerAttributePolicy<i64>(
                        NYT::NOrm::NServer::NObjects::EAttributeGenerationPolicy::Random,
                        GetBootstrap(),
                        NYT::NOrm::NServer::NObjects::DefaultMinIntegerAttributeValue<i64>,
                        NYT::NOrm::NServer::NObjects::DefaultMaxIntegerAttributeValue<i64>,
                        NYT::NOrm::NServer::NObjects::TAttributePolicyOptions{
                            .Type = GetType(),
                            .AttributePath = "/meta/id2",
                            .IndexForIncrement = ""
                        })),
            MakeScalarAttributeSchema("isbn", TBook::IsbnDescriptor)
                ->EnableMetaResponseAttribute(),
            MakeScalarAttributeSchema("finalization_start_time", TBook::FinalizationStartTimeDescriptor)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueReadOnly),
            MakeScalarAttributeSchema("finalizers", TBook::FinalizersDescriptor)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueReadOnly),
            MakeScalarAttributeSchema("ultimate_question_of_life")
                ->SetComputed()
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::ReadOnly)
                ->SetOpaque(/*emitEntity*/ false),
            TBase::MakeEtcAttributeSchema(TBook::MetaEtcDescriptor.AddValidator(
                std::bind_front(&TBookTypeHandler::ValidateMetaEtcOnValueUpdate, this)))
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::Updatable)
                ->template AddValidator<TBook>(std::bind_front(
                    &TBookTypeHandler::ValidateMetaEtcOnTransactionCommit, this))
            });

    SpecAttributeSchema_
        ->AddChildren({
            MakeCompositeAttributeSchema("peer_review")
                ->AddChildren({
                    MakeScalarAttributeSchema("reviewer_ids", TBook::TSpec::TPeerReview::ReviewerIdsDescriptor),
                    MakeEtcAttributeSchema(TBook::TSpec::TPeerReview::EtcDescriptor),
                    MakeScalarAttributeSchema("reviewers", TBook::TSpec::TPeerReview::ReviewersDescriptor),
                }),
            MakeScalarAttributeSchema("name", TBook::TSpec::NameDescriptor),
            MakeScalarAttributeSchema("year", TBook::TSpec::YearDescriptor),
            MakeScalarAttributeSchema("font", TBook::TSpec::FontDescriptor),
            MakeScalarAttributeSchema("genres", TBook::TSpec::GenresDescriptor),
            MakeScalarAttributeSchema("keywords", TBook::TSpec::KeywordsDescriptor),
            MakeScalarAttributeSchema("editor_id", TBook::TSpec::EditorIdDescriptor),
            MakeScalarAttributeSchema("digital_data", TBook::TSpec::DigitalDataDescriptor),
            MakeScalarAttributeSchema("author_ids", TBook::TSpec::AuthorIdsDescriptor),
            MakeScalarAttributeSchema("illustrator_id", TBook::TSpec::IllustratorIdDescriptor),
            MakeScalarAttributeSchema("alternative_publisher_ids", TBook::TSpec::AlternativePublisherIdsDescriptor),
            MakeScalarAttributeSchema("chapter_descriptions", TBook::TSpec::ChapterDescriptionsDescriptor),
            MakeScalarAttributeSchema("cover_illustrator_id", TBook::TSpec::CoverIllustratorIdDescriptor),
            MakeScalarAttributeSchema("api_revision", TBook::TSpec::ApiRevisionDescriptor)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueReadOnly),
            MakeEtcAttributeSchema(TBook::TSpec::EtcDescriptor),
            MakeEtcAttributeSchema("warehouse", TBook::TSpec::WarehouseEtcDescriptor),
            MakeScalarAttributeSchema("author_refs", TBook::TSpec::AuthorRefsDescriptor),
            MakeScalarAttributeSchema("editor", TBook::TSpec::EditorDescriptor),
            MakeScalarAttributeSchema("illustrator", TBook::TSpec::IllustratorDescriptor),
            MakeScalarAttributeSchema("publishers", TBook::TSpec::PublishersDescriptor),
            MakeScalarAttributeSchema("cover_illustrator", TBook::TSpec::CoverIllustratorDescriptor),
            MakeScalarAttributeSchema("authors", TBook::TSpec::AuthorsDescriptor),
        });

    StatusAttributeSchema_
        ->AddChildren({
            MakeScalarAttributeSchema("released", TBook::TStatus::ReleasedDescriptor),
            MakeEtcAttributeSchema(TBook::TStatus::EtcDescriptor),
            MakeScalarAttributeSchema("hitchhikers", TBook::TStatus::HitchhikersDescriptor),
            MakeScalarAttributeSchema("formed_hitchhikers", TBook::TStatus::FormedHitchhikersDescriptor),
            MakeScalarAttributeSchema("hitchhikers_view", TBook::TStatus::HitchhikersViewDescriptor),
            MakeScalarAttributeSchema("formed_hitchhikers_view", TBook::TStatus::FormedHitchhikersViewDescriptor),
        });
    PrepareRevisionTracker(
        /*trackedPaths*/ {
            "/spec",
        },
        /*trackerPath*/ "/spec/api_revision",
        /*lockGroupRestrictionEnabled*/ true);

    ControlAttributeSchema_
        ->AddChildren({
            MakeScalarAttributeSchema("money_touch")
                ->SetControl<TBook, NYT::NOrm::NExample::NClient::NProto::NDataModel::TObjectTouch>(
                    [] (NYT::NOrm::NServer::NObjects::TTransaction* transaction,
                        TBook* object,
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
                            object->ControlTouchEventsToSave().insert("money_touch");
                        }
                    }),
            MakeScalarAttributeSchema("touch")
                ->SetControl<TBook, NYT::NOrm::NExample::NClient::NProto::NDataModel::TObjectTouch>(
                    [] (NYT::NOrm::NServer::NObjects::TTransaction* transaction,
                        TBook* object,
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
                ->SetControl<TBook, NYT::NOrm::NExample::NClient::NProto::NDataModel::TObjectTouchIndex>(
                [] (NYT::NOrm::NServer::NObjects::TTransaction* /*transaction*/,
                    TBook* object,
                    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TObjectTouchIndex& touchIndex)
                {
                    for (const auto& indexName : touchIndex.index_names()) {
                        object->GetIndexOrThrow(indexName)->TouchIndex();
                    }
                }),
            MakeScalarAttributeSchema("touch_revision")
                ->SetControlWithTransactionCallContext<TBook, NYT::NOrm::NExample::NClient::NProto::NDataModel::TObjectTouchRevision>(
                    [&] (NYT::NOrm::NServer::NObjects::TTransaction* transaction,
                        TBook* object,
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
    RegisterScalarAttributeIndex(
        "books_by_authors",
        std::make_unique<NYT::NOrm::NServer::NObjects::TScalarAttributeIndexDescriptor>(
            "books_by_authors",
            &AuthorsToBooksTable,
            std::vector<NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor>{
                NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor(
                    &TBook::TSpec::AuthorIdsDescriptor,
                    ""),
            },
            std::vector<NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor>{
            },
            /*repeated*/ true,
            Config_->TryGetIndexMode("books_by_authors")
                .value_or(NYT::NOrm::NServer::NObjects::EIndexMode::Enabled)));
    RegisterScalarAttributeIndex(
        "books_by_cover_dpi",
        std::make_unique<NYT::NOrm::NServer::NObjects::TScalarAttributeIndexDescriptor>(
            "books_by_cover_dpi",
            &BooksByCoverDpiTable,
            std::vector<NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor>{
                NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor(
                    &TBook::TSpec::EtcDescriptor,
                    "/design/cover/dpi"),
            },
            std::vector<NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor>{
            },
            /*repeated*/ false,
            Config_->TryGetIndexMode("books_by_cover_dpi")
                .value_or(NYT::NOrm::NServer::NObjects::EIndexMode::Enabled)));
    RegisterScalarAttributeIndex(
        "books_by_cover_image",
        std::make_unique<NYT::NOrm::NServer::NObjects::TScalarAttributeIndexDescriptor>(
            "books_by_cover_image",
            &BooksByCoverImageTable,
            std::vector<NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor>{
                NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor(
                    &TBook::TSpec::EtcDescriptor,
                    "/design/cover/image"),
            },
            std::vector<NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor>{
            },
            /*repeated*/ false,
            Config_->TryGetIndexMode("books_by_cover_image")
                .value_or(NYT::NOrm::NServer::NObjects::EIndexMode::Enabled)));
    RegisterScalarAttributeIndex(
        "books_by_cover_size",
        std::make_unique<NYT::NOrm::NServer::NObjects::TScalarAttributeIndexDescriptor>(
            "books_by_cover_size",
            &BooksByCoverSizeTable,
            std::vector<NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor>{
                NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor(
                    &TBook::TSpec::EtcDescriptor,
                    "/design/cover/size"),
            },
            std::vector<NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor>{
            },
            /*repeated*/ false,
            Config_->TryGetIndexMode("books_by_cover_size")
                .value_or(NYT::NOrm::NServer::NObjects::EIndexMode::Enabled)));
    RegisterScalarAttributeIndex(
        "books_by_creation_time",
        std::make_unique<NYT::NOrm::NServer::NObjects::TScalarAttributeIndexDescriptor>(
            "books_by_creation_time",
            &BooksByCreationTimeTable,
            std::vector<NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor>{
                NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor(
                    &TBook::CreationTimeDescriptor,
                    ""),
            },
            std::vector<NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor>{
            },
            /*repeated*/ false,
            Config_->TryGetIndexMode("books_by_creation_time")
                .value_or(NYT::NOrm::NServer::NObjects::EIndexMode::Enabled)));
    RegisterScalarAttributeIndex(
        "books_by_editor",
        std::make_unique<NYT::NOrm::NServer::NObjects::TScalarAttributeIndexDescriptor>(
            "books_by_editor",
            &EditorToBooksTable,
            std::vector<NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor>{
                NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor(
                    &TBook::TSpec::EditorIdDescriptor,
                    ""),
            },
            std::vector<NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor>{
            },
            /*repeated*/ false,
            Config_->TryGetIndexMode("books_by_editor")
                .value_or(NYT::NOrm::NServer::NObjects::EIndexMode::Enabled)));
    RegisterScalarAttributeIndex(
        "books_by_editor_and_year_with_predicate",
        std::make_unique<NYT::NOrm::NServer::NObjects::TScalarAttributeIndexDescriptor>(
            "books_by_editor_and_year_with_predicate",
            &BooksByEditorAndYearWithPredicateTable,
            std::vector<NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor>{
                NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor(
                    &TBook::TSpec::EditorIdDescriptor,
                    ""),
                NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor(
                    &TBook::TSpec::YearDescriptor,
                    ""),
            },
            std::vector<NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor>{
                NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor(
                    &TBook::TSpec::FontDescriptor,
                    ""),
            },
            /*repeated*/ false,
            Config_->TryGetIndexMode("books_by_editor_and_year_with_predicate")
                .value_or(NYT::NOrm::NServer::NObjects::EIndexMode::Enabled),
            NYT::NOrm::NServer::NObjects::TPredicateInfo{
                .Expression = "[/spec/font] != \"YS Text\"",
                .AttributesToValidate = {"/spec/font"}
            }));
    RegisterScalarAttributeIndex(
        "books_by_font",
        std::make_unique<NYT::NOrm::NServer::NObjects::TScalarAttributeIndexDescriptor>(
            "books_by_font",
            &BooksByFontTable,
            std::vector<NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor>{
                NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor(
                    &TBook::TSpec::FontDescriptor,
                    ""),
            },
            std::vector<NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor>{
            },
            /*repeated*/ false,
            Config_->TryGetIndexMode("books_by_font")
                .value_or(NYT::NOrm::NServer::NObjects::EIndexMode::Enabled)));
    RegisterScalarAttributeIndex(
        "books_by_genres",
        std::make_unique<NYT::NOrm::NServer::NObjects::TScalarAttributeIndexDescriptor>(
            "books_by_genres",
            &BooksByGenresTable,
            std::vector<NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor>{
                NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor(
                    &TBook::TSpec::GenresDescriptor,
                    ""),
            },
            std::vector<NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor>{
            },
            /*repeated*/ true,
            Config_->TryGetIndexMode("books_by_genres")
                .value_or(NYT::NOrm::NServer::NObjects::EIndexMode::Enabled)));
    RegisterScalarAttributeIndex(
        "books_by_illustrations",
        std::make_unique<NYT::NOrm::NServer::NObjects::TScalarAttributeIndexDescriptor>(
            "books_by_illustrations",
            &BooksByIllustrationsTable,
            std::vector<NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor>{
                NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor(
                    &TBook::TSpec::EtcDescriptor,
                    "/design/illustrations"),
            },
            std::vector<NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor>{
            },
            /*repeated*/ true,
            Config_->TryGetIndexMode("books_by_illustrations")
                .value_or(NYT::NOrm::NServer::NObjects::EIndexMode::Enabled)));
    RegisterScalarAttributeIndex(
        "books_by_illustrations_with_predicate",
        std::make_unique<NYT::NOrm::NServer::NObjects::TScalarAttributeIndexDescriptor>(
            "books_by_illustrations_with_predicate",
            &BooksByIllustrationsWithPredicateTable,
            std::vector<NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor>{
                NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor(
                    &TBook::TSpec::EtcDescriptor,
                    "/design/illustrations"),
            },
            std::vector<NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor>{
                NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor(
                    &TBook::TSpec::FontDescriptor,
                    ""),
                NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor(
                    &TBook::TSpec::EtcDescriptor,
                    "/design/illustrations"),
            },
            /*repeated*/ true,
            Config_->TryGetIndexMode("books_by_illustrations_with_predicate")
                .value_or(NYT::NOrm::NServer::NObjects::EIndexMode::Enabled),
            NYT::NOrm::NServer::NObjects::TPredicateInfo{
                .Expression = "[/spec/font] != \"Arial\" or list_contains([/spec/design/illustrations], \"cat.svg\")",
                .AttributesToValidate = {"/spec/font", "/spec/design/illustrations"}
            }));
    RegisterScalarAttributeIndex(
        "books_by_isbn",
        std::make_unique<NYT::NOrm::NServer::NObjects::TScalarAttributeIndexDescriptor>(
            "books_by_isbn",
            &BooksByIsbnTable,
            std::vector<NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor>{
                NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor(
                    &TBook::IsbnDescriptor,
                    ""),
            },
            std::vector<NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor>{
            },
            /*repeated*/ false,
            Config_->TryGetIndexMode("books_by_isbn")
                .value_or(NYT::NOrm::NServer::NObjects::EIndexMode::Enabled)));
    RegisterScalarAttributeIndex(
        "books_by_isbn_with_predicate",
        std::make_unique<NYT::NOrm::NServer::NObjects::TScalarAttributeIndexDescriptor>(
            "books_by_isbn_with_predicate",
            &BooksByIsbnWithPredicateTable,
            std::vector<NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor>{
                NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor(
                    &TBook::IsbnDescriptor,
                    ""),
            },
            std::vector<NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor>{
                NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor(
                    &TBook::IsbnDescriptor,
                    ""),
            },
            /*repeated*/ false,
            Config_->TryGetIndexMode("books_by_isbn_with_predicate")
                .value_or(NYT::NOrm::NServer::NObjects::EIndexMode::Disabled),
            NYT::NOrm::NServer::NObjects::TPredicateInfo{
                .Expression = "[/meta/isbn] != 123123",
                .AttributesToValidate = {"/meta/isbn"}
            }));
    RegisterScalarAttributeIndex(
        "books_by_keywords",
        std::make_unique<NYT::NOrm::NServer::NObjects::TScalarAttributeIndexDescriptor>(
            "books_by_keywords",
            &BooksByKeywordsTable,
            std::vector<NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor>{
                NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor(
                    &TBook::TSpec::KeywordsDescriptor,
                    ""),
            },
            std::vector<NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor>{
            },
            /*repeated*/ true,
            Config_->TryGetIndexMode("books_by_keywords")
                .value_or(NYT::NOrm::NServer::NObjects::EIndexMode::Enabled)));
    RegisterScalarAttributeIndex(
        "books_by_name",
        std::make_unique<NYT::NOrm::NServer::NObjects::TScalarAttributeIndexDescriptor>(
            "books_by_name",
            &BooksByNameTable,
            std::vector<NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor>{
                NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor(
                    &TBook::TSpec::NameDescriptor,
                    ""),
            },
            std::vector<NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor>{
            },
            /*repeated*/ false,
            Config_->TryGetIndexMode("books_by_name")
                .value_or(NYT::NOrm::NServer::NObjects::EIndexMode::Disabled)));
    RegisterScalarAttributeIndex(
        "books_by_page_count",
        std::make_unique<NYT::NOrm::NServer::NObjects::TScalarAttributeIndexDescriptor>(
            "books_by_page_count",
            &BooksByPageCountTable,
            std::vector<NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor>{
                NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor(
                    &TBook::TSpec::EtcDescriptor,
                    "/page_count"),
            },
            std::vector<NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor>{
            },
            /*repeated*/ false,
            Config_->TryGetIndexMode("books_by_page_count")
                .value_or(NYT::NOrm::NServer::NObjects::EIndexMode::Building)));
    RegisterScalarAttributeIndex(
        "books_by_peer_reviewers",
        std::make_unique<NYT::NOrm::NServer::NObjects::TScalarAttributeIndexDescriptor>(
            "books_by_peer_reviewers",
            &AuthorsToBooksForPeerReviewersTable,
            std::vector<NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor>{
                NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor(
                    &TBook::TSpec::TPeerReview::ReviewerIdsDescriptor,
                    ""),
            },
            std::vector<NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor>{
            },
            /*repeated*/ true,
            Config_->TryGetIndexMode("books_by_peer_reviewers")
                .value_or(NYT::NOrm::NServer::NObjects::EIndexMode::Enabled)));
    RegisterScalarAttributeIndex(
        "books_by_store_rating",
        std::make_unique<NYT::NOrm::NServer::NObjects::TScalarAttributeIndexDescriptor>(
            "books_by_store_rating",
            &BooksByStoreRatingTable,
            std::vector<NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor>{
                NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor(
                    &TBook::TSpec::DigitalDataDescriptor,
                    "/store_rating"),
            },
            std::vector<NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor>{
            },
            /*repeated*/ false,
            Config_->TryGetIndexMode("books_by_store_rating")
                .value_or(NYT::NOrm::NServer::NObjects::EIndexMode::Enabled)));
    RegisterScalarAttributeIndex(
        "books_by_year",
        std::make_unique<NYT::NOrm::NServer::NObjects::TScalarAttributeIndexDescriptor>(
            "books_by_year",
            &BooksByYearTable,
            std::vector<NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor>{
                NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor(
                    &TBook::TSpec::YearDescriptor,
                    ""),
            },
            std::vector<NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor>{
            },
            /*repeated*/ false,
            Config_->TryGetIndexMode("books_by_year")
                .value_or(NYT::NOrm::NServer::NObjects::EIndexMode::Disabled)));
    RegisterScalarAttributeIndex(
        "books_by_year_and_font",
        std::make_unique<NYT::NOrm::NServer::NObjects::TScalarAttributeIndexDescriptor>(
            "books_by_year_and_font",
            &BooksByYearAndFontTable,
            std::vector<NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor>{
                NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor(
                    &TBook::TSpec::YearDescriptor,
                    ""),
                NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor(
                    &TBook::TSpec::FontDescriptor,
                    ""),
            },
            std::vector<NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor>{
            },
            /*repeated*/ false,
            Config_->TryGetIndexMode("books_by_year_and_font")
                .value_or(NYT::NOrm::NServer::NObjects::EIndexMode::Enabled)));
    RegisterScalarAttributeIndex(
        "books_by_year_and_page_count_with_predicate",
        std::make_unique<NYT::NOrm::NServer::NObjects::TScalarAttributeIndexDescriptor>(
            "books_by_year_and_page_count_with_predicate",
            &BooksByYearAndPageCountWithPredicateTable,
            std::vector<NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor>{
                NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor(
                    &TBook::TSpec::YearDescriptor,
                    ""),
                NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor(
                    &TBook::TSpec::EtcDescriptor,
                    "/page_count"),
            },
            std::vector<NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor>{
                NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor(
                    &TBook::TSpec::EtcDescriptor,
                    "/design/cover/hardcover"),
            },
            /*repeated*/ false,
            Config_->TryGetIndexMode("books_by_year_and_page_count_with_predicate")
                .value_or(NYT::NOrm::NServer::NObjects::EIndexMode::Enabled),
            NYT::NOrm::NServer::NObjects::TPredicateInfo{
                .Expression = "[/spec/design/cover/hardcover] = %true",
                .AttributesToValidate = {"/spec/design/cover/hardcover"}
            }));
    RegisterScalarAttributeIndex(
        "books_by_cover_size_with_predicate",
        std::make_unique<NYT::NOrm::NServer::NObjects::TScalarAttributeIndexDescriptor>(
            "books_by_cover_size_with_predicate",
            &BooksByYearWithPredicateTable,
            std::vector<NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor>{
                NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor(
                    &TBook::TSpec::YearDescriptor,
                    ""),
            },
            std::vector<NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor>{
                NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor(
                    &TBook::TSpec::EtcDescriptor,
                    "/design/cover/dpi"),
            },
            /*repeated*/ false,
            Config_->TryGetIndexMode("books_by_cover_size_with_predicate")
                .value_or(NYT::NOrm::NServer::NObjects::EIndexMode::Enabled),
            NYT::NOrm::NServer::NObjects::TPredicateInfo{
                .Expression = "[/spec/design/cover/dpi] > 100",
                .AttributesToValidate = {"/spec/design/cover/dpi"}
            }));
    AddAttributeSensor("/spec/page_count", NYT::NOrm::NClient::NProto::EAttributeSensorPolicy(1));
}

static const NYT::NOrm::NServer::NObjects::TTagsTreeNode BookTagsTree
{
    .Children = {
        {
            "meta",
            {
                .Children = {
                    {
                        "id",
                        {
                            .AddTags = TTagSet({ /*readonly*/ 4 }),
                        }
                    },
                    {
                        "id2",
                        {
                            .AddTags = TTagSet({ /*readonly*/ 4 }),
                        }
                    },
                    {
                        "isbn",
                        {
                        }
                    },
                    {
                        "custom_meta_etc_test",
                        {
                            .Etc = true,
                        }
                    },
                    {
                        "book_type",
                        {
                            .Etc = true,
                        }
                    },
                    {
                        "ultimate_question_of_life",
                        {
                            .AddTags = TTagSet({ /*computed*/ 1, /*opaque*/ 3, /*readonly*/ 4 }),
                        }
                    },
                    {
                        "acl",
                        {
                            .Children = {
                                {
                                    "action",
                                    {
                                        .Etc = true,
                                    }
                                },
                                {
                                    "permissions",
                                    {
                                        .Etc = true,
                                        .Repeated = true,
                                    }
                                },
                                {
                                    "subjects",
                                    {
                                        .Etc = true,
                                        .Repeated = true,
                                    }
                                },
                                {
                                    "attributes",
                                    {
                                        .Etc = true,
                                        .Repeated = true,
                                    }
                                },
                            },
                            .AddTags = TTagSet({ /*system*/ 0 }),
                            .Repeated = true,
                        }
                    },
                    {
                        "creation_time",
                        {
                            .AddTags = TTagSet({ /*system*/ 0, /*readonly*/ 4 }),
                        }
                    },
                    {
                        "inherit_acl",
                        {
                            .AddTags = TTagSet({ /*system*/ 0 }),
                        }
                    },
                    {
                        "uuid",
                        {
                            .AddTags = TTagSet({ /*readonly*/ 4 }),
                            .Etc = true,
                        }
                    },
                    {
                        "name",
                        {
                            .AddTags = TTagSet({ /*readonly*/ 4 }),
                            .Etc = true,
                        }
                    },
                    {
                        "fqid",
                        {
                            .AddTags = TTagSet({ /*system*/ 0, /*computed*/ 1, /*readonly*/ 4 }),
                        }
                    },
                    {
                        "key",
                        {
                            .AddTags = TTagSet({ /*system*/ 0, /*computed*/ 1, /*readonly*/ 4 }),
                        }
                    },
                    {
                        "parent_key",
                        {
                            .AddTags = TTagSet({ /*system*/ 0, /*computed*/ 1, /*readonly*/ 4 }),
                        }
                    },
                    {
                        "finalization_start_time",
                        {
                            .AddTags = TTagSet({ /*readonly*/ 4 }),
                        }
                    },
                    {
                        "finalizers",
                        {
                            .Children = {
                                {
                                    "user_id",
                                    {
                                        .Etc = true,
                                    }
                                },
                                {
                                    "completion_timestamp",
                                    {
                                        .Etc = true,
                                    }
                                },
                            },
                            .AddTags = TTagSet({ /*readonly*/ 4 }),
                        }
                    },
                    {
                        "active_finalizers",
                        {
                            .Children = {
                                {
                                    "user_id",
                                    {
                                        .Etc = true,
                                    }
                                },
                                {
                                    "completion_timestamp",
                                    {
                                        .Etc = true,
                                    }
                                },
                            },
                            .AddTags = TTagSet({ /*system*/ 0, /*computed*/ 1, /*opaque*/ 3, /*readonly*/ 4 }),
                        }
                    },
                    {
                        "publisher_id",
                        {
                        }
                    },
                    {
                        "type",
                        {
                        }
                    },
                },
                .MessageTags = TTagSet({ /*meta*/ 6 }),
            }
        },
        {
            "spec",
            {
                .Children = {
                    {
                        "name",
                        {
                        }
                    },
                    {
                        "year",
                        {
                        }
                    },
                    {
                        "font",
                        {
                        }
                    },
                    {
                        "genres",
                        {
                            .Repeated = true,
                        }
                    },
                    {
                        "keywords",
                        {
                            .AddTags = TTagSet({ /*digital_data*/ 66 }),
                            .Repeated = true,
                        }
                    },
                    {
                        "design",
                        {
                            .Children = {
                                {
                                    "cover",
                                    {
                                        .Children = {
                                            {
                                                "hardcover",
                                                {
                                                    .Etc = true,
                                                }
                                            },
                                            {
                                                "image",
                                                {
                                                    .Etc = true,
                                                }
                                            },
                                            {
                                                "size",
                                                {
                                                    .Etc = true,
                                                }
                                            },
                                            {
                                                "dpi",
                                                {
                                                    .Etc = true,
                                                }
                                            },
                                        },
                                        .Etc = true,
                                    }
                                },
                                {
                                    "color",
                                    {
                                        .Etc = true,
                                    }
                                },
                                {
                                    "illustrations",
                                    {
                                        .Etc = true,
                                        .Repeated = true,
                                    }
                                },
                            },
                            .Etc = true,
                        }
                    },
                    {
                        "editor_id",
                        {
                        }
                    },
                    {
                        "digital_data",
                        {
                            .Children = {
                                {
                                    "available_formats",
                                    {
                                        .Children = {
                                            {
                                                "format",
                                                {
                                                    .Etc = true,
                                                }
                                            },
                                            {
                                                "size",
                                                {
                                                    .Etc = true,
                                                }
                                            },
                                        },
                                        .Etc = true,
                                        .Repeated = true,
                                    }
                                },
                                {
                                    "store_rating",
                                    {
                                        .Etc = true,
                                    }
                                },
                                {
                                    "store_price",
                                    {
                                        .AddTags = TTagSet({ /*money*/ 67 }),
                                        .Etc = true,
                                    }
                                },
                            },
                            .MessageTags = TTagSet({ /*digital_data*/ 66 }),
                        }
                    },
                    {
                        "page_count",
                        {
                            .Etc = true,
                        }
                    },
                    {
                        "author_ids",
                        {
                            .Repeated = true,
                        }
                    },
                    {
                        "illustrator_id",
                        {
                        }
                    },
                    {
                        "alternative_publisher_ids",
                        {
                            .Repeated = true,
                        }
                    },
                    {
                        "chapter_descriptions",
                        {
                            .Children = {
                                {
                                    "name",
                                    {
                                        .AddTags = TTagSet({ /*public_data*/ 64 }),
                                        .Etc = true,
                                    }
                                },
                            },
                            .Repeated = true,
                        }
                    },
                    {
                        "description",
                        {
                            .Etc = true,
                        }
                    },
                    {
                        "cover_illustrator_id",
                        {
                        }
                    },
                    {
                        "api_revision",
                        {
                            .AddTags = TTagSet({ /*readonly*/ 4 }),
                        }
                    },
                    {
                        "in_stock",
                        {
                            .EtcName = "warehouse",
                            .Etc = true,
                        }
                    },
                    {
                        "peer_review",
                        {
                            .Children = {
                                {
                                    "reviewer_ids",
                                    {
                                        .Repeated = true,
                                    }
                                },
                                {
                                    "summary",
                                    {
                                        .Etc = true,
                                    }
                                },
                            },
                        }
                    },
                    {
                        "lock_type",
                        {
                            .Etc = true,
                        }
                    },
                },
                .MessageTags = TTagSet({ /*spec*/ 7 }),
            }
        },
        {
            "status",
            {
                .Children = {
                    {
                        "released",
                        {
                        }
                    },
                    {
                        "hitchhiker_ids",
                        {
                            .Etc = true,
                            .Repeated = true,
                        }
                    },
                    {
                        "formed_hitchhiker_ids",
                        {
                            .Etc = true,
                            .Repeated = true,
                        }
                    },
                },
                .MessageTags = TTagSet({ /*status*/ 8 }),
            }
        },
        {
            "control",
            {
                .Children = {
                    {
                        "touch",
                        {
                            .Children = {
                                {
                                    "lock_removal",
                                    {
                                        .Etc = true,
                                    }
                                },
                                {
                                    "store_event_to_history",
                                    {
                                        .Etc = true,
                                    }
                                },
                            },
                            .AddTags = TTagSet({ /*book_moderation*/ 68, /*touch*/ 5 }),
                            .Etc = true,
                        }
                    },
                    {
                        "money_touch",
                        {
                            .Children = {
                                {
                                    "lock_removal",
                                    {
                                        .Etc = true,
                                    }
                                },
                                {
                                    "store_event_to_history",
                                    {
                                        .Etc = true,
                                    }
                                },
                            },
                            .AddTags = TTagSet({ /*money*/ 67 }),
                            .Etc = true,
                        }
                    },
                    {
                        "touch_index",
                        {
                            .Children = {
                                {
                                    "index_names",
                                    {
                                        .Etc = true,
                                        .Repeated = true,
                                    }
                                },
                            },
                            .Etc = true,
                        }
                    },
                    {
                        "touch_revision",
                        {
                            .Children = {
                                {
                                    "path",
                                    {
                                        .Etc = true,
                                    }
                                },
                            },
                            .Etc = true,
                        }
                    },
                    {
                        "add_finalizer",
                        {
                            .Children = {
                                {
                                    "finalizer_name",
                                    {
                                        .Etc = true,
                                    }
                                },
                            },
                            .Etc = true,
                        }
                    },
                    {
                        "remove_finalizer",
                        {
                            .Children = {
                                {
                                    "finalizer_name",
                                    {
                                        .Etc = true,
                                    }
                                },
                            },
                            .Etc = true,
                        }
                    },
                    {
                        "complete_finalization",
                        {
                            .Children = {
                                {
                                    "finalizer_name",
                                    {
                                        .Etc = true,
                                    }
                                },
                            },
                            .Etc = true,
                        }
                    },
                },
                .MessageTags = TTagSet({ /*control*/ 9 }),
            }
        },
    },
    .AddTags = TTagSet({ /*any*/ 10 }),
};

void TBookTypeHandler::PostInitialize()
{
    // History is configured in PostInitialize stage, as users may add some evaluated fields via plugins in Initialize stage.
    // NB: Called before TBase::PostInitialize as base type handler prepares cache.
    ConfigureHistory(
        /*attributes*/ {
            THistoryAttribute{
                .Path = "/spec/alternative_publisher_ids",
                .Indexed = false,
            },
            THistoryAttribute{
                .Path = "/spec/design/cover/hardcover",
                .Indexed = false,
            },
            THistoryAttribute{
                .Path = "/spec/design/illustrations",
                .Indexed = false,
            },
            THistoryAttribute{
                .Path = "/spec/digital_data/available_formats",
                .Indexed = true,
                .AllowedInFilter = true,
            },
            THistoryAttribute{
                .Path = "/spec/font",
                .Indexed = false,
            },
            THistoryAttribute{
                .Path = "/spec/genres",
                .Indexed = false,
            },
            THistoryAttribute{
                .Path = "/spec/year",
                .Indexed = false,
            },
            THistoryAttribute{
                .Path = "/status/released",
                .Indexed = false,
            },
            THistoryAttribute{
                .Path = "/annotations/rating",
                .Indexed = false,
            },
            THistoryAttribute{
                .Path = "/labels",
                .Indexed = false,
            },},
        /*filter*/ std::nullopt);

    TBase::PostInitialize();
    BuildAttributeSchemaTags(
        RootAttributeSchema_,
        BookTagsTree);

    FillUpdatePolicies();
}

bool TBookTypeHandler::IsObjectNameSupported() const
{
    return false;
}

bool TBookTypeHandler::SkipStoreWithoutChanges() const
{
    return true;
}

bool TBookTypeHandler::ForceZeroKeyEvaluation() const
{
    return false;
}

std::vector<NYT::NOrm::NServer::NObjects::TWatchLog>
TBookTypeHandler::GetWatchLogs() const
{
    return std::vector<NYT::NOrm::NServer::NObjects::TWatchLog>{
        NYT::NOrm::NServer::NObjects::TWatchLog{
            .ObjectName = "book",
            .Name = "watch_log",
            .Filter{
                .Query{
                    ""
                }
            },
            .Selector{
            },
        },
        NYT::NOrm::NServer::NObjects::TWatchLog{
            .ObjectName = "book",
            .Name = "tagged_watch_log",
            .Filter{
                .Query{
                    ""
                }
            },
            .Selector{
            },
            .RequiredTags = TTagSet({ /*digital_data*/ 66 }),
            .ExcludedTags = TTagSet({ /*money*/ 67 }),
        },
    };
}

bool TBookTypeHandler::AreTagsEnabled() const
{
    return true;
}

const NYT::NYson::TProtobufMessageType*
TBookTypeHandler::GetRootProtobufType() const
{
    static const auto* type = NYT::NYson::ReflectProtobufMessageType<NYT::NOrm::NExample::NClient::NProto::NDataModel::TBook>();
    return type;
}

const TDBFields& TBookTypeHandler::GetKeyFields() const
{
    return KeyFields_;
}

NYT::NOrm::NClient::NObjects::TObjectKey TBookTypeHandler::GetNullKey() const
{
    return NullKey_;
}

const NYT::NOrm::NServer::NObjects::TDBTable* TBookTypeHandler::GetTable() const
{
    return &BooksTable;
}

const NYT::NOrm::NServer::NObjects::TDBTable* TBookTypeHandler::GetParentsTable() const
{
    return &BooksToPublishersTable;
}

NYT::NOrm::NClient::NObjects::TObjectTypeValue
TBookTypeHandler::GetParentType() const
{
    return static_cast<NYT::NOrm::NClient::NObjects::TObjectTypeValue>(EObjectType::Publisher);
}

const TDBFields& TBookTypeHandler::GetParentKeyFields() const
{
    return ParentKeyFields_;
}

NYT::NOrm::NServer::NObjects::TObject*
TBookTypeHandler::GetParent(
    const NYT::NOrm::NServer::NObjects::TObject* object,
    std::source_location location)
{
    return object->As<TBook>()
        ->Publisher().Load(location);
}

std::vector<NYT::NOrm::NServer::NAccessControl::TAccessControlEntry>
TBookTypeHandler::GetDefaultAcl()
{
    return {
        {
            .Action = NYT::NOrm::NServer::NAccessControl::EAccessControlAction::Allow,
            .Permissions = {
                NYT::NOrm::NServer::NAccessControl::TAccessControlPermissionValues::Read,
            },
        },
    };
}

std::unique_ptr<NYT::NOrm::NServer::NObjects::TObject>
TBookTypeHandler::InstantiateObject(
    const NYT::NOrm::NClient::NObjects::TObjectKey& key,
    const NYT::NOrm::NClient::NObjects::TObjectKey& parentKey,
    ISession* session)
{
    return std::make_unique<TBook>(
        std::get<i64>(key[0]),
        std::get<i64>(key[1]),
        parentKey,
        this,
        session);
}

void TBookTypeHandler::InitializeCreatedObject(
    NYT::NOrm::NServer::NObjects::TTransaction* transaction,
    NYT::NOrm::NServer::NObjects::TObject* object)
{
    TBase::InitializeCreatedObject(transaction, object);
    auto* typedObject = object->As<TBook>();
    YT_VERIFY(typedObject);
    typedObject->MetaEtc().MutableLoad()->set_uuid(NYT::NOrm::NClient::NObjects::GenerateUuid());
}

void TBookTypeHandler::ValidateCreatedObject(
    NYT::NOrm::NServer::NObjects::TTransaction* transaction,
    NYT::NOrm::NServer::NObjects::TObject* object)
{
    TBase::ValidateCreatedObject(transaction, object);
    auto* typedObject = object->As<TBook>();
    YT_VERIFY(typedObject);
    ResolveAttribute("/spec/api_revision", /*callback*/ {}, /*validateProtoSchemaCompliance*/ false).Attribute
        ->AsScalar()
        ->GetAttribute<NYT::NOrm::NServer::NObjects::TScalarAttribute<ui64>>(object)
        ->Store(transaction->GetStartTimestamp());
}

void TBookTypeHandler::ValidateMetaEtcOnTransactionCommit(
    NYT::NOrm::NServer::NObjects::TTransaction*,
    const TBook* object)
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

void TBookTypeHandler::ValidateMetaEtcOnValueUpdate(
    NYT::NOrm::NServer::NObjects::TTransaction*,
    const TBook* object,
    const TBook::TMetaEtc& metaEtcOld,
    const TBook::TMetaEtc& metaEtcNew)
{
    ValidateMetaUuid(metaEtcOld, metaEtcNew);

    // All user-defined attributes are considered updatable by any user
    // having corresponding write permission.
    Y_UNUSED(object);
}

void TBookTypeHandler::ValidateMetaUuid(
    const TBook::TMetaEtc& metaEtcOld,
    const TBook::TMetaEtc& metaEtcNew)
{
    // Uuid protects, for example, YP secrets from object identity theft, i.e. from ABA race conditions,
    // so we try hard to make sure uuid doesn't change.
    if (!metaEtcOld.uuid().empty() && metaEtcOld.uuid() != metaEtcNew.uuid()) {
        this->GetBootstrap()->GetAccessControlManager()->ValidateSuperuser("change /meta/uuid");
    }
}

////////////////////////////////////////////////////////////////////////////////

TBufferedTimestampIdTypeHandler::TBufferedTimestampIdTypeHandler(
    IBootstrap* bootstrap,
    NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config)
    : TBase(
        bootstrap,
        static_cast<NYT::NOrm::NClient::NObjects::TObjectTypeValue>(EObjectType::BufferedTimestampId))
    , Config_(std::move(config))
    , KeyFields_({
        &BufferedTimestampIdsTable.Fields.MetaI64Id,
        &BufferedTimestampIdsTable.Fields.MetaUi64Id})
    , NullKey_(NYT::NOrm::NServer::NObjects::GenerateZeroKey(KeyFields_))
{ }

void TBufferedTimestampIdTypeHandler::Initialize()
{
    TBase::Initialize();

    MetaAttributeSchema_
        ->AddChildren({
        MakeScalarAttributeSchema("acl", TDataModelObject::AclDescriptor)
            ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::Updatable)
            ->AddValidator<NYT::NOrm::NServer::NObjects::TObject>(std::bind_front(
                &TBufferedTimestampIdTypeHandler::ValidateAcl, this))
    });
    MetaAttributeSchema_
        ->AddChildren({
            MakeIdAttributeSchema(
                "i64_id",
                &BufferedTimestampIdsTable.Fields.MetaI64Id,
                MakeValueGetter(&TBufferedTimestampId::GetI64Id))
                ->SetKeyFieldPolicy(
                    NYT::NOrm::NServer::NObjects::CreateIntegerAttributePolicy<i64>(
                        NYT::NOrm::NServer::NObjects::EAttributeGenerationPolicy::BufferedTimestamp,
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
                &BufferedTimestampIdsTable.Fields.MetaUi64Id,
                MakeValueGetter(&TBufferedTimestampId::GetUi64Id))
                ->SetKeyFieldPolicy(
                    NYT::NOrm::NServer::NObjects::CreateIntegerAttributePolicy<ui64>(
                        NYT::NOrm::NServer::NObjects::EAttributeGenerationPolicy::BufferedTimestamp,
                        GetBootstrap(),
                        NYT::NOrm::NServer::NObjects::DefaultMinIntegerAttributeValue<ui64>,
                        NYT::NOrm::NServer::NObjects::DefaultMaxIntegerAttributeValue<ui64>,
                        NYT::NOrm::NServer::NObjects::TAttributePolicyOptions{
                            .Type = GetType(),
                            .AttributePath = "/meta/ui64_id",
                            .IndexForIncrement = ""
                        })),
            MakeScalarAttributeSchema("finalization_start_time", TBufferedTimestampId::FinalizationStartTimeDescriptor)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueReadOnly),
            MakeScalarAttributeSchema("finalizers", TBufferedTimestampId::FinalizersDescriptor)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueReadOnly),
            MakeScalarAttributeSchema("ultimate_question_of_life")
                ->SetComputed()
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::ReadOnly)
                ->SetOpaque(/*emitEntity*/ false),
            TBase::MakeEtcAttributeSchema(TBufferedTimestampId::MetaEtcDescriptor.AddValidator(
                std::bind_front(&TBufferedTimestampIdTypeHandler::ValidateMetaEtcOnValueUpdate, this)))
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::Updatable)
                ->template AddValidator<TBufferedTimestampId>(std::bind_front(
                    &TBufferedTimestampIdTypeHandler::ValidateMetaEtcOnTransactionCommit, this))
            });

    SpecAttributeSchema_
        ->AddChildren({
            MakeScalarAttributeSchema("i64_value", TBufferedTimestampId::TSpec::I64ValueDescriptor)
                ->SetPolicy(TBufferedTimestampId::TSpec::I64ValueDescriptor,
                            NYT::NOrm::NServer::NObjects::CreateIntegerAttributePolicy<i64>(
                                NYT::NOrm::NServer::NObjects::EAttributeGenerationPolicy::Timestamp,
                                GetBootstrap(),
                                NYT::NOrm::NServer::NObjects::DefaultMinIntegerAttributeValue<i64>,
                                NYT::NOrm::NServer::NObjects::DefaultMaxIntegerAttributeValue<i64>,
                                NYT::NOrm::NServer::NObjects::TAttributePolicyOptions{
                                    .Type = GetType(),
                                    .AttributePath = "/spec/i64_value",
                                    .IndexForIncrement = ""
                                })),
            MakeEtcAttributeSchema(TBufferedTimestampId::TSpec::EtcDescriptor),
        });

    StatusAttributeSchema_
        ->AddChildren({
            MakeEtcAttributeSchema(TBufferedTimestampId::TStatus::EtcDescriptor),
        });

    ControlAttributeSchema_
        ->AddChildren({
            MakeScalarAttributeSchema("touch")
                ->SetControl<TBufferedTimestampId, NYT::NOrm::NExample::NClient::NProto::NDataModel::TObjectTouch>(
                    [] (NYT::NOrm::NServer::NObjects::TTransaction* transaction,
                        TBufferedTimestampId* object,
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

void TBufferedTimestampIdTypeHandler::PostInitialize()
{
    // History is configured in PostInitialize stage, as users may add some evaluated fields via plugins in Initialize stage.
    // NB: Called before TBase::PostInitialize as base type handler prepares cache.
    ConfigureHistory(
        /*attributes*/ {},
        /*filter*/ std::nullopt);

    TBase::PostInitialize();

    FillUpdatePolicies();
}

bool TBufferedTimestampIdTypeHandler::IsObjectNameSupported() const
{
    return false;
}

bool TBufferedTimestampIdTypeHandler::SkipStoreWithoutChanges() const
{
    return false;
}

bool TBufferedTimestampIdTypeHandler::ForceZeroKeyEvaluation() const
{
    return false;
}

std::vector<NYT::NOrm::NServer::NObjects::TWatchLog>
TBufferedTimestampIdTypeHandler::GetWatchLogs() const
{
    return std::vector<NYT::NOrm::NServer::NObjects::TWatchLog>{
    };
}

const NYT::NYson::TProtobufMessageType*
TBufferedTimestampIdTypeHandler::GetRootProtobufType() const
{
    static const auto* type = NYT::NYson::ReflectProtobufMessageType<NYT::NOrm::NExample::NClient::NProto::NDataModel::TBufferedTimestampId>();
    return type;
}

const TDBFields& TBufferedTimestampIdTypeHandler::GetKeyFields() const
{
    return KeyFields_;
}

NYT::NOrm::NClient::NObjects::TObjectKey TBufferedTimestampIdTypeHandler::GetNullKey() const
{
    return NullKey_;
}

const NYT::NOrm::NServer::NObjects::TDBTable* TBufferedTimestampIdTypeHandler::GetTable() const
{
    return &BufferedTimestampIdsTable;
}

const NYT::NOrm::NServer::NObjects::TDBTable* TBufferedTimestampIdTypeHandler::GetParentsTable() const
{
    return nullptr;
}

std::unique_ptr<NYT::NOrm::NServer::NObjects::TObject>
TBufferedTimestampIdTypeHandler::InstantiateObject(
    const NYT::NOrm::NClient::NObjects::TObjectKey& key,
    const NYT::NOrm::NClient::NObjects::TObjectKey& parentKey,
    ISession* session)
{
    YT_VERIFY(!parentKey);

    return std::make_unique<TBufferedTimestampId>(
        std::get<i64>(key[0]),
        std::get<ui64>(key[1]),
        this,
        session);
}

void TBufferedTimestampIdTypeHandler::InitializeCreatedObject(
    NYT::NOrm::NServer::NObjects::TTransaction* transaction,
    NYT::NOrm::NServer::NObjects::TObject* object)
{
    TBase::InitializeCreatedObject(transaction, object);
    auto* typedObject = object->As<TBufferedTimestampId>();
    YT_VERIFY(typedObject);
    typedObject->MetaEtc().MutableLoad()->set_uuid(NYT::NOrm::NClient::NObjects::GenerateUuid());
}

void TBufferedTimestampIdTypeHandler::ValidateCreatedObject(
    NYT::NOrm::NServer::NObjects::TTransaction* transaction,
    NYT::NOrm::NServer::NObjects::TObject* object)
{
    TBase::ValidateCreatedObject(transaction, object);
    auto* typedObject = object->As<TBufferedTimestampId>();
    YT_VERIFY(typedObject);
}

void TBufferedTimestampIdTypeHandler::ValidateMetaEtcOnTransactionCommit(
    NYT::NOrm::NServer::NObjects::TTransaction*,
    const TBufferedTimestampId* object)
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

void TBufferedTimestampIdTypeHandler::ValidateMetaEtcOnValueUpdate(
    NYT::NOrm::NServer::NObjects::TTransaction*,
    const TBufferedTimestampId* object,
    const TBufferedTimestampId::TMetaEtc& metaEtcOld,
    const TBufferedTimestampId::TMetaEtc& metaEtcNew)
{
    ValidateMetaUuid(metaEtcOld, metaEtcNew);

    // All user-defined attributes are considered updatable by any user
    // having corresponding write permission.
    Y_UNUSED(object);
}

void TBufferedTimestampIdTypeHandler::ValidateMetaUuid(
    const TBufferedTimestampId::TMetaEtc& metaEtcOld,
    const TBufferedTimestampId::TMetaEtc& metaEtcNew)
{
    // Uuid protects, for example, YP secrets from object identity theft, i.e. from ABA race conditions,
    // so we try hard to make sure uuid doesn't change.
    if (!metaEtcOld.uuid().empty() && metaEtcOld.uuid() != metaEtcNew.uuid()) {
        this->GetBootstrap()->GetAccessControlManager()->ValidateSuperuser("change /meta/uuid");
    }
}

////////////////////////////////////////////////////////////////////////////////

TCatTypeHandler::TCatTypeHandler(
    IBootstrap* bootstrap,
    NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config)
    : TBase(
        bootstrap,
        static_cast<NYT::NOrm::NClient::NObjects::TObjectTypeValue>(EObjectType::Cat))
    , Config_(std::move(config))
    , KeyFields_({
        &CatsTable.Fields.MetaId})
    , NullKey_(NYT::NOrm::NServer::NObjects::GenerateZeroKey(KeyFields_))
{ }

TCatTypeHandler::~TCatTypeHandler()
{ }

void TCatTypeHandler::Initialize()
{
    TBase::Initialize();

    MetaAttributeSchema_
        ->AddChildren({
        MakeScalarAttributeSchema("acl", TDataModelObject::AclDescriptor)
            ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::Updatable)
            ->AddValidator<NYT::NOrm::NServer::NObjects::TObject>(std::bind_front(
                &TCatTypeHandler::ValidateAcl, this))
    });
    MetaAttributeSchema_
        ->AddChildren({
            MakeIdAttributeSchema(
                "id",
                &CatsTable.Fields.MetaId,
                MakeValueGetter(&TCat::GetId))
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
            MakeScalarAttributeSchema("breed", TCat::BreedDescriptor)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::ReadOnly),
            MakeScalarAttributeSchema("finalization_start_time", TCat::FinalizationStartTimeDescriptor)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueReadOnly),
            MakeScalarAttributeSchema("finalizers", TCat::FinalizersDescriptor)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueReadOnly),
            MakeScalarAttributeSchema("ultimate_question_of_life")
                ->SetComputed()
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::ReadOnly)
                ->SetOpaque(/*emitEntity*/ false),
            TBase::MakeEtcAttributeSchema(TCat::MetaEtcDescriptor.AddValidator(
                std::bind_front(&TCatTypeHandler::ValidateMetaEtcOnValueUpdate, this)))
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::Updatable)
                ->template AddValidator<TCat>(std::bind_front(
                    &TCatTypeHandler::ValidateMetaEtcOnTransactionCommit, this))
            });

    SpecAttributeSchema_
        ->AddChildren({
            MakeScalarAttributeSchema("last_sleep_duration", TCat::TSpec::LastSleepDurationDescriptor)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueReadOnly)
                ->SetOpaque(),
            MakeScalarAttributeSchema("favourite_food", TCat::TSpec::FavouriteFoodDescriptor)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueUpdatable),
            MakeScalarAttributeSchema("favourite_toy", TCat::TSpec::FavouriteToyDescriptor)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::Updatable),
            MakeScalarAttributeSchema("revision", TCat::TSpec::RevisionDescriptor)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueUpdatable),
            MakeScalarAttributeSchema("mood", TCat::TSpec::MoodDescriptor),
            MakeScalarAttributeSchema("health_condition", TCat::TSpec::HealthConditionDescriptor),
            MakeScalarAttributeSchema("eye_color_with_default_yson_storage_type", TCat::TSpec::EyeColorWithDefaultYsonStorageTypeDescriptor),
            MakeScalarAttributeSchema("friend_cats_count", TCat::TSpec::FriendCatsCountDescriptor)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueUpdatable)
                ->SetOpaque(),
            MakeScalarAttributeSchema("statistics_for_days_of_year", TCat::TSpec::StatisticsForDaysOfYearDescriptor)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueUpdatable)
                ->SetOpaque(),
            MakeEtcAttributeSchema(TCat::TSpec::EtcDescriptor),
            MakeScalarAttributeSchema("sleep_time")
                ->SetComputed()
                ->SetOpaque(),
        });

    StatusAttributeSchema_
        ->AddChildren({
            MakeCompositeAttributeSchema("updatable_nested")
                ->AddChildren({
                    MakeScalarAttributeSchema("inner_first", TCat::TStatus::TUpdatableNested::InnerFirstDescriptor)
                        ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueReadOnly),
                    MakeScalarAttributeSchema("inner_second", TCat::TStatus::TUpdatableNested::InnerSecondDescriptor)
                        ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::Updatable),
                    MakeScalarAttributeSchema("inner_third", TCat::TStatus::TUpdatableNested::InnerThirdDescriptor),
                    MakeEtcAttributeSchema(TCat::TStatus::TUpdatableNested::EtcDescriptor),
                })
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::Updatable)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::Updatable),
            MakeCompositeAttributeSchema("read_only_nested")
                ->AddChildren({
                    MakeScalarAttributeSchema("inner_first", TCat::TStatus::TReadOnlyNested::InnerFirstDescriptor)
                        ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueReadOnly),
                    MakeScalarAttributeSchema("inner_second", TCat::TStatus::TReadOnlyNested::InnerSecondDescriptor)
                        ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::Updatable),
                    MakeScalarAttributeSchema("inner_third", TCat::TStatus::TReadOnlyNested::InnerThirdDescriptor),
                    MakeEtcAttributeSchema(TCat::TStatus::TReadOnlyNested::EtcDescriptor),
                })
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::ReadOnly)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::ReadOnly),
            MakeCompositeAttributeSchema("opaque_ro_nested")
                ->AddChildren({
                    MakeScalarAttributeSchema("inner_first", TCat::TStatus::TOpaqueRoNested::InnerFirstDescriptor)
                        ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueReadOnly),
                    MakeScalarAttributeSchema("inner_second", TCat::TStatus::TOpaqueRoNested::InnerSecondDescriptor)
                        ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::Updatable),
                    MakeScalarAttributeSchema("inner_third", TCat::TStatus::TOpaqueRoNested::InnerThirdDescriptor),
                    MakeEtcAttributeSchema(TCat::TStatus::TOpaqueRoNested::EtcDescriptor),
                })
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueReadOnly)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueReadOnly),
            MakeCompositeAttributeSchema("opaque_nested")
                ->AddChildren({
                    MakeScalarAttributeSchema("inner_first", TCat::TStatus::TOpaqueNested::InnerFirstDescriptor)
                        ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueReadOnly),
                    MakeScalarAttributeSchema("inner_second", TCat::TStatus::TOpaqueNested::InnerSecondDescriptor)
                        ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::Updatable),
                    MakeScalarAttributeSchema("inner_third", TCat::TStatus::TOpaqueNested::InnerThirdDescriptor),
                    MakeEtcAttributeSchema(TCat::TStatus::TOpaqueNested::EtcDescriptor),
                })
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueUpdatable)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueUpdatable),
            MakeEtcAttributeSchema(TCat::TStatus::EtcDescriptor),
        });
    PrepareRevisionTracker(
        /*trackedPaths*/ {
            "/spec/name",
            "/spec/favourite_food",
        },
        /*trackerPath*/ "/spec/revision",
        /*lockGroupRestrictionEnabled*/ false);

    ControlAttributeSchema_
        ->AddChildren({
            MakeScalarAttributeSchema("touch")
                ->SetControl<TCat, NYT::NOrm::NExample::NClient::NProto::NDataModel::TObjectTouch>(
                    [] (NYT::NOrm::NServer::NObjects::TTransaction* transaction,
                        TCat* object,
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
                ->SetControl<TCat, NYT::NOrm::NExample::NClient::NProto::NDataModel::TObjectTouchIndex>(
                [] (NYT::NOrm::NServer::NObjects::TTransaction* /*transaction*/,
                    TCat* object,
                    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TObjectTouchIndex& touchIndex)
                {
                    for (const auto& indexName : touchIndex.index_names()) {
                        object->GetIndexOrThrow(indexName)->TouchIndex();
                    }
                }),
            MakeScalarAttributeSchema("touch_revision")
                ->SetControlWithTransactionCallContext<TCat, NYT::NOrm::NExample::NClient::NProto::NDataModel::TObjectTouchRevision>(
                    [&] (NYT::NOrm::NServer::NObjects::TTransaction* transaction,
                        TCat* object,
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
    RegisterScalarAttributeIndex(
        "cats_by_names_with_predicate",
        std::make_unique<NYT::NOrm::NServer::NObjects::TScalarAttributeIndexDescriptor>(
            "cats_by_names_with_predicate",
            &CatsByNamesWithPredicateTable,
            std::vector<NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor>{
                NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor(
                    &TCat::TSpec::EtcDescriptor,
                    "/name"),
            },
            std::vector<NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor>{
                NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor(
                    &TCat::TSpec::LastSleepDurationDescriptor,
                    ""),
            },
            /*repeated*/ false,
            Config_->TryGetIndexMode("cats_by_names_with_predicate")
                .value_or(NYT::NOrm::NServer::NObjects::EIndexMode::Enabled),
            NYT::NOrm::NServer::NObjects::TPredicateInfo{
                .Expression = "[/spec/last_sleep_duration] > 6 and [/spec/last_sleep_duration] < 18",
                .AttributesToValidate = {"/spec/last_sleep_duration"}
            }));
}

void TCatTypeHandler::PostInitialize()
{
    // History is configured in PostInitialize stage, as users may add some evaluated fields via plugins in Initialize stage.
    // NB: Called before TBase::PostInitialize as base type handler prepares cache.
    ConfigureHistory(
        /*attributes*/ {},
        /*filter*/ std::nullopt);

    TBase::PostInitialize();

    FillUpdatePolicies();
}

bool TCatTypeHandler::IsObjectNameSupported() const
{
    return false;
}

bool TCatTypeHandler::SkipStoreWithoutChanges() const
{
    return true;
}

bool TCatTypeHandler::ForceZeroKeyEvaluation() const
{
    return false;
}

std::vector<NYT::NOrm::NServer::NObjects::TWatchLog>
TCatTypeHandler::GetWatchLogs() const
{
    return std::vector<NYT::NOrm::NServer::NObjects::TWatchLog>{
        NYT::NOrm::NServer::NObjects::TWatchLog{
            .ObjectName = "cat",
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
TCatTypeHandler::GetRootProtobufType() const
{
    static const auto* type = NYT::NYson::ReflectProtobufMessageType<NYT::NOrm::NExample::NClient::NProto::NDataModel::TCat>();
    return type;
}

const TDBFields& TCatTypeHandler::GetKeyFields() const
{
    return KeyFields_;
}

NYT::NOrm::NClient::NObjects::TObjectKey TCatTypeHandler::GetNullKey() const
{
    return NullKey_;
}

const NYT::NOrm::NServer::NObjects::TDBTable* TCatTypeHandler::GetTable() const
{
    return &CatsTable;
}

const NYT::NOrm::NServer::NObjects::TDBTable* TCatTypeHandler::GetParentsTable() const
{
    return nullptr;
}

std::unique_ptr<NYT::NOrm::NServer::NObjects::TObject>
TCatTypeHandler::InstantiateObject(
    const NYT::NOrm::NClient::NObjects::TObjectKey& key,
    const NYT::NOrm::NClient::NObjects::TObjectKey& parentKey,
    ISession* session)
{
    YT_VERIFY(!parentKey);

    return std::make_unique<TCat>(
        std::get<i64>(key[0]),
        this,
        session);
}

void TCatTypeHandler::InitializeCreatedObject(
    NYT::NOrm::NServer::NObjects::TTransaction* transaction,
    NYT::NOrm::NServer::NObjects::TObject* object)
{
    TBase::InitializeCreatedObject(transaction, object);
    auto* typedObject = object->As<TCat>();
    YT_VERIFY(typedObject);
    typedObject->MetaEtc().MutableLoad()->set_uuid(NYT::NOrm::NClient::NObjects::GenerateUuid());
}

void TCatTypeHandler::ValidateCreatedObject(
    NYT::NOrm::NServer::NObjects::TTransaction* transaction,
    NYT::NOrm::NServer::NObjects::TObject* object)
{
    TBase::ValidateCreatedObject(transaction, object);
    auto* typedObject = object->As<TCat>();
    YT_VERIFY(typedObject);
    ResolveAttribute("/spec/revision", /*callback*/ {}, /*validateProtoSchemaCompliance*/ false).Attribute
        ->AsScalar()
        ->GetAttribute<NYT::NOrm::NServer::NObjects::TScalarAttribute<ui64>>(object)
        ->Store(transaction->GetStartTimestamp());
}

void TCatTypeHandler::ValidateMetaEtcOnTransactionCommit(
    NYT::NOrm::NServer::NObjects::TTransaction*,
    const TCat* object)
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

void TCatTypeHandler::ValidateMetaEtcOnValueUpdate(
    NYT::NOrm::NServer::NObjects::TTransaction*,
    const TCat* object,
    const TCat::TMetaEtc& metaEtcOld,
    const TCat::TMetaEtc& metaEtcNew)
{
    ValidateMetaUuid(metaEtcOld, metaEtcNew);

    // All user-defined attributes are considered updatable by any user
    // having corresponding write permission.
    Y_UNUSED(object);
}

void TCatTypeHandler::ValidateMetaUuid(
    const TCat::TMetaEtc& metaEtcOld,
    const TCat::TMetaEtc& metaEtcNew)
{
    // Uuid protects, for example, YP secrets from object identity theft, i.e. from ABA race conditions,
    // so we try hard to make sure uuid doesn't change.
    if (!metaEtcOld.uuid().empty() && metaEtcOld.uuid() != metaEtcNew.uuid()) {
        this->GetBootstrap()->GetAccessControlManager()->ValidateSuperuser("change /meta/uuid");
    }
}

////////////////////////////////////////////////////////////////////////////////

TEditorTypeHandler::TEditorTypeHandler(
    IBootstrap* bootstrap,
    NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config)
    : TBase(
        bootstrap,
        static_cast<NYT::NOrm::NClient::NObjects::TObjectTypeValue>(EObjectType::Editor))
    , Config_(std::move(config))
    , KeyFields_({
        &EditorsTable.Fields.MetaId})
    , NullKey_(NYT::NOrm::NServer::NObjects::GenerateZeroKey(KeyFields_))
{ }

TEditorTypeHandler::~TEditorTypeHandler()
{ }

void TEditorTypeHandler::Initialize()
{
    TBase::Initialize();

    MetaAttributeSchema_
        ->AddChildren({
        MakeScalarAttributeSchema("acl", TDataModelObject::AclDescriptor)
            ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::Updatable)
            ->AddValidator<NYT::NOrm::NServer::NObjects::TObject>(std::bind_front(
                &TEditorTypeHandler::ValidateAcl, this))
    });
    MetaAttributeSchema_
        ->AddChildren({
            MakeIdAttributeSchema(
                "id",
                &EditorsTable.Fields.MetaId,
                MakeValueGetter(&TEditor::GetId))
                ->SetKeyFieldPolicy(
                    NYT::NOrm::NServer::NObjects::CreateStringAttributePolicy(
                        NYT::NOrm::NServer::NObjects::EAttributeGenerationPolicy::Random,
                        NYT::NOrm::NServer::NObjects::DefaultMinStringAttributeLength,
                        NYT::NOrm::NServer::NObjects::DefaultMaxStringAttributeLength,
                        NYT::NOrm::NServer::NObjects::DefaultStringAttributeValidChars)),
            MakeScalarAttributeSchema("finalization_start_time", TEditor::FinalizationStartTimeDescriptor)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueReadOnly),
            MakeScalarAttributeSchema("finalizers", TEditor::FinalizersDescriptor)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueReadOnly),
            MakeScalarAttributeSchema("ultimate_question_of_life")
                ->SetComputed()
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::ReadOnly)
                ->SetOpaque(/*emitEntity*/ false),
            TBase::MakeEtcAttributeSchema(TEditor::MetaEtcDescriptor.AddValidator(
                std::bind_front(&TEditorTypeHandler::ValidateMetaEtcOnValueUpdate, this)))
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::Updatable)
                ->template AddValidator<TEditor>(std::bind_front(
                    &TEditorTypeHandler::ValidateMetaEtcOnTransactionCommit, this))
            });

    SpecAttributeSchema_
        ->AddChildren({
            MakeScalarAttributeSchema("achievements", TEditor::TSpec::AchievementsDescriptor),
            MakeScalarAttributeSchema("phone_number", TEditor::TSpec::PhoneNumberDescriptor),
            MakeEtcAttributeSchema(TEditor::TSpec::EtcDescriptor),
        });

    StatusAttributeSchema_
        ->AddChildren({
            MakeEtcAttributeSchema(TEditor::TStatus::EtcDescriptor),
        });

    ControlAttributeSchema_
        ->AddChildren({
            MakeScalarAttributeSchema("touch")
                ->SetControl<TEditor, NYT::NOrm::NExample::NClient::NProto::NDataModel::TObjectTouch>(
                    [] (NYT::NOrm::NServer::NObjects::TTransaction* transaction,
                        TEditor* object,
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
                ->SetControl<TEditor, NYT::NOrm::NExample::NClient::NProto::NDataModel::TObjectTouchIndex>(
                [] (NYT::NOrm::NServer::NObjects::TTransaction* /*transaction*/,
                    TEditor* object,
                    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TObjectTouchIndex& touchIndex)
                {
                    for (const auto& indexName : touchIndex.index_names()) {
                        object->GetIndexOrThrow(indexName)->TouchIndex();
                    }
                }),
        });
    RegisterScalarAttributeIndex(
        "editors_by_achievements",
        std::make_unique<NYT::NOrm::NServer::NObjects::TScalarAttributeIndexDescriptor>(
            "editors_by_achievements",
            &EditorsByAchievementsTable,
            std::vector<NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor>{
                NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor(
                    &TEditor::TSpec::AchievementsDescriptor,
                    ""),
            },
            std::vector<NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor>{
            },
            /*repeated*/ true,
            Config_->TryGetIndexMode("editors_by_achievements")
                .value_or(NYT::NOrm::NServer::NObjects::EIndexMode::Disabled)));
    RegisterScalarAttributeIndex(
        "editors_by_name",
        std::make_unique<NYT::NOrm::NServer::NObjects::TScalarAttributeIndexDescriptor>(
            "editors_by_name",
            &EditorsByNameTable,
            std::vector<NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor>{
                NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor(
                    &TEditor::TSpec::EtcDescriptor,
                    "/name"),
            },
            std::vector<NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor>{
            },
            /*repeated*/ false,
            Config_->TryGetIndexMode("editors_by_name")
                .value_or(NYT::NOrm::NServer::NObjects::EIndexMode::Enabled)));
    RegisterScalarAttributeIndex(
        "editors_by_phone_number",
        std::make_unique<NYT::NOrm::NServer::NObjects::TScalarAttributeIndexDescriptor>(
            "editors_by_phone_number",
            &EditorsByPhoneNumberTable,
            std::vector<NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor>{
                NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor(
                    &TEditor::TSpec::PhoneNumberDescriptor,
                    ""),
            },
            std::vector<NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor>{
            },
            /*repeated*/ false,
            Config_->TryGetIndexMode("editors_by_phone_number")
                .value_or(NYT::NOrm::NServer::NObjects::EIndexMode::Building)));
    RegisterScalarAttributeIndex(
        "editors_by_post",
        std::make_unique<NYT::NOrm::NServer::NObjects::TScalarAttributeIndexDescriptor>(
            "editors_by_post",
            &EditorsByPostTable,
            std::vector<NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor>{
                NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor(
                    &TEditor::TSpec::EtcDescriptor,
                    "/post"),
            },
            std::vector<NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor>{
            },
            /*repeated*/ false,
            Config_->TryGetIndexMode("editors_by_post")
                .value_or(NYT::NOrm::NServer::NObjects::EIndexMode::Building)));
}

void TEditorTypeHandler::PostInitialize()
{
    // History is configured in PostInitialize stage, as users may add some evaluated fields via plugins in Initialize stage.
    // NB: Called before TBase::PostInitialize as base type handler prepares cache.
    ConfigureHistory(
        /*attributes*/ {},
        /*filter*/ std::nullopt);

    TBase::PostInitialize();

    FillUpdatePolicies();
}

bool TEditorTypeHandler::IsObjectNameSupported() const
{
    return false;
}

bool TEditorTypeHandler::SkipStoreWithoutChanges() const
{
    return false;
}

bool TEditorTypeHandler::ForceZeroKeyEvaluation() const
{
    return false;
}

std::vector<NYT::NOrm::NServer::NObjects::TWatchLog>
TEditorTypeHandler::GetWatchLogs() const
{
    return std::vector<NYT::NOrm::NServer::NObjects::TWatchLog>{
    };
}

const NYT::NYson::TProtobufMessageType*
TEditorTypeHandler::GetRootProtobufType() const
{
    static const auto* type = NYT::NYson::ReflectProtobufMessageType<NYT::NOrm::NExample::NClient::NProto::NDataModel::TEditor>();
    return type;
}

const TDBFields& TEditorTypeHandler::GetKeyFields() const
{
    return KeyFields_;
}

NYT::NOrm::NClient::NObjects::TObjectKey TEditorTypeHandler::GetNullKey() const
{
    return NullKey_;
}

const NYT::NOrm::NServer::NObjects::TDBTable* TEditorTypeHandler::GetTable() const
{
    return &EditorsTable;
}

const NYT::NOrm::NServer::NObjects::TDBTable* TEditorTypeHandler::GetParentsTable() const
{
    return nullptr;
}

std::unique_ptr<NYT::NOrm::NServer::NObjects::TObject>
TEditorTypeHandler::InstantiateObject(
    const NYT::NOrm::NClient::NObjects::TObjectKey& key,
    const NYT::NOrm::NClient::NObjects::TObjectKey& parentKey,
    ISession* session)
{
    YT_VERIFY(!parentKey);

    return std::make_unique<TEditor>(
        std::get<TString>(key[0]),
        this,
        session);
}

void TEditorTypeHandler::InitializeCreatedObject(
    NYT::NOrm::NServer::NObjects::TTransaction* transaction,
    NYT::NOrm::NServer::NObjects::TObject* object)
{
    TBase::InitializeCreatedObject(transaction, object);
    auto* typedObject = object->As<TEditor>();
    YT_VERIFY(typedObject);
    typedObject->MetaEtc().MutableLoad()->set_uuid(NYT::NOrm::NClient::NObjects::GenerateUuid());
}

void TEditorTypeHandler::ValidateCreatedObject(
    NYT::NOrm::NServer::NObjects::TTransaction* transaction,
    NYT::NOrm::NServer::NObjects::TObject* object)
{
    TBase::ValidateCreatedObject(transaction, object);
    auto* typedObject = object->As<TEditor>();
    YT_VERIFY(typedObject);
}

void TEditorTypeHandler::ValidateMetaEtcOnTransactionCommit(
    NYT::NOrm::NServer::NObjects::TTransaction*,
    const TEditor* object)
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

void TEditorTypeHandler::ValidateMetaEtcOnValueUpdate(
    NYT::NOrm::NServer::NObjects::TTransaction*,
    const TEditor* object,
    const TEditor::TMetaEtc& metaEtcOld,
    const TEditor::TMetaEtc& metaEtcNew)
{
    ValidateMetaUuid(metaEtcOld, metaEtcNew);

    // All user-defined attributes are considered updatable by any user
    // having corresponding write permission.
    Y_UNUSED(object);
}

void TEditorTypeHandler::ValidateMetaUuid(
    const TEditor::TMetaEtc& metaEtcOld,
    const TEditor::TMetaEtc& metaEtcNew)
{
    // Uuid protects, for example, YP secrets from object identity theft, i.e. from ABA race conditions,
    // so we try hard to make sure uuid doesn't change.
    if (!metaEtcOld.uuid().empty() && metaEtcOld.uuid() != metaEtcNew.uuid()) {
        this->GetBootstrap()->GetAccessControlManager()->ValidateSuperuser("change /meta/uuid");
    }
}

////////////////////////////////////////////////////////////////////////////////

TEmployerTypeHandler::TEmployerTypeHandler(
    IBootstrap* bootstrap,
    NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config)
    : TBase(
        bootstrap,
        static_cast<NYT::NOrm::NClient::NObjects::TObjectTypeValue>(EObjectType::Employer))
    , Config_(std::move(config))
    , KeyFields_({
        &EmployersTable.Fields.MetaId})
    , NullKey_(NYT::NOrm::NServer::NObjects::GenerateZeroKey(KeyFields_))
{ }

TEmployerTypeHandler::~TEmployerTypeHandler()
{ }

void TEmployerTypeHandler::Initialize()
{
    TBase::Initialize();

    MetaAttributeSchema_
        ->AddChildren({
        MakeScalarAttributeSchema("acl", TDataModelObject::AclDescriptor)
            ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::Updatable)
            ->AddValidator<NYT::NOrm::NServer::NObjects::TObject>(std::bind_front(
                &TEmployerTypeHandler::ValidateAcl, this))
    });
    MetaAttributeSchema_
        ->AddChildren({
            MakeIdAttributeSchema(
                "id",
                &EmployersTable.Fields.MetaId,
                MakeValueGetter(&TEmployer::GetId))
                ->SetKeyFieldPolicy(
                    NYT::NOrm::NServer::NObjects::CreateStringAttributePolicy(
                        NYT::NOrm::NServer::NObjects::EAttributeGenerationPolicy::Manual,
                        NYT::NOrm::NServer::NObjects::DefaultMinStringAttributeLength,
                        NYT::NOrm::NServer::NObjects::DefaultMaxStringAttributeLength,
                        NYT::NOrm::NServer::NObjects::DefaultStringAttributeValidChars)),
            MakeScalarAttributeSchema("email", TEmployer::EmailDescriptor)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::ReadOnly),
            MakeScalarAttributeSchema("finalization_start_time", TEmployer::FinalizationStartTimeDescriptor)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueReadOnly),
            MakeScalarAttributeSchema("finalizers", TEmployer::FinalizersDescriptor)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueReadOnly),
            MakeScalarAttributeSchema("ultimate_question_of_life")
                ->SetComputed()
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::ReadOnly)
                ->SetOpaque(/*emitEntity*/ false),
            TBase::MakeEtcAttributeSchema(TEmployer::MetaEtcDescriptor.AddValidator(
                std::bind_front(&TEmployerTypeHandler::ValidateMetaEtcOnValueUpdate, this)))
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::Updatable)
                ->template AddValidator<TEmployer>(std::bind_front(
                    &TEmployerTypeHandler::ValidateMetaEtcOnTransactionCommit, this))
            });

    SpecAttributeSchema_
        ->AddChildren({
            MakeEtcAttributeSchema(TEmployer::TSpec::EtcDescriptor),
        });

    StatusAttributeSchema_
        ->AddChildren({
            MakeScalarAttributeSchema("salary", TEmployer::TStatus::SalaryDescriptor)
                ->SetCustomPolicyExpected()
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::ReadOnly),
            MakeEtcAttributeSchema(TEmployer::TStatus::EtcDescriptor)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::ReadOnly),
        });

    ControlAttributeSchema_
        ->AddChildren({
            MakeScalarAttributeSchema("touch")
                ->SetControl<TEmployer, NYT::NOrm::NExample::NClient::NProto::NDataModel::TObjectTouch>(
                    [] (NYT::NOrm::NServer::NObjects::TTransaction* transaction,
                        TEmployer* object,
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
}

static const NYT::NOrm::NServer::NObjects::TTagsTreeNode EmployerTagsTree
{
    .Children = {
        {
            "meta",
            {
                .Children = {
                    {
                        "id",
                        {
                            .AddTags = TTagSet({ /*readonly*/ 4 }),
                        }
                    },
                    {
                        "passport",
                        {
                            .Etc = true,
                        }
                    },
                    {
                        "email",
                        {
                        }
                    },
                    {
                        "ultimate_question_of_life",
                        {
                            .AddTags = TTagSet({ /*computed*/ 1, /*opaque*/ 3, /*readonly*/ 4 }),
                        }
                    },
                    {
                        "acl",
                        {
                            .Children = {
                                {
                                    "action",
                                    {
                                        .Etc = true,
                                    }
                                },
                                {
                                    "permissions",
                                    {
                                        .Etc = true,
                                        .Repeated = true,
                                    }
                                },
                                {
                                    "subjects",
                                    {
                                        .Etc = true,
                                        .Repeated = true,
                                    }
                                },
                                {
                                    "attributes",
                                    {
                                        .Etc = true,
                                        .Repeated = true,
                                    }
                                },
                            },
                            .AddTags = TTagSet({ /*system*/ 0 }),
                            .Repeated = true,
                        }
                    },
                    {
                        "creation_time",
                        {
                            .AddTags = TTagSet({ /*system*/ 0, /*readonly*/ 4 }),
                        }
                    },
                    {
                        "inherit_acl",
                        {
                            .AddTags = TTagSet({ /*system*/ 0 }),
                        }
                    },
                    {
                        "uuid",
                        {
                            .AddTags = TTagSet({ /*readonly*/ 4 }),
                            .Etc = true,
                        }
                    },
                    {
                        "name",
                        {
                            .AddTags = TTagSet({ /*readonly*/ 4 }),
                            .Etc = true,
                        }
                    },
                    {
                        "fqid",
                        {
                            .AddTags = TTagSet({ /*system*/ 0, /*computed*/ 1, /*readonly*/ 4 }),
                        }
                    },
                    {
                        "key",
                        {
                            .AddTags = TTagSet({ /*system*/ 0, /*computed*/ 1, /*readonly*/ 4 }),
                        }
                    },
                    {
                        "parent_key",
                        {
                            .AddTags = TTagSet({ /*system*/ 0, /*computed*/ 1, /*readonly*/ 4 }),
                        }
                    },
                    {
                        "finalization_start_time",
                        {
                            .AddTags = TTagSet({ /*readonly*/ 4 }),
                        }
                    },
                    {
                        "finalizers",
                        {
                            .Children = {
                                {
                                    "user_id",
                                    {
                                        .Etc = true,
                                    }
                                },
                                {
                                    "completion_timestamp",
                                    {
                                        .Etc = true,
                                    }
                                },
                            },
                            .AddTags = TTagSet({ /*readonly*/ 4 }),
                        }
                    },
                    {
                        "active_finalizers",
                        {
                            .Children = {
                                {
                                    "user_id",
                                    {
                                        .Etc = true,
                                    }
                                },
                                {
                                    "completion_timestamp",
                                    {
                                        .Etc = true,
                                    }
                                },
                            },
                            .AddTags = TTagSet({ /*system*/ 0, /*computed*/ 1, /*opaque*/ 3, /*readonly*/ 4 }),
                        }
                    },
                    {
                        "type",
                        {
                        }
                    },
                },
                .MessageTags = TTagSet({ /*meta*/ 6 }),
            }
        },
        {
            "spec",
            {
                .Children = {
                },
                .MessageTags = TTagSet({ /*spec*/ 7 }),
            }
        },
        {
            "status",
            {
                .Children = {
                    {
                        "job_title",
                        {
                            .Etc = true,
                        }
                    },
                    {
                        "salary",
                        {
                        }
                    },
                    {
                        "etc_semaphore",
                        {
                            .Children = {
                                {
                                    "spec",
                                    {
                                        .Children = {
                                            {
                                                "budget",
                                                {
                                                    .Etc = true,
                                                }
                                            },
                                        },
                                        .Etc = true,
                                    }
                                },
                                {
                                    "status",
                                    {
                                        .Children = {
                                            {
                                                "leases",
                                                {
                                                    .Children = {
                                                        {
                                                            "last_acquire_time",
                                                            {
                                                                .Etc = true,
                                                            }
                                                        },
                                                        {
                                                            "expiration_time",
                                                            {
                                                                .Etc = true,
                                                            }
                                                        },
                                                        {
                                                            "acquirer_endpoint_description",
                                                            {
                                                                .Etc = true,
                                                            }
                                                        },
                                                        {
                                                            "budget",
                                                            {
                                                                .Etc = true,
                                                            }
                                                        },
                                                    },
                                                    .Etc = true,
                                                }
                                            },
                                            {
                                                "fresh_leases",
                                                {
                                                    .Children = {
                                                        {
                                                            "last_acquire_time",
                                                            {
                                                                .Etc = true,
                                                            }
                                                        },
                                                        {
                                                            "expiration_time",
                                                            {
                                                                .Etc = true,
                                                            }
                                                        },
                                                        {
                                                            "acquirer_endpoint_description",
                                                            {
                                                                .Etc = true,
                                                            }
                                                        },
                                                        {
                                                            "budget",
                                                            {
                                                                .Etc = true,
                                                            }
                                                        },
                                                    },
                                                    .AddTags = TTagSet({ /*computed*/ 1, /*opaque*/ 3 }),
                                                }
                                            },
                                        },
                                        .Etc = true,
                                    }
                                },
                            },
                            .Etc = true,
                        }
                    },
                },
                .MessageTags = TTagSet({ /*status*/ 8 }),
            }
        },
        {
            "control",
            {
                .Children = {
                    {
                        "touch",
                        {
                            .Children = {
                                {
                                    "lock_removal",
                                    {
                                        .Etc = true,
                                    }
                                },
                                {
                                    "store_event_to_history",
                                    {
                                        .Etc = true,
                                    }
                                },
                            },
                            .AddTags = TTagSet({ /*touch*/ 5 }),
                            .Etc = true,
                        }
                    },
                    {
                        "add_finalizer",
                        {
                            .Children = {
                                {
                                    "finalizer_name",
                                    {
                                        .Etc = true,
                                    }
                                },
                            },
                            .Etc = true,
                        }
                    },
                    {
                        "remove_finalizer",
                        {
                            .Children = {
                                {
                                    "finalizer_name",
                                    {
                                        .Etc = true,
                                    }
                                },
                            },
                            .Etc = true,
                        }
                    },
                    {
                        "complete_finalization",
                        {
                            .Children = {
                                {
                                    "finalizer_name",
                                    {
                                        .Etc = true,
                                    }
                                },
                            },
                            .Etc = true,
                        }
                    },
                    {
                        "embedded_semaphore",
                        {
                            .Etc = true,
                        }
                    },
                },
                .MessageTags = TTagSet({ /*control*/ 9 }),
            }
        },
    },
    .AddTags = TTagSet({ /*any*/ 10, /*teal*/ 70 }),
};

void TEmployerTypeHandler::PostInitialize()
{
    // History is configured in PostInitialize stage, as users may add some evaluated fields via plugins in Initialize stage.
    // NB: Called before TBase::PostInitialize as base type handler prepares cache.
    ConfigureHistory(
        /*attributes*/ {
            THistoryAttribute{
                .Path = "",
                .Indexed = false,
            },},
        /*filter*/ TObjectFilter{.Query = "[/labels/skip_history]!=true"});

    TBase::PostInitialize();
    BuildAttributeSchemaTags(
        RootAttributeSchema_,
        EmployerTagsTree);

    FillUpdatePolicies();
}

bool TEmployerTypeHandler::IsObjectNameSupported() const
{
    return true;
}

bool TEmployerTypeHandler::SkipStoreWithoutChanges() const
{
    return false;
}

bool TEmployerTypeHandler::ForceZeroKeyEvaluation() const
{
    return false;
}

std::vector<NYT::NOrm::NServer::NObjects::TWatchLog>
TEmployerTypeHandler::GetWatchLogs() const
{
    return std::vector<NYT::NOrm::NServer::NObjects::TWatchLog>{
        NYT::NOrm::NServer::NObjects::TWatchLog{
            .ObjectName = "employer",
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

bool TEmployerTypeHandler::AreTagsEnabled() const
{
    return true;
}

const NYT::NYson::TProtobufMessageType*
TEmployerTypeHandler::GetRootProtobufType() const
{
    static const auto* type = NYT::NYson::ReflectProtobufMessageType<NYT::NOrm::NExample::NClient::NProto::NDataModel::TEmployer>();
    return type;
}

const TDBFields& TEmployerTypeHandler::GetKeyFields() const
{
    return KeyFields_;
}

NYT::NOrm::NClient::NObjects::TObjectKey TEmployerTypeHandler::GetNullKey() const
{
    return NullKey_;
}

const NYT::NOrm::NServer::NObjects::TDBTable* TEmployerTypeHandler::GetTable() const
{
    return &EmployersTable;
}

const NYT::NOrm::NServer::NObjects::TDBTable* TEmployerTypeHandler::GetParentsTable() const
{
    return nullptr;
}

std::unique_ptr<NYT::NOrm::NServer::NObjects::TObject>
TEmployerTypeHandler::InstantiateObject(
    const NYT::NOrm::NClient::NObjects::TObjectKey& key,
    const NYT::NOrm::NClient::NObjects::TObjectKey& parentKey,
    ISession* session)
{
    YT_VERIFY(!parentKey);

    return std::make_unique<TEmployer>(
        std::get<TString>(key[0]),
        this,
        session);
}

void TEmployerTypeHandler::InitializeCreatedObject(
    NYT::NOrm::NServer::NObjects::TTransaction* transaction,
    NYT::NOrm::NServer::NObjects::TObject* object)
{
    TBase::InitializeCreatedObject(transaction, object);
    auto* typedObject = object->As<TEmployer>();
    YT_VERIFY(typedObject);
    typedObject->MetaEtc().MutableLoad()->set_uuid(NYT::NOrm::NClient::NObjects::GenerateUuid());
}

void TEmployerTypeHandler::ValidateCreatedObject(
    NYT::NOrm::NServer::NObjects::TTransaction* transaction,
    NYT::NOrm::NServer::NObjects::TObject* object)
{
    TBase::ValidateCreatedObject(transaction, object);
    auto* typedObject = object->As<TEmployer>();
    YT_VERIFY(typedObject);
}

void TEmployerTypeHandler::ValidateMetaEtcOnTransactionCommit(
    NYT::NOrm::NServer::NObjects::TTransaction*,
    const TEmployer* object)
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

void TEmployerTypeHandler::ValidateMetaEtcOnValueUpdate(
    NYT::NOrm::NServer::NObjects::TTransaction*,
    const TEmployer* object,
    const TEmployer::TMetaEtc& metaEtcOld,
    const TEmployer::TMetaEtc& metaEtcNew)
{
    ValidateMetaUuid(metaEtcOld, metaEtcNew);

    // Value updates during object creation are naturally considered as an initializers
    // and are not restricted by #UpdatableBySuperuserOnly option.
    if (object->GetState() != NYT::NOrm::NServer::NObjects::EObjectState::Creating) {
        google::protobuf::util::MessageDifferencer differencer;
        differencer.IgnoreField(TEmployer::TMetaEtc::GetDescriptor()
            ->FindFieldByName("name"));
        differencer.IgnoreField(TEmployer::TMetaEtc::GetDescriptor()
            ->FindFieldByName("uuid"));

        // Use #Compare instead of #Equals to honor #IgnoreField calls.
        if (!differencer.Compare(metaEtcOld, metaEtcNew)) {
            this->GetBootstrap()->GetAccessControlManager()
                ->ValidateSuperuser("change user-defined /meta attributes");
        }
    }
}

void TEmployerTypeHandler::ValidateMetaUuid(
    const TEmployer::TMetaEtc& metaEtcOld,
    const TEmployer::TMetaEtc& metaEtcNew)
{
    // Uuid protects, for example, YP secrets from object identity theft, i.e. from ABA race conditions,
    // so we try hard to make sure uuid doesn't change.
    if (!metaEtcOld.uuid().empty() && metaEtcOld.uuid() != metaEtcNew.uuid()) {
        this->GetBootstrap()->GetAccessControlManager()->ValidateSuperuser("change /meta/uuid");
    }
}

////////////////////////////////////////////////////////////////////////////////

TExecutorTypeHandler::TExecutorTypeHandler(
    IBootstrap* bootstrap,
    NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config)
    : TBase(
        bootstrap,
        static_cast<NYT::NOrm::NClient::NObjects::TObjectTypeValue>(EObjectType::Executor))
    , Config_(std::move(config))
    , KeyFields_({
        &ExecutorsTable.Fields.MetaId})
    , NullKey_(NYT::NOrm::NServer::NObjects::GenerateZeroKey(KeyFields_))
{ }

void TExecutorTypeHandler::Initialize()
{
    TBase::Initialize();

    MetaAttributeSchema_
        ->AddChildren({
        MakeScalarAttributeSchema("acl", TDataModelObject::AclDescriptor)
            ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::Updatable)
            ->AddValidator<NYT::NOrm::NServer::NObjects::TObject>(std::bind_front(
                &TExecutorTypeHandler::ValidateAcl, this))
    });
    MetaAttributeSchema_
        ->AddChildren({
            MakeIdAttributeSchema(
                "id",
                &ExecutorsTable.Fields.MetaId,
                MakeValueGetter(&TExecutor::GetId))
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
            MakeScalarAttributeSchema("finalization_start_time", TExecutor::FinalizationStartTimeDescriptor)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueReadOnly),
            MakeScalarAttributeSchema("finalizers", TExecutor::FinalizersDescriptor)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueReadOnly),
            MakeScalarAttributeSchema("ultimate_question_of_life")
                ->SetComputed()
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::ReadOnly)
                ->SetOpaque(/*emitEntity*/ false),
            TBase::MakeEtcAttributeSchema(TExecutor::MetaEtcDescriptor.AddValidator(
                std::bind_front(&TExecutorTypeHandler::ValidateMetaEtcOnValueUpdate, this)))
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::Updatable)
                ->template AddValidator<TExecutor>(std::bind_front(
                    &TExecutorTypeHandler::ValidateMetaEtcOnTransactionCommit, this))
            });

    SpecAttributeSchema_
        ->AddChildren({
            MakeEtcAttributeSchema(TExecutor::TSpec::EtcDescriptor),
        });

    StatusAttributeSchema_
        ->AddChildren({
            MakeEtcAttributeSchema(TExecutor::TStatus::EtcDescriptor),
        });

    ControlAttributeSchema_
        ->AddChildren({
            MakeScalarAttributeSchema("touch")
                ->SetControl<TExecutor, NYT::NOrm::NExample::NClient::NProto::NDataModel::TObjectTouch>(
                    [] (NYT::NOrm::NServer::NObjects::TTransaction* transaction,
                        TExecutor* object,
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

void TExecutorTypeHandler::PostInitialize()
{
    // History is configured in PostInitialize stage, as users may add some evaluated fields via plugins in Initialize stage.
    // NB: Called before TBase::PostInitialize as base type handler prepares cache.
    ConfigureHistory(
        /*attributes*/ {},
        /*filter*/ std::nullopt);

    TBase::PostInitialize();

    FillUpdatePolicies();
}

bool TExecutorTypeHandler::IsObjectNameSupported() const
{
    return false;
}

bool TExecutorTypeHandler::SkipStoreWithoutChanges() const
{
    return false;
}

bool TExecutorTypeHandler::ForceZeroKeyEvaluation() const
{
    return false;
}

std::vector<NYT::NOrm::NServer::NObjects::TWatchLog>
TExecutorTypeHandler::GetWatchLogs() const
{
    return std::vector<NYT::NOrm::NServer::NObjects::TWatchLog>{
        NYT::NOrm::NServer::NObjects::TWatchLog{
            .ObjectName = "executor",
            .Name = "watch_log",
            .Filter{
                .Query{
                    ""
                }
            },
            .Selector{
            },
        },
        NYT::NOrm::NServer::NObjects::TWatchLog{
            .ObjectName = "executor",
            .Name = "khalai_rank_watch_log",
            .Filter{
                .Query{
                    "[/labels/tribe] = 'Khalai'"
                }
            },
            .Selector{
                "/spec/rank",
            },
        },
        NYT::NOrm::NServer::NObjects::TWatchLog{
            .ObjectName = "executor",
            .Name = "selector_watch_log",
            .Filter{
                .Query{
                    ""
                }
            },
            .Selector{
                "/labels/tribe",
            },
        },
    };
}

const NYT::NYson::TProtobufMessageType*
TExecutorTypeHandler::GetRootProtobufType() const
{
    static const auto* type = NYT::NYson::ReflectProtobufMessageType<NYT::NOrm::NExample::NClient::NProto::NDataModel::TExecutor>();
    return type;
}

const TDBFields& TExecutorTypeHandler::GetKeyFields() const
{
    return KeyFields_;
}

NYT::NOrm::NClient::NObjects::TObjectKey TExecutorTypeHandler::GetNullKey() const
{
    return NullKey_;
}

const NYT::NOrm::NServer::NObjects::TDBTable* TExecutorTypeHandler::GetTable() const
{
    return &ExecutorsTable;
}

const NYT::NOrm::NServer::NObjects::TDBTable* TExecutorTypeHandler::GetParentsTable() const
{
    return nullptr;
}

std::unique_ptr<NYT::NOrm::NServer::NObjects::TObject>
TExecutorTypeHandler::InstantiateObject(
    const NYT::NOrm::NClient::NObjects::TObjectKey& key,
    const NYT::NOrm::NClient::NObjects::TObjectKey& parentKey,
    ISession* session)
{
    YT_VERIFY(!parentKey);

    return std::make_unique<TExecutor>(
        std::get<i64>(key[0]),
        this,
        session);
}

void TExecutorTypeHandler::InitializeCreatedObject(
    NYT::NOrm::NServer::NObjects::TTransaction* transaction,
    NYT::NOrm::NServer::NObjects::TObject* object)
{
    TBase::InitializeCreatedObject(transaction, object);
    auto* typedObject = object->As<TExecutor>();
    YT_VERIFY(typedObject);
    typedObject->MetaEtc().MutableLoad()->set_uuid(NYT::NOrm::NClient::NObjects::GenerateUuid());
}

void TExecutorTypeHandler::ValidateCreatedObject(
    NYT::NOrm::NServer::NObjects::TTransaction* transaction,
    NYT::NOrm::NServer::NObjects::TObject* object)
{
    TBase::ValidateCreatedObject(transaction, object);
    auto* typedObject = object->As<TExecutor>();
    YT_VERIFY(typedObject);
}

void TExecutorTypeHandler::ValidateMetaEtcOnTransactionCommit(
    NYT::NOrm::NServer::NObjects::TTransaction*,
    const TExecutor* object)
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

void TExecutorTypeHandler::ValidateMetaEtcOnValueUpdate(
    NYT::NOrm::NServer::NObjects::TTransaction*,
    const TExecutor* object,
    const TExecutor::TMetaEtc& metaEtcOld,
    const TExecutor::TMetaEtc& metaEtcNew)
{
    ValidateMetaUuid(metaEtcOld, metaEtcNew);

    // All user-defined attributes are considered updatable by any user
    // having corresponding write permission.
    Y_UNUSED(object);
}

void TExecutorTypeHandler::ValidateMetaUuid(
    const TExecutor::TMetaEtc& metaEtcOld,
    const TExecutor::TMetaEtc& metaEtcNew)
{
    // Uuid protects, for example, YP secrets from object identity theft, i.e. from ABA race conditions,
    // so we try hard to make sure uuid doesn't change.
    if (!metaEtcOld.uuid().empty() && metaEtcOld.uuid() != metaEtcNew.uuid()) {
        this->GetBootstrap()->GetAccessControlManager()->ValidateSuperuser("change /meta/uuid");
    }
}

////////////////////////////////////////////////////////////////////////////////

TGenreTypeHandler::TGenreTypeHandler(
    IBootstrap* bootstrap,
    NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config)
    : TBase(
        bootstrap,
        static_cast<NYT::NOrm::NClient::NObjects::TObjectTypeValue>(EObjectType::Genre))
    , Config_(std::move(config))
    , KeyFields_({
        &GenresTable.Fields.MetaId})
    , NullKey_(NYT::NOrm::NServer::NObjects::GenerateZeroKey(KeyFields_))
{ }

TGenreTypeHandler::~TGenreTypeHandler()
{ }

void TGenreTypeHandler::Initialize()
{
    TBase::Initialize();

    MetaAttributeSchema_
        ->AddChildren({
        MakeScalarAttributeSchema("acl", TDataModelObject::AclDescriptor)
            ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::Updatable)
            ->AddValidator<NYT::NOrm::NServer::NObjects::TObject>(std::bind_front(
                &TGenreTypeHandler::ValidateAcl, this))
    });
    MetaAttributeSchema_
        ->AddChildren({
            MakeIdAttributeSchema(
                "id",
                &GenresTable.Fields.MetaId,
                MakeValueGetter(&TGenre::GetId))
                ->SetCustomPolicyExpected(),
            MakeScalarAttributeSchema("finalization_start_time", TGenre::FinalizationStartTimeDescriptor)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueReadOnly),
            MakeScalarAttributeSchema("finalizers", TGenre::FinalizersDescriptor)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueReadOnly),
            MakeScalarAttributeSchema("ultimate_question_of_life")
                ->SetComputed()
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::ReadOnly)
                ->SetOpaque(/*emitEntity*/ false),
            TBase::MakeEtcAttributeSchema(TGenre::MetaEtcDescriptor.AddValidator(
                std::bind_front(&TGenreTypeHandler::ValidateMetaEtcOnValueUpdate, this)))
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::Updatable)
                ->template AddValidator<TGenre>(std::bind_front(
                    &TGenreTypeHandler::ValidateMetaEtcOnTransactionCommit, this))
            });

    SpecAttributeSchema_
        ->AddChildren({
            MakeEtcAttributeSchema(TGenre::TSpec::EtcDescriptor),
            MakeScalarAttributeSchema("is_name_substring")
                ->SetComputed(),
        });

    StatusAttributeSchema_
        ->AddChildren({
            MakeEtcAttributeSchema(TGenre::TStatus::EtcDescriptor),
        });

    ControlAttributeSchema_
        ->AddChildren({
            MakeScalarAttributeSchema("touch")
                ->SetControl<TGenre, NYT::NOrm::NExample::NClient::NProto::NDataModel::TObjectTouch>(
                    [] (NYT::NOrm::NServer::NObjects::TTransaction* transaction,
                        TGenre* object,
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
                ->SetControl<TGenre, NYT::NOrm::NExample::NClient::NProto::NDataModel::TObjectTouchIndex>(
                [] (NYT::NOrm::NServer::NObjects::TTransaction* /*transaction*/,
                    TGenre* object,
                    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TObjectTouchIndex& touchIndex)
                {
                    for (const auto& indexName : touchIndex.index_names()) {
                        object->GetIndexOrThrow(indexName)->TouchIndex();
                    }
                }),
        });
    RegisterScalarAttributeIndex(
        "genre_by_name",
        std::make_unique<NYT::NOrm::NServer::NObjects::TScalarAttributeIndexDescriptor>(
            "genre_by_name",
            &GenresByNameTable,
            std::vector<NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor>{
                NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor(
                    &TGenre::TSpec::EtcDescriptor,
                    "/name"),
            },
            std::vector<NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor>{
            },
            /*repeated*/ false,
            Config_->TryGetIndexMode("genre_by_name")
                .value_or(NYT::NOrm::NServer::NObjects::EIndexMode::Enabled)));
}

void TGenreTypeHandler::PostInitialize()
{
    // History is configured in PostInitialize stage, as users may add some evaluated fields via plugins in Initialize stage.
    // NB: Called before TBase::PostInitialize as base type handler prepares cache.
    ConfigureHistory(
        /*attributes*/ {},
        /*filter*/ std::nullopt);

    TBase::PostInitialize();

    FillUpdatePolicies();
}

bool TGenreTypeHandler::IsObjectNameSupported() const
{
    return false;
}

bool TGenreTypeHandler::SkipStoreWithoutChanges() const
{
    return false;
}

bool TGenreTypeHandler::ForceZeroKeyEvaluation() const
{
    return false;
}

std::vector<NYT::NOrm::NServer::NObjects::TWatchLog>
TGenreTypeHandler::GetWatchLogs() const
{
    return std::vector<NYT::NOrm::NServer::NObjects::TWatchLog>{
    };
}

const NYT::NYson::TProtobufMessageType*
TGenreTypeHandler::GetRootProtobufType() const
{
    static const auto* type = NYT::NYson::ReflectProtobufMessageType<NYT::NOrm::NExample::NClient::NProto::NDataModel::TGenre>();
    return type;
}

const TDBFields& TGenreTypeHandler::GetKeyFields() const
{
    return KeyFields_;
}

NYT::NOrm::NClient::NObjects::TObjectKey TGenreTypeHandler::GetNullKey() const
{
    return NullKey_;
}

const NYT::NOrm::NServer::NObjects::TDBTable* TGenreTypeHandler::GetTable() const
{
    return &GenresTable;
}

const NYT::NOrm::NServer::NObjects::TDBTable* TGenreTypeHandler::GetParentsTable() const
{
    return nullptr;
}

std::unique_ptr<NYT::NOrm::NServer::NObjects::TObject>
TGenreTypeHandler::InstantiateObject(
    const NYT::NOrm::NClient::NObjects::TObjectKey& key,
    const NYT::NOrm::NClient::NObjects::TObjectKey& parentKey,
    ISession* session)
{
    YT_VERIFY(!parentKey);

    return std::make_unique<TGenre>(
        std::get<TString>(key[0]),
        this,
        session);
}

void TGenreTypeHandler::InitializeCreatedObject(
    NYT::NOrm::NServer::NObjects::TTransaction* transaction,
    NYT::NOrm::NServer::NObjects::TObject* object)
{
    TBase::InitializeCreatedObject(transaction, object);
    auto* typedObject = object->As<TGenre>();
    YT_VERIFY(typedObject);
    typedObject->MetaEtc().MutableLoad()->set_uuid(NYT::NOrm::NClient::NObjects::GenerateUuid());
}

void TGenreTypeHandler::ValidateCreatedObject(
    NYT::NOrm::NServer::NObjects::TTransaction* transaction,
    NYT::NOrm::NServer::NObjects::TObject* object)
{
    TBase::ValidateCreatedObject(transaction, object);
    auto* typedObject = object->As<TGenre>();
    YT_VERIFY(typedObject);
}

void TGenreTypeHandler::ValidateMetaEtcOnTransactionCommit(
    NYT::NOrm::NServer::NObjects::TTransaction*,
    const TGenre* object)
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

void TGenreTypeHandler::ValidateMetaEtcOnValueUpdate(
    NYT::NOrm::NServer::NObjects::TTransaction*,
    const TGenre* object,
    const TGenre::TMetaEtc& metaEtcOld,
    const TGenre::TMetaEtc& metaEtcNew)
{
    ValidateMetaUuid(metaEtcOld, metaEtcNew);

    // All user-defined attributes are considered updatable by any user
    // having corresponding write permission.
    Y_UNUSED(object);
}

void TGenreTypeHandler::ValidateMetaUuid(
    const TGenre::TMetaEtc& metaEtcOld,
    const TGenre::TMetaEtc& metaEtcNew)
{
    // Uuid protects, for example, YP secrets from object identity theft, i.e. from ABA race conditions,
    // so we try hard to make sure uuid doesn't change.
    if (!metaEtcOld.uuid().empty() && metaEtcOld.uuid() != metaEtcNew.uuid()) {
        this->GetBootstrap()->GetAccessControlManager()->ValidateSuperuser("change /meta/uuid");
    }
}

////////////////////////////////////////////////////////////////////////////////

TGroupTypeHandler::TGroupTypeHandler(
    IBootstrap* bootstrap,
    NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config)
    : TBase(
        bootstrap,
        static_cast<NYT::NOrm::NClient::NObjects::TObjectTypeValue>(EObjectType::Group))
    , Config_(std::move(config))
    , KeyFields_({
        &GroupsTable.Fields.MetaId})
    , NullKey_(NYT::NOrm::NServer::NObjects::GenerateZeroKey(KeyFields_))
{ }

void TGroupTypeHandler::Initialize()
{
    TBase::Initialize();

    MetaAttributeSchema_
        ->AddChildren({
        MakeScalarAttributeSchema("acl", TDataModelObject::AclDescriptor)
            ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::Updatable)
            ->AddValidator<NYT::NOrm::NServer::NObjects::TObject>(std::bind_front(
                &TGroupTypeHandler::ValidateAcl, this))
    });
    MetaAttributeSchema_
        ->AddChildren({
            MakeIdAttributeSchema(
                "id",
                &GroupsTable.Fields.MetaId,
                MakeValueGetter(&TGroup::GetId))
                ->SetKeyFieldPolicy(
                    NYT::NOrm::NServer::NObjects::CreateStringAttributePolicy(
                        NYT::NOrm::NServer::NObjects::EAttributeGenerationPolicy::Random,
                        NYT::NOrm::NServer::NObjects::DefaultMinStringAttributeLength,
                        NYT::NOrm::NServer::NObjects::DefaultMaxStringAttributeLength,
                        NYT::NOrm::NServer::NObjects::DefaultStringAttributeValidChars)),
            MakeScalarAttributeSchema("finalization_start_time", TGroup::FinalizationStartTimeDescriptor)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueReadOnly),
            MakeScalarAttributeSchema("finalizers", TGroup::FinalizersDescriptor)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueReadOnly),
            MakeScalarAttributeSchema("ultimate_question_of_life")
                ->SetComputed()
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::ReadOnly)
                ->SetOpaque(/*emitEntity*/ false),
            TBase::MakeEtcAttributeSchema(TGroup::MetaEtcDescriptor.AddValidator(
                std::bind_front(&TGroupTypeHandler::ValidateMetaEtcOnValueUpdate, this)))
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::Updatable)
                ->template AddValidator<TGroup>(std::bind_front(
                    &TGroupTypeHandler::ValidateMetaEtcOnTransactionCommit, this))
            });

    SpecAttributeSchema_
        ->AddChildren({
            MakeEtcAttributeSchema(TGroup::TSpec::EtcDescriptor),
        });

    StatusAttributeSchema_
        ->AddChildren({
            MakeEtcAttributeSchema(TGroup::TStatus::EtcDescriptor),
        });

    ControlAttributeSchema_
        ->AddChildren({
            MakeScalarAttributeSchema("touch")
                ->SetControl<TGroup, NYT::NOrm::NExample::NClient::NProto::NDataModel::TObjectTouch>(
                    [] (NYT::NOrm::NServer::NObjects::TTransaction* transaction,
                        TGroup* object,
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

void TGroupTypeHandler::PostInitialize()
{
    // History is configured in PostInitialize stage, as users may add some evaluated fields via plugins in Initialize stage.
    // NB: Called before TBase::PostInitialize as base type handler prepares cache.
    ConfigureHistory(
        /*attributes*/ {},
        /*filter*/ std::nullopt);

    TBase::PostInitialize();

    FillUpdatePolicies();
}

bool TGroupTypeHandler::IsObjectNameSupported() const
{
    return true;
}

bool TGroupTypeHandler::SkipStoreWithoutChanges() const
{
    return false;
}

bool TGroupTypeHandler::ForceZeroKeyEvaluation() const
{
    return false;
}

std::vector<NYT::NOrm::NServer::NObjects::TWatchLog>
TGroupTypeHandler::GetWatchLogs() const
{
    return std::vector<NYT::NOrm::NServer::NObjects::TWatchLog>{
        NYT::NOrm::NServer::NObjects::TWatchLog{
            .ObjectName = "group",
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
TGroupTypeHandler::GetRootProtobufType() const
{
    static const auto* type = NYT::NYson::ReflectProtobufMessageType<NYT::NOrm::NExample::NClient::NProto::NDataModel::TGroup>();
    return type;
}

const TDBFields& TGroupTypeHandler::GetKeyFields() const
{
    return KeyFields_;
}

NYT::NOrm::NClient::NObjects::TObjectKey TGroupTypeHandler::GetNullKey() const
{
    return NullKey_;
}

const NYT::NOrm::NServer::NObjects::TDBTable* TGroupTypeHandler::GetTable() const
{
    return &GroupsTable;
}

const NYT::NOrm::NServer::NObjects::TDBTable* TGroupTypeHandler::GetParentsTable() const
{
    return nullptr;
}

std::unique_ptr<NYT::NOrm::NServer::NObjects::TObject>
TGroupTypeHandler::InstantiateObject(
    const NYT::NOrm::NClient::NObjects::TObjectKey& key,
    const NYT::NOrm::NClient::NObjects::TObjectKey& parentKey,
    ISession* session)
{
    YT_VERIFY(!parentKey);

    return std::make_unique<TGroup>(
        std::get<TString>(key[0]),
        this,
        session);
}

void TGroupTypeHandler::InitializeCreatedObject(
    NYT::NOrm::NServer::NObjects::TTransaction* transaction,
    NYT::NOrm::NServer::NObjects::TObject* object)
{
    TBase::InitializeCreatedObject(transaction, object);
    auto* typedObject = object->As<TGroup>();
    YT_VERIFY(typedObject);
    typedObject->MetaEtc().MutableLoad()->set_uuid(NYT::NOrm::NClient::NObjects::GenerateUuid());
}

void TGroupTypeHandler::ValidateCreatedObject(
    NYT::NOrm::NServer::NObjects::TTransaction* transaction,
    NYT::NOrm::NServer::NObjects::TObject* object)
{
    TBase::ValidateCreatedObject(transaction, object);
    auto* typedObject = object->As<TGroup>();
    YT_VERIFY(typedObject);
}

void TGroupTypeHandler::ValidateMetaEtcOnTransactionCommit(
    NYT::NOrm::NServer::NObjects::TTransaction*,
    const TGroup* object)
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

void TGroupTypeHandler::ValidateMetaEtcOnValueUpdate(
    NYT::NOrm::NServer::NObjects::TTransaction*,
    const TGroup* object,
    const TGroup::TMetaEtc& metaEtcOld,
    const TGroup::TMetaEtc& metaEtcNew)
{
    ValidateMetaUuid(metaEtcOld, metaEtcNew);

    // All user-defined attributes are considered updatable by any user
    // having corresponding write permission.
    Y_UNUSED(object);
}

void TGroupTypeHandler::ValidateMetaUuid(
    const TGroup::TMetaEtc& metaEtcOld,
    const TGroup::TMetaEtc& metaEtcNew)
{
    // Uuid protects, for example, YP secrets from object identity theft, i.e. from ABA race conditions,
    // so we try hard to make sure uuid doesn't change.
    if (!metaEtcOld.uuid().empty() && metaEtcOld.uuid() != metaEtcNew.uuid()) {
        this->GetBootstrap()->GetAccessControlManager()->ValidateSuperuser("change /meta/uuid");
    }
}

////////////////////////////////////////////////////////////////////////////////

THitchhikerTypeHandler::THitchhikerTypeHandler(
    IBootstrap* bootstrap,
    NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config)
    : TBase(
        bootstrap,
        static_cast<NYT::NOrm::NClient::NObjects::TObjectTypeValue>(EObjectType::Hitchhiker))
    , Config_(std::move(config))
    , KeyFields_({
        &HitchhikersTable.Fields.MetaId})
    , NullKey_(NYT::NOrm::NServer::NObjects::GenerateZeroKey(KeyFields_))
{ }

void THitchhikerTypeHandler::Initialize()
{
    TBase::Initialize();

    MetaAttributeSchema_
        ->AddChildren({
        MakeScalarAttributeSchema("acl", TDataModelObject::AclDescriptor)
            ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::Updatable)
            ->AddValidator<NYT::NOrm::NServer::NObjects::TObject>(std::bind_front(
                &THitchhikerTypeHandler::ValidateAcl, this))
    });
    MetaAttributeSchema_
        ->AddChildren({
            MakeIdAttributeSchema(
                "id",
                &HitchhikersTable.Fields.MetaId,
                MakeValueGetter(&THitchhiker::GetId))
                ->SetKeyFieldPolicy(
                    NYT::NOrm::NServer::NObjects::CreateIntegerAttributePolicy<i64>(
                        NYT::NOrm::NServer::NObjects::EAttributeGenerationPolicy::Manual,
                        GetBootstrap(),
                        NYT::NOrm::NServer::NObjects::DefaultMinIntegerAttributeValue<i64>,
                        NYT::NOrm::NServer::NObjects::DefaultMaxIntegerAttributeValue<i64>,
                        NYT::NOrm::NServer::NObjects::TAttributePolicyOptions{
                            .Type = GetType(),
                            .AttributePath = "/meta/id",
                            .IndexForIncrement = ""
                        })),
            MakeScalarAttributeSchema("formative_book_id", THitchhiker::FormativeBookIdDescriptor),
            MakeScalarAttributeSchema("formative_book_id2", THitchhiker::FormativeBookId2Descriptor),
            MakeScalarAttributeSchema("finalization_start_time", THitchhiker::FinalizationStartTimeDescriptor)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueReadOnly),
            MakeScalarAttributeSchema("finalizers", THitchhiker::FinalizersDescriptor)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueReadOnly),
    MakeScalarAttributeSchema("formative_book", THitchhiker::FormativeBookDescriptor),
            MakeScalarAttributeSchema("formative_book_view", THitchhiker::FormativeBookViewDescriptor),
            MakeScalarAttributeSchema("ultimate_question_of_life")
                ->SetComputed()
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::ReadOnly)
                ->SetOpaque(/*emitEntity*/ false),
            TBase::MakeEtcAttributeSchema(THitchhiker::MetaEtcDescriptor.AddValidator(
                std::bind_front(&THitchhikerTypeHandler::ValidateMetaEtcOnValueUpdate, this)))
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::Updatable)
                ->template AddValidator<THitchhiker>(std::bind_front(
                    &THitchhikerTypeHandler::ValidateMetaEtcOnTransactionCommit, this))
            });

    SpecAttributeSchema_
        ->AddChildren({
            MakeScalarAttributeSchema("hated_books", THitchhiker::TSpec::HatedBooksDescriptor),
            MakeEtcAttributeSchema(THitchhiker::TSpec::EtcDescriptor),
            MakeScalarAttributeSchema("books", THitchhiker::TSpec::BooksDescriptor),
            MakeScalarAttributeSchema("books_view", THitchhiker::TSpec::BooksViewDescriptor),
        });

    StatusAttributeSchema_
        ->AddChildren({
            MakeEtcAttributeSchema(THitchhiker::TStatus::EtcDescriptor),
        });

    ControlAttributeSchema_
        ->AddChildren({
            MakeScalarAttributeSchema("touch")
                ->SetControl<THitchhiker, NYT::NOrm::NExample::NClient::NProto::NDataModel::TObjectTouch>(
                    [] (NYT::NOrm::NServer::NObjects::TTransaction* transaction,
                        THitchhiker* object,
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

void THitchhikerTypeHandler::PostInitialize()
{
    // History is configured in PostInitialize stage, as users may add some evaluated fields via plugins in Initialize stage.
    // NB: Called before TBase::PostInitialize as base type handler prepares cache.
    ConfigureHistory(
        /*attributes*/ {},
        /*filter*/ std::nullopt);

    TBase::PostInitialize();

    FillUpdatePolicies();
}

bool THitchhikerTypeHandler::IsObjectNameSupported() const
{
    return false;
}

bool THitchhikerTypeHandler::SkipStoreWithoutChanges() const
{
    return false;
}

bool THitchhikerTypeHandler::ForceZeroKeyEvaluation() const
{
    return false;
}

std::vector<NYT::NOrm::NServer::NObjects::TWatchLog>
THitchhikerTypeHandler::GetWatchLogs() const
{
    return std::vector<NYT::NOrm::NServer::NObjects::TWatchLog>{
    };
}

const NYT::NYson::TProtobufMessageType*
THitchhikerTypeHandler::GetRootProtobufType() const
{
    static const auto* type = NYT::NYson::ReflectProtobufMessageType<NYT::NOrm::NExample::NClient::NProto::NDataModel::THitchhiker>();
    return type;
}

const TDBFields& THitchhikerTypeHandler::GetKeyFields() const
{
    return KeyFields_;
}

NYT::NOrm::NClient::NObjects::TObjectKey THitchhikerTypeHandler::GetNullKey() const
{
    return NullKey_;
}

const NYT::NOrm::NServer::NObjects::TDBTable* THitchhikerTypeHandler::GetTable() const
{
    return &HitchhikersTable;
}

const NYT::NOrm::NServer::NObjects::TDBTable* THitchhikerTypeHandler::GetParentsTable() const
{
    return nullptr;
}

std::unique_ptr<NYT::NOrm::NServer::NObjects::TObject>
THitchhikerTypeHandler::InstantiateObject(
    const NYT::NOrm::NClient::NObjects::TObjectKey& key,
    const NYT::NOrm::NClient::NObjects::TObjectKey& parentKey,
    ISession* session)
{
    YT_VERIFY(!parentKey);

    return std::make_unique<THitchhiker>(
        std::get<i64>(key[0]),
        this,
        session);
}

void THitchhikerTypeHandler::InitializeCreatedObject(
    NYT::NOrm::NServer::NObjects::TTransaction* transaction,
    NYT::NOrm::NServer::NObjects::TObject* object)
{
    TBase::InitializeCreatedObject(transaction, object);
    auto* typedObject = object->As<THitchhiker>();
    YT_VERIFY(typedObject);
    typedObject->MetaEtc().MutableLoad()->set_uuid(NYT::NOrm::NClient::NObjects::GenerateUuid());
}

void THitchhikerTypeHandler::ValidateCreatedObject(
    NYT::NOrm::NServer::NObjects::TTransaction* transaction,
    NYT::NOrm::NServer::NObjects::TObject* object)
{
    TBase::ValidateCreatedObject(transaction, object);
    auto* typedObject = object->As<THitchhiker>();
    YT_VERIFY(typedObject);
}

void THitchhikerTypeHandler::ValidateMetaEtcOnTransactionCommit(
    NYT::NOrm::NServer::NObjects::TTransaction*,
    const THitchhiker* object)
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

void THitchhikerTypeHandler::ValidateMetaEtcOnValueUpdate(
    NYT::NOrm::NServer::NObjects::TTransaction*,
    const THitchhiker* object,
    const THitchhiker::TMetaEtc& metaEtcOld,
    const THitchhiker::TMetaEtc& metaEtcNew)
{
    ValidateMetaUuid(metaEtcOld, metaEtcNew);

    // All user-defined attributes are considered updatable by any user
    // having corresponding write permission.
    Y_UNUSED(object);
}

void THitchhikerTypeHandler::ValidateMetaUuid(
    const THitchhiker::TMetaEtc& metaEtcOld,
    const THitchhiker::TMetaEtc& metaEtcNew)
{
    // Uuid protects, for example, YP secrets from object identity theft, i.e. from ABA race conditions,
    // so we try hard to make sure uuid doesn't change.
    if (!metaEtcOld.uuid().empty() && metaEtcOld.uuid() != metaEtcNew.uuid()) {
        this->GetBootstrap()->GetAccessControlManager()->ValidateSuperuser("change /meta/uuid");
    }
}

////////////////////////////////////////////////////////////////////////////////

TIllustratorTypeHandler::TIllustratorTypeHandler(
    IBootstrap* bootstrap,
    NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config)
    : TBase(
        bootstrap,
        static_cast<NYT::NOrm::NClient::NObjects::TObjectTypeValue>(EObjectType::Illustrator))
    , Config_(std::move(config))
    , KeyFields_({
        &IllustratorsTable.Fields.MetaUid})
    , ParentKeyFields_({
        &IllustratorsTable.Fields.MetaPublisherId})
    , AccessControlParentKeyFields_({
        &IllustratorsTable.Fields.MetaPartTimeJob})
    , NullKey_(NYT::NOrm::NServer::NObjects::GenerateZeroKey(KeyFields_))
{ }

void TIllustratorTypeHandler::Initialize()
{
    TBase::Initialize();

    MetaAttributeSchema_
        ->AddChildren({
        MakeScalarAttributeSchema("acl", TDataModelObject::AclDescriptor)
            ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::Updatable)
            ->AddValidator<NYT::NOrm::NServer::NObjects::TObject>(std::bind_front(
                &TIllustratorTypeHandler::ValidateAcl, this))
    });
    MetaAttributeSchema_
        ->AddChildren({
            MakeParentIdAttributeSchema(
                "publisher_id",
                &IllustratorsTable.Fields.MetaPublisherId,
                MakeValueGetter(&TIllustrator::PublisherId)),
            MakeIdAttributeSchema(
                "uid",
                &IllustratorsTable.Fields.MetaUid,
                MakeValueGetter(&TIllustrator::GetUid))
                ->SetKeyFieldPolicy(
                    NYT::NOrm::NServer::NObjects::CreateIntegerAttributePolicy<i64>(
                        NYT::NOrm::NServer::NObjects::EAttributeGenerationPolicy::Random,
                        GetBootstrap(),
                        NYT::NOrm::NServer::NObjects::DefaultMinIntegerAttributeValue<i64>,
                        NYT::NOrm::NServer::NObjects::DefaultMaxIntegerAttributeValue<i64>,
                        NYT::NOrm::NServer::NObjects::TAttributePolicyOptions{
                            .Type = GetType(),
                            .AttributePath = "/meta/uid",
                            .IndexForIncrement = ""
                        })),
            MakeAccessControlParentIdAttributeSchema("part_time_job", TIllustrator::PartTimeJobDescriptor)
                ->SetOpaque(),
            MakeScalarAttributeSchema("finalization_start_time", TIllustrator::FinalizationStartTimeDescriptor)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueReadOnly),
            MakeScalarAttributeSchema("finalizers", TIllustrator::FinalizersDescriptor)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueReadOnly),
            MakeScalarAttributeSchema("part_time_job_view", TIllustrator::PartTimeJobViewDescriptor),
            MakeScalarAttributeSchema("ultimate_question_of_life")
                ->SetComputed()
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::ReadOnly)
                ->SetOpaque(/*emitEntity*/ false),
            TBase::MakeEtcAttributeSchema(TIllustrator::MetaEtcDescriptor.AddValidator(
                std::bind_front(&TIllustratorTypeHandler::ValidateMetaEtcOnValueUpdate, this)))
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::Updatable)
                ->template AddValidator<TIllustrator>(std::bind_front(
                    &TIllustratorTypeHandler::ValidateMetaEtcOnTransactionCommit, this))
            });

    SpecAttributeSchema_
        ->AddChildren({
            MakeEtcAttributeSchema(TIllustrator::TSpec::EtcDescriptor),
        });

    StatusAttributeSchema_
        ->AddChildren({
            MakeEtcAttributeSchema(TIllustrator::TStatus::EtcDescriptor),
        });

    ControlAttributeSchema_
        ->AddChildren({
            MakeScalarAttributeSchema("touch")
                ->SetControl<TIllustrator, NYT::NOrm::NExample::NClient::NProto::NDataModel::TObjectTouch>(
                    [] (NYT::NOrm::NServer::NObjects::TTransaction* transaction,
                        TIllustrator* object,
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
                ->SetControl<TIllustrator, NYT::NOrm::NExample::NClient::NProto::NDataModel::TObjectTouchIndex>(
                [] (NYT::NOrm::NServer::NObjects::TTransaction* /*transaction*/,
                    TIllustrator* object,
                    const NYT::NOrm::NExample::NClient::NProto::NDataModel::TObjectTouchIndex& touchIndex)
                {
                    for (const auto& indexName : touchIndex.index_names()) {
                        object->GetIndexOrThrow(indexName)->TouchIndex();
                    }
                }),
        });
    RegisterScalarAttributeIndex(
        "illustrators_by_publisher",
        std::make_unique<NYT::NOrm::NServer::NObjects::TScalarAttributeIndexDescriptor>(
            "illustrators_by_publisher",
            &PublisherToIllustratorsTable,
            std::vector<NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor>{
                NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor(
                    &TIllustrator::PartTimeJobDescriptor,
                    ""),
            },
            std::vector<NYT::NOrm::NServer::NObjects::TIndexedAttributeDescriptor>{
            },
            /*repeated*/ false,
            Config_->TryGetIndexMode("illustrators_by_publisher")
                .value_or(NYT::NOrm::NServer::NObjects::EIndexMode::Enabled)));
}

void TIllustratorTypeHandler::PostInitialize()
{
    // History is configured in PostInitialize stage, as users may add some evaluated fields via plugins in Initialize stage.
    // NB: Called before TBase::PostInitialize as base type handler prepares cache.
    ConfigureHistory(
        /*attributes*/ {
            THistoryAttribute{
                .Path = "/meta",
                .Indexed = false,
            },},
        /*filter*/ std::nullopt);

    TBase::PostInitialize();

    FillUpdatePolicies();
}

bool TIllustratorTypeHandler::IsObjectNameSupported() const
{
    return true;
}

bool TIllustratorTypeHandler::SkipStoreWithoutChanges() const
{
    return false;
}

bool TIllustratorTypeHandler::ForceZeroKeyEvaluation() const
{
    return false;
}

std::vector<NYT::NOrm::NServer::NObjects::TWatchLog>
TIllustratorTypeHandler::GetWatchLogs() const
{
    return std::vector<NYT::NOrm::NServer::NObjects::TWatchLog>{
    };
}

const NYT::NYson::TProtobufMessageType*
TIllustratorTypeHandler::GetRootProtobufType() const
{
    static const auto* type = NYT::NYson::ReflectProtobufMessageType<NYT::NOrm::NExample::NClient::NProto::NDataModel::TIllustrator>();
    return type;
}

const TDBFields& TIllustratorTypeHandler::GetKeyFields() const
{
    return KeyFields_;
}

NYT::NOrm::NClient::NObjects::TObjectKey TIllustratorTypeHandler::GetNullKey() const
{
    return NullKey_;
}

const NYT::NOrm::NServer::NObjects::TDBTable* TIllustratorTypeHandler::GetTable() const
{
    return &IllustratorsTable;
}

const NYT::NOrm::NServer::NObjects::TDBTable* TIllustratorTypeHandler::GetParentsTable() const
{
    return &IllustratorsToPublishersTable;
}

NYT::NOrm::NClient::NObjects::TObjectTypeValue
TIllustratorTypeHandler::GetParentType() const
{
    return static_cast<NYT::NOrm::NClient::NObjects::TObjectTypeValue>(EObjectType::Publisher);
}

const TDBFields& TIllustratorTypeHandler::GetParentKeyFields() const
{
    return ParentKeyFields_;
}

NYT::NOrm::NServer::NObjects::TObject*
TIllustratorTypeHandler::GetParent(
    const NYT::NOrm::NServer::NObjects::TObject* object,
    std::source_location location)
{
    return object->As<TIllustrator>()
        ->Publisher().Load(location);
}

NYT::NOrm::NClient::NObjects::TObjectTypeValue TIllustratorTypeHandler::GetAccessControlParentType() const
{
    return static_cast<NYT::NOrm::NClient::NObjects::TObjectTypeValue>(EObjectType::Publisher);
}

NYT::NOrm::NServer::NObjects::TObject*
TIllustratorTypeHandler::GetAccessControlParent(
    const NYT::NOrm::NServer::NObjects::TObject* object,
    std::source_location location)
{
    return object->As<TIllustrator>()
        ->PartTimeJob().Load(location);
}

void TIllustratorTypeHandler::ScheduleAccessControlParentKeyLoad(
    const NYT::NOrm::NServer::NObjects::TObject* object)
{
    object->As<TIllustrator>()->PartTimeJob().ScheduleLoad();
}

const TDBFields& TIllustratorTypeHandler::GetAccessControlParentKeyFields()
{
    return AccessControlParentKeyFields_;
}

std::unique_ptr<NYT::NOrm::NServer::NObjects::TObject>
TIllustratorTypeHandler::InstantiateObject(
    const NYT::NOrm::NClient::NObjects::TObjectKey& key,
    const NYT::NOrm::NClient::NObjects::TObjectKey& parentKey,
    ISession* session)
{
    return std::make_unique<TIllustrator>(
        std::get<i64>(key[0]),
        parentKey,
        this,
        session);
}

void TIllustratorTypeHandler::InitializeCreatedObject(
    NYT::NOrm::NServer::NObjects::TTransaction* transaction,
    NYT::NOrm::NServer::NObjects::TObject* object)
{
    TBase::InitializeCreatedObject(transaction, object);
    auto* typedObject = object->As<TIllustrator>();
    YT_VERIFY(typedObject);
    typedObject->MetaEtc().MutableLoad()->set_uuid(NYT::NOrm::NClient::NObjects::GenerateUuid());
}

void TIllustratorTypeHandler::ValidateCreatedObject(
    NYT::NOrm::NServer::NObjects::TTransaction* transaction,
    NYT::NOrm::NServer::NObjects::TObject* object)
{
    TBase::ValidateCreatedObject(transaction, object);
    auto* typedObject = object->As<TIllustrator>();
    YT_VERIFY(typedObject);
}

void TIllustratorTypeHandler::ValidateMetaEtcOnTransactionCommit(
    NYT::NOrm::NServer::NObjects::TTransaction*,
    const TIllustrator* object)
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

void TIllustratorTypeHandler::ValidateMetaEtcOnValueUpdate(
    NYT::NOrm::NServer::NObjects::TTransaction*,
    const TIllustrator* object,
    const TIllustrator::TMetaEtc& metaEtcOld,
    const TIllustrator::TMetaEtc& metaEtcNew)
{
    ValidateMetaUuid(metaEtcOld, metaEtcNew);

    // All user-defined attributes are considered updatable by any user
    // having corresponding write permission.
    Y_UNUSED(object);
}

void TIllustratorTypeHandler::ValidateMetaUuid(
    const TIllustrator::TMetaEtc& metaEtcOld,
    const TIllustrator::TMetaEtc& metaEtcNew)
{
    // Uuid protects, for example, YP secrets from object identity theft, i.e. from ABA race conditions,
    // so we try hard to make sure uuid doesn't change.
    if (!metaEtcOld.uuid().empty() && metaEtcOld.uuid() != metaEtcNew.uuid()) {
        this->GetBootstrap()->GetAccessControlManager()->ValidateSuperuser("change /meta/uuid");
    }
}

////////////////////////////////////////////////////////////////////////////////

TIndexedIncrementIdTypeHandler::TIndexedIncrementIdTypeHandler(
    IBootstrap* bootstrap,
    NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config)
    : TBase(
        bootstrap,
        static_cast<NYT::NOrm::NClient::NObjects::TObjectTypeValue>(EObjectType::IndexedIncrementId))
    , Config_(std::move(config))
    , KeyFields_({
        &IndexedIncrementIdsTable.Fields.MetaI64Id})
    , NullKey_(NYT::NOrm::NServer::NObjects::ParseObjectKey("-1000", KeyFields_))
{ }

void TIndexedIncrementIdTypeHandler::Initialize()
{
    TBase::Initialize();

    MetaAttributeSchema_
        ->AddChildren({
        MakeScalarAttributeSchema("acl", TDataModelObject::AclDescriptor)
            ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::Updatable)
            ->AddValidator<NYT::NOrm::NServer::NObjects::TObject>(std::bind_front(
                &TIndexedIncrementIdTypeHandler::ValidateAcl, this))
    });
    MetaAttributeSchema_
        ->AddChildren({
            MakeIdAttributeSchema(
                "i64_id",
                &IndexedIncrementIdsTable.Fields.MetaI64Id,
                MakeValueGetter(&TIndexedIncrementId::GetI64Id))
                ->SetKeyFieldPolicy(
                    NYT::NOrm::NServer::NObjects::CreateIntegerAttributePolicy<i64>(
                        NYT::NOrm::NServer::NObjects::EAttributeGenerationPolicy::IndexedIncrement,
                        GetBootstrap(),
                        -42,
                        57,
                        NYT::NOrm::NServer::NObjects::TAttributePolicyOptions{
                            .Type = GetType(),
                            .AttributePath = "/meta/i64_id",
                            .IndexForIncrement = ""
                        })),
            MakeScalarAttributeSchema("finalization_start_time", TIndexedIncrementId::FinalizationStartTimeDescriptor)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueReadOnly),
            MakeScalarAttributeSchema("finalizers", TIndexedIncrementId::FinalizersDescriptor)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueReadOnly),
            MakeScalarAttributeSchema("ultimate_question_of_life")
                ->SetComputed()
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::ReadOnly)
                ->SetOpaque(/*emitEntity*/ false),
            TBase::MakeEtcAttributeSchema(TIndexedIncrementId::MetaEtcDescriptor.AddValidator(
                std::bind_front(&TIndexedIncrementIdTypeHandler::ValidateMetaEtcOnValueUpdate, this)))
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::Updatable)
                ->template AddValidator<TIndexedIncrementId>(std::bind_front(
                    &TIndexedIncrementIdTypeHandler::ValidateMetaEtcOnTransactionCommit, this))
            });

    SpecAttributeSchema_
        ->AddChildren({
            MakeEtcAttributeSchema(TIndexedIncrementId::TSpec::EtcDescriptor),
        });

    StatusAttributeSchema_
        ->AddChildren({
            MakeEtcAttributeSchema(TIndexedIncrementId::TStatus::EtcDescriptor),
        });

    ControlAttributeSchema_
        ->AddChildren({
            MakeScalarAttributeSchema("touch")
                ->SetControl<TIndexedIncrementId, NYT::NOrm::NExample::NClient::NProto::NDataModel::TObjectTouch>(
                    [] (NYT::NOrm::NServer::NObjects::TTransaction* transaction,
                        TIndexedIncrementId* object,
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

void TIndexedIncrementIdTypeHandler::PostInitialize()
{
    // History is configured in PostInitialize stage, as users may add some evaluated fields via plugins in Initialize stage.
    // NB: Called before TBase::PostInitialize as base type handler prepares cache.
    ConfigureHistory(
        /*attributes*/ {},
        /*filter*/ std::nullopt);

    TBase::PostInitialize();

    FillUpdatePolicies();
}

bool TIndexedIncrementIdTypeHandler::IsObjectNameSupported() const
{
    return false;
}

bool TIndexedIncrementIdTypeHandler::SkipStoreWithoutChanges() const
{
    return false;
}

bool TIndexedIncrementIdTypeHandler::ForceZeroKeyEvaluation() const
{
    return false;
}

std::vector<NYT::NOrm::NServer::NObjects::TWatchLog>
TIndexedIncrementIdTypeHandler::GetWatchLogs() const
{
    return std::vector<NYT::NOrm::NServer::NObjects::TWatchLog>{
    };
}

const NYT::NYson::TProtobufMessageType*
TIndexedIncrementIdTypeHandler::GetRootProtobufType() const
{
    static const auto* type = NYT::NYson::ReflectProtobufMessageType<NYT::NOrm::NExample::NClient::NProto::NDataModel::TIndexedIncrementId>();
    return type;
}

const TDBFields& TIndexedIncrementIdTypeHandler::GetKeyFields() const
{
    return KeyFields_;
}

NYT::NOrm::NClient::NObjects::TObjectKey TIndexedIncrementIdTypeHandler::GetNullKey() const
{
    return NullKey_;
}

const NYT::NOrm::NServer::NObjects::TDBTable* TIndexedIncrementIdTypeHandler::GetTable() const
{
    return &IndexedIncrementIdsTable;
}

const NYT::NOrm::NServer::NObjects::TDBTable* TIndexedIncrementIdTypeHandler::GetParentsTable() const
{
    return nullptr;
}

std::unique_ptr<NYT::NOrm::NServer::NObjects::TObject>
TIndexedIncrementIdTypeHandler::InstantiateObject(
    const NYT::NOrm::NClient::NObjects::TObjectKey& key,
    const NYT::NOrm::NClient::NObjects::TObjectKey& parentKey,
    ISession* session)
{
    YT_VERIFY(!parentKey);

    return std::make_unique<TIndexedIncrementId>(
        std::get<i64>(key[0]),
        this,
        session);
}

void TIndexedIncrementIdTypeHandler::InitializeCreatedObject(
    NYT::NOrm::NServer::NObjects::TTransaction* transaction,
    NYT::NOrm::NServer::NObjects::TObject* object)
{
    TBase::InitializeCreatedObject(transaction, object);
    auto* typedObject = object->As<TIndexedIncrementId>();
    YT_VERIFY(typedObject);
    typedObject->MetaEtc().MutableLoad()->set_uuid(NYT::NOrm::NClient::NObjects::GenerateUuid());
}

void TIndexedIncrementIdTypeHandler::ValidateCreatedObject(
    NYT::NOrm::NServer::NObjects::TTransaction* transaction,
    NYT::NOrm::NServer::NObjects::TObject* object)
{
    TBase::ValidateCreatedObject(transaction, object);
    auto* typedObject = object->As<TIndexedIncrementId>();
    YT_VERIFY(typedObject);
}

void TIndexedIncrementIdTypeHandler::ValidateMetaEtcOnTransactionCommit(
    NYT::NOrm::NServer::NObjects::TTransaction*,
    const TIndexedIncrementId* object)
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

void TIndexedIncrementIdTypeHandler::ValidateMetaEtcOnValueUpdate(
    NYT::NOrm::NServer::NObjects::TTransaction*,
    const TIndexedIncrementId* object,
    const TIndexedIncrementId::TMetaEtc& metaEtcOld,
    const TIndexedIncrementId::TMetaEtc& metaEtcNew)
{
    ValidateMetaUuid(metaEtcOld, metaEtcNew);

    // All user-defined attributes are considered updatable by any user
    // having corresponding write permission.
    Y_UNUSED(object);
}

void TIndexedIncrementIdTypeHandler::ValidateMetaUuid(
    const TIndexedIncrementId::TMetaEtc& metaEtcOld,
    const TIndexedIncrementId::TMetaEtc& metaEtcNew)
{
    // Uuid protects, for example, YP secrets from object identity theft, i.e. from ABA race conditions,
    // so we try hard to make sure uuid doesn't change.
    if (!metaEtcOld.uuid().empty() && metaEtcOld.uuid() != metaEtcNew.uuid()) {
        this->GetBootstrap()->GetAccessControlManager()->ValidateSuperuser("change /meta/uuid");
    }
}

////////////////////////////////////////////////////////////////////////////////

TInterceptorTypeHandler::TInterceptorTypeHandler(
    IBootstrap* bootstrap,
    NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config)
    : TBase(
        bootstrap,
        static_cast<NYT::NOrm::NClient::NObjects::TObjectTypeValue>(EObjectType::Interceptor))
    , Config_(std::move(config))
    , KeyFields_({
        &InterceptorsTable.Fields.MetaId})
    , ParentKeyFields_({
        &InterceptorsTable.Fields.MetaMotherShipId})
    , NullKey_(NYT::NOrm::NServer::NObjects::GenerateZeroKey(KeyFields_))
{ }

void TInterceptorTypeHandler::Initialize()
{
    TBase::Initialize();

    ForbidParentRemoval_ = true;

    MetaAttributeSchema_
        ->AddChildren({
        MakeScalarAttributeSchema("acl", TDataModelObject::AclDescriptor)
            ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::Updatable)
            ->AddValidator<NYT::NOrm::NServer::NObjects::TObject>(std::bind_front(
                &TInterceptorTypeHandler::ValidateAcl, this))
    });
    MetaAttributeSchema_
        ->AddChildren({
            MakeParentIdAttributeSchema(
                "mother_ship_id",
                &InterceptorsTable.Fields.MetaMotherShipId,
                MakeValueGetter(&TInterceptor::MotherShipId)),
            MakeIdAttributeSchema(
                "id",
                &InterceptorsTable.Fields.MetaId,
                MakeValueGetter(&TInterceptor::GetId))
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
            MakeScalarAttributeSchema("nexus_id", TInterceptor::NexusIdDescriptor)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueReadOnly),
            MakeScalarAttributeSchema("finalization_start_time", TInterceptor::FinalizationStartTimeDescriptor)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueReadOnly),
            MakeScalarAttributeSchema("finalizers", TInterceptor::FinalizersDescriptor)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueReadOnly),
            MakeScalarAttributeSchema("ultimate_question_of_life")
                ->SetComputed()
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::ReadOnly)
                ->SetOpaque(/*emitEntity*/ false),
            TBase::MakeEtcAttributeSchema(TInterceptor::MetaEtcDescriptor.AddValidator(
                std::bind_front(&TInterceptorTypeHandler::ValidateMetaEtcOnValueUpdate, this)))
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::Updatable)
                ->template AddValidator<TInterceptor>(std::bind_front(
                    &TInterceptorTypeHandler::ValidateMetaEtcOnTransactionCommit, this))
            });

    SpecAttributeSchema_
        ->AddChildren({
            MakeEtcAttributeSchema(TInterceptor::TSpec::EtcDescriptor),
        });

    StatusAttributeSchema_
        ->AddChildren({
            MakeEtcAttributeSchema(TInterceptor::TStatus::EtcDescriptor),
        });

    ControlAttributeSchema_
        ->AddChildren({
            MakeScalarAttributeSchema("touch")
                ->SetControl<TInterceptor, NYT::NOrm::NExample::NClient::NProto::NDataModel::TObjectTouch>(
                    [] (NYT::NOrm::NServer::NObjects::TTransaction* transaction,
                        TInterceptor* object,
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

void TInterceptorTypeHandler::PostInitialize()
{
    // History is configured in PostInitialize stage, as users may add some evaluated fields via plugins in Initialize stage.
    // NB: Called before TBase::PostInitialize as base type handler prepares cache.
    ConfigureHistory(
        /*attributes*/ {},
        /*filter*/ std::nullopt);

    TBase::PostInitialize();

    FillUpdatePolicies();
}

bool TInterceptorTypeHandler::IsObjectNameSupported() const
{
    return false;
}

bool TInterceptorTypeHandler::SkipStoreWithoutChanges() const
{
    return false;
}

bool TInterceptorTypeHandler::ForceZeroKeyEvaluation() const
{
    return false;
}

std::vector<NYT::NOrm::NServer::NObjects::TWatchLog>
TInterceptorTypeHandler::GetWatchLogs() const
{
    return std::vector<NYT::NOrm::NServer::NObjects::TWatchLog>{
        NYT::NOrm::NServer::NObjects::TWatchLog{
            .ObjectName = "interceptor",
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
TInterceptorTypeHandler::GetRootProtobufType() const
{
    static const auto* type = NYT::NYson::ReflectProtobufMessageType<NYT::NOrm::NExample::NClient::NProto::NDataModel::TInterceptor>();
    return type;
}

const TDBFields& TInterceptorTypeHandler::GetKeyFields() const
{
    return KeyFields_;
}

NYT::NOrm::NClient::NObjects::TObjectKey TInterceptorTypeHandler::GetNullKey() const
{
    return NullKey_;
}

const NYT::NOrm::NServer::NObjects::TDBTable* TInterceptorTypeHandler::GetTable() const
{
    return &InterceptorsTable;
}

const NYT::NOrm::NServer::NObjects::TDBTable* TInterceptorTypeHandler::GetParentsTable() const
{
    return &InterceptorsToMotherShipsTable;
}

NYT::NOrm::NClient::NObjects::TObjectTypeValue
TInterceptorTypeHandler::GetParentType() const
{
    return static_cast<NYT::NOrm::NClient::NObjects::TObjectTypeValue>(EObjectType::MotherShip);
}

const TDBFields& TInterceptorTypeHandler::GetParentKeyFields() const
{
    return ParentKeyFields_;
}

NYT::NOrm::NServer::NObjects::TObject*
TInterceptorTypeHandler::GetParent(
    const NYT::NOrm::NServer::NObjects::TObject* object,
    std::source_location location)
{
    return object->As<TInterceptor>()
        ->MotherShip().Load(location);
}

std::unique_ptr<NYT::NOrm::NServer::NObjects::TObject>
TInterceptorTypeHandler::InstantiateObject(
    const NYT::NOrm::NClient::NObjects::TObjectKey& key,
    const NYT::NOrm::NClient::NObjects::TObjectKey& parentKey,
    ISession* session)
{
    return std::make_unique<TInterceptor>(
        std::get<i64>(key[0]),
        parentKey,
        this,
        session);
}

void TInterceptorTypeHandler::InitializeCreatedObject(
    NYT::NOrm::NServer::NObjects::TTransaction* transaction,
    NYT::NOrm::NServer::NObjects::TObject* object)
{
    TBase::InitializeCreatedObject(transaction, object);
    auto* typedObject = object->As<TInterceptor>();
    YT_VERIFY(typedObject);
    typedObject->MetaEtc().MutableLoad()->set_uuid(NYT::NOrm::NClient::NObjects::GenerateUuid());
}

void TInterceptorTypeHandler::ValidateCreatedObject(
    NYT::NOrm::NServer::NObjects::TTransaction* transaction,
    NYT::NOrm::NServer::NObjects::TObject* object)
{
    TBase::ValidateCreatedObject(transaction, object);
    auto* typedObject = object->As<TInterceptor>();
    YT_VERIFY(typedObject);
}

void TInterceptorTypeHandler::ValidateMetaEtcOnTransactionCommit(
    NYT::NOrm::NServer::NObjects::TTransaction*,
    const TInterceptor* object)
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

void TInterceptorTypeHandler::ValidateMetaEtcOnValueUpdate(
    NYT::NOrm::NServer::NObjects::TTransaction*,
    const TInterceptor* object,
    const TInterceptor::TMetaEtc& metaEtcOld,
    const TInterceptor::TMetaEtc& metaEtcNew)
{
    ValidateMetaUuid(metaEtcOld, metaEtcNew);

    // All user-defined attributes are considered updatable by any user
    // having corresponding write permission.
    Y_UNUSED(object);
}

void TInterceptorTypeHandler::ValidateMetaUuid(
    const TInterceptor::TMetaEtc& metaEtcOld,
    const TInterceptor::TMetaEtc& metaEtcNew)
{
    // Uuid protects, for example, YP secrets from object identity theft, i.e. from ABA race conditions,
    // so we try hard to make sure uuid doesn't change.
    if (!metaEtcOld.uuid().empty() && metaEtcOld.uuid() != metaEtcNew.uuid()) {
        this->GetBootstrap()->GetAccessControlManager()->ValidateSuperuser("change /meta/uuid");
    }
}

////////////////////////////////////////////////////////////////////////////////

TManualIdTypeHandler::TManualIdTypeHandler(
    IBootstrap* bootstrap,
    NYT::NOrm::NServer::NObjects::TObjectManagerConfigPtr config)
    : TBase(
        bootstrap,
        static_cast<NYT::NOrm::NClient::NObjects::TObjectTypeValue>(EObjectType::ManualId))
    , Config_(std::move(config))
    , KeyFields_({
        &ManualIdsTable.Fields.MetaStrId,
        &ManualIdsTable.Fields.MetaI64Id,
        &ManualIdsTable.Fields.MetaUi64Id})
    , NullKey_(NYT::NOrm::NServer::NObjects::GenerateZeroKey(KeyFields_))
{ }

TManualIdTypeHandler::~TManualIdTypeHandler()
{ }

void TManualIdTypeHandler::Initialize()
{
    TBase::Initialize();

    MetaAttributeSchema_
        ->AddChildren({
        MakeScalarAttributeSchema("acl", TDataModelObject::AclDescriptor)
            ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::Updatable)
            ->AddValidator<NYT::NOrm::NServer::NObjects::TObject>(std::bind_front(
                &TManualIdTypeHandler::ValidateAcl, this))
    });
    MetaAttributeSchema_
        ->AddChildren({
            MakeIdAttributeSchema(
                "str_id",
                &ManualIdsTable.Fields.MetaStrId,
                MakeValueGetter(&TManualId::GetStrId))
                ->SetKeyFieldPolicy(
                    NYT::NOrm::NServer::NObjects::CreateStringAttributePolicy(
                        NYT::NOrm::NServer::NObjects::EAttributeGenerationPolicy::Manual,
                        5,
                        10,
                        "abcde")),
            MakeIdAttributeSchema(
                "i64_id",
                &ManualIdsTable.Fields.MetaI64Id,
                MakeValueGetter(&TManualId::GetI64Id))
                ->SetKeyFieldPolicy(
                    NYT::NOrm::NServer::NObjects::CreateIntegerAttributePolicy<i64>(
                        NYT::NOrm::NServer::NObjects::EAttributeGenerationPolicy::Manual,
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
                &ManualIdsTable.Fields.MetaUi64Id,
                MakeValueGetter(&TManualId::GetUi64Id))
                ->SetKeyFieldPolicy(
                    NYT::NOrm::NServer::NObjects::CreateIntegerAttributePolicy<ui64>(
                        NYT::NOrm::NServer::NObjects::EAttributeGenerationPolicy::Manual,
                        GetBootstrap(),
                        NYT::NOrm::NServer::NObjects::DefaultMinIntegerAttributeValue<ui64>,
                        NYT::NOrm::NServer::NObjects::DefaultMaxIntegerAttributeValue<ui64>,
                        NYT::NOrm::NServer::NObjects::TAttributePolicyOptions{
                            .Type = GetType(),
                            .AttributePath = "/meta/ui64_id",
                            .IndexForIncrement = ""
                        })),
            MakeScalarAttributeSchema("finalization_start_time", TManualId::FinalizationStartTimeDescriptor)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueReadOnly),
            MakeScalarAttributeSchema("finalizers", TManualId::FinalizersDescriptor)
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueReadOnly),
            MakeScalarAttributeSchema("ultimate_question_of_life")
                ->SetComputed()
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::ReadOnly)
                ->SetOpaque(/*emitEntity*/ false),
            TBase::MakeEtcAttributeSchema(TManualId::MetaEtcDescriptor.AddValidator(
                std::bind_front(&TManualIdTypeHandler::ValidateMetaEtcOnValueUpdate, this)))
                ->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::Updatable)
                ->template AddValidator<TManualId>(std::bind_front(
                    &TManualIdTypeHandler::ValidateMetaEtcOnTransactionCommit, this))
            });

    SpecAttributeSchema_
        ->AddChildren({
            MakeScalarAttributeSchema("str_value", TManualId::TSpec::StrValueDescriptor)
                ->SetPolicy(TManualId::TSpec::StrValueDescriptor,
                            NYT::NOrm::NServer::NObjects::CreateStringAttributePolicy(
                                NYT::NOrm::NServer::NObjects::EAttributeGenerationPolicy::Manual,
                                2,
                                3,
                                "fo")),
            MakeScalarAttributeSchema("i32_value", TManualId::TSpec::I32ValueDescriptor)
                ->SetPolicy(TManualId::TSpec::I32ValueDescriptor,
                            NYT::NOrm::NServer::NObjects::CreateIntegerAttributePolicy<i32>(
                                NYT::NOrm::NServer::NObjects::EAttributeGenerationPolicy::Manual,
                                GetBootstrap(),
                                -50,
                                50,
                                NYT::NOrm::NServer::NObjects::TAttributePolicyOptions{
                                    .Type = GetType(),
                                    .AttributePath = "/spec/i32_value",
                                    .IndexForIncrement = ""
                                })),
            MakeScalarAttributeSchema("ui32_value", TManualId::TSpec::Ui32ValueDescriptor)
                ->SetPolicy(TManualId::TSpec::Ui32ValueDescriptor,
                            NYT::NOrm::NServer::NObjects::CreateIntegerAttributePolicy<ui32>(
                                NYT::NOrm::NServer::NObjects::EAttributeGenerationPolicy::Manual,
                                GetBootstrap(),
                                0,
                                50,
                                NYT::NOrm::NServer::NObjects::TAttributePolicyOptions{
                                    .Type = GetType(),
                                    .AttributePath = "/spec/ui32_value",
                                    .IndexForIncrement = ""
                                })),
            MakeEtcAttributeSchema(TManualId::TSpec::EtcDescriptor),
        });

    StatusAttributeSchema_
        ->AddChildren({
            MakeEtcAttributeSchema(TManualId::TStatus::EtcDescriptor),
        });

    ControlAttributeSchema_
        ->AddChildren({
            MakeScalarAttributeSchema("touch")
                ->SetControl<TManualId, NYT::NOrm::NExample::NClient::NProto::NDataModel::TObjectTouch>(
                    [] (NYT::NOrm::NServer::NObjects::TTransaction* transaction,
                        TManualId* object,
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

void TManualIdTypeHandler::PostInitialize()
{
    // History is configured in PostInitialize stage, as users may add some evaluated fields via plugins in Initialize stage.
    // NB: Called before TBase::PostInitialize as base type handler prepares cache.
    ConfigureHistory(
        /*attributes*/ {},
        /*filter*/ std::nullopt);

    TBase::PostInitialize();

    FillUpdatePolicies();
}

bool TManualIdTypeHandler::IsObjectNameSupported() const
{
    return false;
}

bool TManualIdTypeHandler::SkipStoreWithoutChanges() const
{
    return false;
}

bool TManualIdTypeHandler::ForceZeroKeyEvaluation() const
{
    return false;
}

std::vector<NYT::NOrm::NServer::NObjects::TWatchLog>
TManualIdTypeHandler::GetWatchLogs() const
{
    return std::vector<NYT::NOrm::NServer::NObjects::TWatchLog>{
    };
}

const NYT::NYson::TProtobufMessageType*
TManualIdTypeHandler::GetRootProtobufType() const
{
    static const auto* type = NYT::NYson::ReflectProtobufMessageType<NYT::NOrm::NExample::NClient::NProto::NDataModel::TManualId>();
    return type;
}

const TDBFields& TManualIdTypeHandler::GetKeyFields() const
{
    return KeyFields_;
}

NYT::NOrm::NClient::NObjects::TObjectKey TManualIdTypeHandler::GetNullKey() const
{
    return NullKey_;
}

const NYT::NOrm::NServer::NObjects::TDBTable* TManualIdTypeHandler::GetTable() const
{
    return &ManualIdsTable;
}

const NYT::NOrm::NServer::NObjects::TDBTable* TManualIdTypeHandler::GetParentsTable() const
{
    return nullptr;
}

std::unique_ptr<NYT::NOrm::NServer::NObjects::TObject>
TManualIdTypeHandler::InstantiateObject(
    const NYT::NOrm::NClient::NObjects::TObjectKey& key,
    const NYT::NOrm::NClient::NObjects::TObjectKey& parentKey,
    ISession* session)
{
    YT_VERIFY(!parentKey);

    return std::make_unique<TManualId>(
        std::get<TString>(key[0]),
        std::get<i64>(key[1]),
        std::get<ui64>(key[2]),
        this,
        session);
}

void TManualIdTypeHandler::InitializeCreatedObject(
    NYT::NOrm::NServer::NObjects::TTransaction* transaction,
    NYT::NOrm::NServer::NObjects::TObject* object)
{
    TBase::InitializeCreatedObject(transaction, object);
    auto* typedObject = object->As<TManualId>();
    YT_VERIFY(typedObject);
    typedObject->MetaEtc().MutableLoad()->set_uuid(NYT::NOrm::NClient::NObjects::GenerateUuid());
}

void TManualIdTypeHandler::ValidateCreatedObject(
    NYT::NOrm::NServer::NObjects::TTransaction* transaction,
    NYT::NOrm::NServer::NObjects::TObject* object)
{
    TBase::ValidateCreatedObject(transaction, object);
    auto* typedObject = object->As<TManualId>();
    YT_VERIFY(typedObject);
}

void TManualIdTypeHandler::ValidateMetaEtcOnTransactionCommit(
    NYT::NOrm::NServer::NObjects::TTransaction*,
    const TManualId* object)
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

void TManualIdTypeHandler::ValidateMetaEtcOnValueUpdate(
    NYT::NOrm::NServer::NObjects::TTransaction*,
    const TManualId* object,
    const TManualId::TMetaEtc& metaEtcOld,
    const TManualId::TMetaEtc& metaEtcNew)
{
    ValidateMetaUuid(metaEtcOld, metaEtcNew);

    // All user-defined attributes are considered updatable by any user
    // having corresponding write permission.
    Y_UNUSED(object);
}

void TManualIdTypeHandler::ValidateMetaUuid(
    const TManualId::TMetaEtc& metaEtcOld,
    const TManualId::TMetaEtc& metaEtcNew)
{
    // Uuid protects, for example, YP secrets from object identity theft, i.e. from ABA race conditions,
    // so we try hard to make sure uuid doesn't change.
    if (!metaEtcOld.uuid().empty() && metaEtcOld.uuid() != metaEtcNew.uuid()) {
        this->GetBootstrap()->GetAccessControlManager()->ValidateSuperuser("change /meta/uuid");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NExample::NServer::NLibrary

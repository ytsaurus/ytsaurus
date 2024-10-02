#include "type_handler_detail.h"

#include "attribute_schema.h"
#include "attribute_schema_builder.h"
#include "db_config.h"
#include "helpers.h"
#include "object_manager.h"
#include "object_reflection.h"
#include "private.h"
#include "transaction_manager.h"
#include "watch_log.h"
#include "watch_manager.h"

#include <yt/yt/orm/server/access_control/access_control_manager.h>

#include <yt/yt/orm/server/master/bootstrap.h>
#include <yt/yt/orm/server/master/yt_connector.h>

#include <yt/yt_proto/yt/orm/data_model/generic.pb.h>

#include <yt/yt/orm/client/objects/registry.h>

#include <yt/yt/orm/library/query/filter_matcher.h>

#include <yt/yt/orm/library/attributes/attribute_path.h>

#include <yt/yt/core/logging/log.h>
#include <yt/yt/core/ypath/helpers.h>
#include <yt/yt/core/ypath/tokenizer.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>

#include <yt/yt/library/query/base/ast.h>

#include <util/generic/algorithm.h>

namespace NYT::NOrm::NServer::NObjects {

using namespace NAccessControl;

using namespace NYT::NYTree;
using namespace NYT::NYson;
using namespace NYT::NQueryClient;
using namespace NYT::NQueryClient::NAst;

////////////////////////////////////////////////////////////////////////////////

namespace {

void ValidateSubjectExists(TTransaction* transaction, const TObjectId& subjectId)
{
    if (subjectId == EveryoneSubjectId) {
        return;
    }
    auto* user = transaction->GetObject(TObjectTypeValues::User, TObjectKey(subjectId));
    if (user && user->DoesExist()) {
        return;
    }
    auto* group = transaction->GetObject(TObjectTypeValues::Group, TObjectKey(subjectId));
    if (group && group->DoesExist()) {
        return;
    }
    THROW_ERROR_EXCEPTION(
        NOrm::NClient::EErrorCode::NoSuchObject,
        "Subject %Qv does not exist",
        subjectId)
        << TErrorAttribute("object_id", subjectId);
}

void ValidateKeyArrays(
    const TDBFields& keyFields,
    const IObjectTypeHandler::TScalarAttributeSchemas& idAttributeSchemas)
{
    YT_VERIFY(keyFields.size() == idAttributeSchemas.size());

    for (int index = 0; index < std::ssize(keyFields); ++index) {
        YT_VERIFY(keyFields[index] == idAttributeSchemas[index]->TryGetDBField());
    }

    THashSet<const TDBField*> fieldsCheck;
    for (const auto* field : keyFields) {
        YT_VERIFY(field != nullptr);
        InsertOrCrash(fieldsCheck, field);
    }

    THashSet<const TAttributeSchema*> schemasCheck;
    for (const auto* schema : idAttributeSchemas) {
        YT_VERIFY(schema != nullptr);
        InsertOrCrash(schemasCheck, schema);
    }
}

void AddFinalizer(
    TTransaction* /*transaction*/,
    TObject* object,
    const NDataModel::TObjectAddFinalizer& control)
{
    THROW_ERROR_EXCEPTION_IF(object->Finalizers().Load().contains(control.finalizer_name()),
        NOrm::NClient::EErrorCode::InvalidRequestArguments,
        "Finalizer %Qv already exists for %v",
        control.finalizer_name(),
        object->GetDisplayName());
    object->ValidateNotFinalized();

    object->AddFinalizer(control.finalizer_name());
    // TODO(dgolear): Switch to simple locks.
    *object->ExistenceLock().MutableLoad() ^= 1;
}

void RemoveFinalizer(
    TTransaction* /*transaction*/,
    TObject* object,
    const NDataModel::TObjectRemoveFinalizer& control)
{
    object->ValidateNotFinalized();
    auto* finalizers = object->Finalizers().MutableLoad();
    THROW_ERROR_EXCEPTION_UNLESS(finalizers->contains(control.finalizer_name()),
        NOrm::NClient::EErrorCode::InvalidRequestArguments,
        "Finalizer %Qv does not exist for %v",
        control.finalizer_name(),
        object->GetDisplayName());

    if (finalizers->at(control.finalizer_name()).user_id() != NAccessControl::GetAuthenticatedUserIdentity().User) {
        object->GetTypeHandler()->GetBootstrap()->GetAccessControlManager()->ValidatePermission(
            object,
            NAccessControl::TAccessControlPermissionValues::Use,
            NYPath::YPathJoin("/meta/finalizers", control.finalizer_name()));
    }
    finalizers->erase(control.finalizer_name());
    // TODO(dgolear): Switch to simple locks.
    *object->ExistenceLock().MutableLoad() ^= 1;
}

void CompleteFinalization(
    TTransaction* /*transaction*/,
    TObject* object,
    const NDataModel::TObjectCompleteFinalization& control)
{
    const auto& finalizers = object->Finalizers().Load();
    THROW_ERROR_EXCEPTION_UNLESS(finalizers.contains(control.finalizer_name()),
        NOrm::NClient::EErrorCode::InvalidRequestArguments,
        "No such finalizer %Qv for %v",
        control.finalizer_name(),
        object->GetDisplayName());

    if (finalizers.at(control.finalizer_name()).user_id() != NAccessControl::GetAuthenticatedUserIdentity().User) {
        object->GetTypeHandler()->GetBootstrap()->GetAccessControlManager()->ValidatePermission(
            object,
            NAccessControl::TAccessControlPermissionValues::Use,
            NYPath::YPathJoin("/meta/finalizers", control.finalizer_name()));
    }

    object->CompleteFinalizer(control.finalizer_name());
    if (object->HasActiveFinalizers()) {
        object->GetSession()->ScheduleStore([object] (IStoreContext* context) {
            context->WriteRow(
                object->GetTypeHandler()->GetTable(),
                object->GetTypeHandler()->GetObjectTableKey(object),
                std::array{
                    &ObjectsTable.Fields.ExistenceLock,
                },
                ToUnversionedValues(
                    context->GetRowBuffer(),
                    false));
        });
    }
}

bool IsHistoryTouched(const TObject* object)
{
    return object->ControlTouchEventsToSave().contains("touch");
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TObjectTypeHandlerBase::TObjectTypeHandlerBase(
    NMaster::IBootstrap* bootstrap,
    TObjectTypeValue type)
    : Bootstrap_(bootstrap)
    , Type_(type)
    , SchemaId_(NClient::NObjects::GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrCrash(Type_))
{
    Bootstrap_->GetObjectManager()->SubscribeConfigUpdate(BIND(&TObjectTypeHandlerBase::OnConfigUpdate, this));
}

void TObjectTypeHandlerBase::Initialize()
{
    EnsureAncestryDepthInitialized();

    RootAttributeSchema_ = MakeCompositeAttributeSchema(TString())
        ->AddChildren({
            MetaAttributeSchema_ = MakeMetaAttributeSchema(),

            SpecAttributeSchema_ = MakeCompositeAttributeSchema("spec"),

            StatusAttributeSchema_ = MakeCompositeAttributeSchema("status"),

            LabelsAttributeSchema_ = MakeScalarAttributeSchema("labels", TObject::LabelsDescriptor)
                ->SetUpdatePolicy(EUpdatePolicy::Updatable)
                ->TryAsScalar(),

            ControlAttributeSchema_ = MakeCompositeAttributeSchema("control")
                ->SetControlAttribute()
                ->TryAsComposite(),
        });

    MetaAttributeSchema_
        ->AddChildren({
            MakeScalarAttributeSchema("type")
                ->SetConstantChangedGetter(false)
                ->SetValueGetter<TObject>(
                    [typeName = SchemaId_] (
                        TTransaction* /*transaction*/,
                        const TObject* /*object*/,
                        NYson::IYsonConsumer* consumer)
                    {
                        BuildYsonFluently(consumer).Value(typeName);
                    })
                ->SetExpressionBuilder(
                    [typeName = SchemaId_] (IQueryContext* context) {
                        return context->New<NQueryClient::NAst::TLiteralExpression>(
                            NQueryClient::TSourceLocation(),
                            typeName);
                    }),

            MakeScalarAttributeSchema("key")
                ->SetConstantChangedGetter(false)
                ->SetValueGetter<TObject>(std::bind_front(&TObjectTypeHandlerBase::EvaluateKey, this))
                ->EnableMetaResponseAttribute(),

            MakeScalarAttributeSchema("parent_key")
                ->SetConstantChangedGetter(false)
                ->SetValueGetter<TObject>(std::bind_front(&TObjectTypeHandlerBase::EvaluateParentKey, this))
                ->EnableMetaResponseAttribute(),

            MakeScalarAttributeSchema("fqid")
                ->SetConstantChangedGetter(false)
                ->SetValueGetter<TObject>(std::bind_front(&TObjectTypeHandlerBase::EvaluateFqid, this))
                ->SetPreloader<TObject>(std::bind_front(&TObjectTypeHandlerBase::PreloadFqid, this))
                ->EnableMetaResponseAttribute(),

            MakeScalarAttributeSchema("creation_time", TObject::CreationTimeDescriptor)
                ->SetConstantChangedGetter(false)
                ->SetUpdatePolicy(EUpdatePolicy::ReadOnly),
        });

    if (Bootstrap_->GetDBConfig().EnableAnnotations) {
        RootAttributeSchema_->AddChildren({
            AnnotationsAttributeSchema_ = MakeAnnotationsAttributeSchema("annotations")
        });
    }

    if (Type_ != TObjectTypeValues::Schema) {
        MetaAttributeSchema_->AddChild(
            MakeScalarAttributeSchema("inherit_acl", TObject::InheritAclDescriptor)
                ->SetUpdatePolicy(EUpdatePolicy::Updatable));
    }

    if (Bootstrap_->GetDBConfig().EnableFinalizers) {
        MetaAttributeSchema_->AddChild(
            MakeScalarAttributeSchema("active_finalizers")
                ->SetValueGetter<TObject>(&GetActiveFinalizers)
                ->SetConstantChangedGetter(false)
                ->SetPreloader<TObject>([] (const TObject* object) {
                    object->Finalizers().ScheduleLoad();
                })
                ->SetUpdatePolicy(EUpdatePolicy::OpaqueReadOnly)
                ->SetOpaque());

        ControlAttributeSchema_->AddChildren({
            MakeScalarAttributeSchema("add_finalizer")
                ->SetUpdatePreloader<TObject>([] (TTransaction* /*transaction*/, const TObject* object, const TUpdateRequest& /*request*/) {
                    object->ExistenceLock().ScheduleLoad();
                    object->Finalizers().ScheduleLoad();
                })
                ->SetControl<TObject, NDataModel::TObjectAddFinalizer>(&AddFinalizer),
            MakeScalarAttributeSchema("remove_finalizer")
                ->SetUpdatePreloader<TObject>([] (TTransaction* /*transaction*/, const TObject* object, const TUpdateRequest& /*request*/) {
                    object->ExistenceLock().ScheduleLoad();
                    object->Finalizers().ScheduleLoad();
                })
                ->SetControl<TObject, NDataModel::TObjectRemoveFinalizer>(&RemoveFinalizer),

            MakeScalarAttributeSchema("complete_finalization")
                ->SetUpdatePreloader<TObject>([] (TTransaction* /*transaction*/, const TObject* object, const TUpdateRequest& /*request*/) {
                    object->ExistenceLock().ScheduleLoad();
                    object->Finalizers().ScheduleLoad();
                })
                ->SetControl<TObject, NDataModel::TObjectCompleteFinalization>(&CompleteFinalization),
        });
    }
}

void TObjectTypeHandlerBase::PostInitialize()
{
    PostInitializeCalled_ = true;

    auto mountInfoFuture = GetTableMountInfo(Bootstrap_->GetYTConnector(), GetTable());

    // TODO(dgolear): Get rid of it in favor of ConfigureHistory.
    PrepareHistoryEnabledAttributeSchemaCache();
    ValidateKeyArrays(GetKeyFields(), GetIdAttributeSchemas());
    ValidateKeyArrays(GetParentKeyFields(), GetParentIdAttributeSchemas());

    RootAttributeSchema_->ForEachAttribute([] (const TAttributeSchema* schema) {
        if (auto* scalarSchema = schema->TryAsScalar()) {
            scalarSchema->Validate();
        }
        return false;
    });

    RootAttributeSchema_->ForEachAttributeMutable([] (TAttributeSchema* schema) {
        schema->RunPostInitializers();
        return false;
    });
    RootAttributeSchema_->ForEachLeafAttributeMutable([] (TAttributeSchema* schema) {
        schema->AsScalar()->SetProtobufElement(
            NYson::ResolveProtobufElementByYPath(
                schema->GetTypeHandler()
                    ->GetRootProtobufType(),
            schema->GetPath()).Element);
        return false;
    });

    if (Bootstrap_->GetDBConfig().EnableFinalizers) {
        SetupFinalizers();
    }

    if (Bootstrap_->GetTransactionManager()->GetConfig()->BuildKeyExpression) {
        MetaAttributeSchema_->FindChild("key")
            ->AsScalar()
            ->SetExpressionBuilder(std::bind_front(
                &BuildKeyExpression,
                GetKeyFields(),
                GetIdAttributeSchemas(),
                TString(CompositeKeySeparator)));

        if (HasParent()) {
            MetaAttributeSchema_->FindChild("parent_key")
                ->AsScalar()
                ->SetExpressionBuilder(std::bind_front(
                    &BuildKeyExpression,
                    GetParentKeyFields(),
                    GetParentIdAttributeSchemas(),
                    TString(CompositeKeySeparator)));
        }
    }

    Bootstrap_
        ->GetWatchManager()
        ->RegisterLogs(this);

    auto mountInfo = NConcurrency::WaitFor(std::move(mountInfoFuture))
        .ValueOrThrow();
    TableSchema_ = mountInfo->Schemas[NTabletClient::ETableSchemaKind::Primary];
}

void TObjectTypeHandlerBase::Validate() const
{
    THROW_ERROR_EXCEPTION_UNLESS(PostInitializeCalled_, "PostInitialize stage of base class wasn't called")

    auto typeName = NClient::NObjects::GetGlobalObjectTypeRegistry()
        ->GetCapitalizedHumanReadableTypeNameOrThrow(GetType());

    RootAttributeSchema_->ForEachLeafAttribute([&typeName] (const TScalarAttributeSchema* schema) {
        if (schema->IsComputed() && !schema->HasValueGetter()) {
            THROW_ERROR_EXCEPTION("Computed field %Qv in type %Qv has no way to read", schema->GetPath(), typeName);
        }
        if (!schema->HasChangedGetter() && !schema->HasStoreScheduledGetter()) {
            THROW_ERROR_EXCEPTION("Field %Qv in type %Qv has no way to check changes", schema->GetPath(), typeName);
        }
        return false;
    });
}

NMaster::IBootstrap* TObjectTypeHandlerBase::GetBootstrap() const
{
    return Bootstrap_;
}

NTableClient::TTableSchemaPtr TObjectTypeHandlerBase::GetTableSchema() const
{
    return TableSchema_;
}

void TObjectTypeHandlerBase::OnConfigUpdate(const TObjectManagerConfigPtr& /*objectManagerConfig*/)
{ }

TObjectTypeValue TObjectTypeHandlerBase::GetType() const
{
    return Type_;
}

TObjectKey TObjectTypeHandlerBase::GetNullKey() const
{
    return {};
}

TObjectKey TObjectTypeHandlerBase::GetObjectTableKey(const TObject* object, std::source_location location) const
{
    if (HasParent()) {
        return object->GetParentKey(location) + object->GetKey();
    } else {
        return object->GetKey();
    }
}

std::pair<TObjectKey, TObjectKey> TObjectTypeHandlerBase::SplitObjectTableKey(TObjectKey tableKey) const
{
    if (HasParent()) {
        size_t parentKeySize = GetParentKeyFields().size();
        size_t objectKeySize = GetKeyFields().size();

        THROW_ERROR_EXCEPTION_UNLESS(tableKey.size() == (parentKeySize + objectKeySize),
            "Called SplitObjectTableKey on a key of incorrect size %v for %v and parent %v",
            tableKey.size(),
            NClient::NObjects::GetGlobalObjectTypeRegistry()
                ->GetHumanReadableTypeNameOrCrash(GetType()),
            NClient::NObjects::GetGlobalObjectTypeRegistry()
                ->GetHumanReadableTypeNameOrCrash(GetParentType()));

        TObjectKey::TKeyFields fields;
        fields.reserve(parentKeySize);
        for (size_t i = 0; i != parentKeySize; ++i) {
            fields.push_back(std::move(tableKey[i]));
        }
        TObjectKey parentKey(std::move(fields));

        fields.reserve(objectKeySize);
        for (size_t i = 0; i != objectKeySize; ++i) {
            fields.push_back(std::move(tableKey[i + parentKeySize]));
        }
        TObjectKey objectKey(std::move(fields));

        return std::pair(objectKey, parentKey);
    } else {
        THROW_ERROR_EXCEPTION_UNLESS(tableKey.size() == GetKeyFields().size(),
            "Called SplitObjectTableKey on a key of incorrect size %v for %v",
            tableKey.size(),
            NClient::NObjects::GetGlobalObjectTypeRegistry()
            ->GetHumanReadableTypeNameOrCrash(GetType()));
        return std::pair(tableKey, TObjectKey{});
    }
}

bool TObjectTypeHandlerBase::HasParent() const
{
    return GetParentType() != TObjectTypeValues::Null;
}

TObjectTypeValue TObjectTypeHandlerBase::GetParentType() const
{
    return TObjectTypeValues::Null;
}

int TObjectTypeHandlerBase::GetAncestryDepth() const
{
    YT_VERIFY(AncestryDepth_);
    return *AncestryDepth_;
}

TObject* TObjectTypeHandlerBase::GetParent(const TObject* /*object*/, std::source_location /*location*/)
{
    YT_VERIFY(!HasParent());
    return nullptr;
}

const TDBFields& TObjectTypeHandlerBase::GetParentKeyFields() const
{
    static const TDBFields NoParentFields;
    return NoParentFields;
}

const TChildrenAttributeBase* TObjectTypeHandlerBase::GetParentChildrenAttribute(const TObject* parent) const
{
    return parent->GetTypeHandler()->GetChildrenAttribute(parent, GetType());
}

const TChildrenAttributeBase* TObjectTypeHandlerBase::GetChildrenAttribute(
    const TObject* object, TObjectTypeValue childType) const
{
    return ChildrenAttributes_.at(childType)(object);
}

void TObjectTypeHandlerBase::ForEachChildrenAttribute(
    const TObject* object, std::function<void(const TChildrenAttributeBase*)> onChildAttribute) const
{
    for (const auto& [_, getter] : ChildrenAttributes_) {
        onChildAttribute(getter(object));
    }
}

const TDBTable* TObjectTypeHandlerBase::GetParentsTable() const
{
    YT_VERIFY(!HasParent());
    return nullptr;
}

bool TObjectTypeHandlerBase::HasNonTrivialAccessControlParent() const
{
    return GetAccessControlParentType() != GetParentType();
}

TObjectTypeValue TObjectTypeHandlerBase::GetAccessControlParentType() const
{
    return GetParentType();
}

TObject* TObjectTypeHandlerBase::GetAccessControlParent(const TObject* object, std::source_location location)
{
    return GetParent(object, location);
}

void TObjectTypeHandlerBase::ScheduleAccessControlParentKeyLoad(const TObject* /*object*/)
{
    // Parent key is already loaded.
}

const TDBFields& TObjectTypeHandlerBase::GetAccessControlParentKeyFields()
{
    return GetParentKeyFields();
}

TObjectId TObjectTypeHandlerBase::GetSchemaObjectId()
{
    if (Type_ == TObjectTypeValues::Schema) {
        return TObjectId();
    }
    return SchemaId_;
}

TObject* TObjectTypeHandlerBase::GetSchemaObject(const TObject* object)
{
    if (Type_ == TObjectTypeValues::Schema) {
        return nullptr;
    }
    auto* session = object->GetSession();
    return session->GetObject(TObjectTypeValues::Schema, TObjectKey(SchemaId_));
}

TResolveAttributeResult TObjectTypeHandlerBase::GetUuidLocation() const
{
    auto schema = MetaAttributeSchema_->GetEtcChild();
    return {schema, "/uuid"};
}

const TCompositeAttributeSchema* TObjectTypeHandlerBase::GetRootAttributeSchema() const
{
    return RootAttributeSchema_;
}

const TMetaAttributeSchema* TObjectTypeHandlerBase::GetMetaAttributeSchema() const
{
    return MetaAttributeSchema_;
}

const IObjectTypeHandler::TScalarAttributeSchemas& TObjectTypeHandlerBase::GetIdAttributeSchemas() const
{
    return IdAttributeSchemas_;
}

const IObjectTypeHandler::TScalarAttributeSchemas& TObjectTypeHandlerBase::GetParentIdAttributeSchemas() const
{
    return ParentIdAttributeSchemas_;
}

const IObjectTypeHandler::TScalarAttributeSchemas& TObjectTypeHandlerBase::GetAccessControlParentIdAttributeSchemas() const
{
    return HasNonTrivialAccessControlParent()
        ? AccessControlParentIdAttributeSchemas_
        : ParentIdAttributeSchemas_;
}

IObjectTypeHandler::TResolveAttributeMutableResult TObjectTypeHandlerBase::ResolveAttribute(
    const NYPath::TYPath &path,
    TAttributeSchemaCallback callback,
    bool validateProtoSchemaCompliance) const
{
    NYPath::TTokenizer tokenizer(path);

    TAttributeSchema* schema = RootAttributeSchema_;
    TStringBuf suffix = tokenizer.GetSuffix();

    try {
        while (true) {
            if (callback) {
                callback(schema);
            }

            if (tokenizer.Advance() == NYPath::ETokenType::EndOfStream) {
                break;
            }

            if (schema->TryAsScalar()) {
                break;
            }

            auto* compositeSchema = schema->TryAsComposite();
            YT_VERIFY(compositeSchema);

            tokenizer.Expect(NYPath::ETokenType::Slash);
            tokenizer.Advance();
            tokenizer.Expect(NYPath::ETokenType::Literal);

            auto key = tokenizer.GetLiteralValue();

            auto* child = compositeSchema->FindChild(key);

            if (child) {
                suffix = tokenizer.GetSuffix();
            } else if (auto* etcChild = compositeSchema->FindEtcChildByFieldName(key); etcChild) {
                child = etcChild;
            } else if (auto* defaultEtcChild = compositeSchema->GetEtcChild(); defaultEtcChild) {
                child = defaultEtcChild;
            }

            if (!child) {
                THROW_ERROR_EXCEPTION("Attribute %v has no child %v",
                    compositeSchema->FormatPathEtc(),
                    tokenizer.GetToken());
            }

            schema = child;
        }

        YT_VERIFY(schema->TryAsScalar() || suffix.Empty());
        TResolveAttributeMutableResult result{schema, TYPath(suffix)};
        if (validateProtoSchemaCompliance && !schema->IsView()) {
            TResolveProtobufElementByYPathOptions options;
            if (result.Attribute->IsExtensible(result.SuffixPath)) {
                options.AllowUnknownYsonFields = true;
            }
            ResolveProtobufElementByYPath(
                GetRootProtobufType(),
                path,
                options);
        }
        return result;
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error resolving attribute %v", path)
            << TErrorAttribute("object_type",
                NClient::NObjects::GetGlobalObjectTypeRegistry()
                    ->GetHumanReadableTypeNameOrCrash(GetType()))
            << ex;
    }
    Y_UNREACHABLE();
}

const TScalarAttributeIndexDescriptor* TObjectTypeHandlerBase::GetIndexDescriptorOrThrow(const TString& name)
{
    if (auto it = IndexDescriptors_.find(name); it == IndexDescriptors_.end()) {
        THROW_ERROR_EXCEPTION(NClient::EErrorCode::NoSuchIndex,
            "No such index %Qv", name);
    } else {
        return it->second.get();
    }
}

const std::vector<const TScalarAttributeIndexDescriptor*> TObjectTypeHandlerBase::GetIndexes()
{
    std::vector<const TScalarAttributeIndexDescriptor*> result;
    result.reserve(IndexDescriptors_.size());
    for (const auto& [name, indexSchema] : IndexDescriptors_) {
        result.push_back(indexSchema.get());
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

NQuery::IFilterMatcherPtr TObjectTypeHandlerBase::GetHistoryFilter() const
{
    return HistoryFilter_;
}

bool TObjectTypeHandlerBase::HasHistoryEnabledAttributes() const
{
    return HasHistoryEnabledAttributes_;
}

const std::set<TYPath>& TObjectTypeHandlerBase::GetHistoryEnabledAttributePaths() const
{
    return HistoryEnabledAttributePaths_;
}

const std::set<TYPath>& TObjectTypeHandlerBase::GetHistoryIndexedAttributes() const
{
    return HistoryIndexedAttributePaths_;
}

bool TObjectTypeHandlerBase::IsPathAllowedForHistoryFilter(const NYPath::TYPath& path) const
{
    return FindPathParent(HistoryFilterAttributePaths_, path) != HistoryFilterAttributePaths_.end();
}

void TObjectTypeHandlerBase::PreloadHistoryEnabledAttributes(const TObject* object)
{
    return RootAttributeSchema_->PreloadHistoryEnabledAttributes(object);
}

bool TObjectTypeHandlerBase::HasStoreScheduledHistoryAttributes(const TObject* object) const
{
    return IsHistoryTouched(object) || RootAttributeSchema_->HasStoreScheduledHistoryAttributes(object);
}

bool TObjectTypeHandlerBase::HasHistoryEnabledAttributeForStore(const TObject* object) const
{
    return IsHistoryTouched(object) ||
        RootAttributeSchema_->HasHistoryEnabledAttributeForStore(object, EnableVerboseHistoryLogging());
}

////////////////////////////////////////////////////////////////////////////////

std::vector<TWatchLog> TObjectTypeHandlerBase::GetWatchLogs() const
{
    return {};
}

////////////////////////////////////////////////////////////////////////////////

bool TObjectTypeHandlerBase::SkipStoreWithoutChanges() const
{
    return false;
}

bool TObjectTypeHandlerBase::IsObjectNameSupported() const
{
    return false;
}

bool TObjectTypeHandlerBase::IsBuiltin(const TObject* /*object*/) const
{
    return false;
}

bool TObjectTypeHandlerBase::ForceZeroKeyEvaluation() const
{
    return false;
}

std::optional<bool> TObjectTypeHandlerBase::IsAttributeChanged(
    const TObject* object,
    const TResolveAttributeResult& resolveResult,
    std::function<bool(const TScalarAttributeSchema* schema)> bySchemaFilter,
    std::function<bool(const NYPath::TYPath&)> byPathFilter)
{
    const auto& [schema, suffixPath] = resolveResult;

    if (auto* scalarSchema = schema->TryAsScalar()) {
        if (bySchemaFilter && !bySchemaFilter(scalarSchema)) {
            return false;
        }
        std::optional<bool> result;
        if (scalarSchema->HasFilteredChangedGetter() && byPathFilter) {
            result = scalarSchema->RunFilteredChangedGetter(object, suffixPath, byPathFilter);
        } else if (scalarSchema->HasChangedGetter()) {
            result = scalarSchema->RunChangedGetter(object, suffixPath);
        }
        return result;
    } else {
        YT_VERIFY(suffixPath.empty());
        bool isChanged = false;
        int nullCount = 0;
        schema->ForEachLeafAttribute([&] (const TScalarAttributeSchema* leaf) {
            if (bySchemaFilter && !bySchemaFilter(leaf)) {
                return false;
            }
            if (leaf->HasFilteredChangedGetter() && byPathFilter) {
                auto value = leaf->RunFilteredChangedGetter(object, /*path*/ "", byPathFilter);
                if (value) {
                    isChanged = value;
                    return true;
                }
            } else if (leaf->HasChangedGetter()) {
                auto value = leaf->RunChangedGetter(object);
                if (value) {
                    isChanged = value;
                    return true;
                }
            } else if (leaf->HasStoreScheduledGetter()) {
                if (leaf->RunStoreScheduledGetter(object)) {
                    nullCount += 1;
                }
            } else {
                nullCount += 1;
            }
            return false;
        });
        if (isChanged) {
            return true;
        }
        if (nullCount > 0) {
            return std::nullopt;
        }
        return false;
    }
}

std::optional<std::vector<const TTagSet*>> TObjectTypeHandlerBase::CollectChangedAttributeTagSets(const TObject* object)
{
    if (!AreTagsEnabled()) {
        return std::nullopt;
    }

    std::vector<const TTagSet*> changedTagSets;
    const auto* schema = object->GetTypeHandler()->GetRootAttributeSchema();
    schema->ForEachLeafAttribute([&] (const TScalarAttributeSchema* leaf) {
        auto leafChangedTags = leaf->CollectChangedTags(object);
        changedTagSets.reserve(changedTagSets.size() + leafChangedTags.size());
        std::ranges::move(leafChangedTags, std::back_inserter(changedTagSets));
        return false;
    });
    return changedTagSets;
}

bool TObjectTypeHandlerBase::AreTagsEnabled() const
{
    return false;
}

bool TObjectTypeHandlerBase::IsParentRemovalForbidden() const
{
    return ForbidParentRemoval_;
}

////////////////////////////////////////////////////////////////////////////////

void TObjectTypeHandlerBase::InitializeCreatedObject(
    TTransaction* /*transaction*/,
    TObject* object)
{
    for (auto* observers : object->LifecycleObservers()) {
        observers->OnObjectInitialization();
    }

    object->CreationTime().Store(TInstant::Now());

    object->InheritAcl().Store(true);
    auto acl = GetDefaultAcl();
    for (auto& ace : acl) {
        if (HasAuthenticatedUser()) {
            ace.Subjects.push_back(GetAuthenticatedUserIdentity().User);
        }
        object->AddAce(ace);
    }
}

void TObjectTypeHandlerBase::FinishObjectCreation(
    TTransaction* /*transaction*/,
    TObject* object)
{
    object->SetState(EObjectState::Created);
}

void TObjectTypeHandlerBase::ValidateCreatedObject(
    TTransaction* /*transaction*/,
    TObject* object)
{
    if (Bootstrap_->GetDBConfig().EnableFinalizers) {
        const auto& finalizers = object->Finalizers().Load();
        for (auto& [key, finalizer] : finalizers) {
            THROW_ERROR_EXCEPTION_IF(finalizer.has_completion_timestamp(),
                "Finalizer %v cannot be completed on %v creation",
                key,
                object->GetDisplayName());
        }
    }
}

void TObjectTypeHandlerBase::PreloadObjectRemoval(
    TTransaction* /*transaction*/, const TObject* object, IUpdateContext* /*context*/)
{
    for (auto* observer : object->LifecycleObservers()) {
        observer->PreloadObjectRemoval();
    }
    // Later used in FlushTransaction, where history and watching rely on it.
    object->ScheduleMetaResponseLoad();
    if (Bootstrap_->GetDBConfig().EnableFinalizers) {
        object->FinalizationStartTime().ScheduleLoad();
        object->Finalizers().ScheduleLoad();
    }

    ForEachChildrenAttribute(
        object,
        [] (const TChildrenAttributeBase* childrenAttribute) {
            childrenAttribute->ScheduleLoad();
        });
}

void TObjectTypeHandlerBase::CheckObjectRemoval(TTransaction* /*transaction*/, const TObject* object)
{
    THROW_ERROR_EXCEPTION_IF(IsBuiltin(object),
        "Cannot remove builtin %v",
        object->GetDisplayName());
}

void TObjectTypeHandlerBase::StartObjectRemoval(TTransaction* /*transaction*/, TObject* object)
{
    if (Bootstrap_->GetDBConfig().EnableFinalizers) {
        object->FinalizationStartTime().Store(TInstant::Now().GetValue());
    }
}

void TObjectTypeHandlerBase::FinishObjectRemoval(TTransaction* /*transaction*/, TObject* /*object*/)
{ }

////////////////////////////////////////////////////////////////////////////////

template <std::derived_from<TAttributeSchema> TSchema>
static TSchema* EmplaceAttributeSchema(
    std::vector<std::unique_ptr<TAttributeSchema>>& schemas,
    const TString& name,
    IObjectTypeHandler* typeHandler)
{
    auto schemaHolder = std::make_unique<TSchema>(
        typeHandler,
        typeHandler->GetBootstrap()->GetObjectManager().Get(),
        name);
    auto* schema = schemaHolder.get();
    schemas.push_back(std::move(schemaHolder));
    return schema;
}

////////////////////////////////////////////////////////////////////////////////

TScalarAttributeSchema* TObjectTypeHandlerBase::MakeScalarAttributeSchema(
    const TString& name)
{
    return EmplaceAttributeSchema<TScalarAttributeSchema>(
        AttributeSchemas_,
        name,
        this);
}

TCompositeAttributeSchema* TObjectTypeHandlerBase::MakeCompositeAttributeSchema(
    const TString& name)
{
    return EmplaceAttributeSchema<TCompositeAttributeSchema>(
        AttributeSchemas_,
        name,
        this);
}

TMetaAttributeSchema* TObjectTypeHandlerBase::MakeMetaAttributeSchema()
{
    return EmplaceAttributeSchema<TMetaAttributeSchema>(
        AttributeSchemas_,
        /*name*/ "meta",
        this);
}

TScalarAttributeSchema* TObjectTypeHandlerBase::MakeIdAttributeSchema(
    const TString& name,
    const TDBField* field,
    TValueGetter valueGetter)
{
    const auto& keyFields = GetKeyFields();
    IdAttributeSchemas_.resize(keyFields.size());

    const auto index = FindIndex(keyFields, field);

    if (index == NPOS) {
        THROW_ERROR_EXCEPTION("Key field %Qv not found in %Qv",
            field->Name,
            SchemaId_);
    }

    if (IdAttributeSchemas_[index] != nullptr) {
        THROW_ERROR_EXCEPTION("Key field %Qv attached to attribute schemas twice",
            field->Name);
    }

    auto* schema = MakeScalarAttributeSchema(name);
    schema->SetIdAttribute(field, valueGetter);
    schema->SetConstantChangedGetter(false);
    IdAttributeSchemas_[index] = schema;
    schema->EnableMetaResponseAttribute();
    return schema;
}

TScalarAttributeSchema* TObjectTypeHandlerBase::MakeParentIdAttributeSchema(
    const TString& name,
    const TDBField* field,
    TValueGetter valueGetter)
{
    const auto& keyFields = GetParentKeyFields();
    ParentIdAttributeSchemas_.resize(keyFields.size());

    const auto index = FindIndex(keyFields, field);

    if (index == NPOS) {
        THROW_ERROR_EXCEPTION("Parent key field %Qv not found in %Qv",
            field->Name,
            SchemaId_);
    }

    if (ParentIdAttributeSchemas_[index] != nullptr) {
        THROW_ERROR_EXCEPTION("Parent key field %Qv attached to attribute schemas twice",
            field->Name);
    }

    auto* schema = MakeScalarAttributeSchema(name);
    schema->SetParentIdAttribute(field, valueGetter);
    ParentIdAttributeSchemas_[index] = schema;
    schema->SetConstantChangedGetter(false);
    schema->EnableMetaResponseAttribute();
    return schema;
}

TScalarAttributeSchema* TObjectTypeHandlerBase::MakeAnnotationsAttributeSchema(
    const TString& name)
{
    auto* schema = MakeScalarAttributeSchema(name);
    return TScalarAttributeSchemaBuilder(schema).SetAnnotationsAttribute();
}

std::vector<NAccessControl::TAccessControlEntry> TObjectTypeHandlerBase::GetDefaultAcl()
{
    return {
        NAccessControl::TAccessControlEntry{
            .Action = NAccessControl::EAccessControlAction::Allow,
            .Permissions = {TAccessControlPermissionValues::Read, TAccessControlPermissionValues::Write},
        },
    };
}

void TObjectTypeHandlerBase::RegisterScalarAttributeIndex(
    TString indexName,
    std::unique_ptr<TScalarAttributeIndexDescriptor> indexedDescriptor)
{
    EmplaceOrCrash(IndexDescriptors_, std::move(indexName), std::move(indexedDescriptor));
}

void TObjectTypeHandlerBase::RegisterChildrenAttribute(
    TObjectTypeValue objectType,
    std::function<const TChildrenAttributeBase*(const TObject*)> getter)
{
    EmplaceOrCrash(ChildrenAttributes_, objectType, std::move(getter));
}

void TObjectTypeHandlerBase::ConfigureHistory(
    std::vector<THistoryAttribute> attributes,
    std::optional<TObjectFilter> filter)
{
    if (filter) {
        HistoryFilter_ = NQuery::CreateFilterMatcher(filter->Query, {""});
    }
    std::sort(attributes.begin(), attributes.end(), [] (const auto& lhs, const auto& rhs) {
        return lhs.Path < rhs.Path;
    });
    for (const auto& attribute : attributes) {
        if (attribute.Indexed) {
            HistoryIndexedAttributePaths_.insert(attribute.Path);
        }

        if (attribute.AllowedInFilter) {
            HistoryFilterAttributePaths_.insert(attribute.Path);
        }

        auto result = ResolveAttribute(attribute.Path, /*callback*/ {}, /*validateProtoSchemaCompliance*/ true);
        if (!result.Attribute->IsHistoryEnabledFor(result.SuffixPath)) {
            result.Attribute->EnableHistory(THistoryEnabledAttributeSchema()
                .AddPath(result.SuffixPath));
        }
    }
}

bool TObjectTypeHandlerBase::EnableVerboseHistoryLogging() const
{
    return false;
}

void TObjectTypeHandlerBase::FillUpdatePolicies()
{
    TOnMutableAttribute pushPolicyDown = [] (TAttributeSchema* schema) {
        const auto* parent = schema->GetParent();
        auto policyToPushDown = EUpdatePolicy::Updatable;
        if (parent) {
            policyToPushDown = parent->GetUpdatePolicy().value_or(EUpdatePolicy::Updatable);
        }
        if (!schema->GetUpdatePolicy()) {
            if (EUpdatePolicy::OpaqueUpdatable == policyToPushDown) {
                policyToPushDown = EUpdatePolicy::Updatable;
            }
            if (EUpdatePolicy::OpaqueReadOnly == policyToPushDown) {
                policyToPushDown = EUpdatePolicy::ReadOnly;
            }
            schema->SetUpdatePolicy(policyToPushDown);
        }
        return false;
    };
    RootAttributeSchema_->ForEachAttributeMutable(pushPolicyDown);
}

////////////////////////////////////////////////////////////////////////////////

void TObjectTypeHandlerBase::ValidateAcl(TTransaction* transaction, const TObject* object) const
{
    auto oldAcl = object->LoadOldAcl();
    auto acl = object->LoadAcl();

    THashSet<TObjectId> oldSubjects;
    THashSet<TYPath> oldAttributes;
    for (const auto& ace : oldAcl) {
        for (const auto& subject : ace.Subjects) {
            oldSubjects.insert(subject);
        }
        for (const auto& attribute : ace.Attributes) {
            oldAttributes.insert(attribute);
        }
    }

    for (const auto& ace : acl) {
        for (const auto& subject : ace.Subjects) {
            if (!oldSubjects.contains(subject)) {
                ValidateSubjectExists(transaction, subject);
            }
        }
        for (const auto& attribute : ace.Attributes) {
            if (!oldAttributes.contains(attribute)) {
                NAttributes::ValidateAttributePath(attribute);
            }
        }
    }
}

void TObjectTypeHandlerBase::ProfileAttributes(TTransaction* transaction, const TObject* object)
{
    for (auto& [path, attributeSensor] : AttributeSensors_) {
        attributeSensor->Profile(transaction, object);
    }
}

void TObjectTypeHandlerBase::AddAttributeSensor(
    const NYPath::TYPath& path,
    NClient::NProto::EAttributeSensorPolicy policy)
{
    EmplaceOrCrash(AttributeSensors_, path, CreateAttributeProfiler(this, path, policy));
}

void TObjectTypeHandlerBase::SetAttributeSensorValueTransform(
    const NYPath::TYPath& path,
    IAttributeProfiler::TValueTransform transform)
{
    auto it = AttributeSensors_.find(path);
    THROW_ERROR_EXCEPTION_IF(it == AttributeSensors_.end(),
        "Cannot set value transform for nonexistent attribute sensor %v",
        path);
    it->second->SetValueTransform(std::move(transform));
}

void TObjectTypeHandlerBase::PrepareRevisionTracker(
    const THashSet<NYPath::TYPath>& trackedPaths,
    const NYPath::TYPath& trackerPath,
    bool lockGroupRestrictionEnabled,
    const THashSet<NYPath::TYPath>& excludedPaths)
{
    RevisionTrackerOptionsByPath_[trackerPath] = TTrackerOptions{
        .LockGroupRestrictionEnabled = lockGroupRestrictionEnabled,
        .TrackedFields = trackedPaths,
    };
    PrepareExcludedFields(trackerPath, excludedPaths);
    auto* trackerSchema = ResolveAttribute(
        trackerPath,
        /*callback*/ {},
        /*validateProtoSchemaCompliance*/ false).Attribute->AsScalar();
    for (const auto& trackedPath: trackedPaths) {
        auto resolvedSchema = ResolveAttribute(
            trackedPath,
            /*callback*/ {},
            /*validateProtoSchemaCompliance*/ false);
        auto* schema = resolvedSchema.Attribute;

        schema->ForEachLeafAttributeMutable([&] (TAttributeSchema* leaf) {
            if (leaf->GetPath() == trackerPath) {
                return false;
            }
            auto* scalarSchema = leaf->TryAsScalar();
            YT_VERIFY(scalarSchema);

            if (!scalarSchema->HasAttributeDescriptor()) {
                return false;
            }
            auto* attributeDescriptor = scalarSchema->GetAttributeDescriptor();

            auto scheduleRevisionObserver = [
                trackerPath,
                attributeDescriptor,
                scalarSchema
            ] (TObject* owner, bool /*sharedWrite*/) {
                auto* transaction = owner->GetSession()->GetOwner();

                bool hasNonConstantChangedGetter = scalarSchema->HasChangedGetter() &&
                    (scalarSchema->GetChangedGetterType() == EChangedGetterType::ConstantTrue ||
                    scalarSchema->GetChangedGetterType() == EChangedGetterType::ConstantFalse);
                if (!hasNonConstantChangedGetter) {
                    attributeDescriptor->AttributeBaseGetter(owner)->ScheduleLoad();
                }

                transaction->ScheduleCommitAction(TCommitActionTypes::HandleRevisionUpdates, owner);
                owner->TouchRevisionTracker(trackerPath);
            };

            attributeDescriptor->BeforeStoreObservers.Add(scheduleRevisionObserver);
            attributeDescriptor->BeforeMutableLoadObservers.Add(scheduleRevisionObserver);
            return false;
        });

        schema->AddUpdateHandler<TObject>(std::bind(
            &TObjectTypeHandlerBase::ScheduleRevisionTrackerUpdate,
            this,
            std::placeholders::_2,
            std::placeholders::_1,
            trackerPath));
    }

    trackerSchema->SetConstantChangedGetter(false);
    trackerSchema->SetTypedValueSetter<TObject, ui64>(
        [trackerSchema]  (
            TTransaction* /*transaction*/,
            TObject* object,
            const NYPath::TYPath& path,
            const ui64& value,
            bool /*recursive*/,
            std::optional<bool> sharedWrite,
            EAggregateMode aggregateMode,
            const TTransactionCallContext& /*transactionCallContext*/)
        {
            THROW_ERROR_EXCEPTION_UNLESS(path.Empty(),
                "Empty suffix path expected for setting revision attribute value");
            THROW_ERROR_EXCEPTION_UNLESS(value != 0,
                "Zero value cannot be set to revision attribute");
            THROW_ERROR_EXCEPTION_IF(aggregateMode != EAggregateMode::Unspecified,
                NClient::EErrorCode::InvalidRequestArguments,
                "Revision attribute cannot be updated with aggregate mode");
            trackerSchema
                ->GetAttribute<TScalarAttribute<ui64>>(object)
                ->Store(value, sharedWrite);
        });
}

void TObjectTypeHandlerBase::PrepareExcludedFields(
    const NYPath::TYPath& trackerPath,
    const THashSet<NYPath::TYPath>& excludedPaths)
{
    for (const auto& excludedPath : excludedPaths) {
        auto resolvedSchema = ResolveAttribute(
            excludedPath,
            /*callback*/ {},
            /*validateProtoSchemaCompliance*/ false);
        auto* schema = resolvedSchema.Attribute;
        if (auto* compositeSchema = schema->TryAsComposite()) {
            compositeSchema->ForEachLeafAttribute([
                    &excludedPaths = RevisionTrackerOptionsByPath_[trackerPath].ExcludedFields
                ] (const TScalarAttributeSchema* leaf) {
                    excludedPaths.insert(leaf->GetPath());
                    return false;
                });
            return;
        }

        RevisionTrackerOptionsByPath_[trackerPath].ExcludedFields.insert(schema->GetPath() + resolvedSchema.SuffixPath);
    }
}

void TObjectTypeHandlerBase::ScheduleRevisionTrackerUpdate(
    TObject* object,
    TTransaction* transaction,
    const NYPath::TYPath& trackerPath)
{
    transaction->ScheduleCommitAction(TCommitActionTypes::HandleRevisionUpdates, object);
    object->TouchRevisionTracker(trackerPath);
}

bool TObjectTypeHandlerBase::NeedsRevisionUpdate(
    TObject* object,
    TScalarAttributeSchema* trackerSchema,
    const TResolveAttributeResult& trackedAttribute)
{
    if (!SkipStoreWithoutChanges()) {
        return true;
    }
    bool attributeChanged = true;
    auto trackerPath = trackerSchema->GetPath();
    auto revisionOptions = RevisionTrackerOptionsByPath_[trackerPath];
    auto byPathFilter = [
        trackerPath,
        &excludedFields = revisionOptions.ExcludedFields
    ] (const NYPath::TYPath& path) {
        // This check is enough for composite schemas since each path of leaf attribute
        // was added to excludedFields.
        return !excludedFields.contains(path);
    };
    if (revisionOptions.LockGroupRestrictionEnabled) {
        auto* revisionDBField = trackerSchema->TryGetDBField();
        YT_VERIFY(revisionDBField);
        auto revisionLockGroup = revisionDBField->LockGroup;
        auto bySchemaFilter = [revisionLockGroup] (const TScalarAttributeSchema* schema) -> bool {
            auto* schemaDBField = schema->TryGetDBField();
            return schemaDBField && schemaDBField->LockGroup == revisionLockGroup;
        };
        attributeChanged = IsAttributeChanged(
            object,
            trackedAttribute,
            bySchemaFilter,
            byPathFilter).value_or(true);
    } else {
        attributeChanged = IsAttributeChanged(
            object,
            trackedAttribute,
            /*bySchemaFilter*/ nullptr,
            byPathFilter).value_or(true);
    }

    return attributeChanged;
}

void TObjectTypeHandlerBase::DoHandleRevisionTrackerUpdate(
    TObject* object,
    const NYPath::TYPath& trackerPath)
{
    auto* transaction = object->GetSession()->GetOwner();
    const auto& transactionOptions = transaction->GetMutatingTransactionOptions();

    if (transactionOptions.SkipRevisionBump && IsEventGenerationSkipAllowed(object->GetType())) {
        YT_LOG_DEBUG("Revision update was skipped (ObjectKey: %v, Type: %v)",
            object->GetKey(),
            NClient::NObjects::GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrCrash(object->GetType()));
        return;
    }

    auto* trackerSchema = ResolveAttribute(
        trackerPath,
        /*callback*/ {},
        /*validateProtoSchemaCompliance*/ false).Attribute->TryAsScalar();
    for (const auto& trackedPath : RevisionTrackerOptionsByPath_[trackerPath].TrackedFields) {
        auto trackedAttribute = ResolveAttribute(
            trackedPath,
            /*callback*/ {},
            /*validateProtoSchemaCompliance*/ false);
        auto trackedAttributeResolved = TResolveAttributeResult{trackedAttribute.Attribute, trackedAttribute.SuffixPath};

        if (NeedsRevisionUpdate(object, trackerSchema, trackedAttributeResolved)) {
            trackerSchema->GetAttribute<TScalarAttribute<ui64>>(object)
                ->Store(transaction->GetStartTimestamp());
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void TObjectTypeHandlerBase::EnsureAncestryDepthInitialized()
{
    if (AncestryDepth_) {
        return;
    }

    if (HasParent()) {
        auto* parent = static_cast<TObjectTypeHandlerBase*>(
            GetBootstrap()->GetObjectManager()->GetTypeHandlerOrCrash(GetParentType()));
        parent->EnsureAncestryDepthInitialized();
        YT_VERIFY(parent->AncestryDepth_);
        AncestryDepth_.emplace(*parent->AncestryDepth_ + 1);
    } else {
        AncestryDepth_.emplace(0);
    }
}

void TObjectTypeHandlerBase::PrepareHistoryEnabledAttributeSchemaCache()
{
    if (!HistoryFilter_) {
        HistoryFilter_ = NQuery::CreateConstantFilterMatcher(true);
    }
    HistoryEnabledAttributePaths_ = RootAttributeSchema_->GetHistoryEnabledAttributePaths();
    HasHistoryEnabledAttributes_ = !HistoryEnabledAttributePaths_.empty();

    for (const auto& indexedAttributePath : HistoryIndexedAttributePaths_) {
        auto [attribute, suffix] = ResolveAttributeValidated(this, indexedAttributePath);
        YT_VERIFY(attribute->IsHistoryEnabledFor(suffix));
    }
}

////////////////////////////////////////////////////////////////////////////////

void TObjectTypeHandlerBase::EvaluateKey(
    const TTransaction*,
    const TObject* object,
    IYsonConsumer* consumer) const
{
    BuildYsonFluently(consumer).Value(object->GetKey().ToString());
}

void TObjectTypeHandlerBase::EvaluateParentKey(
    const TTransaction*,
    const TObject* object,
    IYsonConsumer* consumer) const
{
    BuildYsonFluently(consumer).Value(object->GetParentKey().ToString());
}

void TObjectTypeHandlerBase::PreloadFqid(const TObject* object) const
{
    object->ScheduleUuidLoad();
}

void TObjectTypeHandlerBase::EvaluateFqid(
    const TTransaction*,
    const TObject* object,
    IYsonConsumer* consumer) const
{
    BuildYsonFluently(consumer).Value(object->GetFqid());
}

bool TObjectTypeHandlerBase::IsEventGenerationSkipAllowed(TObjectTypeValue objectType)
{
    auto accessControlManager = GetBootstrap()->GetAccessControlManager();
    auto userId = TryGetAuthenticatedUserIdentity().value_or(NRpc::GetRootAuthenticationIdentity()).User;
    if (accessControlManager->IsSuperuser(userId)) {
        return true;
    }
    auto checkResult = accessControlManager->CheckCachedPermission(
        userId,
        TObjectTypeValues::Schema,
        TObjectKey(NClient::NObjects::GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrCrash(objectType)),
        NAccessControl::TAccessControlPermissionValues::Use,
        EventGenerationSkipAccessControlPath);
    return checkResult.Action == NAccessControl::EAccessControlAction::Allow;
}

void TObjectTypeHandlerBase::SetupFinalizers()
{
    auto* genericFinalizersSchema = MetaAttributeSchema_->FindChild("finalizers");
    auto* genericFinalizationStartTimeSchema = MetaAttributeSchema_->FindChild("finalization_start_time");
    YT_VERIFY(genericFinalizersSchema && genericFinalizationStartTimeSchema);

    auto* finalizersSchema = genericFinalizersSchema->AsScalar();
    auto* finalizationStartTimeSchema = genericFinalizationStartTimeSchema->AsScalar();

    finalizationStartTimeSchema->AddUpdateHandler<TObject>(TObjectTypeHandlerBase::HandleFinalizationStartTimeUpdate);
    finalizersSchema->AddUpdateHandler<TObject>(TObjectTypeHandlerBase::HandleFinalizersUpdate);

    auto finalizerObserver = [] (TObject* object, bool /*appliedSharedWrite*/) {
        object->GetSession()->GetOwner()->ScheduleCommitAction(
            TCommitActionTypes::RemoveFinalizedObjects, object);
    };
    finalizersSchema->GetAttributeDescriptor()->BeforeStoreObservers.Add(finalizerObserver);
    finalizersSchema->GetAttributeDescriptor()->BeforeMutableLoadObservers.Add(finalizerObserver);
}

void TObjectTypeHandlerBase::GetActiveFinalizers(
    TTransaction* /*transaction*/, const TObject* object, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .DoFor(object->Finalizers().Load(), [object] (auto fluent, const auto& entry) {
                const auto& [finalizerName, finalizer] = entry;
                if (object->IsFinalizerActive(finalizerName)) {
                    fluent.Item(finalizerName).Value(finalizer);
                }
            })
        .EndMap();
}

void TObjectTypeHandlerBase::HandleFinalizationStartTimeUpdate(TTransaction* /*transaction*/, TObject* object)
{
    auto oldFinalizationTime = object->FinalizationStartTime().LoadOld();
    auto finalizationTime = object->FinalizationStartTime().Load();

    THROW_ERROR_EXCEPTION_IF(!oldFinalizationTime && finalizationTime,
        "Cannot set finalization time for %v directly; try removing it",
        object->GetDisplayName());
}

void TObjectTypeHandlerBase::HandleFinalizersUpdate(TTransaction* /*transaction*/, TObject* object)
{
    if (!object->RemovalStarted()) {
        return;
    }
    auto oldFinalizers = object->Finalizers().LoadOld();
    auto finalizers = object->Finalizers().Load();

    for (const auto& [finalizerName, finalizer] : finalizers) {
        THROW_ERROR_EXCEPTION_UNLESS(oldFinalizers.contains(finalizerName),
            NClient::EErrorCode::InvalidObjectState,
            "Cannot add finalizer %Qv to %v after removal started",
            finalizerName,
            object->GetDisplayName())
    }
}

void TObjectTypeHandlerBase::DoPrepareAttributeMigrations(
    TObject* /*object*/,
    const TBitSet<int>& /*attributeMigrations*/,
    const TBitSet<int>& /*forcedAttributeMigrations*/)
{
    YT_ABORT();
}

void TObjectTypeHandlerBase::DoFinalizeAttributeMigrations(
    TObject* /*object*/,
    const TBitSet<int>& /*attributeMigrations*/,
    const TBitSet<int>& /*forcedAttributeMigrations*/)
{
    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects

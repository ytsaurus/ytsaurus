#include "persistence.h"

#include "db_schema.h"
#include "helpers.h"
#include "object.h"
#include "private.h"
#include "type_handler.h"

#include <yp/server/lib/objects/type_info.h>

#include <yt/client/table_client/row_buffer.h>

#include <yt/client/api/rowset.h>

#include <yt/ytlib/query_client/ast.h>

#include <yt/core/misc/collection_helpers.h>

namespace NYP::NServer::NObjects {

using namespace NYT::NTableClient;
using namespace NYT::NApi;
using namespace NYT::NQueryClient::NAst;
using namespace NYT::NYson;

////////////////////////////////////////////////////////////////////////////////

static const TDBField DummyField{"dummy", EValueType::Boolean};

////////////////////////////////////////////////////////////////////////////////

TRange<TUnversionedValue> CaptureCompositeObjectKey(
    const TObject* object,
    const TRowBufferPtr& rowBuffer)
{
    auto* typeHandler = object->GetTypeHandler();
    auto parentType = typeHandler->GetParentType();
    auto capture = [&] (const auto& key) {
        return MakeRange(
            reinterpret_cast<TUnversionedValue*>(rowBuffer->GetPool()->Capture(TRef::FromPod(key))),
            key.size());
    };
    if (parentType == EObjectType::Null) {
        return capture(
            ToUnversionedValues(
                rowBuffer,
                object->GetId()));
    } else {
        return capture(
            ToUnversionedValues(
                rowBuffer,
                object->GetParentId(),
                object->GetId()));
    }
}

////////////////////////////////////////////////////////////////////////////////

TObjectExistenceChecker::TObjectExistenceChecker(TObject* object)
    : Object_(object)
{ }

TObject* TObjectExistenceChecker::GetObject() const
{
    return Object_;
}

void TObjectExistenceChecker::ScheduleCheck() const
{
    auto* this_ = const_cast<TObjectExistenceChecker*>(this);
    if (this_->CheckScheduled_) {
        return;
    }
    this_->CheckScheduled_ = true;
    Object_->GetSession()->ScheduleLoad(
        [=] (ILoadContext* context) {
            this_->LoadFromDB(context);
        },
        ISession::ParentLoadPriority);
}

bool TObjectExistenceChecker::Check() const
{
    if (!Checked_) {
        ScheduleCheck();
        Object_->GetSession()->FlushLoads();
    }
    YT_ASSERT(Checked_);
    return Exists_;
}

void TObjectExistenceChecker::LoadFromDB(ILoadContext* context)
{
    YT_ASSERT(!Checked_);

    auto lookupHandler = [&] (const std::optional<TRange<NYT::NTableClient::TVersionedValue>>& optionalValues) {
        YT_ASSERT(!Checked_);
        Checked_ = true;
        auto* typeHandler = Object_->GetTypeHandler();
        if (typeHandler->GetParentType() == EObjectType::Null) {
            Exists_ = optionalValues.operator bool() && (*optionalValues)[1].Type == EValueType::Null;
        } else {
            Exists_ = optionalValues.operator bool();
        }
    };

    auto* typeHandler = Object_->GetTypeHandler();
    if (typeHandler->GetParentType() == EObjectType::Null) {
        const auto* table = typeHandler->GetTable();
        context->ScheduleLookup(
            table,
            ToUnversionedValues(
                context->GetRowBuffer(),
                Object_->GetId()),
            MakeArray(
                // XXX(babenko): creation time is only needed to work around the bug in write ts
                &ObjectsTable.Fields.Meta_CreationTime,
                &ObjectsTable.Fields.Meta_RemovalTime),
            std::move(lookupHandler));
    } else {
        context->ScheduleLookup(
            &ParentsTable,
            ToUnversionedValues(
                context->GetRowBuffer(),
                Object_->GetId(),
                Object_->GetType()),
            MakeArray(&ParentsTable.Fields.ParentId),
            std::move(lookupHandler));
    }
}

void TObjectExistenceChecker::StoreToDB(IStoreContext* /*context*/)
{
    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

TObjectTombstoneChecker::TObjectTombstoneChecker(TObject* object)
    : Object_(object)
{ }

void TObjectTombstoneChecker::ScheduleCheck() const
{
    auto* this_ = const_cast<TObjectTombstoneChecker*>(this);
    if (this_->CheckScheduled_) {
        return;
    }
    this_->CheckScheduled_ = true;
    Object_->GetSession()->ScheduleLoad(
        [=] (ILoadContext* context) {
            this_->LoadFromDB(context);
        });
}

bool TObjectTombstoneChecker::Check() const
{
    if (!Checked_) {
        ScheduleCheck();
        Object_->GetSession()->FlushLoads();
    }
    YT_ASSERT(Checked_);
    return Tombstone_;
}

void TObjectTombstoneChecker::LoadFromDB(ILoadContext* /* context */)
{
    YT_ASSERT(!Checked_);

    Object_->GetSession()->ScheduleLoad(
        [=] (ILoadContext* context) {
            context->ScheduleLookup(
                &TombstonesTable,
                ToUnversionedValues(
                    context->GetRowBuffer(),
                    Object_->GetId(),
                    Object_->GetType()),
                MakeArray(
                    // Not actually used, just for better diagnostics.
                    &TombstonesTable.Fields.RemovalTime),
                [=] (const std::optional<TRange<TVersionedValue>>& optionalValues) {
                    YT_ASSERT(!Checked_);
                    Checked_ = true;
                    Tombstone_ = optionalValues.operator bool();
                });
        });
}

void TObjectTombstoneChecker::StoreToDB(IStoreContext* /*context*/)
{
    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

TAttributeBase::TAttributeBase(TObject* owner)
    : Owner_(owner)
{
    Owner_->RegisterAttribute(this);
}

TObject* TAttributeBase::GetOwner() const
{
    return Owner_;
}

void TAttributeBase::DoScheduleLoad(int priority) const
{
    auto this_ = const_cast<TAttributeBase*>(this);
    if (this_->LoadScheduled_) {
        return;
    }
    this_->LoadScheduled_ = true;
    Owner_->GetSession()->ScheduleLoad(
        [=] (ILoadContext* context) {
            this_->LoadScheduled_ = false;
            this_->LoadFromDB(context);
        },
        priority);
}

void TAttributeBase::DoScheduleStore() const
{
    auto this_ = const_cast<TAttributeBase*>(this);
    if (this_->StoreScheduled_) {
        return;
    }
    this_->StoreScheduled_ = true;
    Owner_->GetSession()->ScheduleStore(
        [=] (IStoreContext* context) {
            this_->StoreScheduled_ = false;
            this_->StoreToDB(context);
        });
}

void TAttributeBase::LoadFromDB(ILoadContext* /*context*/)
{ }

void TAttributeBase::StoreToDB(IStoreContext* /*context*/)
{ }

void TAttributeBase::OnObjectCreated()
{ }

void TAttributeBase::OnObjectRemoved()
{ }

////////////////////////////////////////////////////////////////////////////////

TParentIdAttribute::TParentIdAttribute(TObject* owner, const TObjectId& parentId)
    : TAttributeBase(owner)
    , NeedsParentId_(Owner_->GetTypeHandler()->GetParentType() != EObjectType::Null)
    , ParentId_(parentId)
{
    if (!ParentId_ && NeedsParentId_) {
        DoScheduleLoad(ISession::ParentLoadPriority);
    }
}

const TObjectId& TParentIdAttribute::GetId() const
{
    if (!NeedsParentId_) {
        return ParentId_;
    }

    if (!ParentId_ && !Missing_) {
        Owner_->GetSession()->FlushLoads();
        YT_ASSERT(ParentId_ || Missing_);
    }

    if (Missing_) {
        THROW_ERROR_EXCEPTION(
            NClient::NApi::EErrorCode::NoSuchObject,
            "%v %Qv is missing",
            GetCapitalizedHumanReadableTypeName(Owner_->GetType()),
            Owner_->GetId());
    }

    return ParentId_;
}

void TParentIdAttribute::LoadFromDB(ILoadContext* context)
{
    YT_ASSERT(!ParentId_);
    YT_ASSERT(!Missing_);

    context->ScheduleLookup(
        &ParentsTable,
        ToUnversionedValues(
            context->GetRowBuffer(),
            Owner_->GetId(),
            Owner_->GetType()),
        MakeArray(&ParentsTable.Fields.ParentId),
        [=] (const std::optional<TRange<TVersionedValue>>& optionalValues) {
            if (optionalValues) {
                YT_ASSERT(optionalValues->Size() == 1);
                try {
                    FromUnversionedValue(&ParentId_, (*optionalValues)[0]);
                    YT_LOG_DEBUG("Object parent resolved (ObjectId: %v, ObjectType: %v, ParentId: %v)",
                        Owner_->GetId(),
                        Owner_->GetType(),
                        ParentId_);
                } catch (const std::exception& ex) {
                    THROW_ERROR_EXCEPTION("Error loading parent id value for %v %v",
                        GetHumanReadableTypeName(Owner_->GetType()),
                        GetObjectDisplayName(Owner_))
                        << ex;
                }
            } else {
                Missing_ = true;
            }
        });
}

////////////////////////////////////////////////////////////////////////////////

TChildrenAttributeBase::TChildrenAttributeBase(TObject* owner)
    : TAttributeBase(owner)
{ }

void TChildrenAttributeBase::ScheduleLoad() const
{
    DoScheduleLoad();
}

const THashSet<TObject*>& TChildrenAttributeBase::UntypedLoad() const
{
    ScheduleLoad();
    Owner_->GetSession()->FlushLoads();
    return *Children_;
}

void TChildrenAttributeBase::DoAdd(TObject* child)
{
    AddedChildren_.insert(child);
    RemovedChildren_.erase(child);
    if (Children_) {
        Children_->insert(child);
    }
}

void TChildrenAttributeBase::DoRemove(TObject* child)
{
    RemovedChildren_.insert(child);
    AddedChildren_.erase(child);
    if (Children_) {
        Children_->erase(child);
    }
}

void TChildrenAttributeBase::LoadFromDB(ILoadContext* context)
{
    if (Children_) {
        return;
    }

    auto childrenType = GetChildrenType();
    auto* session = Owner_->GetSession();
    auto* typeHandler = session->GetTypeHandler(childrenType);

    context->ScheduleSelect(
        Format("%v from %v where %v = %v and is_null(%v)",
            FormatId(typeHandler->GetIdField()->Name),
            FormatId(context->GetTablePath(typeHandler->GetTable())),
            FormatId(typeHandler->GetParentIdField()->Name),
            FormatLiteralValue(Owner_->GetId()),
            FormatId(ObjectsTable.Fields.Meta_RemovalTime.Name)),
        [=] (const IUnversionedRowsetPtr& rowset) {
            YT_ASSERT(!Children_);
            auto rows = rowset->GetRows();
            Children_.emplace();
            Children_->reserve(rows.Size());
            for (auto row : rows) {
                YT_ASSERT(row.GetCount() == 1);
                auto childId = FromUnversionedValue<TObjectId>(row[0]);
                auto* child = Owner_->GetSession()->GetObject(GetChildrenType(), childId);
                Children_->insert(child);
            }
            for (auto* object : AddedChildren_) {
                Children_->insert(object);
            }
            for (auto* object : RemovedChildren_) {
                Children_->erase(object);
            }
        });
}

////////////////////////////////////////////////////////////////////////////////

TScalarAttributeBase::TScalarAttributeBase(TObject* owner, const TScalarAttributeSchemaBase* schema)
    : TAttributeBase(owner)
    , Schema_(schema)
{ }

void TScalarAttributeBase::ScheduleLoad() const
{
    if (LoadScheduled_ || Loaded_) {
        return;
    }

    DoScheduleLoad();
    LoadScheduled_ = true;
}

void TScalarAttributeBase::ScheduleLoadTimestamp() const
{
    ScheduleLoad();
}

TTimestamp TScalarAttributeBase::LoadTimestamp() const
{
    switch (Owner_->GetState()) {
        case EObjectState::Instantiated:
        case EObjectState::Removing:
        case EObjectState::Removed:
            OnLoad();
            return *Timestamp_;

        case EObjectState::Creating:
        case EObjectState::Created:
        case EObjectState::CreatedRemoving:
        case EObjectState::CreatedRemoved: {
            return NullTimestamp;
        }

        default:
            YT_ABORT();
    }
}

void TScalarAttributeBase::ScheduleStore()
{
    if (StoreScheduled_) {
        return;
    }

    DoScheduleStore();
    StoreScheduled_ = true;
}

void TScalarAttributeBase::OnLoad() const
{
    ScheduleLoad();

    if (LoadScheduled_) {
        Owner_->GetSession()->FlushLoads();
    }

    if (!Owner_->DidExist()) {
        THROW_ERROR_EXCEPTION(
            NClient::NApi::EErrorCode::NoSuchObject,
            "%v %Qv does not exist",
            GetCapitalizedHumanReadableTypeName(Owner_->GetType()),
            Owner_->GetId());
    }
}

void TScalarAttributeBase::OnStore()
{
    auto ownerState = Owner_->GetState();
    YT_VERIFY(ownerState != EObjectState::Removed && ownerState != EObjectState::CreatedRemoved);

    Owner_->ValidateExists();

    ScheduleStore();
}

void TScalarAttributeBase::LoadFromDB(ILoadContext* context)
{
    if (!LoadScheduled_) {
        return;
    }

    auto* typeHandler = Owner_->GetTypeHandler();

    // NB: CaptureCompositeObjectKey will be retrieving object's parent and be raising
    // an exception in case the object does not exist. Let's prevent this from happening
    // by pre-checking object's existence.
    if (typeHandler->GetParentType() != EObjectType::Null && !Owner_->DidExist()) {
        Missing_ = true;
        return;
    }

    auto key = CaptureCompositeObjectKey(Owner_, context->GetRowBuffer());
    const auto* table = typeHandler->GetTable();
    context->ScheduleLookup(
        table,
        key,
        MakeArray(Schema_->Field),
        [=] (const std::optional<TRange<TVersionedValue>>& optionalValues) {
            if (optionalValues) {
                YT_ASSERT(optionalValues->Size() == 1);
                try {
                    const auto& value = (*optionalValues)[0];
                    Timestamp_ = value.Timestamp;
                    LoadOldValue(value, context);
                } catch (const std::exception& ex) {
                    THROW_ERROR_EXCEPTION("Error loading value of [%v.%v] for %v %v",
                        table->Name,
                        Schema_->Field->Name,
                        GetHumanReadableTypeName(Owner_->GetType()),
                        GetObjectDisplayName(Owner_))
                        << ex;
                }
            } else {
                Missing_ = true;
            }
        });
}

void TScalarAttributeBase::StoreToDB(IStoreContext* context)
{
    auto ownerState = Owner_->GetState();
    if (ownerState == EObjectState::Removed || ownerState == EObjectState::CreatedRemoved) {
        return;
    }

    auto key = CaptureCompositeObjectKey(Owner_, context->GetRowBuffer());

    auto* typeHandler = Owner_->GetTypeHandler();
    const auto* table = typeHandler->GetTable();

    TUnversionedValue value;
    try {
        StoreNewValue(&value, context);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error storing value of [%v.%v] for %v %v",
            table->Name,
            Schema_->Field->Name,
            GetHumanReadableTypeName(Owner_->GetType()),
            GetObjectDisplayName(Owner_))
            << ex;
    }

    context->WriteRow(
        table,
        key,
        MakeArray(Schema_->Field),
        MakeArray(context->GetRowBuffer()->Capture(value)));
}

void TScalarAttributeBase::OnObjectCreated()
{
    SetDefaultValues();
    Loaded_ = true;
    OnStore();
}

////////////////////////////////////////////////////////////////////////////////

TTimestampAttribute::TTimestampAttribute(TObject* owner, const TTimestampAttributeSchema* schema)
    : TScalarAttributeBase(owner, schema)
{ }

TTimestamp TTimestampAttribute::Load() const
{
    OnLoad();
    return Timestamp_;
}

TTimestampAttribute::operator TTimestamp() const
{
    return Load();
}

void TTimestampAttribute::Touch()
{
    auto ownerState = Owner_->GetState();
    YT_VERIFY(ownerState != EObjectState::Removed && ownerState != EObjectState::CreatedRemoved);

    Owner_->ValidateExists();

    ScheduleStore();
}

void TTimestampAttribute::SetDefaultValues()
{
    Timestamp_ = NullTimestamp;
}

void TTimestampAttribute::LoadOldValue(const TVersionedValue& value, ILoadContext* /*context*/)
{
    Timestamp_ = value.Timestamp;
}

void TTimestampAttribute::StoreNewValue(NTableClient::TUnversionedValue* dbValue, IStoreContext* /*context*/)
{
    *dbValue = MakeUnversionedSentinelValue(EValueType::Null);
}

////////////////////////////////////////////////////////////////////////////////

TOneToManyAttributeBase::TOneToManyAttributeBase(
    TObject* owner,
    const TOneToManyAttributeSchemaBase* schema)
    : TAttributeBase(owner)
    , Schema_(schema)
{ }

void TOneToManyAttributeBase::ScheduleLoad() const
{
    DoScheduleLoad();
}

const THashSet<TObject*>& TOneToManyAttributeBase::UntypedLoad() const
{
    ScheduleLoad();
    Owner_->GetSession()->FlushLoads();
    return *ForeignObjects_;
}

void TOneToManyAttributeBase::DoAdd(TObject* many)
{
    AddedForeignObjects_.insert(many);
    RemovedForeignObjects_.erase(many);
    if (ForeignObjects_) {
        ForeignObjects_->insert(many);
    }
    DoScheduleStore();
}

void TOneToManyAttributeBase::DoRemove(TObject* many)
{
    RemovedForeignObjects_.insert(many);
    AddedForeignObjects_.erase(many);
    if (ForeignObjects_) {
        ForeignObjects_->erase(many);
    }
    DoScheduleStore();
}

void TOneToManyAttributeBase::LoadFromDB(ILoadContext* context)
{
    if (ForeignObjects_) {
        return;
    }

    context->ScheduleSelect(
        Format("%v from %v where %v = %v",
            FormatId(Schema_->ForeignKeyField->Name),
            FormatId(context->GetTablePath(Schema_->Table)),
            FormatId(Schema_->PrimaryKeyField->Name),
            FormatLiteralValue(Owner_->GetId())),
        [=] (const IUnversionedRowsetPtr& rowset) {
            YT_ASSERT(!ForeignObjects_);
            auto rows = rowset->GetRows();
            ForeignObjects_.emplace();
            ForeignObjects_->reserve(rows.Size());
            for (auto row : rows) {
                YT_ASSERT(row.GetCount() == 1);
                auto foreignId = FromUnversionedValue<TObjectId>(row[0]);
                auto* foreignObject = Owner_->GetSession()->GetObject(GetForeignObjectType(), foreignId);
                ForeignObjects_->insert(foreignObject);
            }
            for (auto* object : AddedForeignObjects_) {
                ForeignObjects_->insert(object);
            }
            for (auto* object : RemovedForeignObjects_) {
                ForeignObjects_->erase(object);
            }
        });
}

void TOneToManyAttributeBase::StoreToDB(IStoreContext* context)
{
    const auto& rowBuffer = context->GetRowBuffer();

    for (auto* object : AddedForeignObjects_) {
        context->WriteRow(
            Schema_->Table,
            ToUnversionedValues(
                rowBuffer,
                Owner_->GetId(),
                object->GetId()),
            MakeArray(&DummyField),
            MakeArray(MakeUnversionedSentinelValue(EValueType::Null)));
    }

    for (auto* object : RemovedForeignObjects_) {
        context->DeleteRow(
            Schema_->Table,
            ToUnversionedValues(
                rowBuffer,
                Owner_->GetId(),
                object->GetId()));
    }
}

////////////////////////////////////////////////////////////////////////////////

TAnnotationsAttribute::TAnnotationsAttribute(TObject* owner)
    : TAttributeBase(owner)
{ }

void TAnnotationsAttribute::ScheduleLoad(const TString& key) const
{
    if (KeyToValue_.find(key) != KeyToValue_.end()) {
        return;
    }

    ScheduledLoadKeys_.insert(key);
    DoScheduleLoad();
}

TYsonString TAnnotationsAttribute::Load(const TString& key) const
{
    ScheduleLoad(key);
    Owner_->GetSession()->FlushLoads();
    auto it = KeyToValue_.find(key);
    YT_ASSERT(it != KeyToValue_.end());
    return it->second ? it->second : TYsonString();
}

void TAnnotationsAttribute::ScheduleLoadTimestamp(const TString& key) const
{
    if (KeyToTimestamp_.find(key) != KeyToTimestamp_.end()) {
        return;
    }

    ScheduledLoadKeys_.insert(key);
    DoScheduleLoad();
}

TTimestamp TAnnotationsAttribute::LoadTimestamp(const TString& key) const
{
    ScheduleLoad(key);
    Owner_->GetSession()->FlushLoads();
    auto it = KeyToTimestamp_.find(key);
    YT_ASSERT(it != KeyToTimestamp_.end());
    return it->second;
}

void TAnnotationsAttribute::ScheduleLoadAll() const
{
    if (ScheduledLoadAll_ || LoadedAll_) {
        return;
    }

    ScheduledLoadAll_ = true;
    DoScheduleLoad();
}

std::vector<std::pair<TString, NYT::NYson::TYsonString>> TAnnotationsAttribute::LoadAll() const
{
    ScheduleLoadAll();
    Owner_->GetSession()->FlushLoads();
    std::vector<std::pair<TString, NYT::NYson::TYsonString>> result;
    result.reserve(KeyToValue_.size());
    for (const auto& [key, value] : AllKeysToValue_) {
        auto it = KeyToValue_.find(key);
        if (it == KeyToValue_.end()) {
            result.emplace_back(key, value);
        } else if (it->second) {
            result.emplace_back(key, it->second);
        } else {
            // Key was deleted.
        }
    }
    return result;
}

void TAnnotationsAttribute::Store(const TString& key, const TYsonString& value)
{
    auto ownerState = Owner_->GetState();
    YT_VERIFY(ownerState != EObjectState::Removed && ownerState != EObjectState::CreatedRemoved);

    Owner_->ValidateExists();

    KeyToValue_[key] = value;
    ScheduledStoreKeys_.insert(key);
    DoScheduleStore();
}

bool TAnnotationsAttribute::IsStoreScheduled(const TString& key) const
{
    return ScheduledStoreKeys_.contains(key);
}

void TAnnotationsAttribute::LoadFromDB(ILoadContext* context)
{
    const auto& rowBuffer = context->GetRowBuffer();

    auto ownerState = Owner_->GetState();
    if (ownerState == EObjectState::Removed || ownerState == EObjectState::Removing) {
        auto primaryKey = ToUnversionedValue(Owner_->GetId(), rowBuffer);
        YT_ASSERT(primaryKey.Type == EValueType::String);
        TString primaryKeyString(primaryKey.Data.String, primaryKey.Length);

        context->ScheduleSelect(
            Format("%v from %v where %v = %v and %v = %v",
                FormatId(AnnotationsTable.Fields.Name.Name),
                FormatId(context->GetTablePath(&AnnotationsTable)),
                FormatId(AnnotationsTable.Fields.ObjectId.Name),
                FormatLiteralValue(primaryKeyString),
                FormatId(AnnotationsTable.Fields.ObjectType.Name),
                static_cast<int>(Owner_->GetType())),
            [=] (const IUnversionedRowsetPtr& rowset) {
                auto rows = rowset->GetRows();

                KeyToValue_.clear();
                KeyToValue_.reserve(rows.Size());

                ScheduledStoreKeys_.clear();
                ScheduledStoreKeys_.reserve(rows.Size());

                for (auto row : rows) {
                    YT_ASSERT(row.GetCount() == 1);
                    auto key = FromUnversionedValue<TString>(row[0]);
                    YT_VERIFY(KeyToValue_.emplace(key, TYsonString()).second);
                    YT_VERIFY(ScheduledStoreKeys_.emplace(key).second);
                }
            });
    } else {
        for (const auto& attributeKey : ScheduledLoadKeys_) {
            context->ScheduleLookup(
                &AnnotationsTable,
                ToUnversionedValues(
                    rowBuffer,
                    Owner_->GetId(),
                    Owner_->GetType(),
                    attributeKey),
                MakeArray(&AnnotationsTable.Fields.Value),
                [=] (const std::optional<TRange<TVersionedValue>>& optionalValues) {
                    if (optionalValues) {
                        YT_ASSERT(optionalValues->Size() == 1);
                        const auto& value = (*optionalValues)[0];
                        KeyToValue_[attributeKey] = FromUnversionedValue<TYsonString>(value);
                        KeyToTimestamp_[attributeKey] = value.Timestamp;
                    } else {
                        KeyToValue_[attributeKey] = TYsonString();
                        KeyToTimestamp_[attributeKey] = NullTimestamp;
                    }
                    YT_VERIFY(ScheduledLoadKeys_.erase(attributeKey) == 1);
                });
        }

        if (ScheduledLoadAll_) {
            auto primaryKey = ToUnversionedValue(Owner_->GetId(), rowBuffer);
            YT_ASSERT(primaryKey.Type == EValueType::String);
            TString primaryKeyString(primaryKey.Data.String, primaryKey.Length);

            context->ScheduleSelect(
                Format("%v, %v from %v where %v = %v and %v = %v",
                    FormatId(AnnotationsTable.Fields.Name.Name),
                    FormatId(AnnotationsTable.Fields.Value.Name),
                    FormatId(context->GetTablePath(&AnnotationsTable)),
                    FormatId(AnnotationsTable.Fields.ObjectId.Name),
                    FormatLiteralValue(primaryKeyString),
                    FormatId(AnnotationsTable.Fields.ObjectType.Name),
                    static_cast<int>(Owner_->GetType())),
                [=] (const IUnversionedRowsetPtr& rowset) {
                    auto rows = rowset->GetRows();
                    for (auto row : rows) {
                        YT_ASSERT(row.GetCount() == 2);
                        auto key = FromUnversionedValue<TString>(row[0]);
                        auto value = FromUnversionedValue<TYsonString>(row[1]);
                        YT_VERIFY(AllKeysToValue_.emplace(std::move(key), std::move(value)).second);
                    }
                });
        }
    }
}

void TAnnotationsAttribute::StoreToDB(IStoreContext* context)
{
    for (const auto& key : ScheduledStoreKeys_) {
        auto it = KeyToValue_.find(key);
        YT_ASSERT(it != KeyToValue_.end());
        const auto& value = it->second;

        auto keyValues = ToUnversionedValues(
            context->GetRowBuffer(),
            Owner_->GetId(),
            Owner_->GetType(),
            key);

        if (value) {
            context->WriteRow(
                &AnnotationsTable,
                keyValues,
                MakeArray(&AnnotationsTable.Fields.Value),
                ToUnversionedValues(
                    context->GetRowBuffer(),
                    value));
        } else {
            context->DeleteRow(
                &AnnotationsTable,
                keyValues);
        }
    }
}

void TAnnotationsAttribute::OnObjectCreated()
{
    LoadedAll_ = true;
}

void TAnnotationsAttribute::OnObjectRemoved()
{
    DoScheduleLoad();
    DoScheduleStore();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects


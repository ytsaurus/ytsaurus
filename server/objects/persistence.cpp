#include "persistence.h"
#include "object.h"
#include "type_handler.h"
#include "db_schema.h"
#include "private.h"
#include "helpers.h"

#include <yt/client/table_client/row_buffer.h>

#include <yt/client/api/rowset.h>

#include <yt/ytlib/query_client/ast.h>

#include <yt/core/misc/collection_helpers.h>

namespace NYP {
namespace NServer {
namespace NObjects {

using namespace NYT::NTableClient;
using namespace NYT::NApi;
using namespace NYT::NQueryClient::NAst;
using namespace NYT::NYson;

////////////////////////////////////////////////////////////////////////////////

static const TDBField DummyField{"dummy"};

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
        });
}

bool TObjectExistenceChecker::Check() const
{
    if (!Checked_) {
        ScheduleCheck();
        Object_->GetSession()->FlushLoads();
    }
    Y_ASSERT(Checked_);
    return Exists_;
}

void TObjectExistenceChecker::LoadFromDB(ILoadContext* context)
{
    Y_ASSERT(!Checked_);

    auto lookupHandler = [&] (const TNullable<TRange<NYT::NTableClient::TVersionedValue>>& maybeValues) {
        Y_ASSERT(!Checked_);
        Checked_ = true;
        auto* typeHandler = Object_->GetTypeHandler();
        if (typeHandler->GetParentType() == EObjectType::Null) {
            Exists_ = maybeValues.HasValue() && (*maybeValues)[1].Type == EValueType::Null;
        } else {
            Exists_ = maybeValues.HasValue();
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
    Y_UNREACHABLE();
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
    Y_ASSERT(Checked_);
    return Tombstone_;
}

void TObjectTombstoneChecker::LoadFromDB(ILoadContext* /* context */)
{
    Y_ASSERT(!Checked_);

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
                [=] (const TNullable<TRange<TVersionedValue>>& maybeValues) {
                    Y_ASSERT(!Checked_);
                    Checked_ = true;
                    Tombstone_ = maybeValues.HasValue();
                });
        });
}

void TObjectTombstoneChecker::StoreToDB(IStoreContext* /*context*/)
{
    Y_UNREACHABLE();
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
        Y_ASSERT(ParentId_ || Missing_);
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
    Y_ASSERT(!ParentId_);
    Y_ASSERT(!Missing_);

    context->ScheduleLookup(
        &ParentsTable,
        ToUnversionedValues(
            context->GetRowBuffer(),
            Owner_->GetId(),
            Owner_->GetType()),
        MakeArray(&ParentsTable.Fields.ParentId),
        [=] (const TNullable<TRange<TVersionedValue>>& maybeValues) {
            if (maybeValues) {
                Y_ASSERT(maybeValues->Size() == 1);
                try {
                    FromUnversionedValue(&ParentId_, (*maybeValues)[0]);
                    LOG_DEBUG("Object parent resolved (ObjectId: %v, ObjectType: %v, ParentId: %v)",
                        Owner_->GetId(),
                        Owner_->GetType(),
                        ParentId_);
                } catch (const std::exception& ex) {
                    THROW_ERROR_EXCEPTION("Error loading parent id value for %v %Qv",
                        GetLowercaseHumanReadableTypeName(Owner_->GetType()),
                        Owner_->GetId())
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
            Y_ASSERT(!Children_);
            auto rows = rowset->GetRows();
            Children_.Emplace();
            Children_->reserve(rows.Size());
            for (auto row : rows) {
                Y_ASSERT(row.GetCount() == 1);
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
    YCHECK(ownerState != EObjectState::Removed && ownerState != EObjectState::CreatedRemoved);

    Owner_->ValidateExists();

    ScheduleStore();
}

void TScalarAttributeBase::LoadFromDB(ILoadContext* context)
{
    if (!LoadScheduled_) {
        return;
    }

    auto key = CaptureCompositeObjectKey(Owner_, context->GetRowBuffer());

    auto* typeHandler = Owner_->GetTypeHandler();
    const auto* table = typeHandler->GetTable();

    context->ScheduleLookup(
        table,
        key,
        MakeArray(Schema_->Field),
        [=] (const TNullable<TRange<TVersionedValue>>& maybeValues) {
            if (maybeValues) {
                Y_ASSERT(maybeValues->Size() == 1);
                try {
                    LoadOldValue((*maybeValues)[0], context);
                } catch (const std::exception& ex) {
                    THROW_ERROR_EXCEPTION("Error loading value of [%v.%v] for %v %Qv",
                        table->Name,
                        Schema_->Field->Name,
                        GetLowercaseHumanReadableTypeName(Owner_->GetType()),
                        Owner_->GetId())
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
        THROW_ERROR_EXCEPTION("Error storing value of [%v.%v] for %v %Qv",
            table->Name,
            Schema_->Field->Name,
            GetLowercaseHumanReadableTypeName(Owner_->GetType()),
            Owner_->GetId())
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
    YCHECK(ownerState != EObjectState::Removed && ownerState != EObjectState::CreatedRemoved);

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
            Y_ASSERT(!ForeignObjects_);
            auto rows = rowset->GetRows();
            ForeignObjects_.Emplace();
            ForeignObjects_->reserve(rows.Size());
            for (auto row : rows) {
                Y_ASSERT(row.GetCount() == 1);
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
    if (KeyToValue_.find(key) != KeyToValue_.end() || ScheduledLoadAll_ || LoadedAll_) {
        return;
    }

    ScheduledLoadKeys_.insert(key);
    DoScheduleLoad();
}

TNullable<TYsonString> TAnnotationsAttribute::Load(const TString& key) const
{
    ScheduleLoad(key);
    Owner_->GetSession()->FlushLoads();
    auto it = KeyToValue_.find(key);
    Y_ASSERT(it != KeyToValue_.end());
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
    for (const auto& pair : KeyToValue_) {
        if (pair.second) {
            result.emplace_back(pair.first, *pair.second);
        }
    }
    return result;
}

void TAnnotationsAttribute::Store(const TString& key, const TNullable<TYsonString>& value)
{
    auto ownerState = Owner_->GetState();
    YCHECK(ownerState != EObjectState::Removed && ownerState != EObjectState::CreatedRemoved);

    Owner_->ValidateExists();

    KeyToValue_[key] = value;
    ScheduledStoreKeys_.insert(key);
    DoScheduleStore();
}

void TAnnotationsAttribute::LoadFromDB(ILoadContext* context)
{
    const auto& rowBuffer = context->GetRowBuffer();

    auto ownerState = Owner_->GetState();
    if (ownerState == EObjectState::Removed || ownerState == EObjectState::Removing) {
        auto primaryKey = ToUnversionedValue(Owner_->GetId(), rowBuffer);
        Y_ASSERT(primaryKey.Type == EValueType::String);
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
                    Y_ASSERT(row.GetCount() == 1);
                    auto key = FromUnversionedValue<TString>(row[0]);
                    YCHECK(KeyToValue_.emplace(key, Null).second);
                    YCHECK(ScheduledStoreKeys_.emplace(key).second);
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
                [=] (const TNullable<TRange<TVersionedValue>>& maybeValues) {
                    if (maybeValues) {
                        Y_ASSERT(maybeValues->Size() == 1);
                        const auto& value = (*maybeValues)[0];
                        KeyToValue_[attributeKey] = FromUnversionedValue<TYsonString>(value);
                    } else {
                        KeyToValue_[attributeKey] = Null;
                    }
                    YCHECK(ScheduledLoadKeys_.erase(attributeKey) == 1);
                });
        }

        if (ScheduledLoadAll_) {
            auto primaryKey = ToUnversionedValue(Owner_->GetId(), rowBuffer);
            Y_ASSERT(primaryKey.Type == EValueType::String);
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
                        Y_ASSERT(row.GetCount() == 2);
                        auto key = FromUnversionedValue<TString>(row[0]);
                        auto value = FromUnversionedValue<TYsonString>(row[1]);
                        KeyToValue_.emplace(std::move(key), std::move(value));
                    }
                });
        }
    }
}

void TAnnotationsAttribute::StoreToDB(IStoreContext* context)
{
    for (const auto& attributeKey : ScheduledStoreKeys_) {
        auto it = KeyToValue_.find(attributeKey);
        Y_ASSERT(it != KeyToValue_.end());
        const auto& maybeAttributeValue = it->second;

        auto keyValues = ToUnversionedValues(
            context->GetRowBuffer(),
            Owner_->GetId(),
            Owner_->GetType(),
            attributeKey);

        if (maybeAttributeValue) {
            context->WriteRow(
                &AnnotationsTable,
                keyValues,
                MakeArray(&AnnotationsTable.Fields.Value),
                ToUnversionedValues(
                    context->GetRowBuffer(),
                    *maybeAttributeValue));
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

} // namespace NObjects
} // namespace NServer
} // namespace NYP


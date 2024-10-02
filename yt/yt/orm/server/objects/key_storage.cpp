#include "key_storage.h"

#include "db_schema.h"
#include "helpers.h"
#include "key_util.h"
#include "object.h"
#include "object_manager.h"
#include "private.h"
#include "session.h"
#include "type_handler.h"

#include <library/cpp/iterator/enumerate.h>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

TKeyAttributeDescriptors GetKeyAttributeDescriptors(const TSingleKeyStorageDescriptor& descriptor)
{
    TKeyAttributeDescriptors result;

    Visit(descriptor,
        [&] (const TColumnarKeyStorageDescriptor& descriptor) {
            result.reserve(descriptor.KeyLocators.size());
            for (const auto& locator : descriptor.KeyLocators) {
                result.push_back(locator.KeyAttributeDescriptor);
            }
        },
        [&] (const TProtoKeyStorageDescriptor& descriptor) {
            YT_VERIFY(descriptor.KeyLocators.size() == 1);
            result.reserve(1);
            result.push_back(descriptor.KeyLocators[0].KeyAttributeDescriptor);
        });

    return result;
}

TKeyAttributeDescriptors GetKeyAttributeDescriptors(const TMultiKeyStorageDescriptor& descriptor)
{
    TKeyAttributeDescriptors result;

    Visit(descriptor,
        [&] (const TColumnarKeyStorageDescriptor& descriptor) {
            result.reserve(descriptor.KeyLocators.size());
            for (const auto& locator : descriptor.KeyLocators) {
                result.push_back(locator.KeyAttributeDescriptor);
            }
        },
        [&] (const TProtoKeyStorageDescriptor& descriptor) {
            result.reserve(descriptor.KeyLocators.size());
            for (const auto& locator : descriptor.KeyLocators) {
                result.push_back(locator.KeyAttributeDescriptor);
            }
        },
        [&] (const TTabularKeyStorageDescriptor& descriptor) {
            Y_UNUSED(descriptor);
        });

    return result;
}

void SetNullKey(const TSingleKeyStorageDescriptor& descriptor, TObjectKey nullKey)
{
    Visit(descriptor,
        [&] (const TColumnarKeyStorageDescriptor& descriptor) {
            descriptor.NullKey = std::move(nullKey);
        },
        [&] (const TProtoKeyStorageDescriptor& descriptor) {
            Y_UNUSED(descriptor);
        });
}

////////////////////////////////////////////////////////////////////////////////

TColumnarSingleKeyStorageDriver::TColumnarSingleKeyStorageDriver(
    TObject* owner,
    const TSingleKeyStorageDescriptor& descriptor)
    : Owner_(owner)
    , Descriptor_(std::get<TDescriptor>(descriptor))
{ }

void TColumnarSingleKeyStorageDriver::ScheduleLoad(bool /*forPartialStore*/)
{
    for (const auto& locator : Descriptor_.KeyLocators) {
        const auto* attribute = locator.KeyAttributeDescriptor->AttributeBaseGetter(Owner_);
        attribute->ScheduleLoad();
    }
}

bool TColumnarSingleKeyStorageDriver::IsStoreScheduled() const
{
    for (const auto& locator : Descriptor_.KeyLocators) {
        const auto* attribute = locator.KeyAttributeDescriptor->AttributeBaseGetter(Owner_);
        if (attribute->IsStoreScheduled()) {
            return true;
        }
    }
    return false;
}

bool TColumnarSingleKeyStorageDriver::IsChanged() const
{
    for (const auto& locator : Descriptor_.KeyLocators) {
        const auto* attribute = locator.KeyAttributeDescriptor->AttributeBaseGetter(Owner_);
        if (attribute->IsChanged(locator.Suffix)) {
            return true;
        }
    }
    return false;
}

void TColumnarSingleKeyStorageDriver::Lock(NTableClient::ELockType lockType)
{
    for (const auto& locator : Descriptor_.KeyLocators) {
        locator.KeyAttributeDescriptor->AttributeBaseGetter(Owner_)->Lock(lockType);
    }
}

bool TColumnarSingleKeyStorageDriver::ReadOnly() const
{
    return false;
}

TObjectKey TColumnarSingleKeyStorageDriver::Load(bool old)
{
    TObjectKey::TKeyFields keyFields;
    keyFields.reserve(Descriptor_.KeyLocators.size());
    for (const auto& locator : Descriptor_.KeyLocators) {
        const auto* attribute = locator.KeyAttributeDescriptor->AttributeBaseGetter(Owner_);
        keyFields.push_back(old
            ? attribute->LoadAsKeyFieldOld(locator.Suffix)
            : attribute->LoadAsKeyField(locator.Suffix));
    }

    TObjectKey result(std::move(keyFields));
    if (result == Descriptor_.NullKey) {
        result = {};
    }
    return result;
}

void TColumnarSingleKeyStorageDriver::Store(TObjectKey key)
{
    if (!key) {
        THROW_ERROR_EXCEPTION_UNLESS(Descriptor_.NullKey,
            "The attribute is not nullable");
        key = Descriptor_.NullKey;
    }
    for (const auto& [i, locator] : Enumerate(Descriptor_.KeyLocators)) {
        auto* attribute = locator.KeyAttributeDescriptor->AttributeBaseGetter(Owner_);
        attribute->StoreKeyField(key[i], locator.Suffix);
    }
}

////////////////////////////////////////////////////////////////////////////////

TProtoSingleKeyStorageDriver::TProtoSingleKeyStorageDriver(
    TObject* owner,
    const TSingleKeyStorageDescriptor& descriptor)
    : Owner_(owner)
    , Descriptor_(std::get<TDescriptor>(descriptor))
{
    YT_VERIFY(Descriptor_.KeyLocators.size() == 1);
    for (const auto& suffix : GetKeySuffixes()) {
        THROW_ERROR_EXCEPTION_IF(suffix.Contains("*"),
            "Suffix may not contain repeated fields; got %Qv",
            suffix);
    }
}

void TProtoSingleKeyStorageDriver::ScheduleLoad(bool /*forPartialStore*/)
{
    GetKeyAttribute()->ScheduleLoad();
}

bool TProtoSingleKeyStorageDriver::IsStoreScheduled() const
{
    return GetKeyAttribute()->IsStoreScheduled();
}

bool TProtoSingleKeyStorageDriver::IsChanged() const
{
    auto* attribute = GetKeyAttribute();
    for (const auto& suffix : GetKeySuffixes()) {
        if (attribute->IsChanged(suffix)) {
            return true;
        }
    }
    return false;
}

void TProtoSingleKeyStorageDriver::Lock(NTableClient::ELockType lockType)
{
    GetKeyAttribute()->Lock(lockType);
}

bool TProtoSingleKeyStorageDriver::ReadOnly() const
{
    return false;
}

TObjectKey TProtoSingleKeyStorageDriver::Load(bool old)
{
    auto* attribute = GetKeyAttribute();
    const auto protos = attribute->LoadAsProtos(old);
    THROW_ERROR_EXCEPTION_UNLESS(protos.has_value(), "The attribute does not contain protobuffers");
    THROW_ERROR_EXCEPTION_UNLESS(protos.value().size() == 1, "The attribute is not singular");

    return ParseObjectKey(protos->front(), GetKeySuffixes());
}

void TProtoSingleKeyStorageDriver::Store(TObjectKey key)
{
    auto* attribute = GetKeyAttribute();
    auto protos = attribute->MutableLoadAsProtos();
    THROW_ERROR_EXCEPTION_UNLESS(protos.has_value(), "The attribute does not contain protobuffers");
    THROW_ERROR_EXCEPTION_UNLESS(protos.value().size() == 1, "The attribute is not singular");
    StoreObjectKey(protos->front(), GetKeySuffixes(), key);
}

TScalarAttributeBase* TProtoSingleKeyStorageDriver::GetKeyAttribute() const
{
    return Descriptor_.KeyLocators[0].KeyAttributeDescriptor->AttributeBaseGetter(Owner_);
}

const std::vector<TString>& TProtoSingleKeyStorageDriver::GetKeySuffixes() const
{
    return Descriptor_.KeyLocators[0].Suffixes;
}

////////////////////////////////////////////////////////////////////////////////

TColumnarMultiKeyStorageDriver::TColumnarMultiKeyStorageDriver(
    TObject* owner,
    const TMultiKeyStorageDescriptor& descriptor)
    : Owner_(owner)
    , Descriptor_(std::get<TDescriptor>(descriptor))
{ }

void TColumnarMultiKeyStorageDriver::ScheduleLoad(bool /*forPartialStore*/)
{
    for (auto& locator : Descriptor_.KeyLocators) {
        auto* attribute = locator.KeyAttributeDescriptor->AttributeBaseGetter(Owner_);
        attribute->ScheduleLoad();
    }
}

bool TColumnarMultiKeyStorageDriver::IsStoreScheduled() const
{
    for (auto& locator : Descriptor_.KeyLocators) {
        auto* attribute = locator.KeyAttributeDescriptor->AttributeBaseGetter(Owner_);
        if (attribute->IsStoreScheduled()) {
            return true;
        }
    }
    return false;
}

bool TColumnarMultiKeyStorageDriver::IsChanged() const
{
    for (auto& locator : Descriptor_.KeyLocators) {
        auto* attribute = locator.KeyAttributeDescriptor->AttributeBaseGetter(Owner_);
        if (attribute->IsChanged(locator.Suffix)) {
            return true;
        }
    }
    return false;
}

void TColumnarMultiKeyStorageDriver::Lock(NTableClient::ELockType lockType)
{
    for (auto& locator : Descriptor_.KeyLocators) {
        locator.KeyAttributeDescriptor->AttributeBaseGetter(Owner_)->Lock(lockType);
    }
}

bool TColumnarMultiKeyStorageDriver::ReadOnly() const
{
    return false;
}

auto TColumnarMultiKeyStorageDriver::Load(bool old) -> TObjectKeys
{
    std::vector<TObjectKey::TKeyFields> keyFieldsVec;
    std::optional<size_t> numKeys;
    for (auto& locator : Descriptor_.KeyLocators) {
        auto* attribute = locator.KeyAttributeDescriptor->AttributeBaseGetter(Owner_);
        auto keyFieldsProjected = old
            ? attribute->LoadAsKeyFieldsOld(locator.Suffix)
            : attribute->LoadAsKeyFields(locator.Suffix);

        if (numKeys.has_value()) {
            THROW_ERROR_EXCEPTION_UNLESS(keyFieldsProjected.size() == *numKeys,
                "Inconsistent key field containers");
        } else {
            numKeys = keyFieldsProjected.size();
            keyFieldsVec.resize(*numKeys);
            for (auto& keyFields : keyFieldsVec) {
                keyFields.reserve(Descriptor_.KeyLocators.size());
            }
        }

        for (const auto& [i, fields] : Enumerate(keyFieldsVec)) {
            fields.push_back(std::move(keyFieldsProjected[i]));
        }
    }

    TObjectKeys result;
    result.reserve(*numKeys);
    for (auto& keyFields : keyFieldsVec) {
        result.push_back(TObjectKey(std::move(keyFields)));
    }

    return result;
}

void TColumnarMultiKeyStorageDriver::Store(TObjectKeys keys)
{
    for (const auto& key : keys) {
        THROW_ERROR_EXCEPTION_UNLESS(key.size() == Descriptor_.KeyLocators.size(),
            "Wrong key size");
    }

    for (const auto& [i, locator] : Enumerate(Descriptor_.KeyLocators)) {
        TObjectKey::TKeyFields keyFieldsProjected;
        keyFieldsProjected.reserve(keys.size());
        for (const auto& key : keys) {
            keyFieldsProjected.push_back(key[i]);
        }

        auto* attribute = locator.KeyAttributeDescriptor->AttributeBaseGetter(Owner_);
        attribute->StoreKeyFields(std::move(keyFieldsProjected), locator.Suffix);
    }
}

void TColumnarMultiKeyStorageDriver::Add(TObjectKey key)
{
    // TODO: cache the updates instead.
    auto keys = Load(/*old*/ false);
    auto it = std::find(keys.begin(), keys.end(), key);
    if (it != keys.end()) {
        return;
    }
    keys.push_back(std::move(key));
    Store(std::move(keys));
}

void TColumnarMultiKeyStorageDriver::Remove(TObjectKey key)
{
    auto keys = Load(/*old*/ false);
    auto it = std::find(keys.begin(), keys.end(), key);
    if (it == keys.end()) {
        return;
    }
    keys.erase(it);
    Store(std::move(keys));
}

////////////////////////////////////////////////////////////////////////////////

TProtoMultiKeyStorageDriver::TProtoMultiKeyStorageDriver(
    TObject* owner,
    const TMultiKeyStorageDescriptor& descriptor)
    : Owner_(owner)
    , Descriptor_(std::get<TDescriptor>(descriptor))
{ }

void TProtoMultiKeyStorageDriver::ScheduleLoad(bool /*forPartialStore*/)
{
    for (auto& locator : Descriptor_.KeyLocators) {
        auto* attribute = locator.KeyAttributeDescriptor->AttributeBaseGetter(Owner_);
        attribute->ScheduleLoad();
    }
}

bool TProtoMultiKeyStorageDriver::IsStoreScheduled() const
{
    for (auto& locator : Descriptor_.KeyLocators) {
        auto* attribute = locator.KeyAttributeDescriptor->AttributeBaseGetter(Owner_);
        if (attribute->IsStoreScheduled()) {
            return true;
        }
    }
    return false;
}

bool TProtoMultiKeyStorageDriver::IsChanged() const
{
    for (auto& locator : Descriptor_.KeyLocators) {
        auto* attribute = locator.KeyAttributeDescriptor->AttributeBaseGetter(Owner_);
        for (const auto& suffix : locator.Suffixes) {
            if (attribute->IsChanged(suffix)) {
                return true;
            }
        }
    }
    return false;
}

void TProtoMultiKeyStorageDriver::Lock(NTableClient::ELockType lockType)
{
    for (auto& locator : Descriptor_.KeyLocators) {
        locator.KeyAttributeDescriptor->AttributeBaseGetter(Owner_)->Lock(lockType);
    }
}

bool TProtoMultiKeyStorageDriver::ReadOnly() const
{
    return true;
}

auto TProtoMultiKeyStorageDriver::Load(bool old) -> TObjectKeys
{
    TObjectKeys result;

    for (auto& locator : Descriptor_.KeyLocators) {
        auto* attribute = locator.KeyAttributeDescriptor->AttributeBaseGetter(Owner_);
        const auto protos = attribute->LoadAsProtos(old);
        THROW_ERROR_EXCEPTION_UNLESS(protos.has_value(), "The attribute does not contain protobuffers");

        ParseObjectKeys(&protos.value(), locator.Suffixes, result);
    }

    return result;
}

void TProtoMultiKeyStorageDriver::Store(TObjectKeys keys)
{
    THROW_ERROR_EXCEPTION_UNLESS(keys.empty(),
        "Cannot store references into this object");
}

void TProtoMultiKeyStorageDriver::Add(TObjectKey /*key*/)
{
    THROW_ERROR_EXCEPTION("Cannot store references into this object");
}

void TProtoMultiKeyStorageDriver::Remove(TObjectKey /*key*/)
{
    THROW_ERROR_EXCEPTION("Cannot store references into this object");
}

////////////////////////////////////////////////////////////////////////////////

TTabularMultiKeyStorageDriver::TTabularMultiKeyStorageDriver(
    TObject* owner,
    const TMultiKeyStorageDescriptor& descriptor)
    : Owner_(owner)
    , Descriptor_(std::get<TDescriptor>(descriptor))
{ }

void TTabularMultiKeyStorageDriver::ScheduleLoad(bool forPartialStore)
{
    if (forPartialStore) {
        return;
    }

    if (LoadState_ != EOS_UNSCHEDULED) {
        return;
    }
    LoadState_ = EOS_SCHEDULED;

    Owner_->GetSession()->ScheduleLoad(
        [this] (ILoadContext* context) {
            LoadFromDB(context);
        });
}

bool TTabularMultiKeyStorageDriver::IsStoreScheduled() const
{
    return StoreState_ != EOS_UNSCHEDULED;
}

bool TTabularMultiKeyStorageDriver::IsChanged() const
{
    return !AddedObjectKeys_.empty() || !RemovedObjectKeys_.empty();
}

void TTabularMultiKeyStorageDriver::Lock(NTableClient::ELockType lockType)
{
    if (!PrepareLock(lockType)) {
        return;
    }

    Owner_->ScheduleStore();
    Owner_->GetSession()->ScheduleStore(
        [this] (IStoreContext* context) {
            LockInDB(context);
        });
}

bool TTabularMultiKeyStorageDriver::ReadOnly() const
{
    return false;
}

auto TTabularMultiKeyStorageDriver::Load(bool old) -> TObjectKeys
{
    FlushLoad();

    TObjectKeys result;
    result.reserve(OldObjectKeys_.size() + AddedObjectKeys_.size() - RemovedObjectKeys_.size());

    for (const auto& key : OldObjectKeys_) {
        if (old || !RemovedObjectKeys_.contains(key)) {
            result.push_back(key);
        }
    }
    if (!old) {
        for (const auto& key : AddedObjectKeys_) {
            result.push_back(key);
        }
    }

    std::sort(result.begin(), result.end()); // Avoid instabilities from hash sets.
    return result;
}

void TTabularMultiKeyStorageDriver::Store(TObjectKeys keys)
{
    FlushLoad();

    TObjectKeysHash oldKeys = OldObjectKeys_;
    TObjectKeysHash newKeys;

    for (auto& key : keys) {
        if (oldKeys.erase(key) == 0) {
            newKeys.insert(std::move(key));
            // Mentioning a key twice will cause a (safe) superfluous insert, but the refs code
            // should prevent that.
        }
    }

    AddedObjectKeys_ = std::move(newKeys);
    RemovedObjectKeys_ = std::move(oldKeys);

    ScheduleStore();
}

void TTabularMultiKeyStorageDriver::Add(TObjectKey key)
{
    AddedObjectKeys_.insert(key);
    RemovedObjectKeys_.erase(key);
    ScheduleStore();
}

void TTabularMultiKeyStorageDriver::Remove(TObjectKey key)
{
    RemovedObjectKeys_.insert(key);
    AddedObjectKeys_.erase(key);
    ScheduleStore();
}

void TTabularMultiKeyStorageDriver::FlushLoad()
{
    if (LoadState_ == EOS_COMPLETED) {
        return;
    }
    ScheduleLoad();
    Owner_->GetSession()->FlushLoads();
    YT_VERIFY(LoadState_ == EOS_COMPLETED);
}

void TTabularMultiKeyStorageDriver::ScheduleStore()
{
    if (StoreState_ == EOS_SCHEDULED) {
        return;
    }
    StoreState_ = EOS_SCHEDULED;

    Owner_->ScheduleStore();
    Owner_->GetSession()->ScheduleStore(
        [this] (IStoreContext* context) {
            StoreToDB(context);
        });
}

bool TTabularMultiKeyStorageDriver::PrepareLock(NTableClient::ELockType lockType)
{
    if (NTableClient::GetStrongestLock(LockType_, lockType) == LockType_) {
        return false; // Already locked or scheduled for this phase.
    } else {
        LockType_ = lockType;
    }

    if (LockState_ == EOS_SCHEDULED) {
        return false; // Already scheduled for this phase.
    }

    LockState_ = EOS_SCHEDULED; // Was unscheduled or completed with a weaker lock type.
    return true;
}

void TTabularMultiKeyStorageDriver::LoadFromDB(ILoadContext* context)
{
    if (LoadState_ == EOS_RUNNING || LoadState_ == EOS_COMPLETED) {
        return;
    }
    LoadState_ = EOS_RUNNING;

    auto queryBuilder = BuildSelectQuery(
        context->GetTablePath(Descriptor_.Table),
        Descriptor_.ForeignKeyFields,
        Owner_->GetKey(),
        Descriptor_.OwnerKeyFields);

    context->ScheduleSelect(
        queryBuilder.Flush(),
        [this] (const NApi::IUnversionedRowsetPtr& rowset) {
            YT_VERIFY(LoadState_ == EOS_RUNNING);
            YT_VERIFY(OldObjectKeys_.empty());

            const auto& rows = rowset->GetRows();
            OldObjectKeys_.reserve(rows.size());
            for (const auto& row : rows) {
                YT_VERIFY(row.GetCount() == Descriptor_.ForeignKeyFields.size());
                TObjectKey key;
                FromUnversionedRow(row, &key, static_cast<size_t>(row.GetCount()));
                OldObjectKeys_.insert(std::move(key));
            }

            LoadState_ = EOS_COMPLETED;
        });
}

void TTabularMultiKeyStorageDriver::StoreToDB(IStoreContext* context)
{
    YT_VERIFY(StoreState_ == EOS_SCHEDULED);
    StoreState_ = EOS_RUNNING;

    if ((!AddedObjectKeys_.empty() || !RemovedObjectKeys_.empty()) && Owner_->DoesExist()) {
        if (PrepareLock(NTableClient::ELockType::SharedStrong)) {
            LockInDB(context); // Lock directly instead of creating an extra phase.
        }
    }

    auto ownerKey = Owner_->GetKey();

    for (const auto& key : RemovedObjectKeys_) {
        context->DeleteRow(Descriptor_.Table, ownerKey + key);
    }

    if (!Owner_->DoesExist() || Owner_->IsFinalized()) {
        YT_VERIFY(AddedObjectKeys_.empty());
    }
    for (const auto& key : AddedObjectKeys_) {
        context->WriteRow(
            Descriptor_.Table,
            ownerKey + key,
            std::array{&DummyField},
            std::array{MakeUnversionedSentinelValue(NQueryClient::EValueType::Null)});
    }

    StoreState_ = EOS_COMPLETED;
}

void TTabularMultiKeyStorageDriver::LockInDB(IStoreContext* context)
{
    YT_VERIFY(LockState_ == EOS_SCHEDULED);
    LockState_ = EOS_RUNNING;

    if (Owner_->IsRemoved()) {
        return;
    }
    if (LockType_ == NTableClient::ELockType::None) {
        return;
    }

    auto* typeHandler = Owner_->GetTypeHandler();
    context->LockRow(
        typeHandler->GetTable(),
        typeHandler->GetObjectTableKey(Owner_),
        std::array{&ObjectsTable.Fields.ExistenceLock},
        LockType_);

    LockState_ = EOS_COMPLETED;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects

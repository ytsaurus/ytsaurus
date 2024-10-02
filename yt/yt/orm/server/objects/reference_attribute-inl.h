#include <yt/yt/orm/server/objects/public.h>
#ifndef REFERENCE_ATTRIBUTE_INL_H_
#error "Direct inclusion of this file is not allowed, include reference_attribute.h"
// For the sake of sane code completion.
#include "reference_attribute.h"
#endif

#include <yt/yt/orm/client/objects/registry.h>

#include  <library/cpp/iterator/enumerate.h>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

template <class TOwner, class TForeign>
TReferenceAttribute<TOwner, TForeign>::TReferenceAttribute(TOwner* owner)
    : TReferenceAttributeBase(owner)
    , Owner_(owner)
{ }

template <class TOwner, class TForeign>
TObject* TReferenceAttribute<TOwner, TForeign>::DoGetOwner() const
{
    return TObjectPluginTraits<TObject>::Downcast(GetOwner());
}

////////////////////////////////////////////////////////////////////////////////

template <class TOwner, class TForeign>
TSingleReferenceAttribute<TOwner, TForeign>::TSingleReferenceAttribute(
    TOwner* owner,
    const TDescriptor& descriptor,
    std::unique_ptr<ISingleKeyStorageDriver> keyStorageDriver)
    : TReferenceAttribute<TOwner, TForeign>(owner)
    , Descriptor_(descriptor)
    , KeyStorageDriver_(std::move(keyStorageDriver))
{ }

template <class TOwner, class TForeign>
TObjectPlugin<TForeign>* TSingleReferenceAttribute<TOwner, TForeign>::Load() const
{
    this->BeforeLoad();
    return Cache_;
}

template <class TOwner, class TForeign>
TObjectPlugin<TForeign>* TSingleReferenceAttribute<TOwner, TForeign>::LoadOld() const
{
    return GetForeign(/*old*/ true);
}

template <class TOwner, class TForeign>
void TSingleReferenceAttribute<TOwner, TForeign>::Store(TObjectPlugin<TForeign>* foreign)
{
    if (foreign) {
        this->DoGetOwner()->ValidateNotFinalized();
        DowncastObject(foreign)->ValidateNotFinalized();
    }

    // DoStore is const for the sake of cache operations.
    // Store is nonconst for the sake of interface correctness.
    DoStore(foreign, /*skipStorage*/ false);
}

template <class TOwner, class TForeign>
const TReferenceAttributeSettings& TSingleReferenceAttribute<TOwner, TForeign>::Settings() const
{
    return Descriptor_.Settings;
}

template <class TOwner, class TForeign>
void TSingleReferenceAttribute<TOwner, TForeign>::CheckObjectRemoval() const
{
    auto* foreign = DowncastObject(Load());
    if (foreign == nullptr) {
        return;
    }

    auto allowRemoval = this->GetOwner()
        ->GetSession()
        ->GetOwner()
        ->RemovalWithNonEmptyReferencesAllowed()
        .value_or(Settings().AllowNonEmptyRemoval);

    THROW_ERROR_EXCEPTION_UNLESS(allowRemoval,
        "Cannot remove %v since it holds a reference to %v",
        this->GetOwner()->GetDisplayName(),
        foreign->GetDisplayName());
}

template <class TOwner, class TForeign>
void TSingleReferenceAttribute<TOwner, TForeign>::OnObjectRemovalStart()
{
    if (Load() != nullptr) {
        Store(nullptr);
    }
}

template <class TOwner, class TForeign>
void TSingleReferenceAttribute<TOwner, TForeign>::LoadCache() const
{
    YT_VERIFY(Cache_ == nullptr);
    Cache_ = GetForeign(/*old*/ false);
}

template <class TOwner, class TForeign>
void TSingleReferenceAttribute<TOwner, TForeign>::PreloadForeignKeyUpdate() const
{
    auto* foreign = DowncastObject(GetForeign(/*old*/ false));
    if (foreign == nullptr) {
        return;
    }
    Descriptor_.InverseAttributeGetter(foreign)->ScheduleLoad(/*forPartialStore*/ true);
}

template <class TOwner, class TForeign>
void TSingleReferenceAttribute<TOwner, TForeign>::ApplyKeyChanges() const
{
    auto foreign = GetForeign(/*old*/ false);
    DoStore(foreign, /*skipStorage*/ true);
}

template <class TOwner, class TForeign>
ISingleKeyStorageDriver& TSingleReferenceAttribute<TOwner, TForeign>::GetKeyStorageDriver() const
{
    return *KeyStorageDriver_;
}

template <class TOwner, class TForeign>
void TSingleReferenceAttribute<TOwner, TForeign>::Attach(TObjectPlugin<TForeign>* foreign)
{
    YT_VERIFY(foreign);

    if (this->CacheIsBusy()) {
        return;
    }

    auto current = Load();
    if (foreign == current) {
        return;
    }

    Store(foreign);
}

template <class TOwner, class TForeign>
void TSingleReferenceAttribute<TOwner, TForeign>::Detach(TObjectPlugin<TForeign>* foreign)
{
    YT_VERIFY(foreign);

    if (this->CacheIsBusy()) {
        return;
    }

    auto current = Load();
    THROW_ERROR_EXCEPTION_UNLESS(foreign == current,
        "Trying to detach %v from %v but %v is attached",
        UpcastObject(foreign)->GetDisplayName(),
        this->GetOwner()->GetDisplayName(),
        current ? current->GetDisplayName() : "no object");

    Store(nullptr);
}

template <class TOwner, class TForeign>
void TSingleReferenceAttribute<TOwner, TForeign>::DoStore(TObjectPlugin<TForeign>* foreign, bool skipStorage) const
{
    this->BeforeStore();

    if (foreign != Cache_) {
        if (!skipStorage) {
            KeyStorageDriver_->Store(this->GetForeignStorageKey(foreign));
        }
        if (Cache_) {
            Descriptor_.InverseAttributeGetter(UpcastObject(Cache_))->Detach(this->GetOwner());
        }
        if (foreign) {
            Descriptor_.InverseAttributeGetter(UpcastObject(foreign))->Attach(this->GetOwner());
        }
        Cache_ = foreign;
    }

    this->AfterStore();
}

template <class TOwner, class TForeign>
TObjectPlugin<TForeign>* TSingleReferenceAttribute<TOwner, TForeign>::GetForeign(bool old) const
{
    auto storageKey = KeyStorageDriver_->Load(old);
    if (!storageKey) {
        return nullptr;
    }
    auto [objectKey, parentKey] =
        this->SplitForeignStorageKey(std::move(storageKey), TForeign::Type);
    auto result = this->GetOwner()
        ->GetSession()
        ->GetObject(TForeign::Type, std::move(objectKey), std::move(parentKey))
        ->template As<TForeign>();
    return DowncastObject(result);
}

////////////////////////////////////////////////////////////////////////////////

template <class TOwner, class TForeign>
TMultiReferenceAttribute<TOwner, TForeign>::TMultiReferenceAttribute(
    TOwner* owner,
    const TDescriptor& descriptor,
    std::unique_ptr<IMultiKeyStorageDriver> keyStorageDriver)
    : TReferenceAttribute<TOwner, TForeign>(owner)
    , Descriptor_(descriptor)
    , KeyStorageDriver_(std::move(keyStorageDriver))
{ }

template <class TOwner, class TForeign>
auto TMultiReferenceAttribute<TOwner, TForeign>::Load() const -> TForeigns
{
    this->BeforeLoad();
    return Cache_;
}

template <class TOwner, class TForeign>
auto TMultiReferenceAttribute<TOwner, TForeign>::LoadOld() const -> TForeigns
{
    return GetForeigns(/*old*/ true);
}

template <class TOwner, class TForeign>
bool TMultiReferenceAttribute<TOwner, TForeign>::Contains(TObjectPlugin<TForeign>* foreign) const
{
    this->BeforeLoad();
    return CacheIndex_.contains(foreign);
}

template <class TOwner, class TForeign>
bool TMultiReferenceAttribute<TOwner, TForeign>::IsEmpty() const
{
    this->BeforeLoad();
    return Cache_.empty();
}

template <class TOwner, class TForeign>
void TMultiReferenceAttribute<TOwner, TForeign>::Store(TForeigns foreigns)
{
    if (!foreigns.empty()) {
        this->GetOwner()->ValidateNotFinalized();
    }
    for (const auto& foreign : foreigns) {
        foreign->ValidateNotFinalized();
    }

    // DoStore is const for the sake of cache operations.
    // Store is nonconst for the sake of interface correctness.
    DoStore(std::move(foreigns), /*skipStorage*/ false);
}

template <class TOwner, class TForeign>
void TMultiReferenceAttribute<TOwner, TForeign>::Add(TObjectPlugin<TForeign>* foreign)
{
    if (!foreign) {
        return;
    }
    this->GetOwner()->ValidateNotFinalized();
    foreign->ValidateNotFinalized();

    this->BeforePartialStore();

    if (this->CacheIsActive()) {
        // Not calling Contains because it calls BeforeLoad and the cache is already busy.
        if (CacheIndex_.contains(foreign)) {
            return;
        }
        Cache_.push_back(foreign);
        CacheIndex_[foreign] = Cache_.size() - 1;
    }

    KeyStorageDriver_->Add(this->GetForeignStorageKey(foreign));

    Descriptor_.InverseAttributeGetter(foreign)->Attach(this->GetOwner());

    this->AfterStore();
}

template <class TOwner, class TForeign>
void TMultiReferenceAttribute<TOwner, TForeign>::Remove(TObjectPlugin<TForeign>* foreign)
{
    if (!foreign) {
        return;
    }

    this->BeforePartialStore();

    if (this->CacheIsActive()) {
        auto it = CacheIndex_.find(foreign);
        if (it == CacheIndex_.end()) {
            return;
        }
        size_t pos = it->second;
        YT_VERIFY(Cache_[pos] == foreign);

        Cache_.erase(Cache_.begin() + pos);
        CacheIndex_.erase(foreign);
        for (auto it : CacheIndex_) {
            YT_VERIFY(it.second != pos);
            if (it.second > pos) {
                CacheIndex_[it.first] = it.second - 1;
            }
        }
    }

    KeyStorageDriver_->Remove(this->GetForeignStorageKey(foreign));

    Descriptor_.InverseAttributeGetter(UpcastObject(foreign))->Detach(this->GetOwner());

    this->AfterStore();
}

template <class TOwner, class TForeign>
const TReferenceAttributeSettings& TMultiReferenceAttribute<TOwner, TForeign>::Settings() const
{
    return Descriptor_.Settings;
}

template <class TOwner, class TForeign>
void TMultiReferenceAttribute<TOwner, TForeign>::CheckObjectRemoval() const
{
    if (IsEmpty()) {
        return;
    }

    auto allowRemoval = this->GetOwner()
        ->GetSession()
        ->GetOwner()
        ->RemovalWithNonEmptyReferencesAllowed()
        .value_or(Settings().AllowNonEmptyRemoval);

    THROW_ERROR_EXCEPTION_UNLESS(allowRemoval,
        "Cannot remove %v since it holds %v %v reference(s)",
        this->GetOwner()->GetDisplayName(),
        Cache_.size(),
        NClient::NObjects::GetGlobalObjectTypeRegistry()
            ->GetHumanReadableTypeNameOrCrash(TForeign::Type));
}

template <class TOwner, class TForeign>
void TMultiReferenceAttribute<TOwner, TForeign>::OnObjectRemovalStart()
{
    if (!IsEmpty()) {
        Store({});
    }
}

template <class TOwner, class TForeign>
void TMultiReferenceAttribute<TOwner, TForeign>::LoadCache() const
{
    YT_VERIFY(Cache_.empty());
    Cache_ = GetForeigns(/*old*/ false);
    CacheIndex_ = BuildIndexAndDeduplicate(Cache_);
}

template <class TOwner, class TForeign>
void TMultiReferenceAttribute<TOwner, TForeign>::PreloadForeignKeyUpdate() const
{
    auto foreigns = GetForeigns(/*old*/ false);
    for (auto* foreign : foreigns) {
        Descriptor_.InverseAttributeGetter(foreign)->ScheduleLoad(/*forPartialStore*/ true);
    }
}

template <class TOwner, class TForeign>
void TMultiReferenceAttribute<TOwner, TForeign>::ApplyKeyChanges() const
{
    auto foreigns = GetForeigns(/*old*/ false);
    DoStore(std::move(foreigns), /*skipStorage*/ true);
}

template <class TOwner, class TForeign>
IMultiKeyStorageDriver& TMultiReferenceAttribute<TOwner, TForeign>::GetKeyStorageDriver() const
{
    return *KeyStorageDriver_;
}

template <class TOwner, class TForeign>
void TMultiReferenceAttribute<TOwner, TForeign>::Attach(TObjectPlugin<TForeign>* foreign)
{
    YT_VERIFY(foreign);

    if (this->CacheIsBusy()) {
        return;
    }

    Add(foreign);
}

template <class TOwner, class TForeign>
void TMultiReferenceAttribute<TOwner, TForeign>::Detach(TObjectPlugin<TForeign>* foreign)
{
    YT_VERIFY(foreign);

    if (this->CacheIsBusy()) {
        return;
    }

    Remove(foreign);
}

template <class TOwner, class TForeign>
void TMultiReferenceAttribute<TOwner, TForeign>::DoStore(TForeigns foreigns, bool skipStorage) const
{
    this->BeforeStore();

    bool duplicateDetected = false;
    auto foreignsIndex = BuildIndexAndDeduplicate(foreigns, &duplicateDetected);

    if (duplicateDetected) {
        THROW_ERROR_EXCEPTION_IF(KeyStorageDriver_->ReadOnly(),
            "Duplicate key added but the key storage is read-only");
        skipStorage = false;
    }
    if (!skipStorage) {
        TObjectKeys storageKeys;
        storageKeys.reserve(foreigns.size());
        for (auto foreign : foreigns) {
            storageKeys.push_back(this->GetForeignStorageKey(foreign));
        }
        KeyStorageDriver_->Store(std::move(storageKeys));
    }

    for (const auto& [foreign, pos] : CacheIndex_) {
        if (!foreignsIndex.contains(foreign)) {
            Descriptor_.InverseAttributeGetter(foreign)->Detach(this->GetOwner());
        }
    }

    for (const auto& [foreign, pos] : foreignsIndex) {
        if (!CacheIndex_.contains(foreign)) {
            Descriptor_.InverseAttributeGetter(foreign)->Attach(this->GetOwner());
        }
    }

    Cache_ = std::move(foreigns);
    CacheIndex_ = std::move(foreignsIndex);

    this->AfterStore();
}

template <class TOwner, class TForeign>
auto TMultiReferenceAttribute<TOwner, TForeign>::GetForeigns(bool old) const -> TForeigns
{
    auto storageKeys = KeyStorageDriver_->Load(old);

    TForeigns result;
    result.reserve(storageKeys.size());

    for (auto& storageKey : storageKeys) {
        auto [objectKey, parentKey] = this->SplitForeignStorageKey(std::move(storageKey), TForeign::Type);
        result.push_back(this->GetOwner()
            ->GetSession()
            ->GetObject(TForeign::Type, std::move(objectKey), std::move(parentKey))
            ->template As<TForeign>());
        Descriptor_.InverseAttributeGetter(result.back())->ScheduleLoad();
    }

    return result;
}

template <class TOwner, class TForeign>
auto TMultiReferenceAttribute<TOwner, TForeign>::BuildIndexAndDeduplicate(
    TForeigns& foreigns,
    bool* duplicateDetected) -> TForeignsIndex
{
    TForeignsIndex result;
    int i = 0;
    std::erase_if(foreigns, [&result, &i, &duplicateDetected] (auto* foreign) {
        bool inserted = result.insert({foreign, i}).second;
        if (duplicateDetected && !inserted) {
            *duplicateDetected = true;
        }
        i += inserted; // The standard says: true converts to 1.
        return !inserted; // Not unique -> erase.
    });

    // Sanity check.
    THROW_ERROR_EXCEPTION_UNLESS(result.size() == foreigns.size(),
        "Failed to deduplicate foreign references");

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects

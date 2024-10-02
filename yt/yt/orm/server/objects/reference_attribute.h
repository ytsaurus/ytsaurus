#pragma once

#include "public.h"

#include "key_storage.h"
#include "persistence.h"
#include <library/cpp/yt/misc/property.h>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

struct TReferenceAttributeSettings
{
    bool AllowNonEmptyRemoval = true;
    bool StoreParentKey = false;
    // TODO: nullable, cascading deletion
};

class TReferenceAttributeBase
    : public IPersistentAttribute
    , private TNonCopyable
{
public:
    TReferenceAttributeBase(TObject* owner);
    virtual ~TReferenceAttributeBase() = default;

    TObject* GetOwner() const;

    void ScheduleLoad(bool forPartialStore = false) const;

    bool IsStoreScheduled() const;
    bool IsChanged() const;

    void Lock(NTableClient::ELockType lockType);

    virtual const TReferenceAttributeSettings& Settings() const = 0;

    // IObjectLifecycleObserver
    void OnObjectInitialization() override;

    void PreloadObjectRemoval() const override;
    void OnObjectRemovalFinish() override;

    void BeforeKeyStore() const;
    void BeforeKeyMutableLoad() const;

    bool CacheIsActive() const;
    bool CacheIsBusy() const;

protected:
    void BeforeLoad() const;
    void BeforeStore() const;
    void BeforePartialStore() const;
    void AfterStore() const;
    void Reconcile() const;
    void ScheduleReconcileOnCommit() const;

    TObjectKey GetForeignStorageKey(TObject* foreign) const;
    std::pair<TObjectKey, TObjectKey> SplitForeignStorageKey(
        TObjectKey key,
        TObjectTypeValue foreignType) const;

    virtual TObject* DoGetOwner() const = 0;

    virtual void LoadCache() const = 0;
    virtual void PreloadForeignKeyUpdate() const = 0;
    virtual void ApplyKeyChanges() const = 0;

    virtual IKeyStorageDriver& GetKeyStorageDriver() const = 0;

private:
    enum ECacheState
    {
        // The cache has not been loaded yet.
        ECS_UNLOADED = 0,
        // The cache holds the current value.
        ECS_CURRENT = 1,
        // The cache holds the previous value and a key field has been modified.
        ECS_DIRTY = 2,
        // Somebody called MutableLoad on a key field so the cache is always invalid.
        ECS_TAINTED = 3,
    };
    mutable ECacheState CacheState_ = ECS_UNLOADED;

    // A store operation is in progress.
    mutable bool CacheIsBusy_ = false;

    enum EOperationState {
        EOS_UNSCHEDULED = 0,
        EOS_SCHEDULED = 1,
        EOS_RUNNING = 2,
        EOS_COMPLETED = 3,
    };
    mutable EOperationState ReconcileState_ = EOS_UNSCHEDULED;
};

////////////////////////////////////////////////////////////////////////////////

template <class TOwner, class TForeign>
class TReferenceAttribute
    : public TReferenceAttributeBase
{
public:
    TReferenceAttribute(TOwner* owner);

    DEFINE_BYVAL_RO_PROPERTY_NO_INIT(TObjectPlugin<TOwner>*, Owner);

protected:
    friend class TSingleReferenceAttribute<TForeign, TOwner>;
    friend class TMultiReferenceAttribute<TForeign, TOwner>;

    virtual TObject* DoGetOwner() const override;

    // Call appropriate Store/Add/Remove methods if the cache is not busy.
    virtual void Attach(TObjectPlugin<TForeign>* foreign) = 0;
    virtual void Detach(TObjectPlugin<TForeign>* foreign) = 0;
};

//////////////////////////////////////////////////////////////////////////////

template <class TOwner, class TForeign>
struct TSingleReferenceDescriptor
{
    using TForwardAttributeGetter =
        std::function<TSingleReferenceAttribute<TOwner, TForeign>*(TOwner*)>;
    using TInverseAttributeGetter =
        std::function<TReferenceAttribute<TForeign, TOwner>*(TForeign*)>;

    TForwardAttributeGetter ForwardAttributeGetter;
    TInverseAttributeGetter InverseAttributeGetter;
    TSingleKeyStorageDescriptor KeyStorageDescriptor;
    TReferenceAttributeSettings Settings;
};

template <class TOwner, class TForeign>
class TSingleReferenceAttribute final
    : public TReferenceAttribute<TOwner, TForeign>
{
public:
    using TDescriptor = TSingleReferenceDescriptor<TOwner, TForeign>;

    TSingleReferenceAttribute(
        TOwner* owner,
        const TDescriptor& descriptor,
        std::unique_ptr<ISingleKeyStorageDriver> keyStorageDriver);

    TObjectPlugin<TForeign>* Load() const;
    TObjectPlugin<TForeign>* LoadOld() const;

    void Store(TObjectPlugin<TForeign>* value);

    const TReferenceAttributeSettings& Settings() const override;

    // IObjectLifecycleObserver
    void CheckObjectRemoval() const override;
    void OnObjectRemovalStart() override;

protected:
    void LoadCache() const override;
    void PreloadForeignKeyUpdate() const override;
    void ApplyKeyChanges() const override;

    ISingleKeyStorageDriver& GetKeyStorageDriver() const override;

    void Attach(TObjectPlugin<TForeign>* foreign) override;
    void Detach(TObjectPlugin<TForeign>* foreign) override;

private:
    const TDescriptor& Descriptor_;
    const std::unique_ptr<ISingleKeyStorageDriver> KeyStorageDriver_;
    mutable TObjectPlugin<TForeign>* Cache_ = nullptr;

    void DoStore(TObjectPlugin<TForeign>* value, bool skipStorage) const;
    TObjectPlugin<TForeign>* GetForeign(bool old) const;
};

////////////////////////////////////////////////////////////////////////////////

template <class TOwner, class TForeign>
struct TMultiReferenceDescriptor
{
    using TForwardAttributeGetter =
        std::function<TMultiReferenceAttribute<TOwner, TForeign>*(TOwner*)>;
    using TInverseAttributeGetter =
        std::function<TReferenceAttribute<TForeign, TOwner>*(TForeign*)>;

    TForwardAttributeGetter ForwardAttributeGetter;
    TInverseAttributeGetter InverseAttributeGetter;
    TMultiKeyStorageDescriptor KeyStorageDescriptor;
    TReferenceAttributeSettings Settings;
};

template <class TOwner, class TForeign>
class TMultiReferenceAttribute final
    : public TReferenceAttribute<TOwner, TForeign>
{
public:
    using TDescriptor = TMultiReferenceDescriptor<TOwner, TForeign>;
    using TForeigns = std::vector<TObjectPlugin<TForeign>*>;

    TMultiReferenceAttribute(
        TOwner* owner,
        const TDescriptor& descriptor,
        std::unique_ptr<IMultiKeyStorageDriver> keyStorageDriver);

    // We preserve the ordering of foreigns if the underlying storage (e.g., repeated fields) allows
    // it. If the storage (e.g., an index table) imposes its own order, it will manifest in the next
    // transaction.
    TForeigns Load() const;
    TForeigns LoadOld() const;

    bool Contains(TObjectPlugin<TForeign>* foreign) const;
    bool IsEmpty() const;

    void Store(TForeigns foreigns);
    void Add(TObjectPlugin<TForeign>* foreign);
    void Remove(TObjectPlugin<TForeign>* foreign);

    const TReferenceAttributeSettings& Settings() const override;

    // IObjectLifecycleObserver
    void CheckObjectRemoval() const override;
    void OnObjectRemovalStart() override;

protected:
    void LoadCache() const override;
    void PreloadForeignKeyUpdate() const override;
    void ApplyKeyChanges() const override;

    IMultiKeyStorageDriver& GetKeyStorageDriver() const override;

    void Attach(TObjectPlugin<TForeign>* foreign) override;
    void Detach(TObjectPlugin<TForeign>* foreign) override;

private:
    using TObjectKeys = IMultiKeyStorageDriver::TObjectKeys;
    using TForeignsIndex = THashMap<TObjectPlugin<TForeign>*, size_t>;

    const TDescriptor& Descriptor_;
    const std::unique_ptr<IMultiKeyStorageDriver> KeyStorageDriver_;
    mutable TForeigns Cache_;
    mutable TForeignsIndex CacheIndex_;

    void DoStore(TForeigns foreigns, bool skipStorage) const;
    TForeigns GetForeigns(bool old) const;
    static TForeignsIndex BuildIndexAndDeduplicate(
        TForeigns& foreigns,
        bool* duplicateDetected = nullptr);
};

////////////////////////////////////////////////////////////////////////////////

template <class TOwner, class TForeign>
struct TSingleViewDescriptor
{
    using TViewAttributeGetter = std::function<TViewAttributeBase*(TOwner*)>;

    const TSingleReferenceDescriptor<TOwner, TForeign>& ReferenceDescriptor;
    TViewAttributeGetter ViewAttributeGetter;
};

////////////////////////////////////////////////////////////////////////////////

template <class TOwner, class TForeign>
struct TMultiViewDescriptor
{
    using TViewAttributeGetter = std::function<TViewAttributeBase*(TOwner*)>;

    const TMultiReferenceDescriptor<TOwner, TForeign>& ReferenceDescriptor;
    TViewAttributeGetter ViewAttributeGetter;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects

#define REFERENCE_ATTRIBUTE_INL_H_
#include "reference_attribute-inl.h"
#undef REFERENCE_ATTRIBUTE_INL_H_

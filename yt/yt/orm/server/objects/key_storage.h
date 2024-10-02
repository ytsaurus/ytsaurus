#pragma once

#include "public.h"

#include "persistence.h"

#include <yt/yt/orm/client/objects/key.h>

#include <util/string/split.h>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

struct TColumnarKeyStorageDescriptor
{
    struct TKeyLocator
    {
        const TScalarAttributeDescriptorBase* KeyAttributeDescriptor;
        TString Suffix;
    };

    std::vector<TKeyLocator> KeyLocators;
    mutable TObjectKey NullKey;
};

struct TProtoKeyStorageDescriptor
{
    struct TKeyLocator
    {
        const TScalarAttributeDescriptorBase* KeyAttributeDescriptor;
        std::vector<TString> Suffixes;
    };

    std::vector<TKeyLocator> KeyLocators;
};

struct TTabularKeyStorageDescriptor
{
    const TDBTable* Table;
    const TDBFields OwnerKeyFields;
    const TDBFields ForeignKeyFields;
};

using TSingleKeyStorageDescriptor = std::variant<
    TColumnarKeyStorageDescriptor,
    TProtoKeyStorageDescriptor>;

using TMultiKeyStorageDescriptor = std::variant<
    TColumnarKeyStorageDescriptor,
    TProtoKeyStorageDescriptor,
    TTabularKeyStorageDescriptor>;

using TKeyAttributeDescriptors = TCompactVector<const TScalarAttributeDescriptorBase*, 4>;

TKeyAttributeDescriptors GetKeyAttributeDescriptors(const TSingleKeyStorageDescriptor& descriptor);
TKeyAttributeDescriptors GetKeyAttributeDescriptors(const TMultiKeyStorageDescriptor& descriptor);

void SetNullKey(const TSingleKeyStorageDescriptor& descriptor, TObjectKey nullKey);

////////////////////////////////////////////////////////////////////////////////

struct IKeyStorageDriver
{
    virtual ~IKeyStorageDriver() = default;

    virtual void ScheduleLoad(bool forPartialStore = false) = 0;
    virtual bool IsStoreScheduled() const = 0;
    virtual bool IsChanged() const = 0;
    virtual void Lock(NTableClient::ELockType lockType) = 0;
    virtual bool ReadOnly() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct ISingleKeyStorageDriver
    : public IKeyStorageDriver
{
    virtual TObjectKey Load(bool old) = 0;
    virtual void Store(TObjectKey key) = 0;
};

class TColumnarSingleKeyStorageDriver final
    : public ISingleKeyStorageDriver
{
public:
    using TDescriptor = TColumnarKeyStorageDescriptor;

    TColumnarSingleKeyStorageDriver(TObject* owner, const TSingleKeyStorageDescriptor& descriptor);

    void ScheduleLoad(bool forPartialStore = false) override;
    bool IsStoreScheduled() const override;
    bool IsChanged() const override;
    void Lock(NTableClient::ELockType lockType) override;
    bool ReadOnly() const override;

    TObjectKey Load(bool old) override;
    void Store(TObjectKey key) override;

private:
    TObject* Owner_;
    const TDescriptor& Descriptor_;
};

class TProtoSingleKeyStorageDriver final
    : public ISingleKeyStorageDriver
{
public:
    using TDescriptor = TProtoKeyStorageDescriptor;

    TProtoSingleKeyStorageDriver(TObject* owner, const TSingleKeyStorageDescriptor& descriptor);

    void ScheduleLoad(bool forPartialStore = false) override;
    bool IsStoreScheduled() const override;
    bool IsChanged() const override;
    void Lock(NTableClient::ELockType lockType) override;
    bool ReadOnly() const override;

    TObjectKey Load(bool old) override;
    void Store(TObjectKey key) override;

private:
    TObject* Owner_;
    const TDescriptor& Descriptor_;

    TScalarAttributeBase* GetKeyAttribute() const;
    const std::vector<TString>& GetKeySuffixes() const;
};

////////////////////////////////////////////////////////////////////////////////

struct IMultiKeyStorageDriver
    : public IKeyStorageDriver
{
    using TObjectKeys = std::vector<TObjectKey>;

    virtual TObjectKeys Load(bool old) = 0;
    virtual void Store(TObjectKeys keys) = 0;
    virtual void Add(TObjectKey key) = 0;
    virtual void Remove(TObjectKey key) = 0;
};

class TColumnarMultiKeyStorageDriver
    : public IMultiKeyStorageDriver
{
public:
    using TDescriptor = TColumnarKeyStorageDescriptor;

    TColumnarMultiKeyStorageDriver(TObject* owner, const TMultiKeyStorageDescriptor& descriptor);

    void ScheduleLoad(bool forPartialStore = false) override;
    bool IsStoreScheduled() const override;
    bool IsChanged() const override;
    void Lock(NTableClient::ELockType lockType) override;
    bool ReadOnly() const override;

    TObjectKeys Load(bool old) override;
    void Store(TObjectKeys keys) override;
    void Add(TObjectKey key) override;
    void Remove(TObjectKey key) override;

private:
    TObject* Owner_;
    const TDescriptor& Descriptor_;
};

class TProtoMultiKeyStorageDriver
    : public IMultiKeyStorageDriver
{
public:
    using TDescriptor = TProtoKeyStorageDescriptor;

    TProtoMultiKeyStorageDriver(TObject* owner, const TMultiKeyStorageDescriptor& descriptor);

    void ScheduleLoad(bool forPartialStore = false) override;
    bool IsStoreScheduled() const override;
    bool IsChanged() const override;
    void Lock(NTableClient::ELockType lockType) override;
    TObjectKeys Load(bool old) override;
    bool ReadOnly() const override;

    void Store(TObjectKeys keys) override;
    void Add(TObjectKey key) override;
    void Remove(TObjectKey key) override;

private:
    TObject* Owner_;
    const TDescriptor& Descriptor_;
};

class TTabularMultiKeyStorageDriver
    : public IMultiKeyStorageDriver
{
public:
    using TDescriptor = TTabularKeyStorageDescriptor;

    TTabularMultiKeyStorageDriver(TObject* owner, const TMultiKeyStorageDescriptor& descriptor);

    void ScheduleLoad(bool forPartialStore = false) override;
    bool IsStoreScheduled() const override;
    bool IsChanged() const override;
    void Lock(NTableClient::ELockType lockType) override;
    TObjectKeys Load(bool old) override;
    bool ReadOnly() const override;

    void Store(TObjectKeys keys) override;
    void Add(TObjectKey key) override;
    void Remove(TObjectKey key) override;

private:
    TObject* Owner_;
    const TDescriptor& Descriptor_;

    using TObjectKeysHash = THashSet<TObjectKey>;
    TObjectKeysHash OldObjectKeys_;
    TObjectKeysHash AddedObjectKeys_;
    TObjectKeysHash RemovedObjectKeys_;

    enum EOperationState {
        EOS_UNSCHEDULED = 0,
        EOS_SCHEDULED = 1,
        EOS_RUNNING = 2,
        EOS_COMPLETED = 3,
    };
    EOperationState LoadState_ = EOS_UNSCHEDULED;
    EOperationState StoreState_ = EOS_UNSCHEDULED;
    EOperationState LockState_ = EOS_UNSCHEDULED;
    NTableClient::ELockType LockType_ = NTableClient::ELockType::None;

    void FlushLoad();
    void ScheduleStore();
    // Returns true if LockInDB needs to happen.
    bool PrepareLock(NTableClient::ELockType lockType);

    void LoadFromDB(ILoadContext* context);
    void StoreToDB(IStoreContext* context);
    void LockInDB(IStoreContext* context);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects

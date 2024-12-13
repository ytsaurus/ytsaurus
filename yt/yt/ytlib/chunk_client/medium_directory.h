#pragma once

#include "public.h"
#include "config.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/ytlib/chunk_client/proto/medium_directory.pb.h>

#include <yt/yt/library/s3/public.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

// TODO(achulkov2): [PForReview] Separate medium descriptors into a separate file?

class TMediumDescriptor
    : public TRefCounted
{
public:
    DEFINE_BYVAL_RO_PROPERTY(TString, Name);
    DEFINE_BYVAL_RO_PROPERTY(int, Index, GenericMediumIndex);
    DEFINE_BYVAL_RO_PROPERTY(int, Priority, -1);
    //! Object id. Unused, for now.
    DEFINE_BYVAL_RO_PROPERTY(TGuid, Id, NObjectClient::NullObjectId);

public:
    TMediumDescriptor() = default;
    TMediumDescriptor(TString name, int index, int priority, TGuid id);

    //! Creates polymorphic descriptor from the protobuf according to the type-specific descriptor field.
    //! If no type-specific descriptor is set, a domestic medium instance is created.
    static TMediumDescriptorPtr CreateFromProto(const NProto::TMediumDirectory::TMediumDescriptor& protoItem);

    virtual bool IsDomestic() const = 0;
    bool IsOffshore() const;

    // TODO(achulkov2): [PForReview] Move to -inl.h file.
    template <class TDerived>
    TIntrusivePtr<TDerived> As() {
        return dynamic_cast<TDerived*>(this);
    }

    template <class TDerived>
    TIntrusivePtr<const TDerived> As() const {
        return dynamic_cast<const TDerived*>(this);
    }

    bool operator==(const TMediumDescriptor& other) const;

protected:
    //! Populates protobuf with the type-specific data of this descriptor.
    virtual void FillProto(NProto::TMediumDirectory::TMediumDescriptor* protoItem) const;
    virtual void LoadFrom(const NProto::TMediumDirectory::TMediumDescriptor& protoItem);

    virtual bool Equals(const TMediumDescriptor& other) const;
};

DEFINE_REFCOUNTED_TYPE(TMediumDescriptor)

////////////////////////////////////////////////////////////////////////////////

class TDomesticMediumDescriptor
    : public TMediumDescriptor
{
public:
    TDomesticMediumDescriptor() = default;
    TDomesticMediumDescriptor(TString name, int index, int priority, TGuid id);
    //! Creates medium with generic index, priority = -1 and null id.
    TDomesticMediumDescriptor(TString name);

private:
    void FillProto(NProto::TMediumDirectory::TMediumDescriptor* protoItem) const override;
    void LoadFrom(const NProto::TMediumDirectory::TMediumDescriptor& protoItem) override;
    //! The dynamic type of `other` must be convertible to TDomesticMediumDescriptor.
    bool Equals(const TMediumDescriptor& other) const override;

    bool IsDomestic() const override;
};

DEFINE_REFCOUNTED_TYPE(TDomesticMediumDescriptor)

////////////////////////////////////////////////////////////////////////////////

class TS3MediumDescriptor
    : public TMediumDescriptor
{
public:
    DEFINE_BYVAL_RO_PROPERTY(TS3MediumConfigPtr, Config);
    DEFINE_BYVAL_RO_PROPERTY(NS3::IClientPtr, Client);

public:
    TS3MediumDescriptor() = default;
    TS3MediumDescriptor(TString name, int index, int priority, TGuid id, TS3MediumConfigPtr config);

    struct TS3ObjectPlacement
    {
        TString Bucket;
        TString Key;
    };

    TS3ObjectPlacement GetChunkPlacement(TChunkId chunkId) const;
    TS3ObjectPlacement GetChunkMetaPlacement(TChunkId chunkId) const;

private:
    void FillProto(NProto::TMediumDirectory::TMediumDescriptor* protoItem) const override;
    void LoadFrom(const NProto::TMediumDirectory::TMediumDescriptor& protoItem) override;
    //! The dynamic type of `other` must be convertible to TS3MediumDescriptor.
    bool Equals(const TMediumDescriptor& other) const override;

    bool IsDomestic() const override;

    static NS3::IClientPtr CreateClient(const TS3MediumConfigPtr& config);
};

DEFINE_REFCOUNTED_TYPE(TS3MediumDescriptor)

////////////////////////////////////////////////////////////////////////////////

class TMediumDirectory
    : public TRefCounted
{
public:
    TMediumDescriptorPtr FindByIndex(int index) const;
    TMediumDescriptorPtr GetByIndexOrThrow(int index) const;

    TMediumDescriptorPtr FindByName(const TString& name) const;
    TMediumDescriptorPtr GetByNameOrThrow(const TString& name) const;

    std::vector<int> GetMediumIndexes() const;
    //! Returns the name of the medium with the given index, or "unknown" if the medium is not found.
    TString GetMediumName(int index) const;

    void LoadFrom(const NProto::TMediumDirectory& protoDirectory);

    void Clear();

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, SpinLock_);
    // TODO(achulkov2): [PLater] Once masters start providing medium ids, we should switch to using medium ids as keys.
    // This will be implemented in hand with supporting offshore media without medium indexes.
    using TDescriptorStorage = THashMap<int, TMediumDescriptorPtr>;
    TDescriptorStorage IndexToDescriptor_;
    THashMap<TString, TDescriptorStorage::const_iterator> NameToDescriptor_;
};

DEFINE_REFCOUNTED_TYPE(TMediumDirectory)

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TMediumDirectoryPtr& mediumDirectory, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient

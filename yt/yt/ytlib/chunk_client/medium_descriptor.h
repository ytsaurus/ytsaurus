#pragma once

#include "public.h"
#include "config.h"

#include <yt/ytlib/chunk_client/proto/medium_directory.pb.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

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

    template <class TDerived>
    TIntrusivePtr<TDerived> As();

    template <class TDerived>
    TIntrusivePtr<const TDerived> As() const;

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

    static NS3::IClientPtr CreateClient(const TS3MediumConfigPtr& mediumConfig);
};

DEFINE_REFCOUNTED_TYPE(TS3MediumDescriptor)

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TMediumDescriptorPtr& mediumDescriptor, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient

#define MEDIUM_DESCRIPTOR_INL_H_
#include "medium_descriptor-inl.h"
#undef MEDIUM_DESCRIPTOR_INL_H_
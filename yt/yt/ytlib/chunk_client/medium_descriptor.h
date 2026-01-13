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
    DEFINE_BYREF_RO_PROPERTY(std::string, Name);
    DEFINE_BYVAL_RO_PROPERTY(int, Index, GenericMediumIndex);
    DEFINE_BYVAL_RO_PROPERTY(int, Priority, -1);

public:
    TMediumDescriptor() = default;
    TMediumDescriptor(std::string name, int index, int priority);

    virtual bool IsDomestic() const = 0;
    bool IsOffshore() const;

    template <class TDerived>
    TIntrusivePtr<TDerived> As();

    template <class TDerived>
    TIntrusivePtr<const TDerived> As() const;

    bool operator==(const TMediumDescriptor& other) const;
};

DEFINE_REFCOUNTED_TYPE(TMediumDescriptor)

////////////////////////////////////////////////////////////////////////////////

class TDomesticMediumDescriptor
    : public TMediumDescriptor
{
public:
    TDomesticMediumDescriptor() = default;
    TDomesticMediumDescriptor(std::string name, int index, int priority);
    //! Creates medium with generic index, priority = -1 and null id.
    TDomesticMediumDescriptor(std::string name);

private:
    bool IsDomestic() const override;
};

DEFINE_REFCOUNTED_TYPE(TDomesticMediumDescriptor)

////////////////////////////////////////////////////////////////////////////////

class TS3MediumDescriptor
    : public TMediumDescriptor
{
public:
    DEFINE_BYVAL_RO_PROPERTY(TS3MediumConfigPtr, Config);

public:
    TS3MediumDescriptor() = default;
    TS3MediumDescriptor(std::string name, int index, int priority, TS3MediumConfigPtr config);

    struct TS3ObjectPlacement
    {
        std::string Bucket;
        std::string Key;
    };

    TS3ObjectPlacement GetChunkPlacement(TChunkId chunkId) const;
    TS3ObjectPlacement GetChunkMetaPlacement(TChunkId chunkId) const;

private:
    bool IsDomestic() const override;
};

DEFINE_REFCOUNTED_TYPE(TS3MediumDescriptor)

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TMediumDescriptorPtr& mediumDescriptor, NYson::IYsonConsumer* consumer);
void ToProto(NProto::TMediumDirectory::TMediumDescriptor* protoMediumDescriptor, const TMediumDescriptorPtr& mediumDescriptor);
void FromProto(TMediumDescriptorPtr* mediumDescriptor, const NProto::TMediumDirectory::TMediumDescriptor& protoMediumDescriptor);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient

#define MEDIUM_DESCRIPTOR_INL_H_
#include "medium_descriptor-inl.h"
#undef MEDIUM_DESCRIPTOR_INL_H_

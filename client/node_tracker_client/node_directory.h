#pragma once

#include "public.h"

#include <yt/client/chunk_client/chunk_replica.h>

#include <yt/core/concurrency/rw_spinlock.h>

#include <yt/core/yson/public.h>

#include <yt/core/misc/enum.h>
#include <yt/core/misc/nullable.h>
#include <yt/core/misc/property.h>

namespace NYT {
namespace NNodeTrackerClient {

////////////////////////////////////////////////////////////////////////////////

//! Network-related node information.
class TNodeDescriptor
{
public:
    TNodeDescriptor();
    explicit TNodeDescriptor(const TString& defaultAddress);
    explicit TNodeDescriptor(const TNullable<TString>& defaultAddress);
    explicit TNodeDescriptor(
        TAddressMap addresses,
        TNullable<TString> rack = Null,
        TNullable<TString> dc = Null,
        const std::vector<TString>& tags = {});

    bool IsNull() const;

    const TAddressMap& Addresses() const;

    const TString& GetDefaultAddress() const;

    const TString& GetAddressOrThrow(const TNetworkPreferenceList& networks) const;
    TNullable<TString> FindAddress(const TNetworkPreferenceList& networks) const;

    const TNullable<TString>& GetRack() const;
    const TNullable<TString>& GetDataCenter() const;

    const std::vector<TString>& GetTags() const;

    void Persist(const TStreamPersistenceContext& context);

private:
    TAddressMap Addresses_;
    TString DefaultAddress_;
    TNullable<TString> Rack_;
    TNullable<TString> DataCenter_;
    std::vector<TString> Tags_;
};

bool operator == (const TNodeDescriptor& lhs, const TNodeDescriptor& rhs);
bool operator != (const TNodeDescriptor& lhs, const TNodeDescriptor& rhs);

bool operator == (const TNodeDescriptor& lhs, const NProto::TNodeDescriptor& rhs);
bool operator != (const TNodeDescriptor& lhs, const NProto::TNodeDescriptor& rhs);

void FormatValue(TStringBuilder* builder, const TNodeDescriptor& descriptor, TStringBuf spec);
TString ToString(const TNodeDescriptor& descriptor);

// Accessors for some well-known addresses.
const TString& GetDefaultAddress(const TAddressMap& addresses);
const TString& GetDefaultAddress(const NProto::TAddressMap& addresses);

const TString& GetAddressOrThrow(const TAddressMap& addresses, const TNetworkPreferenceList& networks);
TNullable<TString> FindAddress(const TAddressMap& addresses, const TNetworkPreferenceList& networks);

const TAddressMap& GetAddressesOrThrow(const TNodeAddressMap& nodeAddresses, EAddressType type);

//! Please keep the items in this particular order: the further the better.
DEFINE_ENUM(EAddressLocality,
    (None)
    (SameDataCenter)
    (SameRack)
    (SameHost)
);

EAddressLocality ComputeAddressLocality(const TNodeDescriptor& first, const TNodeDescriptor& second);

namespace NProto {

void ToProto(NNodeTrackerClient::NProto::TAddressMap* protoAddresses, const NNodeTrackerClient::TAddressMap& addresses);
void FromProto(NNodeTrackerClient::TAddressMap* addresses, const NNodeTrackerClient::NProto::TAddressMap& protoAddresses);

void ToProto(NNodeTrackerClient::NProto::TNodeAddressMap* proto, const NNodeTrackerClient::TNodeAddressMap& nodeAddresses);
void FromProto(NNodeTrackerClient::TNodeAddressMap* nodeAddresses, const NNodeTrackerClient::NProto::TNodeAddressMap& proto);

void ToProto(NNodeTrackerClient::NProto::TNodeDescriptor* protoDescriptor, const NNodeTrackerClient::TNodeDescriptor& descriptor);
void FromProto(NNodeTrackerClient::TNodeDescriptor* descriptor, const NNodeTrackerClient::NProto::TNodeDescriptor& protoDescriptor);

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

//! Caches node descriptors obtained by fetch requests.
/*!
 *  \note
 *  Thread affinity: thread-safe
 */
class TNodeDirectory
    : public TIntrinsicRefCounted
{
public:
    void MergeFrom(const NProto::TNodeDirectory& source);
    void MergeFrom(const TNodeDirectoryPtr& source);
    void DumpTo(NProto::TNodeDirectory* destination);
    void Serialize(NYson::IYsonConsumer* consumer) const;

    void AddDescriptor(TNodeId id, const TNodeDescriptor& descriptor);

    const TNodeDescriptor* FindDescriptor(TNodeId id) const;
    const TNodeDescriptor& GetDescriptor(TNodeId id) const;
    const TNodeDescriptor& GetDescriptor(NChunkClient::TChunkReplica replica) const;
    std::vector<TNodeDescriptor> GetDescriptors(const NChunkClient::TChunkReplicaList& replicas) const;
    std::vector<TNodeDescriptor> GetAllDescriptors() const;

    const TNodeDescriptor* FindDescriptor(const TString& address);
    const TNodeDescriptor& GetDescriptor(const TString& address);

    void Save(TStreamSaveContext& context) const;
    void Load(TStreamLoadContext& context);

private:
    mutable NConcurrency::TReaderWriterSpinLock SpinLock_;
    THashMap<TNodeId, const TNodeDescriptor*> IdToDescriptor_;
    THashMap<TString, const TNodeDescriptor*> AddressToDescriptor_;
    std::vector<std::unique_ptr<TNodeDescriptor>> Descriptors_;

    void DoAddDescriptor(TNodeId id, const TNodeDescriptor& descriptor);
    void DoAddDescriptor(TNodeId id, const NProto::TNodeDescriptor& protoDescriptor);
    void DoAddCapturedDescriptor(TNodeId id, std::unique_ptr<TNodeDescriptor> descriptorHolder);

};

void Serialize(const TNodeDirectory& nodeDirectory, NYson::IYsonConsumer* consumer);

DEFINE_REFCOUNTED_TYPE(TNodeDirectory)

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeTrackerClient
} // namespace NYT

////////////////////////////////////////////////////////////////////////////////

template <>
struct THash<NYT::NNodeTrackerClient::TNodeDescriptor>
{
    size_t operator()(const NYT::NNodeTrackerClient::TNodeDescriptor& value) const;
};

////////////////////////////////////////////////////////////////////////////////

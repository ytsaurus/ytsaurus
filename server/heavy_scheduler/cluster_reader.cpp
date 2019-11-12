#include "cluster_reader.h"

#include "config.h"
#include "private.h"

#include <yp/server/lib/cluster/account.h>
#include <yp/server/lib/cluster/cluster_reader.h>
#include <yp/server/lib/cluster/internet_address.h>
#include <yp/server/lib/cluster/ip4_address_pool.h>
#include <yp/server/lib/cluster/node.h>
#include <yp/server/lib/cluster/node_segment.h>
#include <yp/server/lib/cluster/pod.h>
#include <yp/server/lib/cluster/pod_disruption_budget.h>
#include <yp/server/lib/cluster/pod_set.h>
#include <yp/server/lib/cluster/resource.h>

#include <yp/client/api/native/client.h>
#include <yp/client/api/native/helpers.h>
#include <yp/client/api/native/response.h>

#include <yt/core/concurrency/scheduler.h>
#include <yt/core/misc/protobuf_helpers.h>
#include <yt/core/yson/string.h>

#include <contrib/libs/protobuf/repeated_field.h>

namespace NYP::NServer::NHeavyScheduler {

using namespace NCluster;

using namespace NClient::NApi::NNative;

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

namespace {

template <class T>
auto VectorToProtoRepeated(const std::vector<T>& vector)
{
    return NYT::ToProto<::google::protobuf::RepeatedPtrField<T>>(vector);
}

////////////////////////////////////////////////////////////////////////////////

// NB! Gcc does not allow to define TSelectObjectTraits<> and TParseObjectTraits<>
//     template specializations directly inside TClusterReader body.

template <class TObject>
struct TSelectObjectTraits;

template <class TObject>
struct TParseObjectTraits;

template <>
struct TSelectObjectTraits<TIP4AddressPool>
{
    static const std::vector<NYPath::TYPath>& GetPaths()
    {
        static const std::vector<NYPath::TYPath> paths{
            "/meta/id",
            "/labels",
            "/spec",
            "/status"};
        return paths;
    }
};

template <>
struct TParseObjectTraits<TIP4AddressPool>
{
    static std::unique_ptr<TIP4AddressPool> Parse(const std::vector<TPayload>& payloads)
    {
        NObjects::TObjectId id;
        NYson::TYsonString labels;
        NClient::NApi::NProto::TIP4AddressPoolSpec spec;
        NClient::NApi::NProto::TIP4AddressPoolStatus status;

        ParsePayloads(
            payloads,
            &id,
            &labels,
            &spec,
            &status);

        return std::make_unique<TIP4AddressPool>(
            std::move(id),
            std::move(labels),
            std::move(spec),
            std::move(status));
    }
};

////////////////////////////////////////////////////////////////////////////////

template <>
struct TSelectObjectTraits<TInternetAddress>
{
    static const std::vector<NYPath::TYPath>& GetPaths()
    {
        static const std::vector<NYPath::TYPath> paths{
            "/meta/id",
            "/meta/ip4_address_pool_id",
            "/labels",
            "/spec",
            "/status"};
        return paths;
    }
};

template <>
struct TParseObjectTraits<TInternetAddress>
{
    static std::unique_ptr<TInternetAddress> Parse(const std::vector<TPayload>& payloads)
    {
        NObjects::TObjectId id;
        NObjects::TObjectId ip4AddressPoolId;
        NYson::TYsonString labels;
        NClient::NApi::NProto::TInternetAddressSpec spec;
        NClient::NApi::NProto::TInternetAddressStatus status;

        ParsePayloads(
            payloads,
            &id,
            &ip4AddressPoolId,
            &labels,
            &spec,
            &status);

        return std::make_unique<TInternetAddress>(
            std::move(id),
            std::move(ip4AddressPoolId),
            std::move(labels),
            std::move(spec),
            std::move(status));
    }
};

////////////////////////////////////////////////////////////////////////////////

template <>
struct TSelectObjectTraits<TNode>
{
    static const std::vector<NYPath::TYPath>& GetPaths()
    {
        static const std::vector<NYPath::TYPath> paths{
            "/meta/id",
            "/labels",
            "/status/hfsm/state",
            "/status/maintenance/state",
            "/status/unknown_pod_ids",
            "/spec"};
        return paths;
    }
};

template <>
struct TParseObjectTraits<TNode>
{
    static std::unique_ptr<TNode> Parse(const std::vector<TPayload>& payloads)
    {
        NObjects::TObjectId id;
        NYson::TYsonString labels;
        NObjects::EHfsmState hfsmState;
        NObjects::ENodeMaintenanceState nodeMaintenanceState;
        std::optional<std::vector<TString>> unknownPodIds;
        NClient::NApi::NProto::TNodeSpec spec;

        ParsePayloads(
            payloads,
            &id,
            &labels,
            &hfsmState,
            &nodeMaintenanceState,
            &unknownPodIds,
            &spec);

        return std::make_unique<TNode>(
            std::move(id),
            std::move(labels),
            hfsmState,
            nodeMaintenanceState,
            unknownPodIds && unknownPodIds->size() > 0,
            std::move(spec));
    }
};

////////////////////////////////////////////////////////////////////////////////

template <>
struct TSelectObjectTraits<TAccount>
{
    static const std::vector<NYPath::TYPath>& GetPaths()
    {
        static const std::vector<NYPath::TYPath> paths{
            "/meta/id",
            "/labels",
            "/spec/parent_id"};
        return paths;
    }
};

template <>
struct TParseObjectTraits<TAccount>
{
    static std::unique_ptr<TAccount> Parse(const std::vector<TPayload>& payloads)
    {
        NObjects::TObjectId id;
        NYson::TYsonString labels;
        NObjects::TObjectId parentId;

        ParsePayloads(
            payloads,
            &id,
            &labels,
            &parentId);

        return std::make_unique<TAccount>(
            std::move(id),
            std::move(labels),
            std::move(parentId));
    }
};

////////////////////////////////////////////////////////////////////////////////

template <>
struct TSelectObjectTraits<TNodeSegment>
{
    static const std::vector<NYPath::TYPath>& GetPaths()
    {
        static const std::vector<NYPath::TYPath> paths{
            "/meta/id",
            "/labels",
            "/spec/node_filter"};
        return paths;
    }
};

template <>
struct TParseObjectTraits<TNodeSegment>
{
    static std::unique_ptr<TNodeSegment> Parse(const std::vector<TPayload>& payloads)
    {
        NObjects::TObjectId id;
        NYson::TYsonString labels;
        TString nodeFilter;

        ParsePayloads(
            payloads,
            &id,
            &labels,
            &nodeFilter);

        return std::make_unique<TNodeSegment>(
            std::move(id),
            std::move(labels),
            std::move(nodeFilter));
    }
};

////////////////////////////////////////////////////////////////////////////////

template <>
struct TSelectObjectTraits<TPodDisruptionBudget>
{
    static const std::vector<NYPath::TYPath>& GetPaths()
    {
        static const std::vector<NYPath::TYPath> paths{
            "/meta/id",
            "/labels",
            "/meta/uuid",
            "/spec",
            "/status"};
        return paths;
    }
};

template <>
struct TParseObjectTraits<TPodDisruptionBudget>
{
    static std::unique_ptr<TPodDisruptionBudget> Parse(const std::vector<TPayload>& payloads)
    {
        NObjects::TObjectId id;
        NYson::TYsonString labels;
        NObjects::TObjectId uuid;
        NClient::NApi::NProto::TPodDisruptionBudgetSpec spec;
        NClient::NApi::NProto::TPodDisruptionBudgetStatus status;

        ParsePayloads(
            payloads,
            &id,
            &labels,
            &uuid,
            &spec,
            &status);

        return std::make_unique<TPodDisruptionBudget>(
            std::move(id),
            std::move(labels),
            std::move(uuid),
            std::move(spec),
            std::move(status));
    }
};

////////////////////////////////////////////////////////////////////////////////

template <>
struct TSelectObjectTraits<TPodSet>
{
    static const std::vector<NYPath::TYPath>& GetPaths()
    {
        static const std::vector<NYPath::TYPath> paths{
            "/meta/id",
            "/labels",
            "/spec/node_segment_id",
            "/spec/account_id",
            "/spec/pod_disruption_budget_id",
            "/spec/antiaffinity_constraints",
            "/spec/node_filter"};
        return paths;
    }
};

template <>
struct TParseObjectTraits<TPodSet>
{
    static std::unique_ptr<TPodSet> Parse(const std::vector<TPayload>& payloads)
    {
        NObjects::TObjectId id;
        NYson::TYsonString labels;
        NObjects::TObjectId nodeSegmentId;
        NObjects::TObjectId accountId;
        NObjects::TObjectId podDisruptionBudgetId;
        std::vector<NClient::NApi::NProto::TAntiaffinityConstraint> antiaffinityConstraints;
        TString nodeFilter;

        ParsePayloads(
            payloads,
            &id,
            &labels,
            &nodeSegmentId,
            &accountId,
            &podDisruptionBudgetId,
            &antiaffinityConstraints,
            &nodeFilter);

        return std::make_unique<TPodSet>(
            std::move(id),
            std::move(labels),
            std::move(nodeSegmentId),
            std::move(accountId),
            std::move(podDisruptionBudgetId),
            std::move(antiaffinityConstraints),
            std::move(nodeFilter));
    }
};

////////////////////////////////////////////////////////////////////////////////

template <>
struct TSelectObjectTraits<TPod>
{
    static const std::vector<NYPath::TYPath>& GetPaths()
    {
        static const std::vector<NYPath::TYPath> paths{
            "/meta/id",
            "/labels",
            "/meta/pod_set_id",
            "/status/scheduling/node_id",
            "/spec/account_id",
            "/meta/uuid",
            "/spec/resource_requests",
            "/spec/disk_volume_requests",
            "/spec/gpu_requests",
            "/spec/ip6_address_requests",
            "/spec/ip6_subnet_requests",
            "/spec/node_filter",
            "/spec/enable_scheduling",
            "/status/eviction",
            "/status/scheduling"};
        return paths;
    }
};

template <>
struct TParseObjectTraits<TPod>
{
    static std::unique_ptr<TPod> Parse(const std::vector<TPayload>& payloads)
    {
        NObjects::TObjectId id;
        NYson::TYsonString labels;
        NObjects::TObjectId podSetId;
        NObjects::TObjectId nodeId;
        NObjects::TObjectId accountId;
        NObjects::TObjectId uuid;
        NObjects::TPodResourceRequests resourceRequests;
        std::vector<NClient::NApi::NProto::TPodSpec_TDiskVolumeRequest> diskVolumeRequestsVector;
        std::vector<NClient::NApi::NProto::TPodSpec_TGpuRequest> gpuRequestsVector;
        std::vector<NClient::NApi::NProto::TPodSpec_TIP6AddressRequest> ip6AddressRequestsVector;
        std::vector<NClient::NApi::NProto::TPodSpec_TIP6SubnetRequest> ip6SubnetRequestsVector;
        TString nodeFilter;
        bool enableScheduling;
        NClient::NApi::NProto::TPodStatus_TEviction eviction;
        NClient::NApi::NProto::TPodStatus_TScheduling scheduling;

        ParsePayloads(
            payloads,
            &id,
            &labels,
            &podSetId,
            &nodeId,
            &accountId,
            &uuid,
            &resourceRequests,
            &diskVolumeRequestsVector,
            &gpuRequestsVector,
            &ip6AddressRequestsVector,
            &ip6SubnetRequestsVector,
            &nodeFilter,
            &enableScheduling,
            &eviction,
            &scheduling);

        return std::make_unique<TPod>(
            std::move(id),
            std::move(labels),
            std::move(podSetId),
            std::move(nodeId),
            std::move(accountId),
            std::move(uuid),
            std::move(resourceRequests),
            VectorToProtoRepeated(diskVolumeRequestsVector),
            VectorToProtoRepeated(gpuRequestsVector),
            VectorToProtoRepeated(ip6AddressRequestsVector),
            VectorToProtoRepeated(ip6SubnetRequestsVector),
            std::move(nodeFilter),
            enableScheduling,
            std::move(eviction),
            std::move(*scheduling.mutable_error()));
    }
};

////////////////////////////////////////////////////////////////////////////////

template <>
struct TSelectObjectTraits<TResource>
{
    static const std::vector<NYPath::TYPath>& GetPaths()
    {
        static const std::vector<NYPath::TYPath> paths{
            "/meta/id",
            "/labels",
            "/meta/node_id",
            "/meta/kind",
            "/spec",
            "/status/scheduled_allocations",
            "/status/actual_allocations"};
        return paths;
    }
};

template <>
struct TParseObjectTraits<TResource>
{
    static std::unique_ptr<TResource> Parse(const std::vector<TPayload>& payloads)
    {
        NObjects::TObjectId id;
        NYson::TYsonString labels;
        NObjects::TObjectId nodeId;
        NObjects::EResourceKind kind;
        NClient::NApi::NProto::TResourceSpec spec;
        std::vector<NClient::NApi::NProto::TResourceStatus_TAllocation> scheduledAllocations;
        std::vector<NClient::NApi::NProto::TResourceStatus_TAllocation> actualAllocations;

        ParsePayloads(
            payloads,
            &id,
            &labels,
            &nodeId,
            &kind,
            &spec,
            &scheduledAllocations,
            &actualAllocations);

        return std::make_unique<TResource>(
            std::move(id),
            std::move(labels),
            std::move(nodeId),
            kind,
            std::move(spec),
            std::move(scheduledAllocations),
            std::move(actualAllocations));
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TClusterReader
    : public IClusterReader
{
public:
    TClusterReader(
        TClusterReaderConfigPtr config,
        IClientPtr client)
        : Config_(std::move(config))
        , Client_(std::move(client))
    { }

    virtual TTimestamp StartTransaction()
    {
        Timestamp_ = WaitFor(Client_->GenerateTimestamp())
            .ValueOrThrow()
            .Timestamp;
        return Timestamp_;
    }

    virtual void ReadIP4AddressPools(
        TObjectConsumer<TIP4AddressPool> ip4AddressPoolConsumer) override
    {
        ReadImpl<TIP4AddressPool>(std::move(ip4AddressPoolConsumer));
    }

    virtual void ReadInternetAddresses(
        TObjectConsumer<TInternetAddress> internetAddressConsumer) override
    {
        ReadImpl<TInternetAddress>(std::move(internetAddressConsumer));
    }

    virtual void ReadNodes(
        TObjectConsumer<TNode> nodeConsumer) override
    {
        ReadImpl<TNode>(std::move(nodeConsumer));
    }

    virtual void ReadAccounts(
        TObjectConsumer<TAccount> accountConsumer) override
    {
        ReadImpl<TAccount>(std::move(accountConsumer));
    }

    virtual void ReadNodeSegments(
        TObjectConsumer<TNodeSegment> nodeSegmentConsumer) override
    {
        ReadImpl<TNodeSegment>(std::move(nodeSegmentConsumer));
    }

    virtual void ReadPodDisruptionBudgets(
        TObjectConsumer<TPodDisruptionBudget> podDisruptionBudgetConsumer) override
    {
        ReadImpl<TPodDisruptionBudget>(std::move(podDisruptionBudgetConsumer));
    }

    virtual void ReadPodSets(
        TObjectConsumer<TPodSet> podSetConsumer) override
    {
        ReadImpl<TPodSet>(std::move(podSetConsumer));
    }

    virtual void ReadPods(
        TObjectConsumer<TPod> podConsumer) override
    {
        ReadImpl<TPod>(std::move(podConsumer));
    }

    virtual void ReadResources(
        TObjectConsumer<TResource> resourceConsumer) override
    {
        ReadImpl<TResource>(std::move(resourceConsumer));
    }

private:
    const TClusterReaderConfigPtr Config_;
    const IClientPtr Client_;

    NObjects::TTimestamp Timestamp_ = NObjects::NullTimestamp;


    template <class TObject>
    void ReadImpl(TObjectConsumer<TObject>&& consumer)
    {
        const auto& paths = TSelectObjectTraits<TObject>::GetPaths();

        TSelectObjectsOptions options;

        YT_VERIFY(Timestamp_ != NullTimestamp);
        options.Timestamp = Timestamp_;
        options.Limit = Config_->SelectBatchSize;

        YT_LOG_DEBUG("Selecting object attributes (Type: %v, Paths: %v, Timestamp: %llx, Limit: %v)",
            TObject::Type,
            paths,
            options.Timestamp,
            options.Limit);

        auto selectResults = BatchSelectObjects(
            Client_,
            TObject::Type,
            paths,
            options)
            .Results;

        YT_LOG_DEBUG("Selected object attributes (Type: %v, Count: %v)",
            TObject::Type,
            selectResults.size());

        for (const auto& selectResult : selectResults) {
            auto object = TParseObjectTraits<TObject>::Parse(selectResult.ValuePayloads);
            if (object) {
                consumer(std::move(object));
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

IClusterReaderPtr CreateClusterReader(
    TClusterReaderConfigPtr config,
    IClientPtr client)
{
    return New<TClusterReader>(
        std::move(config),
        std::move(client));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NHeavyScheduler

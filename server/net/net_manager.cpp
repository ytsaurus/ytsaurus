#include "net_manager.h"
#include "helpers.h"
#include "config.h"
#include "private.h"

#include <yp/server/master/bootstrap.h>
#include <yp/server/master/yt_connector.h>

#include <yp/server/objects/transaction.h>
#include <yp/server/objects/transaction_manager.h>
#include <yp/server/objects/db_schema.h>
#include <yp/server/objects/object.h>
#include <yp/server/objects/pod.h>
#include <yp/server/objects/node.h>
#include <yp/server/objects/network_project.h>
#include <yp/server/objects/persistence.h>
#include <yp/server/objects/virtual_service.h>

#include <yp/client/api/proto/data_model.pb.h>

#include <yt/ytlib/transaction_client/public.h>

#include <yt/ytlib/api/rowset.h>

#include <yt/core/net/address.h>

#include <yt/core/misc/collection_helpers.h>

#include <array>

namespace NYP {
namespace NServer {
namespace NNet {

using namespace NServer::NMaster;
using namespace NServer::NObjects;

using namespace NYT::NConcurrency;
using namespace NYT::NTransactionClient;
using namespace NYT::NTableClient;
using namespace NYT::NApi;
using namespace NYT::NNet;

////////////////////////////////////////////////////////////////////////////////

class TNetManager::TImpl
    : public TRefCounted
{
public:
    TImpl(TBootstrap* bootstrap, TNetManagerConfigPtr config)
        : Bootstrap_(bootstrap)
        , Config_(std::move(config))
    { }

    TString BuildPersistentPodFqdn(TPod* pod)
    {
        return Format("%v.%v",
            pod->GetId(),
            Config_->PodFqdnSuffix);
    }

    TString BuildTransientPodFqdn(TPod* pod)
    {
        return Format("%v-%v.%v.%v",
            pod->Spec().Node().Load()->Spec().Load().short_name(),
            pod->Status().GenerationNumber().Load(),
            pod->GetId(),
            Config_->PodFqdnSuffix);
    }

    void PrepareUpdatePodAddresses(TPod* pod)
    {
        pod->Spec().Other().ScheduleLoad();
        pod->Status().Other().ScheduleLoad();
    }

    void UpdatePodAddresses(
        const TTransactionPtr& transaction,
        TPod* pod)
    {
        if (ShouldReassignPodAddresses(transaction, pod)) {
            ReleasePodAddresses(transaction, pod);
            AcquirePodAddresss(transaction, pod);
        }

        if (pod->Spec().Node().Load()) {
            const auto& ip6AddressRequests = pod->Spec().Other().Load().ip6_address_requests();
            auto* ip6AddressAllocations = pod->Status().Other()->mutable_ip6_address_allocations();
            Y_ASSERT(ip6AddressRequests.size() == ip6AddressAllocations->size());
            for (size_t index = 0; index < ip6AddressRequests.size(); ++index) {
                UpdateIP6AddressFqdns(pod, ip6AddressRequests[index], &(*ip6AddressAllocations)[index]);
            }
        }

        AcquireVirtualServiceTunnel(transaction, pod);
    }

private:
    TBootstrap* const Bootstrap_;
    const TNetManagerConfigPtr Config_;


    bool IsValidPodIP6Address(const TNode* node, const TIP6Address& podAddress)
    {
        auto podHostSubnet = HostSubnetFromMtnAddress(podAddress);
        for (const auto& subnet : node->Spec().Load().ip6_subnets()) {
            auto network = TIP6Network::FromString(subnet.subnet());
            if (HostSubnetFromMtnAddress(network.GetAddress()) == podHostSubnet) {
                return true;
            }
        }
        return false;
    }

    static bool LabelsMatch(
        const NYT::NYTree::NProto::TAttributeDictionary& lhsAttributes,
        const NYT::NYTree::NProto::TAttributeDictionary& rhsAttributes)
    {
        if (lhsAttributes.attributes_size() != rhsAttributes.attributes_size()) {
            return false;
        }
        for (int index = 0; index < lhsAttributes.attributes_size(); ++index) {
            const auto& lhsAttribute = lhsAttributes.attributes(index);
            const auto& rhsAttribute = rhsAttributes.attributes(index);
            if (lhsAttribute.key() != rhsAttribute.key()) {
                return false;
            }
            if (lhsAttribute.value() != rhsAttribute.value()) {
                return false;
            }
        }
        return true;
    }

    bool ShouldReassignPodAddresses(const TTransactionPtr& transaction, const TPod* pod)
    {
        if (pod->RemovalPending()) {
            return true;
        }

        const auto* oldNode = pod->Spec().Node().LoadOld();
        const auto* newNode = pod->Spec().Node().Load();
        if (oldNode != newNode) {
            return true;
        }
        if (!oldNode && !newNode) {
            return false;
        }

        const auto& ip6AddressRequests = pod->Spec().Other().Load().ip6_address_requests();
        const auto& ip6AddressAllocations = pod->Status().Other().Load().ip6_address_allocations();
        if (ip6AddressRequests.size() != ip6AddressAllocations.size()) {
            return true;
        }

        for (size_t index = 0; index < ip6AddressRequests.size(); ++index) {
            const auto& request = ip6AddressRequests[index];
            const auto& allocation = ip6AddressAllocations[index];
            auto podAddress = TIP6Address::FromString(allocation.address());
            if (request.vlan_id() != allocation.vlan_id()) {
                return true;
            }
            if (request.has_manual_address() != allocation.manual()) {
                return true;
            }
            if (request.has_manual_address()) {
                if (request.manual_address() != allocation.address()) {
                    return true;
                }
            } else {
                auto* networkProject = transaction->GetNetworkProject(request.network_id());
                if (networkProject->Spec().ProjectId().Load() != ProjectIdFromMtnAddress(podAddress)) {
                    return true;
                }
                if (!IsValidPodIP6Address(oldNode, podAddress)) {
                    return true;
                }
            }
            if (!LabelsMatch(request.labels(), allocation.labels())) {
                return true;
            }
        }

        const auto& ip6SubnetRequests = pod->Spec().Other().Load().ip6_subnet_requests();
        const auto& ip6SubnetAllocations = pod->Status().Other().Load().ip6_subnet_allocations();
        if (ip6SubnetRequests.size() != ip6SubnetAllocations.size()) {
            return true;
        }

        for (size_t index = 0; index < ip6SubnetRequests.size(); ++index) {
            const auto& request = ip6SubnetRequests[index];
            const auto& allocation = ip6SubnetAllocations[index];
            if (request.vlan_id() != allocation.vlan_id()) {
                return true;
            }
            auto subnet = TIP6Network::FromString(allocation.subnet());
            if (!IsValidPodIP6Address(oldNode, subnet.GetAddress())) {
                return true;
            }
            if (!LabelsMatch(request.labels(), allocation.labels())) {
                return true;
            }
        }

        return false;
    }

    void RegisterIP6Nonce(
        const TTransactionPtr& transaction,
        const TNode* node,
        TNonce nonce,
        const TPod* pod)
    {
        auto* session = transaction->GetSession();
        session->ScheduleStore(
            [=] (IStoreContext* context) {
                context->WriteRow(
                    &IP6NoncesTable,
                    ToDbValues(
                        context->GetRowBuffer(),
                        node->GetId(),
                        nonce),
                    MakeArray(
                        &IP6NoncesTable.Fields.PodId),
                    ToDbValues(
                        context->GetRowBuffer(),
                        pod->GetId()));
            });
    }

    void UnregisterIP6Nonce(
        const TTransactionPtr& transaction,
        const TNode* node,
        TNonce nonce)
    {
        auto* session = transaction->GetSession();
        session->ScheduleStore(
            [=] (IStoreContext* context) {
                context->DeleteRow(
                    &IP6NoncesTable,
                    ToDbValues(
                        context->GetRowBuffer(),
                        node->GetId(),
                        nonce));
            });
    }

    void ReleasePodAddresses(const TTransactionPtr& transaction, TPod* pod)
    {
        const auto* node = pod->Spec().Node().LoadOld();

        auto* ip6AddressAllocations = pod->Status().Other()->mutable_ip6_address_allocations();
        auto* ip6SubnetAllocations = pod->Status().Other()->mutable_ip6_subnet_allocations();

        if (!node) {
            if (!ip6AddressAllocations->empty()) {
                THROW_ERROR_EXCEPTION("Pod %Qv has no previously assigned node but has IP6 address allocations",
                    pod->GetId());
            }
            if (!ip6SubnetAllocations->empty()) {
                THROW_ERROR_EXCEPTION("Pod %Qv has no previously assigned node but has IP6 subnet allocations",
                    pod->GetId());
            }
            return;
        }

        for (const auto& allocation : *ip6AddressAllocations) {
            if (allocation.manual()) {
                continue;
            }
            auto address = TIP6Address::FromString(allocation.address());
            auto nonce = NonceFromMtnAddress(address);
            UnregisterIP6Nonce(transaction, node, nonce);

            LOG_DEBUG("Pod IP6 address released (PodId: %v, NodeId: %v, Address: %v)",
                pod->GetId(),
                node->GetId(),
                address);
        }

        for (const auto& allocation : *ip6SubnetAllocations) {
            auto subnet = TIP6Network::FromString(allocation.subnet());
            auto nonce = NonceFromMtnAddress(subnet.GetAddress());
            UnregisterIP6Nonce(transaction, node, nonce);

            LOG_DEBUG("Pod IP6 subnet released (PodId: %v, NodeId: %v, Subnet: %v)",
                pod->GetId(),
                node->GetId(),
                subnet);
        }

        ip6AddressAllocations->Clear();
        ip6SubnetAllocations->Clear();
    }

    void AcquirePodAddresss(const TTransactionPtr& transaction, TPod* pod)
    {
        if (pod->RemovalPending()) {
            return;
        }

        const auto* node = pod->Spec().Node().Load();
        if (!node || node->RemovalPending()) {
            return;
        }

        size_t nonceCount = 0;
        for (const auto& request : pod->Spec().Other().Load().ip6_address_requests()) {
            if (!request.has_manual_address()) {
                ++nonceCount;
            }
        }
        for (const auto& request : pod->Spec().Other().Load().ip6_subnet_requests()) {
            Y_UNUSED(request);
            ++nonceCount;
        }

        auto nonces = GenerateNonces(
            transaction,
            node,
            nonceCount);

        size_t nonceIndex = 0;
        auto generateNonce = [&] () mutable {
            return nonces[nonceIndex++];
        };

        THashMap<TString, THostSubnet> vlanIdToHostSubnet;
        for (const auto& subnet : node->Spec().Load().ip6_subnets()) {
            const auto& vlanId = subnet.vlan_id();
            auto network = TIP6Network::FromString(subnet.subnet());
            auto hostSubnet = HostSubnetFromMtnAddress(network.GetAddress());
            if (!vlanIdToHostSubnet.emplace(vlanId, hostSubnet).second) {
                THROW_ERROR_EXCEPTION("Multiple IP6 subnets with VLAN %Qv known for node %Qv",
                    vlanId,
                    node->GetId());
            }
        }

        auto getHostSubnetForVlan = [&] (const TString& vlanId) {
            auto it = vlanIdToHostSubnet.find(vlanId);
            if (it == vlanIdToHostSubnet.end()) {
                THROW_ERROR_EXCEPTION("No VLAN %Qv known for node %Qv",
                    vlanId,
                    node->GetId());
            }
            return it->second;
        };

        google::protobuf::RepeatedPtrField<NClient::NApi::NProto::TPodStatus_TIP6AddressAllocation> ip6AddressAllocations;
        for (const auto& request : pod->Spec().Other().Load().ip6_address_requests()) {
            const auto& vlanId = request.vlan_id();

            auto* allocation = ip6AddressAllocations.Add();
            allocation->set_vlan_id(vlanId);
            allocation->mutable_labels()->CopyFrom(request.labels());

            if (request.has_manual_address()) {
                allocation->set_manual(true);
                allocation->set_address(request.manual_address());
            } else {
                const auto& networkId = request.network_id();
                if (!networkId) {
                    THROW_ERROR_EXCEPTION("Neither \"manual_address\" nor \"network_id\" is given");
                }
                auto hostSubnet = getHostSubnetForVlan(vlanId);
                auto* networkProject = transaction->GetNetworkProject(request.network_id());
                auto projectId = networkProject->Spec().ProjectId().Load();
                auto nonce = generateNonce();
                auto address = MakeMtnAddress(
                    hostSubnet,
                    projectId,
                    nonce);

                allocation->set_manual(false);
                allocation->set_address(ToString(address));

                LOG_DEBUG("Pod IP6 address acquired (PodId: %v, NodeId: %v, VlanId: %v, Address: %v)",
                    pod->GetId(),
                    node->GetId(),
                    vlanId,
                    address);
            }
        }

        google::protobuf::RepeatedPtrField<NClient::NApi::NProto::TPodStatus_TIP6SubnetAllocation> ip6SubnetAllocations;
        for (const auto& request : pod->Spec().Other().Load().ip6_subnet_requests()) {
            const auto& vlanId = request.vlan_id();
            auto hostSubnet = getHostSubnetForVlan(vlanId);
            auto nonce = generateNonce();
            auto subnet = MakeMtnNetwork(hostSubnet, nonce);

            auto* allocation = ip6SubnetAllocations.Add();
            allocation->set_subnet(ToString(subnet));
            allocation->set_vlan_id(vlanId);
            allocation->mutable_labels()->CopyFrom(request.labels());

            LOG_DEBUG("Pod IP6 subnet acquired (PodId: %v, NodeId: %v, VlanId: %v, Subnet: %v)",
                pod->GetId(),
                node->GetId(),
                vlanId,
                subnet);
        }

        pod->Status().Other()->mutable_ip6_address_allocations()->Swap(&ip6AddressAllocations);
        pod->Status().Other()->mutable_ip6_subnet_allocations()->Swap(&ip6SubnetAllocations);

        for (auto nonce : nonces) {
            RegisterIP6Nonce(transaction, node, nonce, pod);
        }
    }

    void AcquireVirtualServiceTunnel(const TTransactionPtr& transaction, TPod* pod)
    {
        auto& podStatusOther = pod->Status().Other();
        podStatusOther->mutable_virtual_service()->Clear();

        const auto& podSpecOther = pod->Spec().Other().Load();
        if (!podSpecOther.has_virtual_service_tunnel()) {
            return;
        }

        const auto& tunnel = podSpecOther.virtual_service_tunnel();
        const auto* virtualService = transaction->GetVirtualService(tunnel.virtual_service_id());
        virtualService->ValidateExists();

        auto* vsStatus = podStatusOther->mutable_virtual_service();
        for (const auto& ip6Address : virtualService->Spec().Load().ip6_addresses()) {
            vsStatus->add_ip6_addresses(ip6Address);
        }
        for (const auto& ip4Address : virtualService->Spec().Load().ip4_addresses()) {
            vsStatus->add_ip4_addresses(ip4Address);
        }
    }

    void UpdateIP6AddressFqdns(
        TPod* pod,
        const NClient::NApi::NProto::TPodSpec_TIP6AddressRequest& request,
        NClient::NApi::NProto::TPodStatus_TIP6AddressAllocation* allocation)
    {
        if (request.enable_dns()) {
            allocation->set_persistent_fqdn(BuildPersistentIP6AddressFqdn(pod, request));
            allocation->set_transient_fqdn(BuildTransientIP6AddressFqdn(pod, request));
        } else {
            allocation->clear_persistent_fqdn();
            allocation->clear_transient_fqdn();
        }
    }

    TString BuildPersistentIP6AddressFqdn(
        TPod* pod,
        const NClient::NApi::NProto::TPodSpec_TIP6AddressRequest& request)
    {
        auto fqdn = BuildPersistentPodFqdn(pod);
        if (request.has_dns_prefix()) {
            fqdn = Format("%v.%v",
                request.dns_prefix(),
                fqdn);
        }
        ValidatePodFqdn(fqdn);
        return fqdn;
    }

    TString BuildTransientIP6AddressFqdn(
        TPod* pod,
        const NClient::NApi::NProto::TPodSpec_TIP6AddressRequest& request)
    {
        auto fqdn = BuildTransientPodFqdn(pod);
        if (request.has_dns_prefix()) {
            fqdn = Format("%v.%v",
                request.dns_prefix(),
                fqdn);
        }
        ValidatePodFqdn(fqdn);
        return fqdn;
    }

    std::vector<TNonce> GenerateNonces(
        const TTransactionPtr& transaction,
        const TNode* node,
        size_t count)
    {
        THashSet<TNonce> nonceSet;
        int attempt = 0;
        while (nonceSet.size() < count) {
            if (attempt++ > Config_->MaxNoncesGenerationAttempts) {
                THROW_ERROR_EXCEPTION("Could not generate %v IP6 nonces for node %Qv after %v attempts",
                    count,
                    node->GetId(),
                    Config_->MaxNoncesGenerationAttempts);
            }

            std::vector<TNonce> candidates;
            for (size_t index = 0; index < count - nonceSet.size(); ++index) {
                candidates.push_back(RandomNumber<TNonce>());
            }

            LOG_DEBUG("Trying IP6 nonces (NodeId: %v, Nonces: %v)",
                node->GetId(),
                candidates);

            auto* session = transaction->GetSession();
            session->ScheduleLoad(
                [&] (ILoadContext* context) mutable {
                    for (auto candidate : candidates) {
                        context->ScheduleLookup(
                            &IP6NoncesTable,
                            ToDbValues(
                                context->GetRowBuffer(),
                                node->GetId(),
                                candidate),
                            MakeArray<const TDbField*>(),
                            [&, candidate = candidate] (const TNullable<TRange<TVersionedValue>>& maybeValues) {
                                if (!maybeValues) {
                                    nonceSet.insert(candidate);
                                }
                            });
                    }
                });
            session->FlushLoads();
        }

        std::vector<TNonce> nonceList(nonceSet.begin(), nonceSet.end());
        LOG_DEBUG("IP6 nonces generated (NodeId: %v, Nonces: %v)",
            node->GetId(),
            nonceList);
        return  nonceList;
    }
};

////////////////////////////////////////////////////////////////////////////////

TNetManager::TNetManager(TBootstrap* bootstrap, TNetManagerConfigPtr config)
    : Impl_(New<TImpl>(bootstrap, std::move(config)))
{ }

TString TNetManager::BuildPersistentPodFqdn(TPod* pod)
{
    return Impl_->BuildPersistentPodFqdn(pod);
}

TString TNetManager::BuildTransientPodFqdn(TPod* pod)
{
    return Impl_->BuildTransientPodFqdn(pod);
}

void TNetManager::PrepareUpdatePodAddresses(TPod* pod)
{
    Impl_->PrepareUpdatePodAddresses(pod);
}

void TNetManager::UpdatePodAddresses(
    const TTransactionPtr& transaction,
    TPod* pod)
{
    Impl_->UpdatePodAddresses(
        transaction,
        pod);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNet
} // namespace NServer
} // namespace NYP


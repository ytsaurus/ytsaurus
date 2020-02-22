#include "net_manager.h"
#include "internet_address_manager.h"
#include "helpers.h"
#include "config.h"
#include "private.h"

#include <yp/server/master/bootstrap.h>
#include <yp/server/master/yt_connector.h>

#include <yp/server/objects/transaction.h>
#include <yp/server/objects/transaction_manager.h>
#include <yp/server/objects/db_schema.h>
#include <yp/server/objects/dns_record_set.h>
#include <yp/server/objects/object.h>
#include <yp/server/objects/pod.h>
#include <yp/server/objects/node.h>
#include <yp/server/objects/network_project.h>
#include <yp/server/objects/persistence.h>
#include <yp/server/objects/virtual_service.h>

#include <yp/client/api/proto/data_model.pb.h>

#include <yt/ytlib/transaction_client/public.h>

#include <yt/client/api/rowset.h>

#include <yt/core/net/address.h>

#include <yt/core/misc/collection_helpers.h>

#include <util/string/hex.h>

#include <algorithm>
#include <array>

namespace NYP::NServer::NNet {

using namespace NServer::NMaster;
using namespace NServer::NObjects;

using namespace NYT::NConcurrency;
using namespace NYT::NTransactionClient;
using namespace NYT::NTableClient;
using namespace NYT::NApi;
using namespace NYT::NNet;

////////////////////////////////////////////////////////////////////////////////

static const TString DnsRecordClass = "IN";
static const TString FastboneVlanId = "fastbone";
static const TString FastboneFqdnPrefix = "fb-";

////////////////////////////////////////////////////////////////////////////////

TString BuildIp6PtrDnsAddress(const TString& ip6Address)
{
    constexpr static TStringBuf DnsIPv6PtrRecordSuffix = AsStringBuf("ip6.arpa.");
    constexpr static char DnsIPv6PtrRecordSymbolsDelimeter = '.';
    constexpr static size_t OctetsInIPv6Address = 8;
    constexpr static size_t CharsInOctet = 4;
    constexpr static size_t IPv6PtrDnsRecordDataLength = 73;

    const auto address = TIP6Address::FromString(ip6Address);
    const ui16* octets = address.GetRawWords();
    TString result;
    result.reserve(IPv6PtrDnsRecordDataLength);
    for (size_t octetIndex = 0; octetIndex < OctetsInIPv6Address; ++octetIndex) {
        char octet[CharsInOctet];
        HexEncode(octets + octetIndex, sizeof(octets[0]), octet);
        std::swap(octet[0], octet[1]);
        std::swap(octet[2], octet[3]);
        for (size_t charIndex = 0; charIndex < CharsInOctet; ++charIndex) {
            result += tolower(octet[charIndex]);
            result += DnsIPv6PtrRecordSymbolsDelimeter;
        }
    }
    result += DnsIPv6PtrRecordSuffix;

    return result;
}

////////////////////////////////////////////////////////////////////////////////

class TNetManager::TImpl
    : public TRefCounted
{
public:
    TImpl(TBootstrap* /*bootstrap*/, TNetManagerConfigPtr config)
        : Config_(std::move(config))
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
        pod->Spec().Etc().ScheduleLoad();
        pod->Status().Etc().ScheduleLoad();
    }

    void UpdatePodAddresses(
        const TTransactionPtr& transaction,
        TInternetAddressManager* internetAddressManager,
        TPod* pod)
    {
        UnregisterDnsRecords(transaction, pod);

        if (ShouldReassignPodAddresses(transaction, pod)) {
            ReleasePodAddresses(transaction, internetAddressManager, pod);
            AcquirePodAddress(transaction, internetAddressManager, pod);
        }

        if (ShouldUpdateAddressAllocations(pod)) {
            UpdateFqdns(pod);
            RegisterDnsRecords(transaction, pod);
            UpdateVirtualServiceTunnel(transaction, pod);
        }
    }

private:
    const TNetManagerConfigPtr Config_;

    bool ShouldUpdateAddressAllocations(TPod* pod)
    {
        return !pod->IsRemoving() && pod->Spec().Node().Load();
    }

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
        if (pod->IsRemoving()) {
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

        const auto& ip6AddressRequests = pod->Spec().Etc().Load().ip6_address_requests();
        const auto& ip6AddressAllocations = pod->Status().Etc().Load().ip6_address_allocations();
        if (ip6AddressRequests.size() != ip6AddressAllocations.size()) {
            return true;
        }

        for (int index = 0; index < ip6AddressRequests.size(); ++index) {
            const auto& request = ip6AddressRequests[index];
            const auto& allocation = ip6AddressAllocations[index];
            auto podAddress = TIP6Address::FromString(allocation.address());

            if (request.vlan_id() != allocation.vlan_id()) {
                return true;
            }

            const auto shouldAllocateIP4Address = request.enable_internet() || !request.ip4_address_pool_id().empty();
            if (shouldAllocateIP4Address != allocation.has_internet_address()) {
                return true;
            }

            const auto& networkProjectId = request.network_id();
            auto* networkProject = transaction->GetNetworkProject(networkProjectId);

            // TODO(babenko): ugly hack for YP-322
            if (networkProject->DoesExist()) {
                if (networkProject->Spec().ProjectId().Load() != ProjectIdFromMtnAddress(podAddress)) {
                    return true;
                }
            } else {
                YT_LOG_DEBUG("Pod refers to non-existing network project; silently skipping update since pod's node did not change"
                    " (PodId: %v, NetworkProjectId: %v)",
                    pod->GetId(),
                    networkProjectId);
            }

            if (!IsValidPodIP6Address(oldNode, podAddress)) {
                return true;
            }

            if (!LabelsMatch(request.labels(), allocation.labels())) {
                return true;
            }
        }

        const auto& ip6SubnetRequests = pod->Spec().Etc().Load().ip6_subnet_requests();
        const auto& ip6SubnetAllocations = pod->Status().Etc().Load().ip6_subnet_allocations();
        if (ip6SubnetRequests.size() != ip6SubnetAllocations.size()) {
            return true;
        }

        for (int index = 0; index < ip6SubnetRequests.size(); ++index) {
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
                    ToUnversionedValues(
                        context->GetRowBuffer(),
                        node->GetId(),
                        nonce),
                    MakeArray(
                        &IP6NoncesTable.Fields.PodId),
                    ToUnversionedValues(
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
                    ToUnversionedValues(
                        context->GetRowBuffer(),
                        node->GetId(),
                        nonce));
            });
    }

    void RegisterDnsRecords(const TTransactionPtr& transaction, TPod* pod)
    {
        const auto& ip6AddressRequests = pod->Spec().Etc().Load().ip6_address_requests();
        const auto& ip6AddressAllocations = pod->Status().Etc().Load().ip6_address_allocations();

        THashMap<TString, NClient::NApi::NProto::TDnsRecordSetSpec> records;

        auto addDnsResourceRecord = [&records] (const TString& key, EDnsResourceRecordType type, const TString& data) {
            auto* record = records[key].add_records();
            record->set_class_(DnsRecordClass);
            record->set_type(static_cast<NClient::NApi::NProto::TDnsRecordSetSpec_TResourceRecord_EType>(type));
            record->set_data(data);
        };

        for (int index = 0; index < ip6AddressRequests.size(); ++index) {
            const auto& request = ip6AddressRequests[index];
            const auto& allocation = ip6AddressAllocations[index];

            if (!request.enable_dns()) {
                continue;
            }

            addDnsResourceRecord(allocation.persistent_fqdn(), EDnsResourceRecordType::AAAA, allocation.address());
            addDnsResourceRecord(allocation.transient_fqdn(), EDnsResourceRecordType::AAAA, allocation.address());
            addDnsResourceRecord(BuildIp6PtrDnsAddress(allocation.address()), EDnsResourceRecordType::PTR, allocation.persistent_fqdn());
        }

        for (auto&& record : records) {
            auto* dnsRecordSet = transaction->CreateDnsRecordSet(record.first);
            dnsRecordSet->Spec() = std::move(record.second);
        }
    }

    void UnregisterDnsRecords(const TTransactionPtr& transaction, TPod* pod)
    {
        const auto& allocations = pod->Status().Etc().Load().ip6_address_allocations();
        for (const auto& allocation : allocations) {
            if (allocation.has_persistent_fqdn()) {
                {
                    auto* record = transaction->GetDnsRecordSet(allocation.persistent_fqdn());
                    if (record) {
                        record->Remove();
                    }
                }
                {
                    auto* record = transaction->GetDnsRecordSet(BuildIp6PtrDnsAddress(allocation.address()));
                    if (record) {
                        record->Remove();
                    }
                }
            }

            if (allocation.has_transient_fqdn()) {
                auto* record = transaction->GetDnsRecordSet(allocation.transient_fqdn());
                if (record) {
                    record->Remove();
                }
            }
        }
    }

    void ReleasePodAddresses(const TTransactionPtr& transaction, TInternetAddressManager* internetAddressManager, TPod* pod)
    {
        const auto* node = pod->Spec().Node().LoadOld();

        auto* ip6AddressAllocations = pod->Status().Etc()->mutable_ip6_address_allocations();
        auto* ip6SubnetAllocations = pod->Status().Etc()->mutable_ip6_subnet_allocations();

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

        internetAddressManager->RevokeInternetAddressesFromPod(transaction, pod);

        for (const auto& allocation : *ip6AddressAllocations) {
            auto address = TIP6Address::FromString(allocation.address());
            auto nonce = NonceFromMtnAddress(address);
            UnregisterIP6Nonce(transaction, node, nonce);

            YT_LOG_DEBUG("Pod IP6 address released (PodId: %v, NodeId: %v, Address: %v)",
                pod->GetId(),
                node->GetId(),
                address);
        }

        for (const auto& allocation : *ip6SubnetAllocations) {
            auto subnet = TIP6Network::FromString(allocation.subnet());
            auto nonce = NonceFromMtnAddress(subnet.GetAddress());
            UnregisterIP6Nonce(transaction, node, nonce);

            YT_LOG_DEBUG("Pod IP6 subnet released (PodId: %v, NodeId: %v, Subnet: %v)",
                pod->GetId(),
                node->GetId(),
                subnet);
        }

        ip6AddressAllocations->Clear();
        ip6SubnetAllocations->Clear();
    }

    void AcquirePodAddress(const TTransactionPtr& transaction, TInternetAddressManager* internetAddressManager, TPod* pod)
    {
        if (pod->IsRemoving()) {
            return;
        }

        const auto* node = pod->Spec().Node().Load();
        if (!node || node->IsRemoving()) {
            return;
        }

        size_t nonceCount =
            pod->Spec().Etc().Load().ip6_address_requests().size() +
            pod->Spec().Etc().Load().ip6_subnet_requests().size();
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

        auto getProjectId = [&] (const TObjectId& networkId) {
            auto* networkProject = transaction->GetNetworkProject(networkId);
            if (!networkProject->DoesExist()) {
                THROW_ERROR_EXCEPTION(
                    NClient::NApi::EErrorCode::PodSchedulingFailure,
                    "Pod %Qv refers to non-existing network project %Qv",
                    pod->GetId(),
                    networkId);
            }
            return networkProject->Spec().ProjectId().Load();
        };

        google::protobuf::RepeatedPtrField<NClient::NApi::NProto::TPodStatus_TIP6AddressAllocation> ip6AddressAllocations;
        for (const auto& request : pod->Spec().Etc().Load().ip6_address_requests()) {
            const auto& vlanId = request.vlan_id();
            auto projectId = getProjectId(request.network_id());
            auto hostSubnet = getHostSubnetForVlan(vlanId);
            auto nonce = generateNonce();
            auto address = MakeMtnAddress(
                hostSubnet,
                projectId,
                nonce);

            auto* allocation = ip6AddressAllocations.Add();
            allocation->set_vlan_id(vlanId);
            allocation->set_address(ToString(address));
            allocation->mutable_labels()->CopyFrom(request.labels());

            YT_LOG_DEBUG("Pod IP6 address acquired (PodId: %v, NodeId: %v, VlanId: %v, Address: %v)",
                pod->GetId(),
                node->GetId(),
                vlanId,
                address);
        }

        google::protobuf::RepeatedPtrField<NClient::NApi::NProto::TPodStatus_TIP6SubnetAllocation> ip6SubnetAllocations;
        for (const auto& request : pod->Spec().Etc().Load().ip6_subnet_requests()) {
            const auto& vlanId = request.vlan_id();
            const auto& networkId = request.network_id();
            auto projectId = networkId ? getProjectId(networkId) : 0;
            auto hostSubnet = getHostSubnetForVlan(vlanId);
            auto nonce = generateNonce();
            auto subnet = MakeMtnSubnet(hostSubnet, projectId, nonce);

            auto* allocation = ip6SubnetAllocations.Add();
            allocation->set_vlan_id(vlanId);
            allocation->set_subnet(ToString(subnet));
            allocation->mutable_labels()->CopyFrom(request.labels());

            YT_LOG_DEBUG("Pod IP6 subnet acquired (PodId: %v, NodeId: %v, VlanId: %v, Subnet: %v)",
                pod->GetId(),
                node->GetId(),
                vlanId,
                subnet);
        }

        pod->Status().Etc()->mutable_ip6_address_allocations()->Swap(&ip6AddressAllocations);
        pod->Status().Etc()->mutable_ip6_subnet_allocations()->Swap(&ip6SubnetAllocations);

        for (auto nonce : nonces) {
            RegisterIP6Nonce(transaction, node, nonce, pod);
        }

        internetAddressManager->AssignInternetAddressesToPod(transaction, node, pod);
    }

    void UpdateVirtualServiceTunnel(const TTransactionPtr& transaction, TPod* pod)
    {
        const auto& ip6AddressRequests = pod->Spec().Etc().Load().ip6_address_requests();
        auto* ip6AddressAllocations = pod->Status().Etc()->mutable_ip6_address_allocations();
        YT_ASSERT(ip6AddressRequests.size() == ip6AddressAllocations->size());

        for (int index = 0; index < ip6AddressRequests.size(); ++index) {
            UpdateIP6AddressVirtualServiceTunnel(
                transaction,
                pod,
                ip6AddressRequests[index],
                &(*ip6AddressAllocations)[index]);
        }
    }

    void UpdateFqdns(TPod* pod)
    {
        const auto& ip6AddressRequests = pod->Spec().Etc().Load().ip6_address_requests();
        auto* ip6AddressAllocations = pod->Status().Etc()->mutable_ip6_address_allocations();
        YT_ASSERT(ip6AddressRequests.size() == ip6AddressAllocations->size());
        for (int index = 0; index < ip6AddressRequests.size(); ++index) {
            UpdateIP6AddressFqdns(pod, ip6AddressRequests[index], &(*ip6AddressAllocations)[index]);
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

    void UpdateIP6AddressVirtualServiceTunnel(
        const TTransactionPtr& transaction,
        TPod* pod,
        const NClient::NApi::NProto::TPodSpec_TIP6AddressRequest& request,
        NClient::NApi::NProto::TPodStatus_TIP6AddressAllocation* allocation)
    {
        allocation->clear_virtual_services();

        for (const auto& virtualServiceId : request.virtual_service_ids()) {
            const auto* virtualService = transaction->GetVirtualService(virtualServiceId);
            if (!virtualService->DoesExist()) {
                // TODO(babenko): similar to YP-322
                if (!pod->Spec().Node().IsChanged()) {
                    YT_LOG_DEBUG("Pod refers to non-existing virtual service; silently skipping update since pod's node did not change"
                        " (PodId: %v, VirtualServiceId: %v)",
                        pod->GetId(),
                        virtualServiceId);
                    return;
                }

                THROW_ERROR_EXCEPTION(
                    NClient::NApi::EErrorCode::PodSchedulingFailure,
                    "Pod %Qv refers to non-existing virtual service %Qv",
                    pod->GetId(),
                    virtualServiceId);
            }

            auto* vsStatus = allocation->add_virtual_services();
            for (const auto& ip6Address : virtualService->Spec().Load().ip6_addresses()) {
                vsStatus->add_ip6_addresses(ip6Address);
            }
            for (const auto& ip4Address : virtualService->Spec().Load().ip4_addresses()) {
                vsStatus->add_ip4_addresses(ip4Address);
            }
        }
    }

    static void ExtendPodFqdn(
        const NClient::NApi::NProto::TPodSpec_TIP6AddressRequest& request,
        TString* fqdn)
    {
        if (request.has_dns_prefix()) {
            *fqdn = Format("%v.%v",
                request.dns_prefix(),
                *fqdn);
        }
        if (request.vlan_id() == FastboneVlanId) {
            *fqdn = FastboneFqdnPrefix + *fqdn;
        }
    }

    TString BuildPersistentIP6AddressFqdn(
        TPod* pod,
        const NClient::NApi::NProto::TPodSpec_TIP6AddressRequest& request)
    {
        auto fqdn = BuildPersistentPodFqdn(pod);
        ExtendPodFqdn(request, &fqdn);
        ValidatePodFqdn(fqdn);
        return fqdn;
    }

    TString BuildTransientIP6AddressFqdn(
        TPod* pod,
        const NClient::NApi::NProto::TPodSpec_TIP6AddressRequest& request)
    {
        auto fqdn = BuildTransientPodFqdn(pod);
        ExtendPodFqdn(request, &fqdn);
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
                    NClient::NApi::EErrorCode::PodSchedulingFailure,
                    count,
                    node->GetId(),
                    Config_->MaxNoncesGenerationAttempts);
            }

            std::vector<TNonce> candidates;
            for (size_t index = 0; index < count - nonceSet.size(); ++index) {
                candidates.push_back(RandomNumber<TNonce>());
            }

            YT_LOG_DEBUG("Trying IP6 nonces (NodeId: %v, Nonces: %v)",
                node->GetId(),
                candidates);

            auto* session = transaction->GetSession();
            session->ScheduleLoad(
                [&] (ILoadContext* context) mutable {
                    for (auto candidate : candidates) {
                        // NB: Optimisation check for neсessity to lookup the table. We want to prevent nonce overcommit.
                        if (transaction->HasAllocatedNonce(candidate)) {
                            continue;
                        }
                        context->ScheduleLookup(
                            &IP6NoncesTable,
                            ToUnversionedValues(
                                context->GetRowBuffer(),
                                node->GetId(),
                                candidate),
                            MakeArray<const TDBField*>(),
                            [&, candidate = candidate] (const std::optional<TRange<TVersionedValue>>& optionalValues) {
                                // NB: Extra lookup nonces allocated in this transaction for thread safety.
                                if (!transaction->HasAllocatedNonce(candidate) && !optionalValues) {
                                    if (nonceSet.insert(candidate).second) {
                                        transaction->AllocateNonce(candidate);
                                    }
                                }
                            });
                    }
                });
            session->FlushLoads();
        }

        std::vector<TNonce> nonceList(nonceSet.begin(), nonceSet.end());
        YT_LOG_DEBUG("IP6 nonces generated (NodeId: %v, Nonces: %v)",
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
    TInternetAddressManager* internetAddressManager,
    TPod* pod)
{
    Impl_->UpdatePodAddresses(
        transaction,
        internetAddressManager,
        pod);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NNet


#include "porto.h"

#include <yp/server/objects/node.h>
#include <yp/server/objects/pod.h>

#include <yp/client/api/proto/data_model.pb.h>

#include <yt/core/misc/format.h>

namespace NYP {
namespace NServer {
namespace NNodes {

using namespace NObjects;

////////////////////////////////////////////////////////////////////////////////

static const THashSet<TString> HostDeviceWhitelist{
    "/dev/kvm",
    "/dev/net/tun"
};

static const std::vector<TString> SysctlPrefixWhitelist{
    "net.ipv4.conf.",
    "net.ipv6.conf.",
    "net.ipv4.neigh.",
    "net.ipv6.neigh."
};

static const std::vector<TString> SysctlPrefixBlacklist {
    "net.ipv4.neigh.default.",
    "net.ipv6.neigh.default."
};

static const THashSet<TString> SysctlWhitelist{
    "net.core.somaxconn",
    "net.unix.max_dgram_qlen",
    "net.ipv4.icmp_echo_ignore_all",
    "net.ipv4.icmp_echo_ignore_broadcasts",
    "net.ipv4.icmp_ignore_bogus_error_responses",
    "net.ipv4.icmp_errors_use_inbound_ifaddr",
    "net.ipv4.icmp_ratelimit",
    "net.ipv4.icmp_ratemask",
    "net.ipv4.ping_group_range",
    "net.ipv4.tcp_ecn",
    "net.ipv4.tcp_ecn_fallback",
    "net.ipv4.ip_dynaddr",
    "net.ipv4.ip_early_demux",
    "net.ipv4.ip_default_ttl",
    "net.ipv4.ip_local_port_range",
    "net.ipv4.ip_local_reserved_ports",
    "net.ipv4.ip_no_pmtu_disc",
    "net.ipv4.ip_forward_use_pmtu",
    "net.ipv4.ip_nonlocal_bind",
    "net.ipv4.tcp_mtu_probing",
    "net.ipv4.tcp_base_mss",
    "net.ipv4.tcp_probe_threshold",
    "net.ipv4.tcp_probe_interval",
    "net.ipv4.tcp_keepalive_time",
    "net.ipv4.tcp_keepalive_probes",
    "net.ipv4.tcp_keepalive_intvl",
    "net.ipv4.tcp_syn_retries",
    "net.ipv4.tcp_synack_retries",
    "net.ipv4.tcp_syncookies",
    "net.ipv4.tcp_reordering",
    "net.ipv4.tcp_retries1",
    "net.ipv4.tcp_retries2",
    "net.ipv4.tcp_orphan_retries",
    "net.ipv4.tcp_fin_timeout",
    "net.ipv4.tcp_notsent_lowat",
    "net.ipv4.tcp_tw_reuse",
    "net.ipv6.bindv6only",
    "net.ipv6.ip_nonlocal_bind",
    "net.ipv6.icmp.ratelimit",
    "net.ipv6.route.gc_thresh",
    "net.ipv6.route.max_size",
    "net.ipv6.route.gc_min_interval",
    "net.ipv6.route.gc_timeout",
    "net.ipv6.route.gc_interval",
    "net.ipv6.route.gc_elasticity",
    "net.ipv6.route.mtu_expires",
    "net.ipv6.route.min_adv_mss",
    "net.ipv6.route.gc_min_interval_ms"
};

void ValidateHostDeviceSpec(const NClient::NApi::NProto::TPodSpec_THostDevice& spec)
{
    const auto& path = spec.path();

    if (HostDeviceWhitelist.count(path) > 0) {
        return;
    }

    THROW_ERROR_EXCEPTION("Host device %Qv cannot be configured",
        path);
}

void ValidateSysctlProperty(const NClient::NApi::NProto::TPodSpec_TSysctlProperty& spec)
{
    const auto& name = spec.name();

    for (const auto& prefix : SysctlPrefixBlacklist) {
        if (name.StartsWith(prefix)) {
            THROW_ERROR_EXCEPTION("Sysctl property %Qv cannot be set since all %Qv properties are blacklisted",
                name,
                prefix + "*");
        }
    }

    for (const auto& prefix : SysctlPrefixWhitelist) {
        if (name.StartsWith(prefix)) {
            return;
        }
    }

    if (SysctlWhitelist.count(name) > 0) {
        return;
    }

    THROW_ERROR_EXCEPTION("Sysctl property %Qv cannot be set since it is not whitelisted",
        name);
}

std::vector<std::pair<TString, TString>> BuildPortoProperties(TNode* node, TPod* pod)
{
    std::vector<std::pair<TString, TString>> result;

    const auto& specOther = pod->Spec().Other().Load();
    const auto& statusOther = pod->Status().Other().Load();

    // Limits, guarantees
    auto vcpuToPortoCpu = [&] (ui64 vcpu) {
        return Format("%.3fc", vcpu / node->Spec().Load().cpu_to_vcpu_factor() / 1000);
    };

    const auto& requests = specOther.resource_requests();
    result.emplace_back("cpu_guarantee", vcpuToPortoCpu(requests.vcpu_guarantee()));
    if (requests.has_vcpu_limit()) {
        result.emplace_back("cpu_limit", vcpuToPortoCpu(requests.vcpu_limit()));
    }
    result.emplace_back("memory_guarantee", ToString(requests.memory_guarantee()));
    if (requests.has_memory_limit()) {
        result.emplace_back("memory_limit", ToString(requests.memory_limit()));
    }
    if (requests.has_anonymous_memory_limit()) {
        result.emplace_back("anon_limit", ToString(requests.anonymous_memory_limit()));
    }
    if (requests.has_dirty_memory_limit()) {
        result.emplace_back("dirty_limit", ToString(requests.dirty_memory_limit()));
    }

    // Network
    result.emplace_back("hostname", statusOther.dns().transient_fqdn());
    result.emplace_back("net", "L3 veth");
    const auto& ip6AddressAllocations = statusOther.ip6_address_allocations();
    if (!ip6AddressAllocations.empty()) {
        result.emplace_back("ip", JoinToString(
            ip6AddressAllocations.begin(),
            ip6AddressAllocations.end(),
            [] (TStringBuilder* builder, const auto& allocation) {
                builder->AppendFormat("veth %v", allocation.address());
            },
            AsStringBuf(";")));
    }

    // Host devices
    const auto& hostDevices = specOther.host_devices();
    if (!hostDevices.empty()) {
        result.emplace_back("devices", JoinToString(
            hostDevices.begin(),
            hostDevices.end(),
            [] (TStringBuilder* builder, const auto& device) {
                builder->AppendFormat("%v %v", device.path(), device.mode());
            },
            AsStringBuf(";")));
    }

    // Sysctl properties
    const auto& sysctlProperties = specOther.sysctl_properties();
    if (!sysctlProperties.empty()) {
        result.emplace_back("sysctl", JoinToString(
            sysctlProperties.begin(),
            sysctlProperties.end(),
            [] (TStringBuilder* builder, const auto& property) {
                builder->AppendFormat("%v:%v", property.name(), property.value());
            },
            AsStringBuf(";")));
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodes
} // namespace NServer
} // namespace NYP


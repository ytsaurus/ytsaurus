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

static const size_t DefaultIP4Mtu = 1450;
static const size_t DefaultIP6Mtu = 1450;
static const size_t DefaultInternetTunnelMtu = 1400;
static const TStringBuf DefaultDecapsulatorAnycastAddress{"2a02:6b8:0:3400::aaaa"};
static const TStringBuf DefaultTunnelFarmAddress{"2a02:6b8:b010:a0ff::1"};
static const TStringBuf BackboneVlanID{"backbone"};

static const THashSet<TString> HostDeviceWhitelist{
    "/dev/kvm",
    "/dev/net/tun"
};

static const THashSet<char> HostDeviceModeAllowedSymbols{
    '-', 'm', 'r', 'w'
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

namespace {

void ValidateBackboneIPsForTunnel(const NProto::TPodStatusOther& statusOther)
{
    size_t backboneIPCount = 0;
    for (const auto& ip6Address : statusOther.ip6_address_allocations()) {
        if (ip6Address.vlan_id() == BackboneVlanID) {
            ++backboneIPCount;
        }
    }

    // TODO(avitella): Intelligent backbone address selection with "filter". YP-230
    if (backboneIPCount != 1) {
        THROW_ERROR_EXCEPTION("Expected exactly one backbone IP6 address but found %v", backboneIPCount);
    }
}

TString GetBackboneIPForTunnel(const NProto::TPodStatusOther& statusOther)
{
    for (const auto& ip6Address : statusOther.ip6_address_allocations()) {
        if (ip6Address.vlan_id() == BackboneVlanID) {
            return ip6Address.address();
        }
    }
    THROW_ERROR_EXCEPTION("There is no suitable backbone address");
}

struct TInternetTunnelProperties
{
    TString IPIP6;
    TString Address;
    TString Mtu;
};

std::vector<TInternetTunnelProperties> CreateInternetTunnelsProperties(const NProto::TPodStatusOther& statusOther)
{
    std::vector<TInternetTunnelProperties> props;

    for (const auto& ip6Address : statusOther.ip6_address_allocations()) {
        if (!ip6Address.has_internet_address()) {
            continue;
        }

        const auto& internetAddress = ip6Address.internet_address();
        const auto name = Format("ip_ext_tun%v", props.size());

        TInternetTunnelProperties prop;
        prop.IPIP6 = Format("ipip6 %v %v %v", name, DefaultTunnelFarmAddress, ip6Address.address());
        prop.Address = Format("%v %v", name, internetAddress.ip4_address());
        prop.Mtu = Format("MTU %v %v", name, DefaultInternetTunnelMtu);

        props.push_back(prop);
    }

    return props;
}

} // namespace

void ValidateHostDeviceSpec(const NClient::NApi::NProto::TPodSpec_THostDevice& spec)
{
    const auto& path = spec.path();

    if (!HostDeviceWhitelist.has(path)) {
        THROW_ERROR_EXCEPTION("Host device %Qv cannot be configured",
            path);
    }

    for (char modeSymbol : spec.mode()) {
        if (!HostDeviceModeAllowedSymbols.has(modeSymbol)) {
            THROW_ERROR_EXCEPTION("Host device %Qv cannot be configured with %Qv mode",
                path,
                modeSymbol);
        }
    }
}

void ValidateSysctlProperty(const NClient::NApi::NProto::TPodSpec_TSysctlProperty& spec)
{
    const auto& name = spec.name();
    const auto& value = spec.value();

    for (char symbol : value) {
        if (symbol == ';') {
            THROW_ERROR_EXCEPTION("%Qv symbol is not allowed in %Qv sysctl property",
                symbol,
                name);
        }
    }

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

    if (SysctlWhitelist.has(name)) {
        return;
    }

    THROW_ERROR_EXCEPTION("Sysctl property %Qv cannot be set since it is not whitelisted",
        name);
}

std::vector<std::pair<TString, TString>> BuildPortoProperties(
    const NClient::NApi::NProto::TNodeSpec& nodeSpec,
    const NProto::TPodSpecOther& podSpecOther,
    const NProto::TPodStatusOther& podStatusOther)
{
    std::vector<std::pair<TString, TString>> result;

    // Limits, guarantees
    auto vcpuToPortoCpu = [&] (ui64 vcpu) {
        return Format("%.3fc", vcpu / nodeSpec.cpu_to_vcpu_factor() / 1000);
    };

    const auto& requests = podSpecOther.resource_requests();
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

    // Internet tunnels
    const auto internetTunnelsProps = CreateInternetTunnelsProperties(podStatusOther);

    // Network
    result.emplace_back("hostname", podStatusOther.dns().transient_fqdn());

    const auto& vsTunnel = podSpecOther.virtual_service_tunnel();
    const auto& vsStatus = podStatusOther.virtual_service();

    std::vector<TString> netTokens;
    netTokens.emplace_back("L3 veth");

    TStringBuf decapsulatorAnycastAddress = DefaultDecapsulatorAnycastAddress;
    if (vsTunnel.has_decapsulator_anycast_address()) {
        decapsulatorAnycastAddress = vsTunnel.decapsulator_anycast_address();
    }
    if (!vsStatus.ip4_addresses().empty()) {
        size_t ip4Mtu = DefaultIP4Mtu;
        if (vsTunnel.has_ip4_mtu()) {
            ip4Mtu = vsTunnel.ip4_mtu();
        }

        ValidateBackboneIPsForTunnel(podStatusOther);
        netTokens.emplace_back(Format("ipip6 tun0 %v %v", decapsulatorAnycastAddress, GetBackboneIPForTunnel(podStatusOther)));
        netTokens.emplace_back(Format("MTU tun0 %v", ip4Mtu));
    }
    if (!vsStatus.ip6_addresses().empty()) {
        size_t ip6Mtu = DefaultIP6Mtu;
        if (vsTunnel.has_ip6_mtu()) {
            ip6Mtu = vsTunnel.ip6_mtu();
        }
        netTokens.emplace_back(Format("MTU ip6tnl0 %v", ip6Mtu));
    }
    for (const auto& prop : internetTunnelsProps) {
        netTokens.emplace_back(prop.IPIP6);
        netTokens.emplace_back(prop.Mtu);
    }
    result.emplace_back("net", JoinToString(netTokens.begin(), netTokens.end(), AsStringBuf(";")));

 
    std::vector<TString> addresses;
    for (const auto& ip6Address : podStatusOther.ip6_address_allocations()) {
        addresses.emplace_back(Format("veth %v", ip6Address.address()));
    }
    for (const auto& ip6tun : vsStatus.ip6_addresses()) {
        addresses.emplace_back(Format("ip6tnl0 %v", ip6tun));
    }
    for (const auto& ip4tun : vsStatus.ip4_addresses()) {
        addresses.emplace_back(Format("tun0 %v", ip4tun));
    }
    for (const auto& internetTunnel : internetTunnelsProps) {
        addresses.emplace_back(internetTunnel.Address);
    }
    if (!addresses.empty()) {
        result.emplace_back("ip", JoinToString(addresses.begin(), addresses.end(), AsStringBuf(";")));
    }

    // Host devices
    const auto& hostDevices = podSpecOther.host_devices();
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
    std::vector<TString> sysctlProperties;
    for (const auto& property : podSpecOther.sysctl_properties()) {
        sysctlProperties.emplace_back(Format("%v:%v", property.name(), property.value()));
    }
    if (!vsStatus.ip4_addresses().empty()) {
        sysctlProperties.emplace_back("net.ipv4.conf.all.rp_filter:0");
        sysctlProperties.emplace_back("net.ipv4.conf.default.rp_filter:0");
        sysctlProperties.emplace_back("net.ipv4.conf.veth.rp_filter:0");
        sysctlProperties.emplace_back("net.ipv4.conf.ip6tnl0.rp_filter:0");
    }
    if (!sysctlProperties.empty()) {
        result.emplace_back("sysctl", JoinToString(sysctlProperties.begin(), sysctlProperties.end(), AsStringBuf(";")));
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodes
} // namespace NServer
} // namespace NYP


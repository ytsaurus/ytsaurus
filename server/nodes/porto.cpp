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

static const ui32 DefaultIP4Mtu = 1450;
static const ui32 DefaultIP6Mtu = 1450;
static const ui32 DefaultInternetTunnelMtu = 1400;
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

struct TVirtualServicesProperties
{
    std::vector<TString> Net;
    std::vector<TString> Addresses;
    std::vector<TString> SysCtl;
};

TVirtualServicesProperties CreateVirtualServicesProperties(
    const NProto::TPodSpecOther& podSpecOther,
    const NProto::TPodStatusOther& podStatusOther)
{
    TVirtualServicesProperties props;

    const auto& options = podSpecOther.virtual_service_options();

    TStringBuf decapsulatorAnycastAddress = DefaultDecapsulatorAnycastAddress;
    if (options.has_decapsulator_anycast_address()) {
        decapsulatorAnycastAddress = options.decapsulator_anycast_address();
    }

    ui32 ip6Mtu = DefaultIP6Mtu;
    if (options.has_ip6_mtu()) {
        ip6Mtu = options.ip6_mtu();
    }

    ui32 ip4Mtu = DefaultIP4Mtu;
    if (options.has_ip4_mtu()) {
        ip4Mtu = options.ip4_mtu();
    }

    bool hasIP4VirtualServices = false;
    bool hasIP6VirtualServices = false;

    auto processIP6AddressVirtualServices = [&] (const NClient::NApi::NProto::TPodStatus_TIP6AddressAllocation& allocation) {
        bool allocationHasIP4VirtualServices = false;

        for (const auto& vs : allocation.virtual_services()) {
            if (!vs.ip4_addresses().empty()) {
                allocationHasIP4VirtualServices = true;
                hasIP4VirtualServices = true;
            }

            if (!vs.ip6_addresses().empty()) {
                hasIP6VirtualServices = true;
            }

            for (const auto& ip4Address : vs.ip4_addresses()) {
                props.Addresses.emplace_back(Format("tun0 %v", ip4Address));
            }

            for (const auto& ip6Address : vs.ip6_addresses()) {
                props.Addresses.emplace_back(Format("ip6tnl0 %v", ip6Address));
            }
        }

        if (allocationHasIP4VirtualServices) {
            props.Net.emplace_back(Format(
                "ipip6 tun0 %v %v",
                decapsulatorAnycastAddress,
                allocation.address()));
        }
    };

    for (const auto& allocation : podStatusOther.ip6_address_allocations()) {
        processIP6AddressVirtualServices(allocation);
    }

    if (hasIP4VirtualServices) {
        props.Net.emplace_back(Format("MTU tun0 %v", ip4Mtu));
        props.SysCtl.emplace_back("net.ipv4.conf.all.rp_filter:0");
        props.SysCtl.emplace_back("net.ipv4.conf.default.rp_filter:0");
        props.SysCtl.emplace_back("net.ipv4.conf.veth.rp_filter:0");
        props.SysCtl.emplace_back("net.ipv4.conf.ip6tnl0.rp_filter:0");
    }
    if (hasIP6VirtualServices) {
        props.Net.emplace_back(Format("MTU ip6tnl0 %v", ip6Mtu));
    }

    Sort(props.Addresses);
    props.Addresses.erase(Unique(props.Addresses.begin(), props.Addresses.end()), props.Addresses.end());

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
    const NClient::NApi::NProto::TResourceSpec_TCpuSpec& cpuSpec,
    const NProto::TPodSpecOther& podSpecOther,
    const NProto::TPodStatusOther& podStatusOther)
{
    std::vector<std::pair<TString, TString>> result;

    // Limits, guarantees
    auto vcpuToPortoCpu = [&] (ui64 vcpu) {
        return Format("%.3fc", vcpu / cpuSpec.cpu_to_vcpu_factor() / 1000);
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

    // VirtualService tunnels
    const auto virtualServicesProps = CreateVirtualServicesProperties(podSpecOther, podStatusOther);

    // Network
    switch (podSpecOther.host_name_kind()) {
        case NClient::NApi::NProto::PHNK_TRANSIENT:
            result.emplace_back("hostname", podStatusOther.dns().transient_fqdn());
            break;
        case NClient::NApi::NProto::PHNK_PERSISTENT:
            result.emplace_back("hostname", podStatusOther.dns().persistent_fqdn());
            break;
        default:
            Y_UNREACHABLE();
    }

    std::vector<TString> netTokens;
    netTokens.emplace_back("L3 veth");

    for (const auto& net: virtualServicesProps.Net) {
        netTokens.emplace_back(net);
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
    for (const auto& internetTunnel : internetTunnelsProps) {
        addresses.emplace_back(internetTunnel.Address);
    }
    for (const auto& vsTunnel : virtualServicesProps.Addresses) {
        addresses.emplace_back(vsTunnel);
    }
    for (const auto& ip6Subnet : podStatusOther.ip6_subnet_allocations()) {
        addresses.emplace_back(Format("veth %v", ip6Subnet.subnet()));
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
    for (const auto& property : virtualServicesProps.SysCtl) {
        sysctlProperties.emplace_back(property);
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


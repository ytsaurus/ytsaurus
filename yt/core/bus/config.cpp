#include "config.h"

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

TTcpBusServerConfigPtr TTcpBusServerConfig::CreateTcp(int port)
{
    auto config = New<TTcpBusServerConfig>();
    config->Port = port;
    return config;
}

TTcpBusServerConfigPtr TTcpBusServerConfig::CreateUnixDomain(const Stroka& address)
{
    auto config = New<TTcpBusServerConfig>();
    config->UnixDomainName = address;
    return config;
}

TTcpBusClientConfigPtr TTcpBusClientConfig::CreateTcp(const Stroka& address)
{
    auto config = New<TTcpBusClientConfig>();
    config->Address = address;
    return config;
}

TTcpBusClientConfigPtr TTcpBusClientConfig::CreateUnixDomain(const Stroka& address)
{
    auto config = New<TTcpBusClientConfig>();
    config->UnixDomainName = address;
    return config;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
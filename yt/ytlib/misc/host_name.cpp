#include "stdafx.h"
#include "host_name.h"

#include <util/system/hostname.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

Stroka GetHostName()
{
    return ::GetHostName();
}

Stroka BuildServiceAddress(const Stroka& hostName, int port)
{
    return hostName + ":" + ToString(port);
}

void ParseServiceAddress(const Stroka& address, Stroka* hostName, int* port)
{
    int colonIndex = address.find_first_of(':');
    if (colonIndex == Stroka::npos) {
        ythrow yexception() << Sprintf("Service address %s is malformed",
            ~address.Quote());
    }

    if (hostName) {
        *hostName =  address.substr(0, colonIndex);
    }

    if (port) {
        *port = FromString<int>(address.substr(colonIndex + 1));
    }
}

int GetServicePort(const Stroka& address)
{
    int result;
    ParseServiceAddress(address, NULL, &result);
    return result;
}

Stroka GetServiceHostName(const Stroka& address)
{
    Stroka result;
    ParseServiceAddress(address, &result, NULL);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT


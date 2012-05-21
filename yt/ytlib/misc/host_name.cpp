#include "stdafx.h"
#include "host_name.h"

#include <util/system/hostname.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TStringBuf GetHostName()
{
    return ::GetHostName();
}

Stroka BuildServiceAddress(const TStringBuf& hostName, int port)
{
    return Stroka(hostName) + ":" + ToString(port);
}

void ParseServiceAddress(const TStringBuf& address, TStringBuf* hostName, int* port)
{
    int colonIndex = address.find_last_of(':');
    if (colonIndex == Stroka::npos) {
        ythrow yexception() << Sprintf("Service address %s is malformed, <host>:<port> format is expected",
            ~Stroka(address).Quote());
    }

    if (hostName) {
        *hostName = address.substr(0, colonIndex);
    }

    if (port) {
        *port = FromString<int>(address.substr(colonIndex + 1));
    }
}

int GetServicePort(const TStringBuf& address)
{
    int result;
    ParseServiceAddress(address, NULL, &result);
    return result;
}

TStringBuf GetServiceHostName(const TStringBuf& address)
{
    TStringBuf result;
    ParseServiceAddress(address, &result, NULL);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT


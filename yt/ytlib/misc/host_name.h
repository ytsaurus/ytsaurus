#pragma once

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Returns the FQDN of the local host.
Stroka GetHostName();

//! Constructs an address of the form |hostName:port|.
Stroka BuildServiceAddress(const Stroka& hostName, int port);

//! Parses service address into host name and port number.
//! Both #hostName and #port can be |NULL|.
//! Throws if the address is malformed.
void ParseServiceAddress(
    const Stroka& address,
    Stroka* hostName,
    int* port);

//! Extracts port number from a service address.
//! Throws if the address is malformed.
int GetServicePort(const Stroka& address);

//! Extracts host name from a service address.
Stroka GetServiceHostName(const Stroka& address);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
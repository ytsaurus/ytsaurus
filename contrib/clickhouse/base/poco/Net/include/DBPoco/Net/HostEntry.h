//
// HostEntry.h
//
// Library: Net
// Package: NetCore
// Module:  HostEntry
//
// Definition of the HostEntry class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_Net_HostEntry_INCLUDED
#define DB_Net_HostEntry_INCLUDED


#include <vector>
#include "DBPoco/Net/IPAddress.h"
#include "DBPoco/Net/Net.h"
#include "DBPoco/Net/SocketDefs.h"


namespace DBPoco
{
namespace Net
{


    class Net_API HostEntry
    /// This class stores information about a host
    /// such as host name, alias names and a list
    /// of IP addresses.
    {
    public:
        typedef std::vector<std::string> AliasList;
        typedef std::vector<IPAddress> AddressList;

        HostEntry();
        /// Creates an empty HostEntry.

        HostEntry(struct hostent * entry);
        /// Creates the HostEntry from the data in a hostent structure.

#if defined(DB_POCO_HAVE_IPv6) || defined(DB_POCO_HAVE_ADDRINFO)
        HostEntry(struct addrinfo * info);
        /// Creates the HostEntry from the data in an addrinfo structure.
#endif


        HostEntry(const HostEntry & entry);
        /// Creates the HostEntry by copying another one.

        HostEntry & operator=(const HostEntry & entry);
        /// Assigns another HostEntry.

        void swap(HostEntry & hostEntry);
        /// Swaps the HostEntry with another one.

        ~HostEntry();
        /// Destroys the HostEntry.

        const std::string & name() const;
        /// Returns the canonical host name.

        const AliasList & aliases() const;
        /// Returns a vector containing alias names for
        /// the host name.

        const AddressList & addresses() const;
        /// Returns a vector containing the IPAddresses
        /// for the host.

    private:
        std::string _name;
        AliasList _aliases;
        AddressList _addresses;
    };


    //
    // inlines
    //
    inline const std::string & HostEntry::name() const
    {
        return _name;
    }


    inline const HostEntry::AliasList & HostEntry::aliases() const
    {
        return _aliases;
    }


    inline const HostEntry::AddressList & HostEntry::addresses() const
    {
        return _addresses;
    }


    inline void swap(HostEntry & h1, HostEntry & h2)
    {
        h1.swap(h2);
    }


}
} // namespace DBPoco::Net


#endif // DB_Net_HostEntry_INCLUDED

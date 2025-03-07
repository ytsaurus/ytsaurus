//
// NamePool.h
//
// Library: XML
// Package: XML
// Module:  NamePool
//
// Definition of the NamePool class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_XML_NamePool_INCLUDED
#define DB_XML_NamePool_INCLUDED


#include "DBPoco/XML/Name.h"
#include "DBPoco/XML/XML.h"
#include "DBPoco/XML/XMLString.h"


#ifndef DB_POCO_XML_NAMEPOOL_DEFAULT_SIZE
#    define DB_POCO_XML_NAMEPOOL_DEFAULT_SIZE 509
#endif


namespace DBPoco
{
namespace XML
{


    class NamePoolItem;


    class XML_API NamePool
    /// A hashtable that stores XML names consisting of an URI, a
    /// local name and a qualified name.
    {
    public:
        NamePool(unsigned long size = DB_POCO_XML_NAMEPOOL_DEFAULT_SIZE);
        /// Creates a name pool with room for up to size strings.
        ///
        /// The given size should be a suitable prime number,
        /// e.g. 251, 509, 1021 or 4093.

        const Name & insert(const XMLString & qname, const XMLString & namespaceURI, const XMLString & localName);
        /// Returns a const reference to an Name for the given names.
        /// Creates the Name if it does not already exist.
        /// Throws a PoolOverflowException if the name pool is full.

        const Name & insert(const Name & name);
        /// Returns a const reference to an Name for the given name.
        /// Creates the Name if it does not already exist.
        /// Throws a PoolOverflowException if the name pool is full.

        void duplicate();
        /// Increments the reference count.

        void release();
        /// Decrements the reference count and deletes the object if the reference count reaches zero.

    protected:
        unsigned long hash(const XMLString & qname, const XMLString & namespaceURI, const XMLString & localName);
        ~NamePool();

    private:
        NamePool(const NamePool &);
        NamePool & operator=(const NamePool &);

        NamePoolItem * _pItems;
        unsigned long _size;
        unsigned long _salt;
        int _rc;
    };


}
} // namespace DBPoco::XML


#endif // DB_XML_NamePool_INCLUDED

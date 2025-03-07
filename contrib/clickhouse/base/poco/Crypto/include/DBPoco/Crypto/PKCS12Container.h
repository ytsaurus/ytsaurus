//
// PKCS12Container.h
//
// Library: Crypto
// Package: Certificate
// Module:  PKCS12Container
//
// Definition of the PKCS12Container class.
//
// Copyright (c) 2006-2009, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_Crypto_PKCS12Container_INCLUDED
#define DB_Crypto_PKCS12Container_INCLUDED


#include <istream>
#include <memory>
#include <openssl/pkcs12.h>
#include "DBPoco/Crypto/Crypto.h"
#include "DBPoco/Crypto/EVPPKey.h"
#include "DBPoco/Crypto/OpenSSLInitializer.h"
#include "DBPoco/Crypto/X509Certificate.h"
#include "DBPoco/Path.h"


namespace DBPoco
{
namespace Crypto
{


    class Crypto_API PKCS12Container
    /// This class implements PKCS#12 container functionality.
    {
    public:
        typedef X509Certificate::List CAList;
        typedef std::vector<std::string> CANameList;

        explicit PKCS12Container(std::istream & istr, const std::string & password = "");
        /// Creates the PKCS12Container object from a stream.

        explicit PKCS12Container(const std::string & path, const std::string & password = "");
        /// Creates the PKCS12Container object from a file.

        PKCS12Container(const PKCS12Container & cont);
        /// Copy constructor.

        PKCS12Container & operator=(const PKCS12Container & cont);
        /// Assignment operator.

#ifdef DB_POCO_ENABLE_CPP11

        PKCS12Container(PKCS12Container && cont);
        /// Move constructor.

        PKCS12Container & operator=(PKCS12Container && cont);
        /// Move assignment operator.

#endif // DB_POCO_ENABLE_CPP11

        ~PKCS12Container();
        /// Destroys the PKCS12Container.

        bool hasKey() const;
        /// Returns true if container contains the key.

        EVPPKey getKey() const;
        /// Return key as openssl EVP_PKEY wrapper object.

        bool hasX509Certificate() const;
        /// Returns true if container has X509 certificate.

        const X509Certificate & getX509Certificate() const;
        /// Returns the X509 certificate.
        /// Throws NotFoundException if there is no certificate.

        const CAList & getCACerts() const;
        /// Returns the list of CA certificates in this container.

        const std::string & getFriendlyName() const;
        /// Returns the friendly name of the certificate bag.

        const CANameList & getFriendlyNamesCA() const;
        /// Returns a list of CA certificates friendly names.

    private:
        void load(PKCS12 * pPKCS12, const std::string & password = "");
        std::string extractFriendlyName(X509 * pCert);

#ifdef DB_POCO_ENABLE_CPP11
        typedef std::unique_ptr<X509Certificate> CertPtr;
#else
        typedef std::auto_ptr<X509Certificate> CertPtr;
#endif // #ifdef DB_POCO_ENABLE_CPP11

        OpenSSLInitializer _openSSLInitializer;
        EVP_PKEY * _pKey;
        CertPtr _pX509Cert;
        CAList _caCertList;
        CANameList _caCertNames;
        std::string _pkcsFriendlyName;
    };


    //
    // inlines
    //

    inline bool PKCS12Container::hasX509Certificate() const
    {
        return _pX509Cert.get() != 0;
    }


    inline const X509Certificate & PKCS12Container::getX509Certificate() const
    {
        if (!hasX509Certificate())
            throw NotFoundException("PKCS12Container X509 certificate");
        return *_pX509Cert;
    }


    inline const std::string & PKCS12Container::getFriendlyName() const
    {
        return _pkcsFriendlyName;
    }


    inline const PKCS12Container::CAList & PKCS12Container::getCACerts() const
    {
        return _caCertList;
    }


    inline const PKCS12Container::CANameList & PKCS12Container::getFriendlyNamesCA() const
    {
        return _caCertNames;
    }


    inline bool PKCS12Container::hasKey() const
    {
        return _pKey != 0;
    }


    inline EVPPKey PKCS12Container::getKey() const
    {
        return EVPPKey(_pKey);
    }


}
} // namespace DBPoco::Crypto


#endif // DB_Crypto_PKCS12Container_INCLUDED

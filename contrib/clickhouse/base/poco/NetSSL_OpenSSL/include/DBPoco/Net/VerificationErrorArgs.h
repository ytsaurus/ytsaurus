//
// VerificationErrorArgs.h
//
// Library: NetSSL_OpenSSL
// Package: SSLCore
// Module:  VerificationErrorArgs
//
// Definition of the VerificationErrorArgs class.
//
// Copyright (c) 2006-2009, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_NetSSL_VerificationErrorArgs_INCLUDED
#define DB_NetSSL_VerificationErrorArgs_INCLUDED


#include "DBPoco/Net/NetSSL.h"
#include "DBPoco/Net/X509Certificate.h"


namespace DBPoco
{
namespace Net
{


    class NetSSL_API VerificationErrorArgs
    /// A utility class for certificate error handling.
    {
    public:
        VerificationErrorArgs(const X509Certificate & cert, int errDepth, int errNum, const std::string & errMsg);
        /// Creates the VerificationErrorArgs. _ignoreError is per default set to false.

        ~VerificationErrorArgs();
        /// Destroys the VerificationErrorArgs.

        const X509Certificate & certificate() const;
        /// Returns the certificate that caused the error.

        int errorDepth() const;
        /// Returns the position of the certificate in the certificate chain.

        int errorNumber() const;
        /// Returns the id of the error

        const std::string & errorMessage() const;
        /// Returns the textual presentation of the errorNumber.

        void setIgnoreError(bool ignoreError);
        /// setIgnoreError to true, if a verification error is judged non-fatal by the user.

        bool getIgnoreError() const;
        /// returns the value of _ignoreError

    private:
        X509Certificate _cert;
        int _errorDepth;
        int _errorNumber;
        std::string _errorMessage; /// Textual representation of the _errorNumber
        bool _ignoreError;
    };


    //
    // inlines
    //
    inline const X509Certificate & VerificationErrorArgs::certificate() const
    {
        return _cert;
    }


    inline int VerificationErrorArgs::errorDepth() const
    {
        return _errorDepth;
    }


    inline int VerificationErrorArgs::errorNumber() const
    {
        return _errorNumber;
    }


    inline const std::string & VerificationErrorArgs::errorMessage() const
    {
        return _errorMessage;
    }


    inline void VerificationErrorArgs::setIgnoreError(bool ignoreError)
    {
        _ignoreError = ignoreError;
    }


    inline bool VerificationErrorArgs::getIgnoreError() const
    {
        return _ignoreError;
    }


}
} // namespace DBPoco::Net


#endif // DB_NetSSL_VerificationErrorArgs_INCLUDED

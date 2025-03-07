//
// KeyFileHandler.h
//
// Library: NetSSL_OpenSSL
// Package: SSLCore
// Module:  KeyFileHandler
//
// Definition of the KeyFileHandler class.
//
// Copyright (c) 2006-2009, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_NetSSL_KeyFileHandler_INCLUDED
#define DB_NetSSL_KeyFileHandler_INCLUDED


#include "DBPoco/Net/NetSSL.h"
#include "DBPoco/Net/PrivateKeyPassphraseHandler.h"


namespace DBPoco
{
namespace Net
{


    class NetSSL_API KeyFileHandler : public PrivateKeyPassphraseHandler
    /// An implementation of PrivateKeyPassphraseHandler that
    /// reads the key for a certificate from a configuration file
    /// under the path "openSSL.privateKeyPassphraseHandler.options.password".
    {
    public:
        KeyFileHandler(bool server);
        /// Creates the KeyFileHandler.

        virtual ~KeyFileHandler();
        /// Destroys the KeyFileHandler.

        void onPrivateKeyRequested(const void * pSender, std::string & privateKey);

    private:
        static const std::string CFG_PRIV_KEY_FILE;
    };


}
} // namespace DBPoco::Net


#endif // DB_NetSSL_KeyFileHandler_INCLUDED

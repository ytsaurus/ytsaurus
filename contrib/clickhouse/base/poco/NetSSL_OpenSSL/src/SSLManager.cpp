//
// SSLManager.cpp
//
// Library: NetSSL_OpenSSL
// Package: SSLCore
// Module:  SSLManager
//
// Copyright (c) 2006-2010, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DBPoco/Net/SSLManager.h"
#include "DBPoco/Net/Context.h"
#include "DBPoco/Net/Utility.h"
#include "DBPoco/Net/PrivateKeyPassphraseHandler.h"
#include "DBPoco/Net/RejectCertificateHandler.h"
#include "DBPoco/Crypto/OpenSSLInitializer.h"
#include "DBPoco/Net/SSLException.h"
#include "DBPoco/SingletonHolder.h"
#include "DBPoco/Delegate.h"
#include "DBPoco/StringTokenizer.h"
#include "DBPoco/Util/Application.h"
#include "DBPoco/Util/OptionException.h"


namespace DBPoco {
namespace Net {


const std::string SSLManager::CFG_PRIV_KEY_FILE("privateKeyFile");
const std::string SSLManager::CFG_CERTIFICATE_FILE("certificateFile");
const std::string SSLManager::CFG_CA_LOCATION("caConfig");
const std::string SSLManager::CFG_VER_MODE("verificationMode");
const Context::VerificationMode SSLManager::VAL_VER_MODE(Context::VERIFY_RELAXED);
const std::string SSLManager::CFG_VER_DEPTH("verificationDepth");
const int         SSLManager::VAL_VER_DEPTH(9);
const std::string SSLManager::CFG_ENABLE_DEFAULT_CA("loadDefaultCAFile");
const bool        SSLManager::VAL_ENABLE_DEFAULT_CA(true);
const std::string SSLManager::CFG_CIPHER_LIST("cipherList");
const std::string SSLManager::CFG_CYPHER_LIST("cypherList");
const std::string SSLManager::VAL_CIPHER_LIST("ALL:!ADH:!LOW:!EXP:!MD5:@STRENGTH");
const std::string SSLManager::CFG_PREFER_SERVER_CIPHERS("preferServerCiphers");
const std::string SSLManager::CFG_DELEGATE_HANDLER("privateKeyPassphraseHandler.name");
const std::string SSLManager::VAL_DELEGATE_HANDLER("KeyConsoleHandler");
const std::string SSLManager::CFG_CERTIFICATE_HANDLER("invalidCertificateHandler.name");
const std::string SSLManager::VAL_CERTIFICATE_HANDLER("RejectCertificateHandler");
const std::string SSLManager::CFG_SERVER_PREFIX("openSSL.server.");
const std::string SSLManager::CFG_CLIENT_PREFIX("openSSL.client.");
const std::string SSLManager::CFG_CACHE_SESSIONS("cacheSessions");
const std::string SSLManager::CFG_SESSION_ID_CONTEXT("sessionIdContext");
const std::string SSLManager::CFG_SESSION_CACHE_SIZE("sessionCacheSize");
const std::string SSLManager::CFG_SESSION_TIMEOUT("sessionTimeout");
const std::string SSLManager::CFG_EXTENDED_VERIFICATION("extendedVerification");
const std::string SSLManager::CFG_REQUIRE_TLSV1("requireTLSv1");
const std::string SSLManager::CFG_REQUIRE_TLSV1_1("requireTLSv1_1");
const std::string SSLManager::CFG_REQUIRE_TLSV1_2("requireTLSv1_2");
const std::string SSLManager::CFG_DISABLE_PROTOCOLS("disableProtocols");
const std::string SSLManager::CFG_DH_PARAMS_FILE("dhParamsFile");
const std::string SSLManager::CFG_ECDH_CURVE("ecdhCurve");
#ifdef OPENSSL_FIPS
const std::string SSLManager::CFG_FIPS_MODE("openSSL.fips");
const bool        SSLManager::VAL_FIPS_MODE(false);
#endif


SSLManager::SSLManager()
{
}


SSLManager::~SSLManager()
{
	try
	{
		shutdown();
	}
	catch (...)
	{
		DB_poco_unexpected();
	}
}


void SSLManager::shutdown()
{
	PrivateKeyPassphraseRequired.clear();
	ClientVerificationError.clear();
	ServerVerificationError.clear();
	_ptrDefaultServerContext = 0;
	_ptrDefaultClientContext = 0;
}


namespace
{
	static DBPoco::SingletonHolder<SSLManager> singleton;
}


SSLManager& SSLManager::instance()
{
	return *singleton.get();
}


void SSLManager::initializeServer(PrivateKeyPassphraseHandlerPtr ptrPassphraseHandler, InvalidCertificateHandlerPtr ptrHandler, Context::Ptr ptrContext)
{
	_ptrServerPassphraseHandler  = ptrPassphraseHandler;
	_ptrServerCertificateHandler = ptrHandler;
	_ptrDefaultServerContext     = ptrContext;
}


void SSLManager::initializeClient(PrivateKeyPassphraseHandlerPtr ptrPassphraseHandler, InvalidCertificateHandlerPtr ptrHandler, Context::Ptr ptrContext)
{
	_ptrClientPassphraseHandler  = ptrPassphraseHandler;
	_ptrClientCertificateHandler = ptrHandler;
	_ptrDefaultClientContext     = ptrContext;
}


Context::Ptr SSLManager::defaultServerContext()
{
	DBPoco::FastMutex::ScopedLock lock(_mutex);

	if (!_ptrDefaultServerContext)
		initDefaultContext(true);

	return _ptrDefaultServerContext;
}


Context::Ptr SSLManager::defaultClientContext()
{
	DBPoco::FastMutex::ScopedLock lock(_mutex);

	if (!_ptrDefaultClientContext)
	{
		try
		{
			initDefaultContext(false);
		}
		catch (DBPoco::IllegalStateException&)
		{
			_ptrClientCertificateHandler = new RejectCertificateHandler(false);
			_ptrDefaultClientContext = new Context(Context::CLIENT_USE, "", Context::VERIFY_RELAXED, 9, true);
			_ptrDefaultClientContext->disableProtocols(Context::PROTO_SSLV2 | Context::PROTO_SSLV3);
		}
	}

	return _ptrDefaultClientContext;
}


SSLManager::PrivateKeyPassphraseHandlerPtr SSLManager::serverPassphraseHandler()
{
	DBPoco::FastMutex::ScopedLock lock(_mutex);

	if (!_ptrServerPassphraseHandler)
		initPassphraseHandler(true);

	return _ptrServerPassphraseHandler;
}


SSLManager::PrivateKeyPassphraseHandlerPtr SSLManager::clientPassphraseHandler()
{
	DBPoco::FastMutex::ScopedLock lock(_mutex);

	if (!_ptrClientPassphraseHandler)
		initPassphraseHandler(false);

	return _ptrClientPassphraseHandler;
}


SSLManager::InvalidCertificateHandlerPtr SSLManager::serverCertificateHandler()
{
	DBPoco::FastMutex::ScopedLock lock(_mutex);

	if (!_ptrServerCertificateHandler)
		initCertificateHandler(true);

	return _ptrServerCertificateHandler;
}


SSLManager::InvalidCertificateHandlerPtr SSLManager::clientCertificateHandler()
{
	DBPoco::FastMutex::ScopedLock lock(_mutex);

	if (!_ptrClientCertificateHandler)
		initCertificateHandler(false);

	return _ptrClientCertificateHandler;
}


int SSLManager::verifyCallback(bool server, int ok, X509_STORE_CTX* pStore)
{
	if (!ok)
	{
		X509* pCert = X509_STORE_CTX_get_current_cert(pStore);
		X509Certificate x509(pCert, true);
		int depth = X509_STORE_CTX_get_error_depth(pStore);
		int err = X509_STORE_CTX_get_error(pStore);
		std::string error(X509_verify_cert_error_string(err));
		VerificationErrorArgs args(x509, depth, err, error);
		if (server)
			SSLManager::instance().ServerVerificationError.notify(&SSLManager::instance(), args);
		else
			SSLManager::instance().ClientVerificationError.notify(&SSLManager::instance(), args);
		ok = args.getIgnoreError() ? 1 : 0;
	}

	return ok;
}


int SSLManager::privateKeyPassphraseCallback(char* pBuf, int size, int flag, void* userData)
{
	std::string pwd;
	SSLManager::instance().PrivateKeyPassphraseRequired.notify(&SSLManager::instance(), pwd);

	strncpy(pBuf, (char *)(pwd.c_str()), size);
	pBuf[size - 1] = '\0';
	if (size > pwd.length())
		size = (int) pwd.length();

	return size;
}


void SSLManager::initDefaultContext(bool server)
{
	if (server && _ptrDefaultServerContext) return;
	if (!server && _ptrDefaultClientContext) return;

	DBPoco::Crypto::OpenSSLInitializer openSSLInitializer;
	initEvents(server);
	DBPoco::Util::AbstractConfiguration& config = appConfig();

#ifdef OPENSSL_FIPS
	bool fipsEnabled = config.getBool(CFG_FIPS_MODE, VAL_FIPS_MODE);
	if (fipsEnabled && !DBPoco::Crypto::OpenSSLInitializer::isFIPSEnabled())
	{
		DBPoco::Crypto::OpenSSLInitializer::enableFIPSMode(true);
	}
#endif

	std::string prefix = server ? CFG_SERVER_PREFIX : CFG_CLIENT_PREFIX;

	Context::Params params;
	// mandatory options
	params.privateKeyFile = config.getString(prefix + CFG_PRIV_KEY_FILE, "");
	params.certificateFile = config.getString(prefix + CFG_CERTIFICATE_FILE, params.privateKeyFile);
	params.caLocation = config.getString(prefix + CFG_CA_LOCATION, "");

	if (server && params.certificateFile.empty() && params.privateKeyFile.empty())
		throw SSLException("Configuration error: no certificate file has been specified");

	// optional options for which we have defaults defined
	params.verificationMode = VAL_VER_MODE;
	if (config.hasProperty(prefix + CFG_VER_MODE))
	{
		// either: none, relaxed, strict, once
		std::string mode = config.getString(prefix + CFG_VER_MODE);
		params.verificationMode = Utility::convertVerificationMode(mode);
	}

	params.verificationDepth = config.getInt(prefix + CFG_VER_DEPTH, VAL_VER_DEPTH);
	params.loadDefaultCAs = config.getBool(prefix + CFG_ENABLE_DEFAULT_CA, VAL_ENABLE_DEFAULT_CA);
	params.cipherList = config.getString(prefix + CFG_CIPHER_LIST, VAL_CIPHER_LIST);
	params.cipherList = config.getString(prefix + CFG_CYPHER_LIST, params.cipherList); // for backwards compatibility
	bool requireTLSv1 = config.getBool(prefix + CFG_REQUIRE_TLSV1, false);
	bool requireTLSv1_1 = config.getBool(prefix + CFG_REQUIRE_TLSV1_1, false);
	bool requireTLSv1_2 = config.getBool(prefix + CFG_REQUIRE_TLSV1_2, false);

	params.dhParamsFile = config.getString(prefix + CFG_DH_PARAMS_FILE, "");
	params.ecdhCurve    = config.getString(prefix + CFG_ECDH_CURVE, "");

	Context::Usage usage;

	if (server)
	{
		if (requireTLSv1_2)
			usage = Context::TLSV1_2_SERVER_USE;
		else if (requireTLSv1_1)
			usage = Context::TLSV1_1_SERVER_USE;
		else if (requireTLSv1)
			usage = Context::TLSV1_SERVER_USE;
		else
			usage = Context::SERVER_USE;
		_ptrDefaultServerContext = new Context(usage, params);
	}
	else
	{
		if (requireTLSv1_2)
			usage = Context::TLSV1_2_CLIENT_USE;
		else if (requireTLSv1_1)
			usage = Context::TLSV1_1_CLIENT_USE;
		else if (requireTLSv1)
			usage = Context::TLSV1_CLIENT_USE;
		else
			usage = Context::CLIENT_USE;
		_ptrDefaultClientContext = new Context(usage, params);
	}

	std::string disabledProtocolsList = config.getString(prefix + CFG_DISABLE_PROTOCOLS, "");
	DBPoco::StringTokenizer dpTok(disabledProtocolsList, ";,", DBPoco::StringTokenizer::TOK_TRIM | DBPoco::StringTokenizer::TOK_IGNORE_EMPTY);
	int disabledProtocols = 0;
	for (DBPoco::StringTokenizer::Iterator it = dpTok.begin(); it != dpTok.end(); ++it)
	{
		if (*it == "sslv2")
			disabledProtocols |= Context::PROTO_SSLV2;
		else if (*it == "sslv3")
			disabledProtocols |= Context::PROTO_SSLV3;
		else if (*it == "tlsv1")
			disabledProtocols |= Context::PROTO_TLSV1;
		else if (*it == "tlsv1_1")
			disabledProtocols |= Context::PROTO_TLSV1_1;
		else if (*it == "tlsv1_2")
			disabledProtocols |= Context::PROTO_TLSV1_2;
	}
	if (server)
		_ptrDefaultServerContext->disableProtocols(disabledProtocols);
	else
		_ptrDefaultClientContext->disableProtocols(disabledProtocols);

	bool cacheSessions = config.getBool(prefix + CFG_CACHE_SESSIONS, false);
	if (server)
	{
		std::string sessionIdContext = config.getString(prefix + CFG_SESSION_ID_CONTEXT, config.getString("application.name", ""));
		_ptrDefaultServerContext->enableSessionCache(cacheSessions, sessionIdContext);
		if (config.hasProperty(prefix + CFG_SESSION_CACHE_SIZE))
		{
			int cacheSize = config.getInt(prefix + CFG_SESSION_CACHE_SIZE);
			_ptrDefaultServerContext->setSessionCacheSize(cacheSize);
		}
		if (config.hasProperty(prefix + CFG_SESSION_TIMEOUT))
		{
			int timeout = config.getInt(prefix + CFG_SESSION_TIMEOUT);
			_ptrDefaultServerContext->setSessionTimeout(timeout);
		}
	}
	else
	{
		_ptrDefaultClientContext->enableSessionCache(cacheSessions);
	}
	bool extendedVerification = config.getBool(prefix + CFG_EXTENDED_VERIFICATION, false);
	if (server)
		_ptrDefaultServerContext->enableExtendedCertificateVerification(extendedVerification);
	else
		_ptrDefaultClientContext->enableExtendedCertificateVerification(extendedVerification);

	bool preferServerCiphers = config.getBool(prefix + CFG_PREFER_SERVER_CIPHERS, false);
	if (preferServerCiphers)
	{
		if (server)
			_ptrDefaultServerContext->preferServerCiphers();
		else
			_ptrDefaultClientContext->preferServerCiphers();
	}
}


void SSLManager::initEvents(bool server)
{
	initPassphraseHandler(server);
	initCertificateHandler(server);
}


void SSLManager::initPassphraseHandler(bool server)
{
	if (server && _ptrServerPassphraseHandler) return;
	if (!server && _ptrClientPassphraseHandler) return;

	std::string prefix = server ? CFG_SERVER_PREFIX : CFG_CLIENT_PREFIX;
	DBPoco::Util::AbstractConfiguration& config = appConfig();

	std::string className(config.getString(prefix + CFG_DELEGATE_HANDLER, VAL_DELEGATE_HANDLER));

	const PrivateKeyFactory* pFactory = 0;
	if (privateKeyFactoryMgr().hasFactory(className))
	{
		pFactory = privateKeyFactoryMgr().getFactory(className);
	}

	if (pFactory)
	{
		if (server)
			_ptrServerPassphraseHandler = pFactory->create(server);
		else
			_ptrClientPassphraseHandler = pFactory->create(server);
	}
	else throw DBPoco::Util::UnknownOptionException(std::string("No passphrase handler known with the name ") + className);
}


void SSLManager::initCertificateHandler(bool server)
{
	if (server && _ptrServerCertificateHandler) return;
	if (!server && _ptrClientCertificateHandler) return;

	std::string prefix = server ? CFG_SERVER_PREFIX : CFG_CLIENT_PREFIX;
	DBPoco::Util::AbstractConfiguration& config = appConfig();

	std::string className(config.getString(prefix+CFG_CERTIFICATE_HANDLER, VAL_CERTIFICATE_HANDLER));

	const CertificateHandlerFactory* pFactory = 0;
	if (certificateHandlerFactoryMgr().hasFactory(className))
	{
		pFactory = certificateHandlerFactoryMgr().getFactory(className);
	}

	if (pFactory)
	{
		if (server)
			_ptrServerCertificateHandler = pFactory->create(true);
		else
			_ptrClientCertificateHandler = pFactory->create(false);
	}
	else throw DBPoco::Util::UnknownOptionException(std::string("No InvalidCertificate handler known with the name ") + className);
}


Context::Ptr SSLManager::getCustomServerContext(const std::string & name)
{
	DBPoco::FastMutex::ScopedLock lock(_mutex);
	auto it = _mapPtrServerContexts.find(name);
	if (it != _mapPtrServerContexts.end())
		return it->second;
	return nullptr;
}

Context::Ptr SSLManager::setCustomServerContext(const std::string & name, Context::Ptr ctx)
{
	DBPoco::FastMutex::ScopedLock lock(_mutex);
	ctx = _mapPtrServerContexts.insert({name, ctx}).first->second;
	return ctx;
}


DBPoco::Util::AbstractConfiguration& SSLManager::appConfig()
{
	try
	{
		return DBPoco::Util::Application::instance().config();
	}
	catch (DBPoco::NullPointerException&)
	{
		throw DBPoco::IllegalStateException(
			"An application configuration is required to initialize the DBPoco::Net::SSLManager, "
			"but no DBPoco::Util::Application instance is available."
		);
	}
}


void initializeSSL()
{
	DBPoco::Crypto::initializeCrypto();
}


void uninitializeSSL()
{
	SSLManager::instance().shutdown();
	DBPoco::Crypto::uninitializeCrypto();
}


} } // namespace DBPoco::Net

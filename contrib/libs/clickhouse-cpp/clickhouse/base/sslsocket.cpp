#include "sslsocket.h"
#include "../client.h"
#include "../exceptions.h"

#include <iostream>
#include <stdexcept>

#include <openssl/ssl.h>
#include <openssl/x509v3.h>
#include <openssl/err.h>
#include <openssl/asn1.h>


namespace {

std::string getCertificateInfo(X509* cert)
{
    if (!cert)
        return "No certificate";

    std::unique_ptr<BIO, decltype(&BIO_free)> mem_bio(BIO_new(BIO_s_mem()), &BIO_free);
    X509_print(mem_bio.get(), cert);

    char * data = nullptr;
    auto len = BIO_get_mem_data(mem_bio.get(), &data);
    if (len < 0)
        return "Can't get certificate info due to BIO error " + std::to_string(len);

    return std::string(data, len);
}

void throwSSLError(SSL * ssl, int error, const char * /*location*/, const char * /*statement*/, const std::string prefix = "OpenSSL error: ") {
    const auto detail_error = ERR_get_error();
    auto reason = ERR_reason_error_string(detail_error);
    reason = reason ? reason : "Unknown SSL error";

    std::string reason_str = reason;
    if (ssl) {
        // Print certificate only if handshake isn't completed 
        if (auto ssl_session = SSL_get_session(ssl); ssl_session && SSL_get_state(ssl) != TLS_ST_OK)
            reason_str += "\nServer certificate: " + getCertificateInfo(SSL_SESSION_get0_peer(ssl_session));
    }

//    std::cerr << "!!! SSL error at " << location
//              << "\n\tcaused by " << statement
//              << "\n\t: "<< reason_str << "(" << error << ")"
//              << "\n\t last err: " << ERR_peek_last_error()
//              << std::endl;

    throw clickhouse::OpenSSLError(prefix + std::to_string(error) + " : " + reason_str);
}

void configureSSL(const clickhouse::SSLParams::ConfigurationType & configuration, SSL * ssl, SSL_CTX * context = nullptr) {
#ifdef USE_BORINGSSL
    // BoringSSL doesn't ship the SSL_CONF_* command API, so the
    // configuration vector cannot be applied. Users who rely on
    // SSL_CONF commands should build against
    // OpenSSL (e.g. --@clickhouse_cpp//:tls=openssl with Bazel).
    (void)ssl;
    (void)context;
    if (!configuration.empty()) {
        throw clickhouse::OpenSSLError(
            "SSLParams::configuration cannot be used when the library is built against BoringSSL "
            "To apply these settings, build against OpenSSL by using the `tls=openssl` option in Bazel.");
    }
#else
    std::unique_ptr<SSL_CONF_CTX, decltype(&SSL_CONF_CTX_free)> conf_ctx_holder(SSL_CONF_CTX_new(), SSL_CONF_CTX_free);
    auto conf_ctx = conf_ctx_holder.get();

    // To make both cmdline and flag file commands start with no prefix.
    SSL_CONF_CTX_set1_prefix(conf_ctx, "");
    // Allow all set of client commands, also turn on proper error reporting to reuse throwSSLError().
    SSL_CONF_CTX_set_flags(conf_ctx, SSL_CONF_FLAG_CMDLINE | SSL_CONF_FLAG_FILE | SSL_CONF_FLAG_CLIENT | SSL_CONF_FLAG_SHOW_ERRORS | SSL_CONF_FLAG_CERTIFICATE );
    if (ssl)
        SSL_CONF_CTX_set_ssl(conf_ctx, ssl);
    else if (context)
        SSL_CONF_CTX_set_ssl_ctx(conf_ctx, context);

    for (const auto & kv : configuration) {
        const int err = SSL_CONF_cmd(conf_ctx, kv.first.c_str(), (kv.second ? kv.second->c_str() : nullptr));
        // From the documentation:
        //  2 - both key and value used
        //  1 - only key used
        //  0 - error during processing
        // -2 - key not recodnized
        // -3 - missing value
        const bool value_present = !!kv.second;
        if (err == 2 || (err == 1 && !value_present))
            continue;
        else if (err == 0)
            throwSSLError(ssl, SSL_ERROR_NONE, nullptr, nullptr, "Failed to configure OpenSSL with command '" + kv.first + "' ");
        else if (err == 1 && value_present)
            throw clickhouse::OpenSSLError("Failed to configure OpenSSL: command '" + kv.first + "' needs no value");
        else if (err == -2)
            throw clickhouse::OpenSSLError("Failed to configure OpenSSL: unknown command '" + kv.first + "'");
        else if (err == -3)
            throw clickhouse::OpenSSLError("Failed to configure OpenSSL: command '" + kv.first + "' requires a value");
        else
            throw clickhouse::OpenSSLError("Failed to configure OpenSSL: command '" + kv.first + "' unknown error: " + std::to_string(err));
    }
#endif
}

#define STRINGIFY_HELPER(x) #x
#define STRINGIFY(x) STRINGIFY_HELPER(x)
#define LOCATION __FILE__  ":" STRINGIFY(__LINE__)

struct SSLInitializer {
    SSLInitializer() {
        SSL_library_init();
        SSLeay_add_ssl_algorithms();
        SSL_load_error_strings();
    }
};

SSL_CTX * prepareSSLContext(const clickhouse::SSLParams & context_params) {
    static const SSLInitializer ssl_initializer;

    const SSL_METHOD *method = TLS_client_method();
    std::unique_ptr<SSL_CTX, decltype(&SSL_CTX_free)> ctx(SSL_CTX_new(method), &SSL_CTX_free);

    if (!ctx)
        throw clickhouse::OpenSSLError("Failed to initialize SSL context");

#define HANDLE_SSL_CTX_ERROR(statement) do { \
    if (const auto ret_code = (statement); !ret_code) \
        throwSSLError(nullptr, static_cast<int>(ERR_peek_error()), LOCATION, #statement); \
} while(false);

    if (context_params.use_default_ca_locations)
        HANDLE_SSL_CTX_ERROR(SSL_CTX_set_default_verify_paths(ctx.get()));
    if (!context_params.path_to_ca_directory.empty())
        HANDLE_SSL_CTX_ERROR(
            SSL_CTX_load_verify_locations(
                ctx.get(),
                nullptr,
                context_params.path_to_ca_directory.c_str())
        );

    for (const auto & f : context_params.path_to_ca_files)
        HANDLE_SSL_CTX_ERROR(SSL_CTX_load_verify_locations(ctx.get(), f.c_str(), nullptr));

    if (context_params.context_options != -1)
        SSL_CTX_set_options(ctx.get(), context_params.context_options);
    if (context_params.min_protocol_version != -1)
        HANDLE_SSL_CTX_ERROR(
            SSL_CTX_set_min_proto_version(ctx.get(), context_params.min_protocol_version));
    if (context_params.max_protocol_version != -1)
        HANDLE_SSL_CTX_ERROR(
            SSL_CTX_set_max_proto_version(ctx.get(), context_params.max_protocol_version));

    return ctx.release();
#undef HANDLE_SSL_CTX_ERROR
}

auto convertConfiguration(const decltype(clickhouse::ClientOptions::SSLOptions::configuration) & configuration)
{
    auto result = decltype(clickhouse::SSLParams::configuration){};
    for (const auto & cv : configuration)
        result.push_back({cv.command, cv.value});

    return result;
}

clickhouse::SSLParams GetSSLParams(const clickhouse::ClientOptions& opts) {
    const auto& ssl_options = *opts.ssl_options;
    return clickhouse::SSLParams{
            ssl_options.path_to_ca_files,
            ssl_options.path_to_ca_directory,
            ssl_options.use_default_ca_locations,
            ssl_options.context_options,
            ssl_options.min_protocol_version,
            ssl_options.max_protocol_version,
            ssl_options.use_sni,
            ssl_options.skip_verification,
            ssl_options.host_flags,
            convertConfiguration(ssl_options.configuration)
    };
}

}

namespace clickhouse {

SSLContext::SSLContext(SSL_CTX & context)
    : context_(&context, &SSL_CTX_free)
{
    SSL_CTX_up_ref(context_.get());
}

SSLContext::SSLContext(const SSLParams & context_params)
    : context_(prepareSSLContext(context_params), &SSL_CTX_free)
{
}

SSL_CTX * SSLContext::getContext() {
    return context_.get();
}

// Allows caller to use returned value of `statement` if there was no error, throws exception otherwise.
#define HANDLE_SSL_ERROR(SSL_PTR, statement) [&] { \
    if (const auto ret_code = (statement); ret_code <= 0) { \
        throwSSLError(SSL_PTR, SSL_get_error(SSL_PTR, static_cast<int>(ret_code)), LOCATION, #statement); \
        return static_cast<std::decay_t<decltype(ret_code)>>(0); \
    } \
    else \
        return ret_code; \
} ()

/* // debug macro for tracing SSL state
#define LOG_SSL_STATE() std::cerr << "!!!!" << LOCATION << " @" << __FUNCTION__ \
    << "\t" << SSL_get_version(ssl_) << " state: "  << SSL_state_string_long(ssl_) \
    << "\n\t handshake state: " << SSL_get_state(ssl_) \
    << std::endl
*/
SSLSocket::SSLSocket(const NetworkAddress& addr, const SocketTimeoutParams& timeout_params,
                     const SSLParams & ssl_params, SSLContext& context)
    : Socket(addr, timeout_params)
    , ssl_(SSL_new(context.getContext()), &SSL_free)
{
    auto ssl = ssl_.get();
    if (!ssl)
        throw clickhouse::OpenSSLError("Failed to create SSL instance");

    std::unique_ptr<ASN1_OCTET_STRING, decltype(&ASN1_OCTET_STRING_free)> ip_addr(a2i_IPADDRESS(addr.Host().c_str()), &ASN1_OCTET_STRING_free);

    HANDLE_SSL_ERROR(ssl, SSL_set_fd(ssl, static_cast<int>(handle_)));
    if (ssl_params.use_SNI)
        HANDLE_SSL_ERROR(ssl, SSL_set_tlsext_host_name(ssl, addr.Host().c_str()));

    if (ssl_params.host_flags != -1)
        SSL_set_hostflags(ssl, ssl_params.host_flags);
    HANDLE_SSL_ERROR(ssl, SSL_set1_host(ssl, addr.Host().c_str()));

    // DO NOT use SSL_set_verify(ssl, SSL_VERIFY_PEER, nullptr), since
    // we check verification result later, and that provides better error message.

    if (ssl_params.configuration.size() > 0)
        configureSSL(ssl_params.configuration, ssl);

    SSL_set_connect_state(ssl);
    HANDLE_SSL_ERROR(ssl, SSL_connect(ssl));
    HANDLE_SSL_ERROR(ssl, SSL_set_mode(ssl, SSL_MODE_AUTO_RETRY));

    if (const auto verify_result = SSL_get_verify_result(ssl); !ssl_params.skip_verification && verify_result != X509_V_OK) {
        auto error_message = X509_verify_cert_error_string(verify_result);
        throw clickhouse::OpenSSLError("Failed to verify SSL connection, X509_v error: "
                + std::to_string(verify_result)
                + " " + error_message
                + "\nServer certificate: " + getCertificateInfo(SSL_get_peer_certificate(ssl)));
    }

    // Host name verification is done by OpenSSL itself, however if we are connecting to an ip-address,
    // no verification is made, so we have to do it manually.
    // Just in case if this is ever required, leave it here commented out.
//    if (ip_addr) {
//        // if hostname is actually an IP address
//        HANDLE_SSL_ERROR(ssl, X509_check_ip(
//                SSL_get_peer_certificate(ssl),
//                ASN1_STRING_get0_data(ip_addr.get()),
//                ASN1_STRING_length(ip_addr.get()),
//                0));
//    }
}

void SSLSocket::validateParams(const SSLParams & ssl_params) {
    // We need either SSL or SSL_CTX to properly validate configuration, so create a temporary one.
    std::unique_ptr<SSL_CTX, decltype(&SSL_CTX_free)> ctx(SSL_CTX_new(TLS_client_method()), &SSL_CTX_free);
    configureSSL(ssl_params.configuration, nullptr, ctx.get());
}


SSLSocketFactory::SSLSocketFactory(const ClientOptions& opts)
    : NonSecureSocketFactory()
    , ssl_params_(GetSSLParams(opts)) {
    if (opts.ssl_options->ssl_context) {
        ssl_context_ = std::make_unique<SSLContext>(*opts.ssl_options->ssl_context);
    } else {
        ssl_context_ = std::make_unique<SSLContext>(ssl_params_);
    }
}

SSLSocketFactory::~SSLSocketFactory() = default;

std::unique_ptr<Socket> SSLSocketFactory::doConnect(const NetworkAddress& address, const ClientOptions& opts) {
    SocketTimeoutParams timeout_params { opts.connection_connect_timeout, opts.connection_recv_timeout, opts.connection_send_timeout };
    return std::make_unique<SSLSocket>(address, timeout_params, ssl_params_, *ssl_context_);
}

std::unique_ptr<InputStream> SSLSocket::makeInputStream() const {
    return std::make_unique<SSLSocketInput>(ssl_.get());
}

std::unique_ptr<OutputStream> SSLSocket::makeOutputStream() const {
    return std::make_unique<SSLSocketOutput>(ssl_.get());
}

SSLSocketInput::SSLSocketInput(SSL *ssl)
    : ssl_(ssl)
{}

size_t SSLSocketInput::DoRead(void* buf, size_t len) {
#ifdef USE_BORINGSSL
    // BoringSSL doesn't have SSL_read_ex (OpenSSL 3.0+ API); fall back to
    // SSL_read with the length clamped to INT_MAX.
    const int max_read = static_cast<int>(
        std::min<std::size_t>(len, std::numeric_limits<int>::max()));
    return static_cast<size_t>(
        HANDLE_SSL_ERROR(ssl_, SSL_read(ssl_, buf, max_read)));
#else
    size_t actually_read;
    HANDLE_SSL_ERROR(ssl_, SSL_read_ex(ssl_, buf, len, &actually_read));
    return actually_read;
#endif
}

SSLSocketOutput::SSLSocketOutput(SSL *ssl)
    : ssl_(ssl)
{}

size_t SSLSocketOutput::DoWrite(const void* data, size_t len) {
    if (len > std::numeric_limits<int>::max())
        // FIXME(vnemkov): We should do multiple `SSL_write`s in this case.
        throw AssertionError("Failed to write too big chunk at once "
                + std::to_string(len) + " > " + std::to_string(std::numeric_limits<int>::max()));
    return static_cast<size_t>(HANDLE_SSL_ERROR(ssl_, SSL_write(ssl_, data, static_cast<int>(len))));
}

#undef HANDLE_SSL_ERROR

}

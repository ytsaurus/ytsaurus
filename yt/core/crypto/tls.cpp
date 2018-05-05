#include "tls.h"

#include <yt/core/misc/error.h>
#include <yt/core/misc/ref.h>
#include <yt/core/misc/finally.h>

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/poller.h>

#include <yt/core/net/connection.h>
#include <yt/core/net/dialer.h>
#include <yt/core/net/listener.h>

#include <yt/core/logging/log.h>

#include <contrib/libs/openssl/include/openssl/bio.h>
#include <contrib/libs/openssl/include/openssl/ssl.h>
#include <contrib/libs/openssl/include/openssl/err.h>
#include <contrib/libs/openssl/include/openssl/evp.h>
#include <contrib/libs/openssl/include/openssl/pem.h>

namespace NYT {
namespace NCrypto {

using namespace NNet;
using namespace NConcurrency;
using namespace NLogging;

static const TLogger Logger{"Tls"};

////////////////////////////////////////////////////////////////////////////////

namespace {

TErrorAttribute GetLastSslError()
{
    char errorStr[256];
    ERR_error_string_n(ERR_get_error(), errorStr, sizeof(errorStr));
    return TErrorAttribute("ssl_error", TString(errorStr));
}

constexpr auto TlsBufferSize = 1_MB;

} // namespace

////////////////////////////////////////////////////////////////////////////////

struct TSslContextImpl
    : public TRefCounted
{
    SSL_CTX* Ctx = nullptr;

    TSslContextImpl()
    {
        Ctx = SSL_CTX_new(TLSv1_2_method());
        if (!Ctx) {
            THROW_ERROR_EXCEPTION("SSL_CTX_new(TLSv1_2_method()) failed")
                << GetLastSslError();
        }
    }

    ~TSslContextImpl()
    {
        SSL_CTX_free(Ctx);
        Ctx = nullptr;
    }
};

DEFINE_REFCOUNTED_TYPE(TSslContextImpl)

////////////////////////////////////////////////////////////////////////////////

struct TTlsBufferTag
{ };

class TTlsConnection
    : public IConnection
{
public:
    TTlsConnection(
        TSslContextImplPtr ctx,
        IInvokerPtr underlyingInvoker,
        IConnectionPtr connection)
        : Ctx_(std::move(ctx))
        , Invoker_(CreateSerializedInvoker(std::move(underlyingInvoker)))
        , Underlying_(std::move(connection))
    {
        Ssl_ = SSL_new(Ctx_->Ctx);
        if (!Ssl_) {
            THROW_ERROR_EXCEPTION("SSL_new failed")
                << GetLastSslError();
        }

        InputBIO_ = BIO_new(BIO_s_mem());
        YCHECK(InputBIO_);
        // Makes InputBIO_ non-blocking.

        BIO_set_mem_eof_return(InputBIO_, -1); 
        OutputBIO_ = BIO_new(BIO_s_mem());
        YCHECK(OutputBIO_);

        SSL_set_bio(Ssl_, InputBIO_, OutputBIO_);

        InputBuffer_ = TSharedMutableRef::Allocate<TTlsBufferTag>(TlsBufferSize);
        OutputBuffer_ = TSharedMutableRef::Allocate<TTlsBufferTag>(TlsBufferSize);
    }

    ~TTlsConnection()
    {
        SSL_free(Ssl_);
        Ssl_ = nullptr;
        // BIO is owned by SSL.
    }

    void StartClient()
    {
        SSL_set_connect_state(Ssl_);
        auto sslResult = SSL_do_handshake(Ssl_);
        sslResult = SSL_get_error(Ssl_, sslResult);
        YCHECK(sslResult == SSL_ERROR_WANT_READ);

        Invoker_->Invoke(BIND(&TTlsConnection::DoRun, MakeStrong(this)));
    }

    void StartServer()
    {
        SSL_set_accept_state(Ssl_);

        Invoker_->Invoke(BIND(&TTlsConnection::DoRun, MakeStrong(this)));
    }

    virtual int GetHandle() const override
    {
        Y_UNIMPLEMENTED();
    }

    virtual i64 GetReadByteCount() const override
    {
        return Underlying_->GetReadByteCount();
    }

    virtual TConnectionStatistics GetReadStatistics() const override
    {
        return Underlying_->GetReadStatistics();
    }

    virtual i64 GetWriteByteCount() const override
    {
        return Underlying_->GetWriteByteCount();
    }

    virtual const TNetworkAddress& LocalAddress() const override
    {
        return Underlying_->LocalAddress();
    }

    virtual const TNetworkAddress& RemoteAddress() const override
    {
        return Underlying_->RemoteAddress();
    }

    virtual TConnectionStatistics GetWriteStatistics() const override
    {
        return Underlying_->GetWriteStatistics();
    }

    virtual TFuture<size_t> Read(const TSharedMutableRef& buffer) override
    {
        auto promise = NewPromise<size_t>();
        Invoker_->Invoke(BIND([this, this_ = MakeStrong(this), promise, buffer] () {
            ReadBuffer_ = buffer;
            ReadResult_ = promise;

            YCHECK(!ReadActive_);
            ReadActive_ = true;

            DoRun();
        }));
        return promise.ToFuture();
    }

    virtual TFuture<void> Write(const TSharedRef& buffer) override
    {
        return WriteV(TSharedRefArray(buffer));
    }

    virtual TFuture<void> WriteV(const TSharedRefArray& buffer) override
    {
        auto promise = NewPromise<void>();
        Invoker_->Invoke(BIND([this, this_ = MakeStrong(this), promise, buffer] () {
            WriteBuffer_ = buffer;
            WriteResult_ = promise;

            YCHECK(!WriteActive_);
            WriteActive_ = true;

            DoRun();
        }));
        return promise.ToFuture();
    }

    virtual TFuture<void> CloseRead() override
    {
        // TLS does not support half-open connection state.
        return Close();
    }

    virtual TFuture<void> CloseWrite() override
    {
        // TLS does not support half-open connection state.
        return Close();
    }

    virtual TFuture<void> Close() override
    {
        return BIND([this, this_ = MakeStrong(this)] () {
            CloseRequested_ = true;

            DoRun();
        })
            .AsyncVia(Invoker_)
            .Run();
    }

    virtual TFuture<void> Abort() override
    {
        return BIND([this, this_ = MakeStrong(this)] () {
            if (Error_.IsOK()) {
                Error_ = TError("TLS connection aborted");
                CheckError();
            }
        })
            .AsyncVia(Invoker_)
            .Run();
    }

private:
    TSslContextImplPtr Ctx_;
    IInvokerPtr Invoker_;
    IConnectionPtr Underlying_;
    TLogger Logger;

    SSL* Ssl_ = nullptr;
    BIO* InputBIO_ = nullptr;
    BIO* OutputBIO_ = nullptr;

    // FSM
    TError Error_;
    bool HandshakeInProgress_ = true;
    bool CloseRequested_ = false;
    bool ReadActive_ = false;
    bool WriteActive_ = false;
    bool UnderlyingReadActive_ = false;
    bool UnderlyingWriteActive_ = false;

    TSharedMutableRef InputBuffer_;
    TSharedMutableRef OutputBuffer_;

    // Active read
    TSharedMutableRef ReadBuffer_;
    TPromise<size_t> ReadResult_;

    // Active write
    TSharedRefArray WriteBuffer_;
    TPromise<void> WriteResult_;

    void CheckError()
    {
        if (Error_.IsOK()) {
            return;
        }

        if (ReadActive_) {
            ReadResult_.Set(Error_);
            ReadActive_ = false;
        }

        if (WriteActive_) {
            WriteResult_.Set(Error_);
            WriteActive_ = false;
        }
    }

    void MaybeStartUnderlyingIO(bool sslWantRead)
    {
        if (!UnderlyingReadActive_ && sslWantRead) {
            UnderlyingReadActive_ = true;
            auto callback = BIND([this, this_ = MakeStrong(this)] (const TErrorOr<size_t>& result) {
                UnderlyingReadActive_ = false;
                if (result.IsOK()) {
                    if (result.Value() > 0) {
                        int count = BIO_write(InputBIO_, InputBuffer_.Begin(), result.Value());
                        YCHECK(count == result.Value());
                    } else {
                        BIO_set_mem_eof_return(InputBIO_, 0);
                    }
                } else {
                    Error_ = result;
                }

                DoRun();
                MaybeStartUnderlyingIO(false);
            })
                .Via(Invoker_);

            Underlying_->Read(InputBuffer_)
                .Subscribe(callback);
        }

        if (!UnderlyingWriteActive_ && BIO_ctrl_pending(OutputBIO_)) {
            UnderlyingWriteActive_ = true;
            auto callback = BIND([this, this_ = MakeStrong(this)] (const TError& result) {
                UnderlyingWriteActive_ = false;
                if (result.IsOK()) {
                    // Hooray!
                } else {
                    Error_ = result;
                }

                DoRun();
            })
                .Via(Invoker_);

            int count = BIO_read(OutputBIO_, OutputBuffer_.Begin(), OutputBuffer_.Size());
            YCHECK(count > 0);
            Underlying_->Write(OutputBuffer_.Slice(0, count))
                .Subscribe(callback);
        }
    }

    void DoRun()
    {
        CheckError();

        if (CloseRequested_ && !HandshakeInProgress_) {
            SSL_shutdown(Ssl_);
            MaybeStartUnderlyingIO(false);
        }
    
        if (HandshakeInProgress_) {
            int sslResult = SSL_do_handshake(Ssl_);
            if (sslResult == 1) {
                HandshakeInProgress_ = false;
            } else {
                int sslError = SSL_get_error(Ssl_, sslResult);
                if (sslError == SSL_ERROR_WANT_READ) {
                    MaybeStartUnderlyingIO(true);
                } else {
                    Error_ = TError("SSL_do_handshake failed")
                        << GetLastSslError();
                    LOG_DEBUG(Error_, "TLS handshake failed");
                    CheckError();
                    return;
                }
            }
        }

        if (HandshakeInProgress_) {
            return;
        }

        // Second condition acts as a poor-man backpressure.
        if (WriteActive_ && !UnderlyingWriteActive_) {
            for (const auto& ref : WriteBuffer_) {
                int count = SSL_write(Ssl_, ref.Begin(), ref.Size());

                if (count < 0) {
                    Error_ = TError("SSL_write failed")
                        << GetLastSslError();
                    LOG_DEBUG(Error_, "TLS write failed");
                    CheckError();
                    return;
                }

                YCHECK(count == ref.Size());
            }
            
            MaybeStartUnderlyingIO(false);

            WriteActive_ = false;
            WriteBuffer_.Reset();
            WriteResult_.Set();
            WriteResult_.Reset();
        }

        if (ReadActive_) {
            int count = SSL_read(Ssl_, ReadBuffer_.Begin(), ReadBuffer_.Size());
            if (count >= 0) {
                ReadActive_ = false;
                ReadResult_.Set(count);
                ReadResult_.Reset();
                ReadBuffer_.Reset();
            } else {
                int sslError = SSL_get_error(Ssl_, count);
                if (sslError == SSL_ERROR_WANT_READ) {
                    MaybeStartUnderlyingIO(true);
                } else {
                    Error_ = TError("SSL_read failed")
                        << GetLastSslError();
                    LOG_DEBUG(Error_, "TLS read failed");
                    CheckError();
                    return;
                }
            }
        }
    }
};

DEFINE_REFCOUNTED_TYPE(TTlsConnection)

////////////////////////////////////////////////////////////////////////////////

class TTlsDialer
    : public IDialer
{
public:
    TTlsDialer(
        const TSslContextImplPtr& ctx,
        const IDialerPtr& dialer,
        const IInvokerPtr& invoker)
        : Ctx_(ctx)
        , Underlying_(dialer)
        , Invoker_(invoker)
    { }

    virtual TFuture<IConnectionPtr> Dial(const TNetworkAddress& remote) override
    {
        return Underlying_->Dial(remote).Apply(BIND([ctx=Ctx_, invoker=Invoker_] (IConnectionPtr underlying) -> IConnectionPtr {
            auto connection = New<TTlsConnection>(ctx, invoker, underlying);
            connection->StartClient();
            return connection;
        }));
    }

private:
    TSslContextImplPtr Ctx_;
    IDialerPtr Underlying_;
    IInvokerPtr Invoker_;
};

////////////////////////////////////////////////////////////////////////////////

class TTlsListener
    : public IListener
{
public:
    TTlsListener(
        const TSslContextImplPtr& ctx,
        const IListenerPtr& listener,
        const IInvokerPtr& invoker)
        : Ctx_(ctx)
        , Underlying_(listener)
        , Invoker_(invoker)
    { }

    const TNetworkAddress& Address() const override
    {
        return Underlying_->Address();
    }

    virtual TFuture<IConnectionPtr> Accept() override
    {
        return Underlying_->Accept().Apply(
            BIND([ctx = Ctx_, invoker = Invoker_] (const IConnectionPtr& underlying) -> IConnectionPtr {
                auto connection = New<TTlsConnection>(ctx, invoker, underlying);
                connection->StartServer();
                return connection;
            }));
    }

private:
    const TSslContextImplPtr Ctx_;
    const IListenerPtr Underlying_;
    const IInvokerPtr Invoker_;
};

////////////////////////////////////////////////////////////////////////////////

TSslContext::TSslContext()
    : Impl_(New<TSslContextImpl>())
{ }

void TSslContext::SetCipherList(const TString& list)
{
    if (SSL_CTX_set_cipher_list(Impl_->Ctx, ~list) == 0) {
        THROW_ERROR_EXCEPTION("SSL_CTX_set_cipher_list failed")
            << TErrorAttribute("cipher_list", list)
            << GetLastSslError();
    }
}

void TSslContext::AddCertificateFromFile(const TString& path)
{
    if (SSL_CTX_use_certificate_file(Impl_->Ctx, path.c_str(), SSL_FILETYPE_PEM) != 1) {
        THROW_ERROR_EXCEPTION("SSL_CTX_use_certificate_file failed")
            << TErrorAttribute("path", path)
            << GetLastSslError();
    }
}

void TSslContext::AddCertificateChainFromFile(const TString& path)
{
    if (SSL_CTX_use_certificate_chain_file(Impl_->Ctx, path.c_str()) != 1) {
        THROW_ERROR_EXCEPTION("SSL_CTX_use_certificate_chain_file failed")
            << TErrorAttribute("path", path)
            << GetLastSslError();
    }
}

void TSslContext::AddPrivateKeyFromFile(const TString& path)
{
    if (SSL_CTX_use_PrivateKey_file(Impl_->Ctx, path.c_str(), SSL_FILETYPE_PEM) != 1) {
        THROW_ERROR_EXCEPTION("SSL_CTX_use_PrivateKey_file failed")
            << TErrorAttribute("path", path)
            << GetLastSslError();
    }
}

void TSslContext::AddCertificate(const TString& certificate)
{
    auto bio = BIO_new_mem_buf(certificate.c_str(), certificate.size());
    YCHECK(bio);
    auto freeBio = Finally([&] {
        BIO_free(bio);
    });

    auto certificateObject = PEM_read_bio_X509_AUX(bio, nullptr, nullptr, nullptr);
    if (!certificateObject) {
        THROW_ERROR_EXCEPTION("PEM_read_bio_X509_AUX")
            << GetLastSslError();
    }
    auto freeCertificate = Finally([&] {
        X509_free(certificateObject);
    });

    if (SSL_CTX_use_certificate(Impl_->Ctx, certificateObject) != 1) {
        THROW_ERROR_EXCEPTION("SSL_CTX_use_certificate failed")
            << GetLastSslError();
    }    
}

void TSslContext::AddPrivateKey(const TString& privateKey)
{
    auto bio = BIO_new_mem_buf(privateKey.c_str(), privateKey.size());
    YCHECK(bio);
    auto freeBio = Finally([&] {
        BIO_free(bio);
    });

    auto privateKeyObject = PEM_read_bio_PrivateKey(bio, nullptr, nullptr, nullptr);
    if (!privateKeyObject) {
        THROW_ERROR_EXCEPTION("PEM_read_bio_PrivateKey")
            << GetLastSslError();
    }
    auto freePrivateKey = Finally([&] {
        EVP_PKEY_free(privateKeyObject);
    });

    if (SSL_CTX_use_PrivateKey(Impl_->Ctx, privateKeyObject) != 1) {
        THROW_ERROR_EXCEPTION("SSL_CTX_use_PrivateKey failed")
            << GetLastSslError();
    }
}

IDialerPtr TSslContext::CreateDialer(
    const TDialerConfigPtr& config,
    const IPollerPtr& poller,
    const TLogger& logger)
{
    auto dialer = NNet::CreateDialer(config, poller, logger);
    return New<TTlsDialer>(Impl_, dialer, poller->GetInvoker());
}

IListenerPtr TSslContext::CreateListener(
    const TNetworkAddress& at,
    const IPollerPtr& poller)
{
    auto listener = NNet::CreateListener(at, poller);
    return New<TTlsListener>(Impl_, listener, poller->GetInvoker());
}

IListenerPtr TSslContext::CreateListener(
    const IListenerPtr& underlying,
    const IPollerPtr& poller)
{
    return New<TTlsListener>(Impl_, underlying, poller->GetInvoker());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCrypto
} // namespace NYT

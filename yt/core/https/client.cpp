#include "client.h"
#include "config.h"

#include <yt/core/http/client.h>
#include <yt/core/http/private.h>

#include <yt/core/net/config.h>
#include <yt/core/net/address.h>

#include <yt/core/rpc/grpc/dispatcher.h>

#include <yt/core/crypto/tls.h>

#include <yt/core/concurrency/poller.h>

namespace NYT {
namespace NHttps {

using namespace NNet;
using namespace NHttp;
using namespace NCrypto;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TClient
    : public IClient
{
public:
    TClient(
        NRpc::NGrpc::TGrpcLibraryLockPtr libraryLock,
        IClientPtr underlying)
        : LibraryLock_(std::move(libraryLock))
        , Underlying_(std::move(underlying))
    { }

    virtual TFuture<IResponsePtr> Get(
        const TString& url,
        const THeadersPtr& headers) override
    {
        return Underlying_->Get(url, headers);
    }

    virtual TFuture<IResponsePtr> Post(
        const TString& url,
        const TSharedRef& body,
        const THeadersPtr& headers) override
    {
        return Underlying_->Post(url, body, headers);
    }

private:
    const NRpc::NGrpc::TGrpcLibraryLockPtr LibraryLock_;
    const IClientPtr Underlying_;
};

IClientPtr CreateClient(
    const TClientConfigPtr& config,
    const IPollerPtr& poller)
{
    // Initialize SSL.
    auto libraryLock = NRpc::NGrpc::TDispatcher::Get()->CreateLibraryLock();

    auto sslContext =  New<TSslContext>();
    if (config->Credentials->CertChain->FileName) {
        sslContext->AddCertificateChainFromFile(*config->Credentials->CertChain->FileName);
    } else if (config->Credentials->CertChain->Value) {
        sslContext->AddCertificateChain(*config->Credentials->CertChain->Value);
    } else {
        Y_UNREACHABLE();
    }
    if (config->Credentials->PrivateKey->FileName) {
        sslContext->AddPrivateKeyFromFile(*config->Credentials->PrivateKey->FileName);
    } else if (config->Credentials->PrivateKey->Value) {
        sslContext->AddPrivateKey(*config->Credentials->PrivateKey->Value);
    } else {
        Y_UNREACHABLE();
    }

    auto tlsDialer = sslContext->CreateDialer(
        New<TDialerConfig>(),
        poller,
        HttpLogger);

    auto httpClient = NHttp::CreateClient(
        config,
        tlsDialer,
        poller->GetInvoker());

    return New<TClient>(
        std::move(libraryLock),
        std::move(httpClient));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHttps
} // namespace NYT

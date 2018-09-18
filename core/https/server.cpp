#include "server.h"
#include "config.h"

#include <yt/core/http/server.h>

#include <yt/core/rpc/grpc/dispatcher.h>

#include <yt/core/crypto/tls.h>

#include <yt/core/net/address.h>

#include <yt/core/concurrency/poller.h>

namespace NYT {
namespace NHttps {

using namespace NNet;
using namespace NHttp;
using namespace NCrypto;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TServer
    : public IServer
{
public:
    TServer(
        NRpc::NGrpc::TGrpcLibraryLockPtr libraryLock,
        IServerPtr underlying)
        : LibraryLock_(std::move(libraryLock))
        , Underlying_(std::move(underlying))
    { }

    virtual void AddHandler(
        const TString& pattern,
        const IHttpHandlerPtr& handler) override
    {
        Underlying_->AddHandler(pattern, handler);
    }

    virtual const TNetworkAddress& GetAddress() const override
    {
        return Underlying_->GetAddress();
    }

    //! Starts the server.
    virtual void Start() override
    {
        Underlying_->Start();
    }

    //! Stops the server.
    virtual void Stop() override
    {
        Underlying_->Stop();
    }

private:
    const NRpc::NGrpc::TGrpcLibraryLockPtr LibraryLock_;
    const IServerPtr Underlying_;
};

IServerPtr CreateServer(
    const TServerConfigPtr& config,
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

    auto address = TNetworkAddress::CreateIPv6Any(config->Port);
    auto tlsListener = sslContext->CreateListener(address, poller);

    auto httpServer = NHttp::CreateServer(config, tlsListener, poller);

    return New<TServer>(std::move(libraryLock), std::move(httpServer));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHttps
} // namespace NYT

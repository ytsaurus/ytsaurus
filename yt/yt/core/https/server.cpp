#include "server.h"
#include "config.h"

#include <yt/core/http/server.h>

#include <yt/core/rpc/grpc/dispatcher.h>

#include <yt/core/crypto/tls.h>

#include <yt/core/net/address.h>

#include <yt/core/concurrency/poller.h>

namespace NYT::NHttps {

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
    const IPollerPtr& poller,
    const IPollerPtr& acceptor)
{
    // Initialize SSL.
    auto libraryLock = NRpc::NGrpc::TDispatcher::Get()->CreateLibraryLock();

    auto sslContext =  New<TSslContext>();
    if (config->Credentials->CertChain->FileName) {
        sslContext->AddCertificateChainFromFile(*config->Credentials->CertChain->FileName);
    } else if (config->Credentials->CertChain->Value) {
        sslContext->AddCertificateChain(*config->Credentials->CertChain->Value);
    } else {
        YT_ABORT();
    }
    if (config->Credentials->PrivateKey->FileName) {
        sslContext->AddPrivateKeyFromFile(*config->Credentials->PrivateKey->FileName);
    } else if (config->Credentials->PrivateKey->Value) {
        sslContext->AddPrivateKey(*config->Credentials->PrivateKey->Value);
    } else {
        YT_ABORT();
    }

    auto address = TNetworkAddress::CreateIPv6Any(config->Port);
    auto tlsListener = sslContext->CreateListener(address, poller, acceptor);

    auto configCopy = CloneYsonSerializable(config);
    configCopy->IsHttps = true;
    auto httpServer = NHttp::CreateServer(configCopy, tlsListener, poller, acceptor);

    return New<TServer>(std::move(libraryLock), std::move(httpServer));
}

IServerPtr CreateServer(const TServerConfigPtr& config, const IPollerPtr& poller)
{
    return CreateServer(config, poller, poller);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttps

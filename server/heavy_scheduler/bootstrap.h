#pragma once

#include "public.h"

namespace NYP::NServer::NHeavyScheduler {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
{
public:
    explicit TBootstrap(THeavySchedulerProgramConfigPtr config);

    const IInvokerPtr& GetControlInvoker();

    const TString& GetFqdn();

    const NClient::NApi::NNative::IClientPtr& GetClient();

    const TYTConnectorPtr& GetYTConnector();
    const THeavySchedulerPtr& GetHeavyScheduler();

    void Run();

private:
    class TImpl;
    const std::unique_ptr<TImpl> Impl_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NHeavyScheduler

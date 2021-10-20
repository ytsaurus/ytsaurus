#include "cell_balancer.h"

#include "bootstrap.h"
#include "config.h"

#include <yt/yt/server/lib/cypress_election/election_manager.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NCellBalancer {

using namespace NTransactionClient;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

class TCellBalancer
    : public ICellBalancer
{
public:
    TCellBalancer(IBootstrap* bootstrap, TCellBalancerConfigPtr config)
        : Bootstrap_(bootstrap)
        , Config_(std::move(config))
    { }

    void Start() override
    {
        VERIFY_INVOKER_AFFINITY(Bootstrap_->GetControlInvoker());
    }

    NYTree::IYPathServicePtr CreateOrchidService() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return IYPathService::FromProducer(BIND(&TCellBalancer::BuildOrchid, MakeStrong(this)))
            ->Via(Bootstrap_->GetControlInvoker());
    }

private:
    IBootstrap* const Bootstrap_;
    const TCellBalancerConfigPtr Config_;

    bool IsLeader()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Bootstrap_->GetElectionManager()->IsLeader();
    }

    void BuildOrchid(IYsonConsumer* consumer)
    {
        VERIFY_INVOKER_AFFINITY(Bootstrap_->GetControlInvoker());

        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("service").BeginMap()
                    .Item("connected").Value(IsLeader())
                .EndMap()
                .Item("config").Value(Config_)
            .EndMap();
    }
};

////////////////////////////////////////////////////////////////////////////////

ICellBalancerPtr CreateCellBalancer(IBootstrap* bootstrap, TCellBalancerConfigPtr config)
{
    return New<TCellBalancer>(bootstrap, std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer

#include "private.h"
#include "bootstrap.h"
#include "slot_manager.h"
#include "slot_provider.h"
#include "chaos_slot.h"

#include <yt/yt/server/lib/cellar_agent/cellar_manager.h>
#include <yt/yt/server/lib/cellar_agent/cellar.h>
#include <yt/yt/server/lib/cellar_agent/occupant.h>

#include <yt/yt/server/lib/chaos_node/config.h>

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

namespace NYT::NChaosNode {

using namespace NConcurrency;
using namespace NCellarAgent;
using namespace NCellarClient;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ChaosNodeLogger;

////////////////////////////////////////////////////////////////////////////////

class TSlotManager
    : public ISlotManager
{
public:
    TSlotManager(
        TChaosNodeConfigPtr config,
        IBootstrap* bootstrap)
        : Config_(config)
        , Bootstrap_(bootstrap)
        , SlotScanExecutor_(New<TPeriodicExecutor>(
            Bootstrap_->GetControlInvoker(),
            BIND(&TSlotManager::OnScanSlots, Unretained(this)),
            Config_->SlotScanPeriod))
    { }

    void Initialize() override
    {
        auto cellar = Bootstrap_->GetCellarManager()->GetCellar(ECellarType::Chaos);
        cellar->RegisterOccupierProvider(CreateChaosCellarOccupierProvider(Config_, Bootstrap_));
    }

    void Start() override
    {
        SlotScanExecutor_->Start();
    }

    DEFINE_SIGNAL_OVERRIDE(void(), BeginSlotScan);
    DEFINE_SIGNAL_OVERRIDE(void(IChaosSlotPtr), ScanSlot);
    DEFINE_SIGNAL_OVERRIDE(void(), EndSlotScan);

private:
    const TChaosNodeConfigPtr Config_;
    IBootstrap* const Bootstrap_;

    TPeriodicExecutorPtr SlotScanExecutor_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

    void OnScanSlots()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YT_LOG_DEBUG("Slot scan started");

        BeginSlotScan_.Fire();

        std::vector<TFuture<void>> asyncResults;
        for (const auto& occupant : Occupants()) {
            if (!occupant) {
                continue;
            }

            auto occupier = occupant->GetTypedOccupier<IChaosSlot>();
            if (!occupier) {
                continue;
            }

            asyncResults.push_back(
                BIND([=, this, this_ = MakeStrong(this)] () {
                    ScanSlot_.Fire(occupier);
                })
                .AsyncVia(occupier->GetGuardedAutomatonInvoker())
                .Run()
                // Silent any error to avoid premature return from WaitFor.
                .Apply(BIND([] (const TError&) { })));
        }
        auto result = WaitFor(AllSucceeded(asyncResults));
        YT_VERIFY(result.IsOK());

        EndSlotScan_.Fire();

        YT_LOG_DEBUG("Slot scan completed");
    }

    const std::vector<ICellarOccupantPtr>& Occupants() const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return Bootstrap_->GetCellarManager()->GetCellar(ECellarType::Chaos)->Occupants();
    }
};

////////////////////////////////////////////////////////////////////////////////

ISlotManagerPtr CreateSlotManager(
    TChaosNodeConfigPtr config,
    IBootstrap* bootstrap)
{
    return New<TSlotManager>(
        config,
        bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChaosNode::NYT

#include "cellar.h"

#include "bootstrap_proxy.h"
#include "cellar_manager.h"
#include "occupant.h"
#include "occupier.h"
#include "public.h"
#include "private.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/client/transaction_client/timestamp_provider.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/ytree/ypath_service.h>
#include <yt/yt/core/ytree/virtual.h>

namespace NYT::NCellarAgent {

using namespace NCellarClient;
using namespace NCellarNodeTrackerClient::NProto;
using namespace NConcurrency;
using namespace NElection;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = CellarAgentLogger;

////////////////////////////////////////////////////////////////////////////////

class TCellar
    : public ICellar
{
public:
    DEFINE_SIGNAL_OVERRIDE(void(), CreateOccupant);
    DEFINE_SIGNAL_OVERRIDE(void(), RemoveOccupant);
    DEFINE_SIGNAL_OVERRIDE(void(), UpdateOccupant);

public:
    TCellar(
        ECellarType type,
        TCellarConfigPtr config,
        ICellarBootstrapProxyPtr bootstrap)
        : Type_(type)
        , Config_(std::move(config))
        , Bootstrap_(std::move(bootstrap))
        , OrchidService_(TOrchidService::Create(MakeWeak(this), Bootstrap_->GetControlInvoker()))
    { }

    void Initialize() override
    {
        Occupants_.resize(Config_->Size);

        YT_LOG_DEBUG("Cellar initialized (CellarType: %v, Size: %v)",
            Type_,
            Config_->Size);
    }

    void Reconfigure(const TCellarDynamicConfigPtr& config) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        SetCellarSize(config->Size.value_or(Config_->Size));

        HydraDynamicConfig_ = config->HydraManager;

        for (auto& occupant : Occupants_) {
            if (occupant) {
                occupant->Reconfigure(HydraDynamicConfig_);
            }
        }
    }

    int GetAvailableSlotCount() const override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return Occupants_.size() - OccupantCount_;
    }

    int GetOccupantCount() const override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return OccupantCount_;
    }

    const std::vector<ICellarOccupantPtr>& Occupants() const override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return Occupants_;
    }

    ICellarOccupantPtr FindOccupant(TCellId id) const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto guard = ReaderGuard(CellIdToOccupantLock_);
        auto it = CellIdToOccupant_.find(id);
        return it == CellIdToOccupant_.end() ? nullptr : it->second;
    }

    ICellarOccupantPtr GetOccupant(TCellId id) const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto occupant = FindOccupant(id);
        YT_VERIFY(occupant);
        return occupant;
    }

    void CreateOccupant(const TCreateCellSlotInfo& createInfo) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        int index = GetFreeOccupantIndex();
        auto occupier = CreateOccupier(index);

        auto occupant = CreateCellarOccupant(index, Config_->Occupant, Bootstrap_, createInfo, occupier);
        Occupants_[index] = occupant;
        occupier->SetOccupant(occupant);
        occupant->Initialize();
        ++OccupantCount_;
        YT_VERIFY(OccupantCount_ <= std::ssize(Occupants_));

        {
            auto guard = WriterGuard(CellIdToOccupantLock_);
            YT_VERIFY(CellIdToOccupant_.emplace(occupant->GetCellId(), occupant).second);
        }

        CreateOccupant_.Fire();

        YT_LOG_DEBUG("Created cellar occupant (CellarType: %v, Index: %v)",
            Type_,
            index);
    }

    void ConfigureOccupant(
        const ICellarOccupantPtr& occupant,
        const TConfigureCellSlotInfo& configureInfo) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        occupant->Configure(configureInfo);
        if (HydraDynamicConfig_) {
            occupant->Reconfigure(HydraDynamicConfig_);
        }
    }

    void UpdateOccupant(
        const ICellarOccupantPtr& occupant,
        const TUpdateCellSlotInfo& updateInfo) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        occupant->UpdateDynamicConfig(updateInfo);

        UpdateOccupant_.Fire();
    }

    TFuture<void> RemoveOccupant(const ICellarOccupantPtr& occupant) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return occupant->Finalize()
            .Apply(BIND([=, this, this_ = MakeStrong(this)] (const TError&) {
                VERIFY_THREAD_AFFINITY(ControlThread);

                if (occupant->GetIndex() < std::ssize(Occupants_) && Occupants_[occupant->GetIndex()] == occupant) {
                    Occupants_[occupant->GetIndex()].Reset();
                    --OccupantCount_;
                    YT_VERIFY(OccupantCount_ >= 0);
                }

                {
                    auto guard = WriterGuard(CellIdToOccupantLock_);
                    if (auto it = CellIdToOccupant_.find(occupant->GetCellId()); it && it->second == occupant) {
                        CellIdToOccupant_.erase(it);
                    }
                }

                RemoveOccupant_.Fire();
            }).Via(Bootstrap_->GetControlInvoker()));
    }

    void RegisterOccupierProvider(ICellarOccupierProviderPtr provider) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (OccupierProvider_) {
            THROW_ERROR_EXCEPTION("Cellar %Qlv already has a provider", Type_);
        }

        YT_LOG_DEBUG("Registered occupier provider (CellarType: %v)", Type_);

        OccupierProvider_ = std::move(provider);
    }

    IYPathServicePtr GetOrchidService() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return OrchidService_;
    }

    void PopulateAlerts(std::vector<TError>* error) override
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        for (const auto& occupant : Occupants_) {
            if (occupant) {
                occupant->PopulateAlerts(error);
            }
        }
    }

private:
    class TOrchidService
        : public TVirtualMapBase
    {
    public:
        static IYPathServicePtr Create(TWeakPtr<TCellar> impl, IInvokerPtr invoker)
        {
            return New<TOrchidService>(std::move(impl))
                ->Via(invoker);
        }

        std::vector<TString> GetKeys(i64 limit) const override
        {
            std::vector<TString> keys;
            if (auto owner = Owner_.Lock()) {
                for (const auto& occupant : owner->Occupants()) {
                    if (std::ssize(keys) >= limit) {
                        break;
                    }
                    if (occupant) {
                        keys.push_back(ToString(occupant->GetCellId()));
                    }
                }
            }
            return keys;
        }

        i64 GetSize() const override
        {
            if (auto owner = Owner_.Lock()) {
                return owner->GetOccupantCount();
            }
            return 0;
        }

        IYPathServicePtr FindItemService(TStringBuf key) const override
        {
            if (auto owner = Owner_.Lock()) {
                if (auto occupant = owner->FindOccupant(TCellId::FromString(key))) {
                    return occupant->GetOrchidService();
                }
            }
            return nullptr;
        }

    private:
        const TWeakPtr<TCellar> Owner_;

        explicit TOrchidService(TWeakPtr<TCellar> owner)
            : Owner_(std::move(owner))
        { }

        DECLARE_NEW_FRIEND()
    };

    const ECellarType Type_;
    const TCellarConfigPtr Config_;
    const ICellarBootstrapProxyPtr Bootstrap_;

    int OccupantCount_ = 0;
    std::vector<ICellarOccupantPtr> Occupants_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, CellIdToOccupantLock_);
    THashMap<TCellId, ICellarOccupantPtr> CellIdToOccupant_;

    ICellarOccupierProviderPtr OccupierProvider_;

    const IYPathServicePtr OrchidService_;

    NHydra::TDynamicDistributedHydraManagerConfigPtr HydraDynamicConfig_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

    int GetFreeOccupantIndex()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        for (int index = 0; index < std::ssize(Occupants_); ++index) {
            if (!Occupants_[index]) {
                return index;
            }
        }

        THROW_ERROR_EXCEPTION("Cellar %Qlv is full", Type_);
    }

    ICellarOccupierPtr CreateOccupier(int index)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YT_LOG_DEBUG("Create occupier (CellarType: %v)",
            Type_);

        if (!OccupierProvider_) {
            THROW_ERROR_EXCEPTION("No provider at cellar %Qlv", Type_);
        }

        return OccupierProvider_->CreateCellarOccupier(index);
    }

    void SetCellarSize(int cellarSize)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (std::ssize(Occupants_) == cellarSize) {
            return;
        }

        YT_LOG_INFO("Updating cellar size (CellarType: %v, OldCellarSize: %v, NewCellarSize: %v)",
            Type_,
            Occupants_.size(),
            cellarSize);

        if (cellarSize < std::ssize(Occupants_)) {
            std::vector<TFuture<void>> futures;
            for (int index = cellarSize; index < std::ssize(Occupants_); ++index) {
                if (const auto& occupant = Occupants_[index]) {
                    futures.push_back(RemoveOccupant(occupant));
                }
            }

            auto error = WaitFor(AllSet(std::move(futures)));
            YT_LOG_ALERT_UNLESS(error.IsOK(), error, "Failed to finalize occpant during cellar reconfiguration");
        }

        while (std::ssize(Occupants_) > cellarSize) {
            if (const auto& occupant = Occupants_.back()) {
                THROW_ERROR_EXCEPTION("Slot %v with cell %d did not finalize properly, total slot count update failed",
                    occupant->GetIndex(),
                    occupant->GetCellId());
            }
            Occupants_.pop_back();
        }

        Occupants_.resize(cellarSize);

        if (cellarSize > 0) {
            // Requesting latest timestamp enables periodic background time synchronization.
            // For tablet nodes, it is crucial because of non-atomic transactions that require
            // in-sync time for clients.
            Bootstrap_
                ->GetClient()
                ->GetNativeConnection()
                ->GetTimestampProvider()
                ->GetLatestTimestamp();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

ICellarPtr CreateCellar(
    ECellarType type,
    TCellarConfigPtr config,
    ICellarBootstrapProxyPtr bootstrap)
{
    return New<TCellar>(
        type,
        std::move(config),
        std::move(bootstrap));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellarAgent

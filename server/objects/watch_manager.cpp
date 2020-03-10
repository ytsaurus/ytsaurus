#include "watch_manager.h"
#include "config.h"
#include "db_schema.h"
#include "helpers.h"
#include "persistence.h"
#include "private.h"
#include "table_version_checker.h"

#include <yp/server/master/bootstrap.h>
#include <yp/server/master/yt_connector.h>

#include <yt/client/api/transaction.h>

#include <yt/ytlib/api/native/client.h>

#include <yt/core/profiling/timing.h>

#include <util/datetime/cputimer.h>

namespace NYP::NServer::NObjects {

using namespace NYT::NApi;
using namespace NYT::NConcurrency;
using namespace NYT::NTableClient;
using namespace NYT::NYTree;
using namespace NYT::NYson;
using namespace NYT::NProfiling;

////////////////////////////////////////////////////////////////////////////////

class TWatchManager::TImpl
    : public TRefCounted
{
public:
    TImpl(NMaster::TBootstrap* bootstrap, TWatchManagerConfigPtr config)
        : Bootstrap_(bootstrap)
        , Config_(std::move(config))
    { }

    void Initialize()
    {
        const auto& ytConnector = Bootstrap_->GetYTConnector();
        ytConnector->SubscribeValidateConnection(BIND(&TImpl::OnValidateConnection, MakeWeak(this)));
    }

    bool Enabled() const
    {
        return Config_->Enabled;
    }

    std::vector<TTabletInfo> GetTabletInfos(
        EObjectType objectType)
    {
        const auto& ytConnector = Bootstrap_->GetYTConnector();
        const auto& client = ytConnector->GetClient();
        const auto tablePath = ytConnector->GetTablePath(GetWatchLogTable(objectType));

        std::vector<int> indexes(GetTabletCount(objectType));
        std::iota(indexes.begin(), indexes.end(), 0);

        return WaitFor(client->GetTabletInfos(tablePath, indexes))
            .ValueOrThrow();
    }

    std::vector<TTabletInfo> WaitForTabletInfos(
        EObjectType objectType,
        TTimestamp barrierTimestamp,
        TDuration timeLimit)
    {
        YT_LOG_DEBUG("Started waiting for tablet infos");

        if (timeLimit > Config_->MaxWaitForTabletInfosTimeLimit) {
            THROW_ERROR_EXCEPTION("Time limit should be less or equal than %v, actually %v",
                Config_->MaxWaitForTabletInfosTimeLimit,
                timeLimit);
        }

        std::vector<TTabletInfo> tabletInfos;

        TWallTimer timer;
        timer.Start();

        do {
            tabletInfos = GetTabletInfos(objectType);
            const auto currentBarrierTimestamp = GetBarrierTimestamp(tabletInfos);

            if (currentBarrierTimestamp >= barrierTimestamp) {
                YT_LOG_DEBUG("Barrier timestamp reached (expected: %v, actual: %v)",
                    barrierTimestamp,
                    currentBarrierTimestamp);
                break;
            }

            if (timer.GetElapsedTime() > timeLimit) {
                THROW_ERROR_EXCEPTION("Time limit exceeded waiting for tablet infos")
                    << TErrorAttribute("time_limit", timeLimit);
            }

            YT_LOG_DEBUG("Tablet infos not ready (expected: %v, actual: %v), so fall asleep for %v",
                barrierTimestamp,
                currentBarrierTimestamp,
                Config_->TabletInfosPollInterval);

            TDelayedExecutor::WaitForDuration(Config_->TabletInfosPollInterval);
        } while (true);

        YT_LOG_DEBUG("Finished waiting for tablet infos");
        return tabletInfos;
    }

    void RegisterWatchableObjectType(
        EObjectType objectType,
        const TString& objectTable)
    {
        static const TString ObjectTableSuffix = "_watch_log";

        const auto watchLog = objectTable + ObjectTableSuffix;
        YT_LOG_INFO("Register watch log (Type: %v, Table: %v, WatchLog: %v)",
            objectType,
            objectTable,
            watchLog);
        TablePerObjectType_[objectType] = std::make_unique<TDBTable>(watchLog);
    }

    const TDBTable* GetWatchLogTable(EObjectType type) const
    {
        const auto& table = TablePerObjectType_[type];
        YT_VERIFY(table);
        return table.get();
    }

    i64 GetTabletCount(EObjectType type) const
    {
        const auto& table = TablePerObjectType_[type];
        YT_VERIFY(table);
        return TabletCountPerObjectType_[type];
    }

private:
    NMaster::TBootstrap* const Bootstrap_;
    const TWatchManagerConfigPtr Config_;

    TEnumIndexedVector<EObjectType, std::unique_ptr<TDBTable>> TablePerObjectType_;
    TEnumIndexedVector<EObjectType, i64> TabletCountPerObjectType_;

    void OnValidateConnection()
    {
        const auto& ytConnector = Bootstrap_->GetYTConnector();
        auto transaction = ytConnector->GetInstanceLockTransaction();

        {
            TTableVersionChecker tableVersionChecker(ytConnector);
            for (const auto& table : TablePerObjectType_) {
                if (table) {
                    tableVersionChecker.ScheduleCheck(table.get());
                }
            }
            tableVersionChecker.Check();
        }

        {
            std::vector<TFuture<void>> asyncResults;
            for (const auto objectType : TEnumTraits<EObjectType>::GetDomainValues()) {
                const auto* table = TablePerObjectType_[objectType].get();
                if (!table) {
                    continue;
                }

                const auto path = ytConnector->GetTablePath(table);
                asyncResults.push_back(transaction->GetNode(path + "/@tablet_count")
                    .Apply(BIND([=] (const TErrorOr<TYsonString>& ysonTabletCountOrError) {
                        THROW_ERROR_EXCEPTION_IF_FAILED(ysonTabletCountOrError, "Error getting tablet count of table %v",
                            path);
                        const auto tabletCount = ConvertTo<int>(ysonTabletCountOrError.Value());
                        TabletCountPerObjectType_[objectType] = tabletCount;
                    })));
            }

            WaitFor(Combine(asyncResults))
                .ThrowOnError();
        }
    }
};

TWatchManager::TWatchManager(
    NMaster::TBootstrap* bootstrap,
    TWatchManagerConfigPtr config)
    : Impl_(New<TImpl>(bootstrap, std::move(config)))
{ }

TWatchManager::~TWatchManager()
{ }

void TWatchManager::Initialize()
{
    Impl_->Initialize();
}

bool TWatchManager::Enabled() const
{
    return Impl_->Enabled();
}

std::vector<TTabletInfo> TWatchManager::GetTabletInfos(EObjectType objectType)
{
    return Impl_->GetTabletInfos(objectType);
}

std::vector<TTabletInfo> TWatchManager::WaitForTabletInfos(
    EObjectType objectType,
    TTimestamp barrierTimestamp,
    TDuration timeLimit)
{
    return Impl_->WaitForTabletInfos(objectType, barrierTimestamp, timeLimit);
}

void TWatchManager::RegisterWatchableObjectType(
    EObjectType objectType,
    const TString& objectTable)
{
    Impl_->RegisterWatchableObjectType(objectType, objectTable);
}

const TDBTable* TWatchManager::GetWatchLogTable(EObjectType type) const
{
    return Impl_->GetWatchLogTable(type);
}

i64 TWatchManager::GetTabletCount(EObjectType type) const
{
    return Impl_->GetTabletCount(type);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects

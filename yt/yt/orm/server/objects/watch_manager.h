#pragma once

#include "config.h"

#include <yt/yt/client/api/client.h>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

struct TWatchQueryProfilingValues
{
    THashMap<int, i64> PerTabletSkippedRows;
};

////////////////////////////////////////////////////////////////////////////////

class TWatchManager
    : public TRefCounted
{
public:
    TWatchManager(
        NMaster::IBootstrap* bootstrap,
        TWatchManagerConfigPtr config);
    ~TWatchManager();

    void Initialize();

    TWatchManagerConfigPtr GetConfig() const;

    TFuture<std::vector<NYT::NApi::TTabletInfo>> GetTabletInfos(
        TObjectTypeValue objectType,
        const TString& logName,
        const std::vector<int>& tablets);

    TFuture<int> GetTabletCount(TObjectTypeValue objectType, const TString& logName);

    TErrorOr<std::vector<NYT::NApi::TTabletInfo>> WaitForBarrier(
        TObjectTypeValue objectType,
        const TString& logName,
        const std::vector<int>& tablets,
        TTimestamp barrierTimestamp,
        TDuration timeLimit);

    void RegisterLogs(const IObjectTypeHandler* typeHandler);

    const TDBTable* GetLogTableOrCrash(
        TObjectTypeValue objectType,
        const TString& logName) const;

    const TDBTable* GetLogTableOrThrow(
        TObjectTypeValue objectType,
        const TString& logName) const;

    const std::vector<TWatchLog>& GetLogs(
        TObjectTypeValue objectType) const;

    bool IsLogRegistered(
        TObjectTypeValue objectType,
        const TString& logName) const;

    const TWatchLog& GetLogByNameOrCrash(
        TObjectTypeValue objectType,
        const TString& logName) const;

    const TWatchLog& GetLogByNameOrThrow(
        TObjectTypeValue objectType,
        const TString& logName) const;

    const IWatchLogEventMatcher* GetLogEventMatcher(
        TObjectTypeValue objectType,
        const TString& logName) const;

    void UpdateQueryProfiling(
        TObjectTypeValue objectType,
        const TString& logName,
        TWatchQueryProfilingValues values) const;

    TString GetConsumerPath(
        const TString& consumerName) const;

    TWatchManagerExtendedConfig GetExtendedConfig() const;

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TWatchManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects

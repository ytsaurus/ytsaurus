#pragma once

#include "timers.h"

#include <util/generic/fwd.h>
#include <util/generic/set.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <yt/yt/client/api/public.h>
#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/transaction.h>

namespace NRoren::NPrivate
{

void CreateTimerTable(const NYT::NApi::IClientPtr ytClient, const NYT::NYPath::TYPath& timerTable);
void CreateTimerIndexTable(const NYT::NApi::IClientPtr ytClient, const NYT::NYPath::TYPath& timerIndexTable);
void CreateTimerMigrateTable(const NYT::NApi::IClientPtr ytClient, const NYT::NYPath::TYPath& timerMigrateTable);

TVector<TTimer> YtSelectIndex(const NYT::NApi::IClientPtr ytClient, const NYT::NYPath::TYPath& timerIndexTable, const TTimer::TShardId shardId, const size_t limit);
TVector<TTimer> YtSelectMigrate(const NYT::NApi::IClientPtr ytClient, const NYT::NYPath::TYPath& migrateTable, const TTimer::TShardId shardId, const size_t limit);
TVector<TTimer> YtLookupTimers(const NYT::NApi::IClientBasePtr tx, const NYT::NYPath::TYPath& timerTable, const TVector<TTimer::TKey>& keys);
void YtInsertMigrate(const NYT::NApi::ITransactionPtr tx, const NYT::NYPath::TYPath& migrateTable, const TTimer& timer, const TTimer::TShardId shardId);
void YtInsertTimer(const NYT::NApi::ITransactionPtr tx, const NYT::NYPath::TYPath& timerTable, const TTimer& timer);
void YtDeleteTimer(const NYT::NApi::ITransactionPtr tx, const NYT::NYPath::TYPath& timerTable, const TTimer::TKey& key);
void YtInsertIndex(const NYT::NApi::ITransactionPtr tx, const NYT::NYPath::TYPath& timerIndexTable, const TTimer& timer, TTimer::TShardId shardId);
void YtDeleteIndex(const NYT::NApi::ITransactionPtr tx, const NYT::NYPath::TYPath& timerIndexTable, const TTimer& timer, TTimer::TShardId shardId);

}  // namespace NRoren::NPrivate


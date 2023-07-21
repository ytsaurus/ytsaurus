#pragma once

#include <util/generic/fwd.h>
#include <util/generic/set.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/mutex.h>
#include <yt/yt/client/api/public.h>
#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/transaction.h>
#include <yt/cpp/roren/library/timers/proto/timers.pb.h>

namespace NRoren::NPrivate
{

class TTimers
{
public:
    struct TTimer:
        public TTimerProto
    {
        using TShardId = uint64_t;
        using TRawKey = TString;
        using TTimerId = TString;
        using TCallbackId = TString;
        using TTimestamp = uint64_t;
        using TUserData = std::optional<TString>;

        enum class EMergePolicy
        {
            REPLACE,
            MIN,
            MAX
        };

        using TKey = TTimerKeyProto;
        using TValue = TTimerValueProto;

        TTimer(TTimerProto timerProto);
        TTimer(TTimer&&) = default;
        TTimer(const TTimer&) = default;
        TTimer(const TRawKey& rawKey, const TTimerId& timerId, const TCallbackId& callbackId, const TTimestamp& timestamp, const TUserData& userData);
        TTimer& operator = (TTimer&&) = default;

        bool operator == (const TTimer& other) const noexcept;
        bool operator < (const TTimer& other) const noexcept;

        struct TKeyHasher
        {
            size_t operator () (const TKey& key) const;
        };

        struct TValueHasher
        {
            size_t operator () (const TValue& value) const;
        };

        struct THasher
        {
            size_t operator () (const TTimer& timer) const;
        };

        struct TKeyEqual
        {
            bool operator () (const TTimer::TKey& a, const TTimer::TKey& b);
        };
    };  // TTimerData

    using TShardProvider = std::function<TTimer::TShardId(const TTimer::TRawKey&)>;
    using TTimersHashMap = THashMap<TTimer::TKey, std::tuple<TTimer, TTimer::EMergePolicy>, TTimer::TKeyHasher, TTimer::TKeyEqual>;

    struct TYtTimerIndex
    {
        TTimer::TShardId ShardId;
        TTimer Timer;
    };

    void ReInit();
    void Commit(const NYT::NApi::ITransactionPtr tx, const TTimers::TTimersHashMap& updates);
    void OnCommit();

    TVector<TTimer> GetReadyTimers(size_t limit);

    bool IsValidForExecute(const TTimer& timer, const bool isTimerChanged);

    TTimers(const NYT::NApi::IClientPtr ytClient, NYT::NYPath::TYPath ytPath, TTimer::TShardId shardId, TShardProvider shardProvider);

protected:
    static TTimer MergeTimers(const std::optional<TTimer>& oldTimer, const TTimer& newTimer, const TTimer::EMergePolicy policy);
    void Cleanup();
    void Migrate(const TTimer& timer, const TTimer::TShardId shardId);
    void PopulateIndex();

    TVector<TTimer> YtSelectIndex();
    TVector<TTimer> YtSelectMigrate();
    TVector<TTimer> YtLookupTimers(const NYT::NApi::IClientBasePtr tx, const TVector<TTimer::TKey>& keys);
    void YtInsertMigrate(const NYT::NApi::ITransactionPtr tx, const TTimer& timer, const TTimer::TShardId shardId);
    void YtInsertTimer(const NYT::NApi::ITransactionPtr tx, const TTimer& timer);
    void YtInsertIndex(const NYT::NApi::ITransactionPtr tx, const TTimer& timer);
    void YtDeleteTimer(const NYT::NApi::ITransactionPtr tx, const TTimer::TKey& key);
    void YtDeleteIndex(const NYT::NApi::ITransactionPtr tx, const TTimer& timer);

    TGuard<TMutex> RAIILock();

    TMutex  Mutex_;
    std::atomic<bool> PopulateInProgress_ = false;
    uint64_t IndexLimit_ = 16384;
    uint64_t IndexSelectBatch_ = 1024;
    NYT::NApi::IClientPtr YtClient_;
    NYT::NYPath::TYPath YTimersPath_;
    NYT::NYPath::TYPath YTimersIndexPath_;
    NYT::NYPath::TYPath YTimersMigratePath_;
    TTimer::TShardId ShardId_ = 0;
    TSet<TTimer> TimerIndex_;
    TShardProvider GetShardId_;
    THashSet<TTimer, TTimer::THasher> TimerInFly_;
    THashSet<TTimer, TTimer::THasher> DeletedTimers_;
};

using TYtTimerIndex = TTimers::TYtTimerIndex;

}  // namespace NRoren::NPrivate

namespace NRoren {
    using TTimer = NPrivate::TTimers::TTimer;
}


#include "private.h"

#include "job_balancer.h"
#include "job_balancer_common.h"
#include "job_balancer_greedy.h"
#include "job_balancer_resource_queue.h"

#include <util/generic/map.h>

#include <yt/yt/flow/library/cpp/common/computation_controller.h>
#include <yt/yt/flow/library/cpp/common/flow_view.h>

#include <yt/yt/flow/library/cpp/misc/weighted_random.h>

namespace NYT::NFlow::NBalancer {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = NController::ControllerLogger;
using TControllersMap = THashMap<TComputationId, IComputationControllerPtr>;

////////////////////////////////////////////////////////////////////////////////

//! Minimum interval when we can consider the metrics from a job reliable. Set equal to the maximum EMA window for performance metrics (with some margin to make sure 10-minute counters are actually delivered).
static constexpr TDuration StableJobInterval = TDuration::Minutes(12);
//! Pretty much arbitrary value.
static constexpr TDuration MaxJobInterval = TDuration::Minutes(40);

////////////////////////////////////////////////////////////////////////////////
//! All information about partition that is used for partition distribution over workers.
//! We may assume that cpu usage of each partition (Up) equals to:
//!  Up = Kc * Kw * Wp * Cp
//! where:
//!  Kc - computation coefficient.
//!  Kw - worker coefficient.
//!  Wp - (current) weight of the partition.
//!  Cp - individual complexity of the partition.
struct TPartitionDistributionInfo
{
    //! State from TPartition.
    EPartitionState State{};
    //! ComputationId from TPartition.
    TComputationId ComputationId;
    //! ComputationController->ComputePartitionWeight
    double Weight = 0.;
    //! Id of an original job if any.
    TJobId JobId;
    //! Original worker of a job if any.
    TWorkerPtr Worker;
    //! One of the original counters from status of an active job, if there's one.
    std::optional<double> InputCpuUsage;
    //! Calculated complexity (Cp from the formula above).
    double Complexity{};
    //! Calculated normalized CPU usage (Kc *  Wp * Cp).
    double NormalizedCpuUsage{};
    //! Time a job has been active.
    TDuration TimeSinceStart;
};

////////////////////////////////////////////////////////////////////////////////

class TSequenceIdGenerator
    : public TRefCounted
{
public:
    TSequenceId Next();
    void AdvanceTill(TSequenceId sequenceId);

private:
    std::atomic<i64> SequenceId_ = 0;
};

using TSequenceIdGeneratorPtr = TIntrusivePtr<TSequenceIdGenerator>;

////////////////////////////////////////////////////////////////////////////////

class TRebalanceActions
{
public:
    struct TRebalanceAction
    {
        ERebalanceActionType Type;
        TPartitionId PartitionId;
        std::string WorkerAddress;
        TPartitionDistributionInfo Info;

        TRebalanceAction MakeReverted() const;
    };

    //! A set of actions, that make sense only if applied together.
    class TRebalanceTransaction
    {
    public:
        TSequenceId SequenceId;

        //! Reattach to the specified generator and generate new sequenceId if previously was attached to a different generator.
        void ResequenceIfNeeded(const TSequenceIdGeneratorPtr& sequenceIdGenerator);

        //! Used to track down transactions both when created and applied.
        TGuid Id = TGuid::Create();

        std::vector<TRebalanceAction> Actions;

        explicit TRebalanceTransaction(const TSequenceIdGeneratorPtr& sequenceIdGenerator);
        TRebalanceTransaction(const TSequenceIdGeneratorPtr& sequenceIdGenerator, const std::vector<TRebalanceAction>& actions);

        TRebalanceTransaction MakeReverted() const;

        void Add(TRebalanceAction&& action)
        {
            Actions.push_back(std::move(action));
        }

        void Add(const TRebalanceAction& action)
        {
            Actions.push_back(action);
        }

        template <typename... Args>
        void Emplace(Args&&... args)
        {
            Actions.emplace_back(std::forward<Args>(args)...);
        }

        bool IsEmpty() const
        {
            return Actions.empty();
        }

        // Tries to apply all the actions: one by one. Before each action runs checker to greenlight attempt.
        // If some check fails, will apply reverter to all the actions already applied in reverse order.
        // Default reverter applies the reversed action.
        bool TransactionalApply(
            const std::function<void(const TRebalanceAction& action)>& doer,
            const std::function<bool(const TRebalanceAction& newAction)>& checker,
            const std::function<void(const TRebalanceAction& action)>& reverter) const;

        bool TransactionalApply(
            const std::function<void(const TRebalanceAction& action)>& doer,
            const std::function<bool(const TRebalanceAction& newAction)>& checker =
                [] (const TRebalanceAction&) {
                    return true;
                }) const;

    private:
        friend class TRebalanceActions;

        // If transaction is inside TRebalanceActions's Transactions, it will inherit the generator.
        TSequenceIdGeneratorPtr SequenceIdGenerator_;
    };

private:
    TSequenceIdGeneratorPtr SequenceIdGenerator_;

    //! If added object belongs to the same generator, verify sequenceId is higher that mine.
    void AssertSequenceId(const TRebalanceTransaction& transaction) const;
    //! If added object belongs to the same generator, verify sequenceId is higher that mine.
    void AssertSequenceId(const TRebalanceActions& actions) const;
    //! Verify that the other generator does (doesn't) match as expected.
    void AssertSameGenerator(const TSequenceIdGeneratorPtr& generator, bool fromSameGenerator) const;

public:
    std::deque<TRebalanceTransaction> Transactions;

    explicit TRebalanceActions(const TSequenceIdGeneratorPtr& sequenceIdGenerator = New<TSequenceIdGenerator>());
    TRebalanceActions MakeReverted() const;

    void AddAsTransaction(TRebalanceAction&& action);
    void AddAsTransaction(const TRebalanceAction& action);

    template <typename... Args>
    void EmplaceAsTransaction(Args&&... args)
    {
        Transactions.emplace_back(SequenceIdGenerator_, std::vector<TRebalanceAction>{TRebalanceAction{std::forward<Args>(args)...}});
    }

    void Merge(const TRebalanceActions& actions, std::optional<bool> fromSameGenerator = std::nullopt);
    void Merge(TRebalanceActions&& actions, std::optional<bool> fromSameGenerator = std::nullopt);

    TRebalanceTransaction& StartTransaction();

    void AddTransaction(TRebalanceTransaction&& transaction, std::optional<bool> fromSameGenerator = std::nullopt);
    void AddTransaction(const TRebalanceTransaction& transaction, std::optional<bool> fromSameGenerator = std::nullopt);

    bool IsEmpty() const;

    void DropAlreadyApplied(TSequenceId maxApplied);

    // TODO(vv-glazkov): use templates to allow rvalue references to callbacks.
    //! Applies all the transactions independently (each transaction is atomic).
    //! Returns the count of applied actions.
    template <class... Args>
    size_t TransactionalApply(const Args&... args) const
    {
        size_t count = 0;
        for (auto& transaction : Transactions) {
            count += static_cast<size_t>(transaction.TransactionalApply(args...));
        }
        return count;
    }

    std::optional<TSequenceId> GetSequenceId() const;

    //! Creates empty TRebalanceActions, inheriting SequenceIdGenerator from another.
    static TRebalanceActions NewSequencedAs(const TRebalanceActions& actions);
};

////////////////////////////////////////////////////////////////////////////////

struct TBalancerLoopContext
{
    struct TComputationProcessing
    {
        TComputationId Id;
        TInstant StartTime;
    };

    std::optional<TComputationProcessing> Computation;
    std::vector<std::string> WorkersRemaining;
};

////////////////////////////////////////////////////////////////////////////////

//! Stores internal balancer data that should outlive a single TBalancer instance.
class TPersistentBalanceManager
    : public TRefCounted
{
public:
    TPersistentBalanceManager();

    TBalancerLoopContext& GetLoopContext();
    double ActionBufferScore = std::numeric_limits<double>::infinity();
    TRebalanceActions ActionsBuffer;

private:
    TInstant Timestamp_;
    TBalancerLoopContext LoopContext_;
};

using TPersistentBalanceManagerPtr = TIntrusivePtr<TPersistentBalanceManager>;

////////////////////////////////////////////////////////////////////////////////

TSequenceId TSequenceIdGenerator::Next()
{
    return TSequenceId{++SequenceId_};
}

void TSequenceIdGenerator::AdvanceTill(TSequenceId sequenceId)
{
    // TODO(thenewone) having c++26 replace all the code below with SequenceId_.fetch_max(sequenceId.Underlying());
    auto prev_value = SequenceId_.load(std::memory_order::relaxed); // Non-atomic initial load can be an optimization.
    while (prev_value < sequenceId.Underlying() &&
        !SequenceId_.compare_exchange_weak(prev_value, sequenceId.Underlying(),
            std::memory_order::seq_cst,    // Success memory order.
            std::memory_order::relaxed)) { // Failure memory order.
        // The loop continues if the value was updated by another thread
        // or a spurious failure occurred (for compare_exchange_weak).
        // prev_value is updated by compare_exchange_weak on failure.
    }
}

////////////////////////////////////////////////////////////////////////////////

TRebalanceActions::TRebalanceAction TRebalanceActions::TRebalanceAction::MakeReverted() const
{
    auto result = *this;
    if (result.Type == ERebalanceActionType::Del) {
        result.Type = ERebalanceActionType::Add;
    } else {
        result.Type = ERebalanceActionType::Del;
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

TRebalanceActions::TRebalanceTransaction::TRebalanceTransaction(const TSequenceIdGeneratorPtr& sequenceIdGenerator, const std::vector<typename TRebalanceActions::TRebalanceAction>& actions)
    : Actions(actions)
    , SequenceIdGenerator_(sequenceIdGenerator)
{
    SequenceId = sequenceIdGenerator->Next();
}

TRebalanceActions::TRebalanceTransaction::TRebalanceTransaction(const TSequenceIdGeneratorPtr& sequenceIdGenerator)
    : SequenceIdGenerator_(sequenceIdGenerator)
{
    SequenceId = sequenceIdGenerator->Next();
}

TRebalanceActions::TRebalanceTransaction TRebalanceActions::TRebalanceTransaction::MakeReverted() const
{
    TRebalanceActions::TRebalanceTransaction result(SequenceIdGenerator_);

    result.Actions.reserve(Actions.size());
    for (const auto& t : std::ranges::reverse_view(Actions)) {
        result.Actions.push_back(t.MakeReverted());
    }
    return result;
}

void TRebalanceActions::TRebalanceTransaction::ResequenceIfNeeded(
    const TSequenceIdGeneratorPtr& sequenceIdGenerator)
{
    if (sequenceIdGenerator != SequenceIdGenerator_) {
        SequenceIdGenerator_ = sequenceIdGenerator;
        SequenceId = sequenceIdGenerator->Next();
    }
}

////////////////////////////////////////////////////////////////////////////////

TRebalanceActions::TRebalanceActions(const TSequenceIdGeneratorPtr& sequenceIdGenerator)
{
    SequenceIdGenerator_ = sequenceIdGenerator;
}

void TRebalanceActions::AssertSameGenerator(const TSequenceIdGeneratorPtr& generator, bool fromSameGenerator) const
{
    if (fromSameGenerator) {
        YT_ASSERT(generator == SequenceIdGenerator_);
    } else {
        YT_ASSERT(generator != SequenceIdGenerator_);
    }
}

void TRebalanceActions::AssertSequenceId(const TRebalanceTransaction& transaction) const
{
    if (transaction.SequenceIdGenerator_ == SequenceIdGenerator_ && !IsEmpty()) {
        YT_ASSERT(transaction.SequenceId > Transactions.back().SequenceId);
    }
}

void TRebalanceActions::AssertSequenceId(const TRebalanceActions& actions) const
{
    if (!actions.IsEmpty()) {
        // SequenceIdGenerator in actions and actions.Transactions.front() must be the same.
        AssertSequenceId(actions.Transactions.front());
    }
}

void TRebalanceActions::AddTransaction(TRebalanceTransaction&& transaction, std::optional<bool> fromSameGenerator)
{
    if (fromSameGenerator.has_value()) {
        AssertSameGenerator(transaction.SequenceIdGenerator_, *fromSameGenerator);
    }
    AssertSequenceId(transaction);
    Transactions.push_back(std::move(transaction));
    Transactions.back().ResequenceIfNeeded(SequenceIdGenerator_);
}

void TRebalanceActions::AddTransaction(
    const TRebalanceTransaction& transaction, std::optional<bool> fromSameGenerator)
{
    if (fromSameGenerator.has_value()) {
        AssertSameGenerator(transaction.SequenceIdGenerator_, *fromSameGenerator);
    }
    AssertSequenceId(transaction);
    Transactions.push_back(transaction);
    Transactions.back().ResequenceIfNeeded(SequenceIdGenerator_);
}

TRebalanceActions TRebalanceActions::MakeReverted() const
{
    TRebalanceActions result{SequenceIdGenerator_};

    for (const auto& t : std::ranges::reverse_view(Transactions)) {
        result.Transactions.push_back(t.MakeReverted());
        result.Transactions.back().ResequenceIfNeeded(result.SequenceIdGenerator_);
    }
    return result;
}

void TRebalanceActions::AddAsTransaction(TRebalanceAction&& action)
{
    Transactions.emplace_back(SequenceIdGenerator_, std::vector<TRebalanceAction>{std::move(action)});
}

void TRebalanceActions::AddAsTransaction(const TRebalanceAction& action)
{
    Transactions.emplace_back(SequenceIdGenerator_, std::vector<TRebalanceAction>{action});
}

void TRebalanceActions::Merge(const TRebalanceActions& actions, std::optional<bool> fromSameGenerator)
{
    if (fromSameGenerator.has_value()) {
        AssertSameGenerator(actions.SequenceIdGenerator_, *fromSameGenerator);
    }
    AssertSequenceId(actions);

    std::ranges::copy(actions.Transactions | std::views::transform([&] (const auto& value) {
        auto result = value;
        result.ResequenceIfNeeded(SequenceIdGenerator_);
        return result;
    }),
        std::back_inserter(Transactions));
}

void TRebalanceActions::Merge(TRebalanceActions&& actions, std::optional<bool> fromSameGenerator)
{
    if (fromSameGenerator.has_value()) {
        AssertSameGenerator(actions.SequenceIdGenerator_, *fromSameGenerator);
    }
    AssertSequenceId(actions);

    std::ranges::for_each(actions.Transactions, [&] (auto& value) {
        value.ResequenceIfNeeded(SequenceIdGenerator_);
    });
    std::ranges::move(actions.Transactions, std::back_inserter(Transactions));
}

void TRebalanceActions::DropAlreadyApplied(TSequenceId maxApplied)
{
    while (!Transactions.empty() && Transactions.front().SequenceId <= maxApplied) {
        Transactions.pop_front();
    }
    SequenceIdGenerator_->AdvanceTill(maxApplied);
}

TRebalanceActions::TRebalanceTransaction& TRebalanceActions::StartTransaction()
{
    return Transactions.emplace_back(SequenceIdGenerator_);
}

bool TRebalanceActions::IsEmpty() const
{
    for (const auto& transaction : Transactions) {
        if (!transaction.IsEmpty()) {
            return false;
        }
    }
    return true;
}

bool TRebalanceActions::TRebalanceTransaction::TransactionalApply(
    const std::function<void(const TRebalanceAction& action)>& doer,
    const std::function<bool(const TRebalanceAction& newAction)>& checker,
    const std::function<void(const TRebalanceAction& action)>& reverter) const
{
    int passedOk = 0;
    // We will apply this if some checker fails to revert all the actions already done.
    auto revertAction = [&] () {
        for (int i = passedOk - 1; i >= 0; i--) {
            reverter(Actions[i]);
        }
    };

    auto undoGuard = Finally(revertAction);

    for (const auto& action : Actions) {
        if (checker(action)) {
            doer(action);
            passedOk++;
        } else {
            return false;
        }
    }
    undoGuard.Release();
    return true;
}

bool TRebalanceActions::TRebalanceTransaction::TransactionalApply(
    const std::function<void(const TRebalanceAction& action)>& doer,
    const std::function<bool(const TRebalanceAction& newAction)>& checker) const
{
    auto reverter = [&] (const TRebalanceAction& action) {
        doer(action.MakeReverted());
    };
    return TransactionalApply(doer, checker, reverter);
}

TRebalanceActions TRebalanceActions::NewSequencedAs(const TRebalanceActions& actions)
{
    return TRebalanceActions(actions.SequenceIdGenerator_);
}

std::optional<TSequenceId> TRebalanceActions::GetSequenceId() const
{
    if (Transactions.empty()) {
        return std::nullopt;
    }
    return Transactions.back().SequenceId;
}

////////////////////////////////////////////////////////////////////////////////

//! All information about all partitions that is used for partition distribution over workers.
class TPartitionDistributionData
{
public:
    TPartitionDistributionData(const TFlowViewPtr& flowView, const TControllersMap& controllers, const TWorkerGroupId& workerGroup);
    TPartitionDistributionData(const TPartitionDistributionData&) = delete;
    TPartitionDistributionData& operator=(const TPartitionDistributionData&) = delete;

    const THashMap<TPartitionId, TPartitionDistributionInfo>& PartitionInfos() const
    {
        return PartitionInfos_;
    }

    double GetWorkerCoef(const std::string& address) const
    {
        return GetOrDefault(WorkerCoefs_, address, 1.);
    }

    double GetComputationCoef(const TComputationId& id) const
    {
        return GetOrDefault(ComputationCoefs_, id, AvgComputationCoef_);
    }

    double GetAverageCpuUsage(const std::string& address, const TComputationId& id);

    TDuration GetWorkerAvgJobInterval(const std::string& address) const
    {
        //! If the worker has no jobs, considering interval minimal possible.
        return GetOrDefault(WorkerAvgJobIntervals_, address, TDuration::Zero());
    }

private:
    //! Main table of partition information.
    THashMap<TPartitionId, TPartitionDistributionInfo> PartitionInfos_;
    //! Index in the table by ComputationId.
    THashMap<TComputationId, std::vector<TPartitionId>> ComputationPartitions_;
    //! Worker (by address) coefficient of CPU usage. If not present assumed to be 1.
    THashMap<std::string, double> WorkerCoefs_;
    //! Computation coefficient of CPU usage. If not present assumed to be AvgComputationCoef.
    THashMap<TComputationId, double> ComputationCoefs_;
    //! Average computation coefficient of CPU usage.
    double AvgComputationCoef_ = 1.;
    //! Average time a job is active on a worker.
    THashMap<std::string, TDuration> WorkerAvgJobIntervals_;

    //! Accumulate all known data in one table in data.
    void CollectPartitions(const TFlowViewPtr& flowView, const TControllersMap& controllers, const TWorkerGroupId& workerGroup);
    //! Make indexes of the main table in data.
    void GenerateIndexes();
    //! Find worker coefficients.
    //! Normalize them to be about 1 on average, so we could take 1 for unknown workers.
    void CalculateWorkerCoefs();
    //! Find computation coefficients.
    //! Normalize them so that individual partition complexities will be around 1 on average,
    //!  so we could take 1 as partition complexity if cpu load is unknown.
    void CalculateComputationCoefs();
    //! Calculate partition complexity, using interpolation when CPU usage is not known.
    void InterpolateComplexities();
    //! Normalize partition complexity.
    void NormalizeComplexities();
    //! Finalize CPU usage model.
    void FinalizeCpuUsage();
    //! Calculate the coefficient of AvgJobInterval of the workers' numbers.
    //! Is considered to be proportional to the average interval of the jobs currently executed on the worker (with upper limit on a single job interval).
    void CalculateWorkerAvgJobIntervals();
};

////////////////////////////////////////////////////////////////////////////////

//! Wise set of partitions with their CPU usage (doesn't matter normalized or not).
//! Allows searching by CPU usage.
class TEmulationPartitionSet
{
public:
    int Count = 0;
    double CpuUsage = 0.;
    std::set<std::pair<double, TPartitionId>> Spectre;
    std::set<TPartitionId> Partitions;

    void Add(const TPartitionId& id, double cpuUsage)
    {
        Partitions.insert(id);
        Count++;
        CpuUsage += cpuUsage;
        Spectre.emplace(cpuUsage, id);
    }

    void Del(const TPartitionId& id, double cpuUsage)
    {
        Partitions.erase(id);
        Count--;
        CpuUsage -= cpuUsage;
        Spectre.erase(std::pair(cpuUsage, id));
    }

    TPartitionId FindClosest(double cpuUsage) const
    {
        std::pair<double, TPartitionId> key{cpuUsage, {}};
        auto it = Spectre.lower_bound(key);
        if (it == Spectre.end()) {
            it = std::prev(it);
        } else if (it != Spectre.begin()) {
            double found2 = it->first;
            it = std::prev(it);
            double found1 = it->first;
            if (std::abs(cpuUsage - found1) > std::abs(cpuUsage - found2)) {
                it = std::next(it);
            }
        }
        return it->second;
    }
};

//! More complex set of partitions with their CPU usage.
//! Internally has separate sets for different partition statuses.
class TEmulationInfo
{
public:
    TEmulationPartitionSet All;
    TEmulationPartitionSet Executing;
    TEmulationPartitionSet Interrupting;

    void Add(const TPartitionId& id, const TPartitionDistributionInfo& info, double cpuUsage)
    {
        All.Add(id, cpuUsage);
        if (info.State == EPartitionState::Executing) {
            Executing.Add(id, cpuUsage);
        }
        if (info.State == EPartitionState::Interrupting || info.State == EPartitionState::Completing) {
            Interrupting.Add(id, cpuUsage);
        }
    }

    void Del(const TPartitionId& id, const TPartitionDistributionInfo& info, double cpuUsage)
    {
        All.Del(id, cpuUsage);
        if (info.State == EPartitionState::Executing) {
            Executing.Del(id, cpuUsage);
        }
        if (info.State == EPartitionState::Interrupting || info.State == EPartitionState::Completing) {
            Interrupting.Del(id, cpuUsage);
        }
    }

    bool Contains(const TPartitionId& id) const
    {
        return All.Partitions.contains(id);
    }
};

//! Empty info set, used as result when result is not found.
static const TEmulationInfo EmptyInfo;

////////////////////////////////////////////////////////////////////////////////

//! Approximately calculated desired number of jobs and CPU usage of a worker.
struct TEmulationTarget
{
    double Count = 0;
    double CpuUsage{};
    double AvgCpuUsage{};
};

//! Approximately calculated desired numbers of different states of jobs.
struct TEmulationTargets
{
    TEmulationTarget All;
    TEmulationTarget Executing;
    TEmulationTarget Interrupting;
};

////////////////////////////////////////////////////////////////////////////////

//! Emulated worker. Holds emulated metrics and indexed data about its computations and partitions.
struct TEmulationWorker
{
    //! Real worker.
    TWorkerPtr Worker;
    //! Copy of worker coef from TPartitionDistributionData.
    double WorkerCoef{};

    //! Current overall distribution Emulation_. All spectres in it a build by actual CPU usage.
    TEmulationInfo InfoOverall;
    //! Current distribution Emulation_ by each computation. All spectres in it a build by actual CPU usage.
    TMap<TComputationId, TEmulationInfo> InfoByComputations;

    //! Approximately calculated desired numbers per each computation.
    THashMap<TComputationId, TEmulationTargets> Targets;
};

////////////////////////////////////////////////////////////////////////////////

//! One single action of job distribution: add or remove of partition to/from worker.
struct TEmulationAction
{
    ERebalanceActionType Type;
    std::string WorkerAddress;
};

////////////////////////////////////////////////////////////////////////////////

//!  Distribution statistics of some value (actually CPU usage) over some keys (actually workers).
class TDistributionStat
{
public:
    int Count = 0;
    double Sum = 0.;
    double SumSq = 0.;
    std::set<std::pair<double, std::string>> Set;

    //! Take into account another value.
    void Add(double value, const std::string& id)
    {
        Count++;
        Sum += value;
        SumSq += value * value;
        Set.emplace(value, id);
        YT_VERIFY(Count == std::ssize(Set));
    }

    //! Remove a value for statistics set.
    void Del(double value, const std::string& id)
    {
        Count--;
        Sum -= value;
        SumSq -= value * value;
        Set.erase(std::pair(value, id));
        YT_VERIFY(Count == std::ssize(Set));
    }

    //! Get deviation of distribution.
    double Deviation() const
    {
        if (Count == 0) {
            return 0.;
        }
        double avg = Sum / Count;
        double avgSq = SumSq / Count;
        double variance = avgSq - avg * avg;
        if (variance < 0.) {
            variance = 0.;
        }
        double deviation = std::sqrt(variance);
        return deviation;
    }

    //! Get relative deviation of distribution.
    double RelativeDeviation() const
    {
        if (Count == 0) {
            return 0.;
        }
        double avg = Sum / Count;
        return Deviation() / avg;
    }
};

////////////////////////////////////////////////////////////////////////////////

//! Wise collection of workers, their partitions, stray partitions.
class TDistributionEmulation
{
public:
    TDistributionEmulation(const TFlowViewPtr& flowView, const TPartitionDistributionData& partitionData, const TWorkerGroupId& workerGroup);
    TDistributionEmulation(const TDistributionEmulation&) = delete;
    TDistributionEmulation& operator=(const TDistributionEmulation&) = delete;

    //! Map of workers by their addresses , see TEmulationWorker for details.
    const TMap<std::string, TEmulationWorker>& Workers() const
    {
        return Workers_;
    }

    //! Map of computation's informations by computation's IDs.
    const TMap<TComputationId, TEmulationInfo>& ComputationInfos() const
    {
        return InfoByComputations_;
    }

    //! Map of stray computation's informations by computation's IDs.
    const THashMap<TComputationId, TEmulationInfo>& StrayComputationInfos() const
    {
        return StrayInfoByComputations_;
    }

    const TEmulationInfo& GetInfo() const
    {
        return InfoOverall_;
    }

    const TEmulationInfo& GetInfo(const TComputationId& computationId) const
    {
        auto it = InfoByComputations_.find(computationId);
        return it == InfoByComputations_.end() ? EmptyInfo : it->second;
    }

    const TEmulationInfo& GetInfo(const TEmulationWorker& worker, const TComputationId& computationId) const
    {
        auto it = worker.InfoByComputations.find(computationId);
        return it == worker.InfoByComputations.end() ? EmptyInfo : it->second;
    }

    const TEmulationInfo& GetInfo(const TEmulationWorker& worker) const
    {
        return worker.InfoOverall;
    }

    const TEmulationInfo& GetStrayInfo() const
    {
        return StrayInfoOverall_;
    }

    //! Get relative deviation of CPU usage distribution between workers.
    double GetRelativeDeviation() const
    {
        return WorkerStat_.RelativeDeviation();
    }

    //! Get relative deviation of CPU usage distribution of given computation between workers.
    double GetRelativeDeviation(const TComputationId& computationId) const
    {
        if (!WorkerStatByComputations_.contains(computationId)) {
            return 0;
        }
        return WorkerStatByComputations_.at(computationId).RelativeDeviation();
    }

    //! Get computations, ordered by relative deviation of CPU usage distribution over workers.
    const std::set<std::pair<double, TComputationId>>& ComputationsByDeviation() const
    {
        return ComputationsByDeviation_;
    }

    //! Get current worker of a partition. The partition must be not stray!.
    std::string PartitionWorker(const TPartitionId& id) const
    {
        return PartitionWorker_.at(id);
    }

    //! Get saved normalized CPU usage of a partition. The partition must be not stray!.
    double PartitionNormalizedCpuUsage(const TPartitionId& id) const
    {
        return PartitionNormalizedUsage_.at(id);
    }

    //! Get actions that were made during rebalancing.
    const THashMap<TPartitionId, std::vector<TEmulationAction>>& Actions() const
    {
        return Actions_;
    }

    //! Obtain the distribution of the performance of workers (overall).
    const TDistributionStat& GetWorkerDistributionOverall() const&
    {
        return WorkerStat_;
    }

    TDistributionStat&& GetWorkerDistributionOverall() &&
    {
        return std::move(WorkerStat_);
    }

    //! Obtain the distribution of the performance of workers (by computation).
    const THashMap<TComputationId, TDistributionStat>& GetWorkerDistributionByComputations() const&
    {
        return WorkerStatByComputations_;
    }

    THashMap<TComputationId, TDistributionStat>&& GetWorkerDistributionByComputations() &&
    {
        return std::move(WorkerStatByComputations_);
    }

    int ActionCount() const
    {
        return ActionCount_;
    }

    //! Emulate adding of a partition to some worker.
    void AddPartition(const TPartitionId& id, const TPartitionDistributionInfo& info, const std::string& workerAddress);
    //! Emulate removal of a partition from some worker.
    void DelPartition(const TPartitionId& id, const TPartitionDistributionInfo& info, const std::string& workerAddress);
    //! Take into account a partition that does not belong to any worker.
    void AddStrayPartition(const TPartitionId& id, const TPartitionDistributionInfo& info);
    //! Stop accounting of a partition as not belonging to any worker.
    void DelStrayPartition(const TPartitionId& id, const TPartitionDistributionInfo& info);
    //! Apply all the actions from the given TRebalanceActions.
    void ApplyAll(const TRebalanceActions& actions, const TPartitionDistributionData& partitionData);

private:
    //! Emulation stores a number of PartitionSet by different datapoints. PartitionSet holds information about CPU usage.
    //! If a PartitionSet belongs to a particular worker, it uses real CPU usage; otherwise it used normalized CPU usage.
    //! Normalized CPU usage is CPU usage divided by WorkerCoef to be independent from worker power.

    //! Saved normalized CPU usage.
    THashMap<TPartitionId, double> PartitionNormalizedUsage_;
    //! Saved CPU usage. Is set only if a partition belongs to some worker.
    THashMap<TPartitionId, double> PartitionUsage_;
    //! Current worker of a partitions.
    THashMap<TPartitionId, std::string> PartitionWorker_;

    //! List of workers, see TEmulationWorker for details.
    TMap<std::string, TEmulationWorker> Workers_;
    //! Current distribution overall Emulation_.
    TEmulationInfo InfoOverall_;
    //! Current distribution Emulation_ by each computation.
    TMap<TComputationId, TEmulationInfo> InfoByComputations_;
    //! Information about partitions that don't belong to any worker.
    TEmulationInfo StrayInfoOverall_;
    //! Information about partitions that don't belong to any worker, grouped by computations.
    THashMap<TComputationId, TEmulationInfo> StrayInfoByComputations_;

    //! Statistics of CPU usage distribution over workers.
    TDistributionStat WorkerStat_;
    //! Statistics of CPU usage distribution over workers per each computation.
    THashMap<TComputationId, TDistributionStat> WorkerStatByComputations_;
    //! Computations, ordered by relative deviation of CPU usage distribution over workers.
    std::set<std::pair<double, TComputationId>> ComputationsByDeviation_;

    //! Action that were made during rebalancing.
    THashMap<TPartitionId, std::vector<TEmulationAction>> Actions_;
    int ActionCount_ = 0;

    void CollectWorkers(const TFlowViewPtr& flowView, const TPartitionDistributionData& partitionData, const TWorkerGroupId& workerGroup);
    void CollectPartitions(const TPartitionDistributionData& partitionData);
    void CalculateTargetValues();
};

////////////////////////////////////////////////////////////////////////////////

// Verifies that actions decided on before remain valid in the current FlowView.
class TRebalanceActionsVerifier
{
public:
    using TPartitionLocations = THashMap<TPartitionId, std::string>;

    TRebalanceActionsVerifier(const TFlowViewPtr& flowView)
        : FlowView_(flowView)
    { }

    TRebalanceActions VerifyWithKnownLocations(const TRebalanceActions& actions, TPartitionLocations& whereIs, bool rollback = false)
    {
        const TFlowStatePtr& flowState = FlowView_->State;
        const TExecutionSpecPtr& executionSpec = flowState->ExecutionSpec;
        const TFlowLayoutPtr& layout = executionSpec->Layout;
        TRebalanceActions result = TRebalanceActions::NewSequencedAs(actions);

        auto checker = [&] (const TRebalanceActions::TRebalanceAction& action) {
            if (!layout->Partitions.contains(action.PartitionId)) {
                return false;
            }

            if (!flowState->Workers.contains(action.WorkerAddress)) {
                return false;
            }

            if (layout->Partitions.at(action.PartitionId)->State != action.Info.State) {
                return false;
            }

            if (action.Type == ERebalanceActionType::Add) {
                if (whereIs.contains(action.PartitionId)) {
                    return false;
                }
            } else if (action.Type == ERebalanceActionType::Del) {
                if (!whereIs.contains(action.PartitionId) || whereIs[action.PartitionId] != action.WorkerAddress) {
                    return false;
                }
            }

            return true;
        };

        THashMap<TPartitionId, std::optional<std::string>> originalWhereIs;
        auto saveIfNeeded = [&] (const TPartitionId& partitionId) {
            if (rollback && !originalWhereIs.contains(partitionId)) {
                originalWhereIs[partitionId] = whereIs.contains(partitionId) ? std::optional<std::string>(whereIs[partitionId]) : std::nullopt;
            }
        };

        auto applier = [&] (const TRebalanceActions::TRebalanceAction& action) {
            if (action.Type == ERebalanceActionType::Add) {
                saveIfNeeded(action.PartitionId);
                whereIs[action.PartitionId] = action.WorkerAddress;
            } else if (action.Type == ERebalanceActionType::Del) {
                saveIfNeeded(action.PartitionId);
                whereIs.erase(action.PartitionId);
            }
        };

        for (const auto& transaction : actions.Transactions) {
            if (transaction.TransactionalApply(applier, checker) && transaction.Actions.size() > 0) {
                result.AddTransaction(transaction);
            }
        }

        // If no rollback is required, originalWhereIs will be empty.
        for (const auto& [partitionId, location] : originalWhereIs) {
            if (location.has_value()) {
                whereIs[partitionId] = *location;
            } else {
                whereIs.erase(partitionId);
            }
        }

        return result;
    }

    TPartitionLocations BuildPartitionLocations()
    {
        const TFlowStatePtr& flowState = FlowView_->State;
        const TExecutionSpecPtr& executionSpec = flowState->ExecutionSpec;
        const TFlowLayoutPtr& layout = executionSpec->Layout;
        TPartitionLocations whereIs;

        for (const auto& [partitionId, info] : layout->Partitions) {
            if (info->CurrentJobId.has_value()) {
                whereIs[partitionId] = layout->Jobs.at(info->CurrentJobId.value())->WorkerAddress;
                // A partition chosen for a graceful move keeps its job on the SOURCE worker until that
                // job finishes, but the emulation seed (CollectPartitions) already counts it on the
                // TARGET. Mirror that redirect here so both location maps agree; otherwise the verifier
                // keeps a "Del from source" the emulation can't reconcile and DelPartition aborts (:1539).
                auto* ephemeralStatePtr = FlowView_->EphemeralState->Partitions.FindPtr(partitionId);
                if (ephemeralStatePtr && (*ephemeralStatePtr)->PendingGracefulRebalanceWorkerAddress.has_value()) {
                    const auto& targetAddress = *(*ephemeralStatePtr)->PendingGracefulRebalanceWorkerAddress;
                    if (GetOrDefault(FlowView_->State->Workers, targetAddress, nullptr)) {
                        whereIs[partitionId] = targetAddress;
                    }
                }
            }
        }
        return whereIs;
    }

    TPartitionLocations BuildPartitionLocations(const TRebalanceActions& actions)
    {
        TPartitionLocations whereIs = BuildPartitionLocations();

        auto applier = [&] (const TRebalanceActions::TRebalanceAction& action) {
            if (action.Type == ERebalanceActionType::Add) {
                whereIs[action.PartitionId] = action.WorkerAddress;
            } else if (action.Type == ERebalanceActionType::Del) {
                whereIs.erase(action.PartitionId);
            }
        };
        actions.TransactionalApply(applier);
        return whereIs;
    }

    TRebalanceActions Verify(const TRebalanceActions& actions)
    {
        auto locations = BuildPartitionLocations();
        return VerifyWithKnownLocations(actions, locations);
    }

    TRebalanceActions VerifyWithPreapplied(const TRebalanceActions& preapplied, const TRebalanceActions& actions)
    {
        auto locations = BuildPartitionLocations(preapplied);
        return VerifyWithKnownLocations(actions, locations);
    }

    template <class TBackInsertable>
    TBackInsertable VerifyWorkers(const TBackInsertable& workers)
    {
        TBackInsertable result;
        std::copy_if(workers.begin(), workers.end(), std::back_inserter(result), [&] (const std::string& worker) {
            return FlowView_->State->Workers.contains(worker);
        });
        return result;
    }

    template <class TBackInsertable>
    TBackInsertable VerifyComputations(const TBackInsertable& computations)
    {
        THashSet<TComputationId> actualComputations;
        for (const auto& [_, partition] : FlowView_->State->ExecutionSpec->Layout->Partitions) {
            actualComputations.insert(partition->ComputationId);
        }

        TBackInsertable result;
        std::copy_if(computations.begin(), computations.end(), std::back_inserter(result), [&] (const TComputationId& computation) {
            return actualComputations.contains(computation);
        });
        return result;
    }

private:
    TFlowViewPtr FlowView_;
};

////////////////////////////////////////////////////////////////////////////////

//! Class that encapsulates CPU aware algorithm of job balancing.
class TBalancer
{
public:
    TBalancer(
        const TFlowViewPtr& flowView,
        const TControllersMap& controllers,
        const TDynamicJobBalancerSpecPtr& balancerSpec,
        const TWorkerGroupId& workerGroup,
        const TPersistentBalanceManagerPtr& persistentManager);

    //! Do the rebalancing.
    TRebalanceActions DoFastBalancing();
    TRebalanceActions DoSlowBalancing(const TInstant& until);
    void ApplyAll(const TRebalanceActions& actions);
    double GetTotalScore() const;

    //! Verifies that deferred actions still make some sense.
    TRebalanceActions ValidateDeferredActions(const TRebalanceActions& deferredActions);

    //! Get overall and per-computation worker performance distributions for the current emulation.
    const TDistributionStat& GetWorkerDistributionOverall() const&;
    TDistributionStat&& GetWorkerDistributionOverall() &&;
    const THashMap<TComputationId, TDistributionStat>& GetWorkerDistributionByComputations() const&;
    THashMap<TComputationId, TDistributionStat>&& GetWorkerDistributionByComputations() &&;

    //! Returns true if there are partitions not assigned to any worker.
    bool HasStrayPartitions() const;

    //! Returns true if worker CPU loads are uneven enough to warrant rebalancing. The pipeline is
    //! considered uneven only when ALL of three thresholds are exceeded: the absolute spread
    //! (RebalanceMinCpuSpread), the max/min ratio (RebalanceMinCpuRatio) and the relative deviation
    //! (> 2 * RebalanceTargetDeviation). The test-only DisableEvenLoadGate flag forces true.
    bool WorkerLoadUneven() const;

private:
    //! Collected and interpolated data about current state of the cluster.
    TPartitionDistributionData Data_;
    //! Staging area of experimenting with job balance.
    TDistributionEmulation Emulation_;

    TPersistentBalanceManagerPtr PersistentManager_;

    //! Dynamic spec from where we take magic constants.
    TDynamicJobBalancerSpecPtr ManagerSpec_;

    TRebalanceActions AlreadyApplied_;
    TRebalanceActionsVerifier Verifier_;

    //! Remove overcount executing partitions.
    TRebalanceActions KickPartitionsFromOvercountedWorkers();

    //! Find a place for stray partitions.
    TRebalanceActions DistributeStrayPartitions();
    TRebalanceActions DistributeStrayPartitions(EPartitionState partitionState);

    //! Find a place for stray partitions to reach target partition count.
    TRebalanceActions DistributeStrayPartitionsPhase1(EPartitionState partitionState);
    //! Distribute the remaining stray partitions.
    TRebalanceActions DistributeStrayPartitionsPhase2(EPartitionState partitionState);

    //! Try to move and exchange partitions to get better balance.
    TRebalanceActions RelieveWorker(const TComputationId& computationId, const std::string& myWorkerAddress);

    TRebalanceActions GetActions();
    std::optional<TPartitionId> SelectNextComputationToBalance();

    std::optional<TComputationId> AdvanceContextComputation();

    //! Get current score for given computation.
    double GetScore(const TComputationId& computationId);

    //! Get score of the current computation with actions applied.
    double AssessScore(const TRebalanceActions& actions, const TComputationId& computationId);

    //! Generate string report for logging.
    std::string GenerateInterimReport();

    //! Process one worker address.
    bool ProceedWithWorker(std::vector<std::string>& workerAddresses);
};

////////////////////////////////////////////////////////////////////////////////

TPartitionDistributionData::TPartitionDistributionData(const TFlowViewPtr& flowView, const TControllersMap& controllers, const TWorkerGroupId& workerGroup)
{
    CollectPartitions(flowView, controllers, workerGroup);
    GenerateIndexes();
    CalculateWorkerAvgJobIntervals();
    CalculateWorkerCoefs();
    CalculateComputationCoefs();
    InterpolateComplexities();
    NormalizeComplexities();
    FinalizeCpuUsage();
}

void TPartitionDistributionData::CollectPartitions(const TFlowViewPtr& flowView, const TControllersMap& controllers, const TWorkerGroupId& workerGroup)
{
    const auto& layout = flowView->State->ExecutionSpec->Layout;
    THashMap<TJobId, TPartitionId> jobIdToPartitionId;

    for (const auto& [partitionId, partition] : layout->Partitions) {
        if (partition->State != EPartitionState::Executing && partition->State != EPartitionState::Completing && partition->State != EPartitionState::Interrupting) {
            continue;
        }
        if (!ComputationBelongsToGroup(GetOrCrash(flowView->CurrentSpec->GetValue()->Computations, partition->ComputationId), workerGroup)) {
            continue;
        }
        auto controller = GetOrCrash(controllers, partition->ComputationId);
        double weight = controller->ComputePartitionWeight(partitionId, flowView);
        THROW_ERROR_EXCEPTION_IF(std::isnan(weight), "Partition weight is none");
        TPartitionDistributionInfo& info = PartitionInfos_[partitionId];
        info.State = partition->State;
        info.ComputationId = partition->ComputationId;
        info.Weight = weight;
    }

    for (const auto& [jobId, job] : layout->Jobs) {
        auto it = PartitionInfos_.find(job->PartitionId);
        if (it == PartitionInfos_.end()) {
            continue; // Perhaps it's wrong worker group, or not EPartitionState::Executing etc.
        }
        TPartitionDistributionInfo& info = it->second;
        auto worker = GetOrDefault(flowView->State->Workers, job->WorkerAddress, nullptr);
        // Skip unknown workers. We should have already make stopping mutation for them.
        if (worker && worker->IncarnationId == job->WorkerIncarnationId) {
            info.JobId = jobId;
            info.Worker = worker;
            jobIdToPartitionId[jobId] = job->PartitionId;

            // If this partition is being gracefully migrated to another worker, treat it as
            // already residing on the target worker for balancing purposes. This prevents the
            // balancer from repeatedly proposing the same (or a different) move while the job
            // is still finishing its current epoch.
            auto* ephemeralStatePtr = flowView->EphemeralState->Partitions.FindPtr(job->PartitionId);
            if (ephemeralStatePtr && (*ephemeralStatePtr)->PendingGracefulRebalanceWorkerAddress.has_value()) {
                const auto& targetAddress = *(*ephemeralStatePtr)->PendingGracefulRebalanceWorkerAddress;
                auto targetWorker = GetOrDefault(flowView->State->Workers, targetAddress, nullptr);
                if (targetWorker) {
                    info.Worker = targetWorker;
                }
            }
        }
    }

    for (const auto& [partitionId, partitionJobStatus] : flowView->Feedback->PartitionJobStatuses) {
        auto& currentJobStatus = partitionJobStatus->CurrentJobStatus;
        auto it2 = PartitionInfos_.find(partitionId);
        if (it2 == PartitionInfos_.end()) {
            continue;
        }
        auto& info = it2->second;
        if (currentJobStatus) {
            if (currentJobStatus->PerformanceMetrics->CpuUsage10m) {
                info.InputCpuUsage = currentJobStatus->PerformanceMetrics->CpuUsage10m;
            } else if (currentJobStatus->PerformanceMetrics->CpuUsage30s) {
                info.InputCpuUsage = currentJobStatus->PerformanceMetrics->CpuUsage30s;
            } else {
                info.InputCpuUsage = currentJobStatus->PerformanceMetrics->CpuUsageCurrent;
            }
            info.TimeSinceStart = TInstant::Now() - currentJobStatus->StartTime;

            //! It is not always practical to set up StartTime in UT, therefore for uninited startTime we will consider TimeSinceStart 0.
            if (currentJobStatus->StartTime == TInstant::Zero()) {
                info.TimeSinceStart = TDuration::Zero();
            }
        }
    }
}

void TPartitionDistributionData::GenerateIndexes()
{
    for (const auto& [partitionId, info] : PartitionInfos_) {
        ComputationPartitions_[info.ComputationId].push_back(partitionId);
    }
}

void TPartitionDistributionData::CalculateWorkerCoefs()
{
    auto ignorePartition = [] (const TPartitionDistributionInfo& info) {
        return info.State != EPartitionState::Executing || !info.Worker ||
            !info.InputCpuUsage.has_value() || info.InputCpuUsage.value() <= 0.;
    };

    THashMap<TComputationId, double> sumComputationCpuUsage;
    THashMap<TComputationId, int> numComputationCpuUsage;
    for (const auto& [partitionId, info] : PartitionInfos_) {
        if (ignorePartition(info)) {
            continue;
        }
        double value = info.InputCpuUsage.value() / info.Weight;
        sumComputationCpuUsage[info.ComputationId] += value;
        numComputationCpuUsage[info.ComputationId]++;
    }
    THashMap<TComputationId, double> avgComputationCpuUsage;
    for (const auto& [computationId, count] : numComputationCpuUsage) {
        if (count > 1) {
            avgComputationCpuUsage[computationId] = sumComputationCpuUsage.at(computationId) / count;
        }
    }

    THashMap<std::string, double> sumWorkerCoef;
    THashMap<std::string, int> numWorkerCoef;
    for (const auto& [partitionId, info] : PartitionInfos_) {
        if (ignorePartition(info) || !avgComputationCpuUsage.contains(info.ComputationId)) {
            continue;
        }
        double value = info.InputCpuUsage.value() / info.Weight;
        double normalizedValue = value / avgComputationCpuUsage.at(info.ComputationId);
        sumWorkerCoef[info.Worker->RpcAddress] += normalizedValue;
        numWorkerCoef[info.Worker->RpcAddress]++;
    }
    THashMap<std::string, double> avgWorkerCoef;
    for (const auto& [address, count] : numWorkerCoef) {
        if (count > 1) {
            avgWorkerCoef[address] = sumWorkerCoef.at(address) / count;
        }
    }

    for (const auto& [address, avgCoef] : avgWorkerCoef) {
        const double safeWorkerCoef = 1;

        TDuration avgJobInterval = GetWorkerAvgJobInterval(address);
        TDuration startSwitchToUnsafe = StableJobInterval;
        TDuration endSwitchToUnsafe = MaxJobInterval;
        if (avgJobInterval < startSwitchToUnsafe) {
            // If no reliable metrics is available, it's safer to assume all workercoefs are 1.
            // Among other things, it will address the cases of malfunctioning pipeline (e. g. due to YT errors)
            // by effectively freezing the balancing until sufficient number of jobs are working stably.
            WorkerCoefs_[address] = safeWorkerCoef;
        } else if (avgJobInterval >= endSwitchToUnsafe) {
            WorkerCoefs_[address] = avgCoef;
        } else {
            // When we got somewhat reliable data, we should start smooth (to not cause massive job migrations) transition to actual workercoefs.
            double percent = (avgJobInterval - startSwitchToUnsafe) / (endSwitchToUnsafe - startSwitchToUnsafe);
            WorkerCoefs_[address] = std::lerp(safeWorkerCoef, avgCoef, percent);
        }
        YT_LOG_EVENT(
            NController::BalancerLogger,
            NLogging::ELogLevel::Debug,
            "Worker coef calculated (Worker: %v, Safe: 1, Unsafe: %v, AvgInterval: %v, WorkerCoef: %v)",
            address,
            avgCoef,
            avgJobInterval,
            WorkerCoefs_[address]);
    }
}

void TPartitionDistributionData::CalculateComputationCoefs()
{
    int avgComputationCoefCount = 0;
    for (const auto& [computationId, partitions] : ComputationPartitions_) {
        double coefSum = 0.;
        int coefCount = 0;
        for (const auto& partitionId : partitions) {
            const auto& info = PartitionInfos_.at(partitionId);
            if (!info.Worker || !info.InputCpuUsage.has_value() || info.InputCpuUsage.value() <= 0.) {
                continue;
            }
            double workerCoef = GetWorkerCoef(info.Worker->RpcAddress);
            coefSum += info.InputCpuUsage.value() / info.Weight / workerCoef;
            coefCount++;
        }
        if (coefCount == 0) {
            continue;
        }
        double coef = coefSum / coefCount;
        ComputationCoefs_[computationId] = coef;
        AvgComputationCoef_ += coef;
        avgComputationCoefCount++;
    }
    if (avgComputationCoefCount == 0) {
        AvgComputationCoef_ = 1.;
    } else {
        AvgComputationCoef_ /= avgComputationCoefCount;
    }
}

void TPartitionDistributionData::InterpolateComplexities()
{
    for (auto& [_, info] : PartitionInfos_) {
        if (!info.Worker || !info.InputCpuUsage.has_value() || info.InputCpuUsage.value() <= 0.) {
            info.Complexity = 1.;
        } else {
            double computationCoef = GetComputationCoef(info.ComputationId);
            double workerCoef = GetWorkerCoef(info.Worker->RpcAddress);
            info.Complexity = info.InputCpuUsage.value() / computationCoef / workerCoef / info.Weight;
        }
    }
}

void TPartitionDistributionData::NormalizeComplexities()
{
    for (auto& [_, info] : PartitionInfos_) {
        // Usual complexity is expected to be around 1. But some partitions can be sporadic.
        // So even current known CPU usage is very low we should expect it to rise to some reasonable value.
        // TODO(thenewone): avoid magic constants.
        if (info.Complexity < 0.1) {
            info.Complexity = 0.1;
        }
    }
}

void TPartitionDistributionData::FinalizeCpuUsage()
{
    for (auto& [_, info] : PartitionInfos_) {
        double computationCoef = GetComputationCoef(info.ComputationId);
        info.NormalizedCpuUsage = info.Complexity * computationCoef * info.Weight;
    }
}

void TPartitionDistributionData::CalculateWorkerAvgJobIntervals()
{
    THashMap<std::string, std::pair<TDuration, size_t>> data;
    for (const auto& [partitionId, partition] : PartitionInfos_) {
        if (partition.Worker) {
            // We should limit the value. Otherwise, if for example 1 job has been working for a week and 100 - for a minute, we would get average
            // job interval of 100 minutes, which doesn't represent the state of the pipeline well.
            data[partition.Worker->RpcAddress].first += std::min(partition.TimeSinceStart, MaxJobInterval);
            data[partition.Worker->RpcAddress].second++;
        }
    }

    for (const auto& [workerAddress, data] : data) {
        WorkerAvgJobIntervals_[workerAddress] = data.first / data.second;
    }
}

////////////////////////////////////////////////////////////////////////////////

TDistributionEmulation::TDistributionEmulation(const TFlowViewPtr& flowView, const TPartitionDistributionData& partitionData, const TWorkerGroupId& workerGroup)
{
    CollectWorkers(flowView, partitionData, workerGroup);
    CollectPartitions(partitionData);
    CalculateTargetValues();
}

void TDistributionEmulation::CollectWorkers(const TFlowViewPtr& flowView, const TPartitionDistributionData& partitionData, const TWorkerGroupId& workerGroup)
{
    for (const auto& [address, worker] : flowView->State->Workers) {
        if (!WorkerBelongsToGroup(worker, workerGroup)) {
            continue;
        }
        auto& entry = Workers_[address];
        entry.Worker = worker;
        entry.WorkerCoef = partitionData.GetWorkerCoef(address);
    }
}

void TDistributionEmulation::CollectPartitions(const TPartitionDistributionData& partitionData)
{
    const auto& partitionInfos = partitionData.PartitionInfos();
    THashSet<TComputationId> computations;
    for (const auto& [partitionId, info] : partitionInfos) {
        computations.insert(info.ComputationId);
    }
    for (const auto& computationId : computations) {
        ComputationsByDeviation_.emplace(0., computationId);
    }
    for (const auto& [workerAddress, worker] : Workers_) {
        WorkerStat_.Add(0, workerAddress);
        for (const auto& computationId : computations) {
            WorkerStatByComputations_[computationId].Add(0, workerAddress);
        }
    }
    for (const auto& [partitionId, info] : partitionInfos) {
        PartitionNormalizedUsage_[partitionId] = info.NormalizedCpuUsage;
        if (info.Worker) {
            AddPartition(partitionId, info, info.Worker->RpcAddress);
        } else {
            AddStrayPartition(partitionId, info);
        }
    }
    Actions_.clear();
    ActionCount_ = 0;
}

void TDistributionEmulation::CalculateTargetValues()
{
    double workerMightSum = 0.;
    for (const auto& [address, worker] : Workers_) {
        double workerMight = 1. / worker.WorkerCoef;
        workerMightSum += workerMight;
    }

    for (auto& [address, worker] : Workers_) {
        double workerMight = 1. / worker.WorkerCoef;
        double coef = workerMight / workerMightSum;
        for (const auto& [computationId, info] : InfoByComputations_) {
            auto apply = [&coef, &workerMightSum] (TEmulationTarget& target, const TEmulationPartitionSet& info) {
                target.Count = info.Count * coef;
                target.CpuUsage = info.CpuUsage / workerMightSum;
                target.AvgCpuUsage = target.CpuUsage / target.Count;
            };
            auto& targets = worker.Targets[computationId];
            apply(targets.All, info.All);
            apply(targets.Executing, info.Executing);
            apply(targets.Interrupting, info.Interrupting);
        }
    }
}

void TDistributionEmulation::AddPartition(const TPartitionId& id, const TPartitionDistributionInfo& info, const std::string& workerAddress)
{
    auto& worker = Workers_.at(workerAddress);
    double normalizedUsage = PartitionNormalizedUsage_[id];
    double usage = normalizedUsage * worker.WorkerCoef;
    YT_VERIFY(!InfoOverall_.Contains(id));

    PartitionUsage_[id] = usage;
    PartitionWorker_[id] = workerAddress;

    auto& workerStatByComputation = WorkerStatByComputations_[info.ComputationId];
    const auto& infoOverall = worker.InfoOverall;
    const auto& infoByComputations = worker.InfoByComputations[info.ComputationId];

    ComputationsByDeviation_.erase(std::pair(workerStatByComputation.Deviation(), info.ComputationId));
    WorkerStat_.Del(infoOverall.Executing.CpuUsage, workerAddress);
    workerStatByComputation.Del(infoByComputations.Executing.CpuUsage, workerAddress);

    InfoOverall_.Add(id, info, normalizedUsage);
    InfoByComputations_[info.ComputationId].Add(id, info, normalizedUsage);
    worker.InfoOverall.Add(id, info, usage);
    worker.InfoByComputations[info.ComputationId].Add(id, info, usage);

    WorkerStat_.Add(infoOverall.Executing.CpuUsage, workerAddress);
    workerStatByComputation.Add(infoByComputations.Executing.CpuUsage, workerAddress);
    ComputationsByDeviation_.emplace(workerStatByComputation.Deviation(), info.ComputationId);

    auto& actions = Actions_[id];
    YT_VERIFY(actions.empty() || actions.back().Type == ERebalanceActionType::Del);
    if (!actions.empty() && actions.back().WorkerAddress == workerAddress) {
        actions.pop_back();
        ActionCount_--;
    } else {
        actions.emplace_back(TEmulationAction{ERebalanceActionType::Add, workerAddress});
        ActionCount_++;
    }
}

void TDistributionEmulation::DelPartition(const TPartitionId& id, const TPartitionDistributionInfo& info, const std::string& workerAddress)
{
    auto& worker = Workers_.at(workerAddress);
    double normalizedUsage = PartitionNormalizedUsage_[id];
    double usage = PartitionUsage_[id];
    YT_VERIFY(InfoOverall_.Contains(id));

    PartitionUsage_.erase(id);
    PartitionWorker_.erase(id);

    auto& workerStatByComputation = WorkerStatByComputations_[info.ComputationId];
    const auto& infoOverall = worker.InfoOverall;
    const auto& infoByComputations = worker.InfoByComputations[info.ComputationId];

    ComputationsByDeviation_.erase(std::pair(workerStatByComputation.Deviation(), info.ComputationId));
    WorkerStat_.Del(infoOverall.Executing.CpuUsage, workerAddress);
    workerStatByComputation.Del(infoByComputations.Executing.CpuUsage, workerAddress);

    InfoOverall_.Del(id, info, normalizedUsage);
    InfoByComputations_[info.ComputationId].Del(id, info, normalizedUsage);
    worker.InfoOverall.Del(id, info, usage);
    worker.InfoByComputations[info.ComputationId].Del(id, info, usage);

    WorkerStat_.Add(infoOverall.Executing.CpuUsage, workerAddress);
    workerStatByComputation.Add(infoByComputations.Executing.CpuUsage, workerAddress);
    ComputationsByDeviation_.emplace(workerStatByComputation.Deviation(), info.ComputationId);

    auto& actions = Actions_[id];
    YT_VERIFY(actions.empty() || (actions.back().Type == ERebalanceActionType::Add && actions.back().WorkerAddress == workerAddress));

    if (!actions.empty()) {
        actions.pop_back();
        ActionCount_--;
    } else {
        actions.emplace_back(TEmulationAction{ERebalanceActionType::Del, workerAddress});
        ActionCount_++;
    }
}

void TDistributionEmulation::AddStrayPartition(const TPartitionId& id, const TPartitionDistributionInfo& info)
{
    double normalizedUsage = PartitionNormalizedUsage_[id];
    YT_VERIFY(!StrayInfoOverall_.Contains(id));

    InfoOverall_.Add(id, info, normalizedUsage);
    InfoByComputations_[info.ComputationId].Add(id, info, normalizedUsage);
    StrayInfoOverall_.Add(id, info, normalizedUsage);
    StrayInfoByComputations_[info.ComputationId].Add(id, info, normalizedUsage);
}

void TDistributionEmulation::DelStrayPartition(const TPartitionId& id, const TPartitionDistributionInfo& info)
{
    double normalizedUsage = PartitionNormalizedUsage_[id];
    YT_VERIFY(StrayInfoOverall_.Contains(id));

    InfoOverall_.Del(id, info, normalizedUsage);
    InfoByComputations_[info.ComputationId].Del(id, info, normalizedUsage);
    StrayInfoOverall_.Del(id, info, normalizedUsage);
    StrayInfoByComputations_[info.ComputationId].Del(id, info, normalizedUsage);
}

void TDistributionEmulation::ApplyAll(const TRebalanceActions& actions, const TPartitionDistributionData& partitionData)
{
    auto applier = [&] (const TRebalanceActions::TRebalanceAction& action) {
        const auto& [type, partitionId, workerAddress, info] = action;

        if (!GetInfo().All.Partitions.contains(partitionId)) {
            YT_LOG_ERROR("Requested applying partition, while it is not present among executing partitions (Partition: %v, Worker: %s)",
                partitionId,
                workerAddress);
            return;
        }
        if (!Workers().contains(workerAddress)) {
            YT_LOG_ERROR("Requested applying partition, while it the worker is not present (Partition: %v, Worker: %s)",
                partitionId,
                workerAddress);
            return;
        }

        if (type == ERebalanceActionType::Del) {
            if (GetStrayInfo().All.Partitions.contains(partitionId)) {
                YT_LOG_ERROR("Requested deleting partition, while it is not already assigned to a worker (Partition: %v, Worker: %s)",
                    partitionId,
                    workerAddress);
                return;
            }
            DelPartition(partitionId, partitionData.PartitionInfos().at(partitionId), workerAddress);
            AddStrayPartition(partitionId, partitionData.PartitionInfos().at(partitionId));
        }

        if (type == ERebalanceActionType::Add) {
            if (!GetStrayInfo().All.Partitions.contains(partitionId)) {
                YT_LOG_ERROR("Requested adding partition, while it is already assigned to a worker (Partition: %v, Worker: %s)",
                    partitionId,
                    workerAddress);
                return;
            }
            DelStrayPartition(partitionId, partitionData.PartitionInfos().at(partitionId));
            AddPartition(partitionId, partitionData.PartitionInfos().at(partitionId), workerAddress);
        }
    };
    actions.TransactionalApply(applier);
}

////////////////////////////////////////////////////////////////////////////////

TBalancer::TBalancer(
    const TFlowViewPtr& flowView,
    const TControllersMap& controllers,
    const TDynamicJobBalancerSpecPtr& balancerSpec,
    const TWorkerGroupId& workerGroup,
    const TPersistentBalanceManagerPtr& persistentManager)
    : Data_(flowView, controllers, workerGroup)
    , Emulation_(flowView, Data_, workerGroup)
    , PersistentManager_(persistentManager)
    , ManagerSpec_(balancerSpec)
    , Verifier_(flowView)
{
    PersistentManager_->ActionsBuffer = Verifier_.Verify(PersistentManager_->ActionsBuffer);
    auto& workersRemaining = PersistentManager_->GetLoopContext().WorkersRemaining;
    workersRemaining = Verifier_.VerifyWorkers(workersRemaining);

    if (persistentManager->GetLoopContext().Computation.has_value()) {
        TComputationId computationId = persistentManager->GetLoopContext().Computation.value().Id;
        if (Verifier_.VerifyComputations(std::vector<TComputationId>{computationId}).empty()) {
            PersistentManager_->GetLoopContext().Computation = std::nullopt;
        }
    }

    PersistentManager_->ActionBufferScore = std::numeric_limits<double>::infinity();
    if (persistentManager->GetLoopContext().Computation.has_value()) {
        PersistentManager_->ActionBufferScore = AssessScore(PersistentManager_->ActionsBuffer, PersistentManager_->GetLoopContext().Computation.value().Id);
    }
}

TRebalanceActions TBalancer::KickPartitionsFromOvercountedWorkers()
{
    const auto& workers = Emulation_.Workers();
    const auto& partitionInfos = Data_.PartitionInfos();
    TRebalanceActions result;

    // Whether to run this overcount kick at all is decided by the caller (DoFastBalancing) — it is
    // gated by rebalance_min_cpu_spread, like deep rebalance.
    // Do not account that moves in the limit since it's a bit different rebalance.
    for (const auto& [computationId, computationInfo] : Emulation_.ComputationInfos()) {
        for (const auto& [workerAddress, worker] : workers) {
            const auto& workerInfo = Emulation_.GetInfo(worker, computationId).Executing;
            const auto& targets = worker.Targets.at(computationId);
            int maxCount = std::floor(targets.Executing.Count * ManagerSpec_->RebalanceCountExceedAllowed) + 1;
            double targetCpuUsagePerJob = targets.Executing.AvgCpuUsage;
            double targetCpuUsagePerWorker = maxCount * targets.Executing.AvgCpuUsage;
            while (workerInfo.Count > maxCount) {
                int plannedToRemove = workerInfo.Count - maxCount;
                double removeCpuUsage = (workerInfo.CpuUsage - targetCpuUsagePerWorker) / plannedToRemove + targetCpuUsagePerJob;
                TPartitionId partitionId = workerInfo.FindClosest(removeCpuUsage);
                const auto& info = partitionInfos.at(partitionId);
                Emulation_.DelPartition(partitionId, info, workerAddress);
                Emulation_.AddStrayPartition(partitionId, info);
                result.EmplaceAsTransaction(ERebalanceActionType::Del, partitionId, workerAddress, info);
                YT_LOG_EVENT(
                    NController::BalancerLogger,
                    NLogging::ELogLevel::Info,
                    "Job was kicked because worker is overloaded (JobId: %v, Worker: %v)",
                    info.JobId,
                    workerAddress);
            }
        }
    }

    return result;
}

TRebalanceActions TBalancer::DistributeStrayPartitions()
{
    TRebalanceActions result;
    for (auto partitionState : {EPartitionState::Executing, EPartitionState::Interrupting}) {
        result.Merge(DistributeStrayPartitions(partitionState));
    }
    return result;
}

TRebalanceActions TBalancer::DistributeStrayPartitions(EPartitionState partitionState)
{
    auto result = DistributeStrayPartitionsPhase1(partitionState);
    result.Merge(DistributeStrayPartitionsPhase2(partitionState));
    return result;
}

TRebalanceActions TBalancer::DistributeStrayPartitionsPhase1(EPartitionState partitionState)
{
    const auto& workers = Emulation_.Workers();
    const auto& partitionInfos = Data_.PartitionInfos();
    auto targetsType = partitionState == EPartitionState::Executing ? &TEmulationTargets::Executing : &TEmulationTargets::Interrupting;
    auto infoType = partitionState == EPartitionState::Executing ? &TEmulationInfo::Executing : &TEmulationInfo::Interrupting;
    TRebalanceActions result;
    for (const auto& [computationId, strayInfo] : Emulation_.StrayComputationInfos()) {
        THashSet<std::string> workerCandidates;
        for (const auto& [workerAddress, worker] : workers) {
            const auto& targets = worker.Targets.at(computationId);
            const auto& executingInfo = Emulation_.GetInfo(worker, computationId).*infoType;
            if (executingInfo.Count < std::floor((targets.*targetsType).Count)) {
                workerCandidates.insert(workerAddress);
            }
        }
        while ((strayInfo.*infoType).Count != 0 && !workerCandidates.empty()) {
            THashSet<std::string> workerNoMoreCandidates;
            for (const auto& workerAddress : workerCandidates) {
                if ((strayInfo.*infoType).Count == 0) {
                    break;
                }
                const auto& worker = workers.at(workerAddress);
                const auto& targets = worker.Targets.at(computationId);
                double targetCount = (targets.*targetsType).Count;
                double targetCpuUsagePerJob = (targets.*targetsType).AvgCpuUsage;
                const auto& executingInfo = Emulation_.GetInfo(worker, computationId).*infoType;
                int plannedToAdd = std::floor(targetCount) - executingInfo.Count;
                if (plannedToAdd <= 0) {
                    workerNoMoreCandidates.insert(workerAddress);
                    break;
                }
                double targetCpuUsagePerWorker = std::floor(targetCount) * targetCpuUsagePerJob;
                double addCpuUsage = (targetCpuUsagePerWorker - executingInfo.CpuUsage) / plannedToAdd;
                double addNormalized = addCpuUsage / worker.WorkerCoef;
                TPartitionId partitionId = (strayInfo.*infoType).FindClosest(addNormalized);
                const auto& info = partitionInfos.at(partitionId);
                Emulation_.DelStrayPartition(partitionId, info);
                Emulation_.AddPartition(partitionId, info, workerAddress);
                result.EmplaceAsTransaction(ERebalanceActionType::Add, partitionId, workerAddress, info);
            }
            for (const auto& workerAddress : workerNoMoreCandidates) {
                workerCandidates.erase(workerAddress);
            }
        }
    }
    return result;
}

TRebalanceActions TBalancer::DistributeStrayPartitionsPhase2(EPartitionState partitionState)
{
    const auto& workers = Emulation_.Workers();
    const auto& partitionInfos = Data_.PartitionInfos();
    auto targetsType = partitionState == EPartitionState::Executing ? &TEmulationTargets::Executing : &TEmulationTargets::Interrupting;
    auto infoType = partitionState == EPartitionState::Executing ? &TEmulationInfo::Executing : &TEmulationInfo::Interrupting;

    // Candidate workers per computation: those not yet at their per-computation count target.
    THashMap<TComputationId, THashSet<std::string>> workerCandidatesByComputations;
    TRebalanceActions result;

    while ((Emulation_.GetStrayInfo().*infoType).Count != 0) {
        auto mostLoaded = (Emulation_.GetStrayInfo().*infoType).Spectre.rbegin();
        auto partitionId = mostLoaded->second;
        const auto& info = partitionInfos.at(partitionId);
        const auto& computationId = info.ComputationId;
        if (!workerCandidatesByComputations.contains(computationId)) {
            auto& workerCandidates = workerCandidatesByComputations[computationId];
            for (const auto& [workerAddress, worker] : workers) {
                const auto& targets = worker.Targets.at(computationId);
                const auto& executingInfo = Emulation_.GetInfo(worker, computationId).*infoType;
                if (executingInfo.Count <= std::floor((targets.*targetsType).Count)) {
                    workerCandidates.insert(workerAddress);
                }
            }
        }
        auto& workerCandidates = workerCandidatesByComputations[computationId];
        if (workerCandidates.empty()) {
            return result;
        }

        // Choose the worker that minimizes the deep-balancing score after placing this partition.
        Emulation_.DelStrayPartition(partitionId, info);
        std::optional<std::string> bestWorkerAddress;
        double bestScore = 0;
        bool found = false;
        for (const auto& workerAddress : workerCandidates) {
            Emulation_.AddPartition(partitionId, info, workerAddress);
            double score = GetScore(computationId);
            Emulation_.DelPartition(partitionId, info, workerAddress);
            if (!found || score < bestScore) {
                bestScore = score;
                bestWorkerAddress = workerAddress;
                found = true;
            }
        }
        YT_VERIFY(bestWorkerAddress);
        workerCandidates.erase(*bestWorkerAddress);
        Emulation_.AddPartition(partitionId, info, *bestWorkerAddress);
        result.EmplaceAsTransaction(ERebalanceActionType::Add, partitionId, *bestWorkerAddress, info);
    }
    return result;
}

TRebalanceActions TBalancer::RelieveWorker(const TComputationId& computationId, const std::string& myWorkerAddress)
{
    const auto& partitionInfos = Data_.PartitionInfos();
    const auto& workers = Emulation_.Workers();
    const auto& myWorker = workers.at(myWorkerAddress);
    std::optional<TPartitionId> bestPartitionId;
    std::string bestPeerWorkerAddress;
    std::optional<TPartitionId> bestPeerPartitionId;

    if (Emulation_.GetInfo().Executing.Count == 0) {
        return TRebalanceActions();
    }

    double origTotalScore = Emulation_.GetRelativeDeviation();
    double origComputationScore = Emulation_.GetRelativeDeviation(computationId);
    double initScore = GetScore(computationId);
    double bestScore = GetScore(computationId);

    auto checkScore = [&] (const TPartitionId& myPartitionId, std::optional<TPartitionId> peerPartitionId, const auto& peerWorkerAddress) {
        double curScore = GetScore(computationId);
        if (curScore < bestScore) {
            bestPartitionId = myPartitionId;
            bestPeerPartitionId = peerPartitionId;
            bestPeerWorkerAddress = peerWorkerAddress;
            bestScore = curScore;
        }
    };

    double currentCpu = myWorker.InfoOverall.Executing.CpuUsage;
    std::vector<TPartitionId> partitions;

    partitions.clear();
    partitions.reserve(Emulation_.GetInfo(myWorker, computationId).Executing.Spectre.size());
    for (const auto& [_, partitionId] : Emulation_.GetInfo(myWorker, computationId).Executing.Spectre) {
        partitions.push_back(partitionId);
    }

    for (const auto& [peerWorkerAddress, peerWorker] : workers) {
        if (myWorkerAddress == peerWorkerAddress) {
            continue;
        }
        double currentPeerCpu = peerWorker.InfoOverall.Executing.CpuUsage;
        double needMoveCpuNormalized = (currentCpu - currentPeerCpu) / (myWorker.WorkerCoef + peerWorker.WorkerCoef);

        TStringStream finegrainedReports;
        finegrainedReports << "Finegrained report for worker " << myWorkerAddress << " begins\n\n";

        for (const auto& partitionId : partitions) {
            const auto& info = partitionInfos.at(partitionId);
            double sendCpuNormalized = info.NormalizedCpuUsage;
            double recvCpuNormalized = sendCpuNormalized - needMoveCpuNormalized;
            std::optional<TPartitionId> peerPartitionId;
            if (Emulation_.GetInfo(peerWorker, computationId).Executing.Count > 0) {
                const auto& peerEmulation = Emulation_.GetInfo(peerWorker, computationId).Executing;
                peerPartitionId = peerEmulation.FindClosest(recvCpuNormalized * peerWorker.WorkerCoef);
            }

            const auto& myInfo = partitionInfos.at(partitionId);

            Emulation_.DelPartition(partitionId, myInfo, myWorkerAddress);
            Emulation_.AddPartition(partitionId, myInfo, peerWorkerAddress);

            // Try move.
            const auto& targets = peerWorker.Targets.at(computationId);

            // Allow at least one partition of this computation per worker. Otherwise an
            // under-partitioned computation (fewer partitions than workers) has a per-worker target
            // count below 1, so floor(target.Count * exceed) rounds to 0 and no worker is ever an
            // acceptable destination — the computation's partitions can never be moved/spread.
            double maxComputationCountOnWorker = std::max(1.0, std::floor(targets.Executing.Count * ManagerSpec_->RebalanceCountExceedAllowed));
            if (Emulation_.GetInfo(peerWorker, computationId).Executing.Count <= maxComputationCountOnWorker) {
                checkScore(partitionId, {}, peerWorkerAddress);
            }

            finegrainedReports << "Finegrained report on "
                               << computationId << "'s " << partitionId.Underlying().Parts64[0]
                               << " from " << myWorkerAddress << "      "
                               << "Tried to move to " << peerWorkerAddress
                               << " and the new scores are " << Emulation_.GetRelativeDeviation() << ":" << Emulation_.GetRelativeDeviation(computationId)
                               << " as opposed to original " << origTotalScore << ":" << origComputationScore << "\n";

            // Try swap.
            if (peerPartitionId.has_value()) {
                const auto& peerInfo = partitionInfos.at(peerPartitionId.value());
                Emulation_.DelPartition(peerPartitionId.value(), peerInfo, peerWorkerAddress);
                Emulation_.AddPartition(peerPartitionId.value(), peerInfo, myWorkerAddress);
                checkScore(partitionId, peerPartitionId, peerWorkerAddress);

                finegrainedReports << "Finegrained report on "
                                   << computationId << "'s " << partitionId.Underlying().Parts64[0]
                                   << " from " << myWorkerAddress << "      "
                                   << "Tried to swap with " << peerPartitionId.value().Underlying().Parts64[0]
                                   << " from  " << peerWorkerAddress
                                   << " and the new scores are " << Emulation_.GetRelativeDeviation() << ":" << Emulation_.GetRelativeDeviation(computationId)
                                   << " as opposed to original " << origTotalScore << ":" << origComputationScore << "\n";

                Emulation_.DelPartition(peerPartitionId.value(), peerInfo, myWorkerAddress);
                Emulation_.AddPartition(peerPartitionId.value(), peerInfo, peerWorkerAddress);
            }
            Emulation_.DelPartition(partitionId, myInfo, peerWorkerAddress);
            Emulation_.AddPartition(partitionId, myInfo, myWorkerAddress);
        }

        finegrainedReports << "\nFinegrained report for worker " << myWorkerAddress << " terminates\n\n";
        YT_LOG_EVENT(NController::BalancerLogger, NLogging::ELogLevel::Trace, finegrainedReports.Str());
    }

    TRebalanceActions result;
    auto& transaction = result.StartTransaction();

    // To allow the action to be added, it should give at least as much improvement,
    // as the minimal average improvement for the deferred jobs to merge (targetdeviation / total count of partitions).
    double requiredImprovement = ManagerSpec_->RebalanceTargetDeviation / Emulation_.GetInfo().Executing.Count;

    if (bestScore < initScore - requiredImprovement) {
        auto& myInfo = Data_.PartitionInfos().at(bestPartitionId.value());
        transaction.Emplace(ERebalanceActionType::Del, bestPartitionId.value(), myWorkerAddress, myInfo);
        transaction.Emplace(ERebalanceActionType::Add, bestPartitionId.value(), bestPeerWorkerAddress, myInfo);

        if (bestPeerPartitionId.has_value()) {
            const auto& peerInfo = partitionInfos.at(bestPeerPartitionId.value());
            transaction.Emplace(ERebalanceActionType::Del, bestPeerPartitionId.value(), bestPeerWorkerAddress, peerInfo);
            transaction.Emplace(ERebalanceActionType::Add, bestPeerPartitionId.value(), myWorkerAddress, peerInfo);
        }
    }

    return result;
}

std::optional<TComputationId> TBalancer::AdvanceContextComputation()
{
    if (Emulation_.ComputationInfos().empty()) {
        return std::nullopt;
    }
    if (std::ranges::max(Emulation_.ComputationsByDeviation() | std::views::transform(&std::pair<double, TComputationId>::first)) > std::numeric_limits<double>::epsilon()) {
        const auto& computations = Emulation_.ComputationsByDeviation() | std::views::transform([] (const auto& pair) {
            return std::pair(pair.second, pair.first);
        });
        TWeightedRandom<TComputationId> randomGen(computations);
        PersistentManager_->GetLoopContext().Computation = {randomGen(), TInstant::Now()};
        return PersistentManager_->GetLoopContext().Computation.value().Id;
    } else {
        YT_LOG_EVENT(NController::BalancerLogger, NLogging::ELogLevel::Info, "All computations are balanced");
        return std::nullopt;
    }
}

bool TBalancer::ProceedWithWorker(std::vector<std::string>& workerAddresses)
{
    if (workerAddresses.empty()) {
        return false;
    }

    auto workerAddress = workerAddresses.back();
    workerAddresses.pop_back();
    const TComputationId& computationId = PersistentManager_->GetLoopContext().Computation.value().Id;
    YT_LOG_EVENT(NController::BalancerLogger, NLogging::ELogLevel::Debug, "ProceedWithWorker started (Worker: %v)", workerAddress);
    auto actions = RelieveWorker(computationId, workerAddress);

    if (double score = AssessScore(actions, computationId); score < PersistentManager_->ActionBufferScore) {
        PersistentManager_->ActionBufferScore = score;
        PersistentManager_->ActionsBuffer = std::move(actions);
    }

    return true;
}

const TDistributionStat& TBalancer::GetWorkerDistributionOverall() const&
{
    return Emulation_.GetWorkerDistributionOverall();
}

TDistributionStat&& TBalancer::GetWorkerDistributionOverall() &&
{
    return std::move(Emulation_).GetWorkerDistributionOverall();
}

const THashMap<TComputationId, TDistributionStat>& TBalancer::GetWorkerDistributionByComputations() const&
{
    return Emulation_.GetWorkerDistributionByComputations();
}

THashMap<TComputationId, TDistributionStat>&& TBalancer::GetWorkerDistributionByComputations() &&
{
    return std::move(Emulation_).GetWorkerDistributionByComputations();
}

bool TBalancer::HasStrayPartitions() const
{
    return Emulation_.GetStrayInfo().All.Count > 0;
}

bool TBalancer::WorkerLoadUneven() const
{
    // Test-only override: bypass the even-load gate and always rebalance.
    if (ManagerSpec_->DisableEvenLoadGate.value_or(false)) {
        return true;
    }

    const auto& stat = Emulation_.GetWorkerDistributionOverall();
    if (stat.Set.size() < 2) {
        return false;
    }

    const double minCpu = stat.Set.begin()->first;
    const double maxCpu = stat.Set.rbegin()->first;

    const double spread = maxCpu - minCpu;
    const double ratio = minCpu > 0.0 ? maxCpu / minCpu : std::numeric_limits<double>::infinity();
    const double relativeDeviation = stat.RelativeDeviation();

    // Rebalance only when the load is uneven by ALL three measures.
    return spread >= ManagerSpec_->RebalanceMinCpuSpread && ratio >= ManagerSpec_->RebalanceMinCpuRatio && relativeDeviation > 2.0 * ManagerSpec_->RebalanceTargetDeviation;
}

double TBalancer::GetScore([[maybe_unused]] const TComputationId& computationId)
{
    double totalSize = Emulation_.GetInfo().Executing.Count;
    double computationSize = Emulation_.GetInfo(computationId).Executing.Count;
    return Emulation_.GetRelativeDeviation() + Emulation_.GetRelativeDeviation(computationId) * computationSize / totalSize;
}

double TBalancer::AssessScore(const TRebalanceActions& actions, const TComputationId& computationId)
{
    Emulation_.ApplyAll(actions, Data_);
    double result = GetScore(computationId);
    Emulation_.ApplyAll(actions.MakeReverted(), Data_);
    return result;
}

std::string TBalancer::GenerateInterimReport()
{
    TStringStream out;

    out << "Balancer interim report begins at time " << TInstant::Now().ToString() << "\n";

    for (const auto& [workerAddress, worker] : Emulation_.Workers()) {
        out << "Worker: " << workerAddress << " count of tasks: " << worker.InfoOverall.All.Count << ", CPU load: " << worker.InfoOverall.All.CpuUsage << ", Coefficient: " << Emulation_.Workers().at(workerAddress).WorkerCoef << "\n";
    }

    out << "\n\nApplied actions as follows:\n";

    PersistentManager_->ActionsBuffer.TransactionalApply(
        [&] (const TRebalanceActions::TRebalanceAction& action) {
            out << "Action: " << (action.Type == ERebalanceActionType::Del ? "Del" : "Add") << " on " << action.PartitionId.Underlying().Parts64[0] << " on Worker: " << action.WorkerAddress << "\n";
        });

    out << "\nBalancer interim report terminates \n";
    return out.Str();
}

TRebalanceActions TBalancer::DoFastBalancing()
{
    YT_LOG_EVENT(NController::BalancerLogger, NLogging::ELogLevel::Info, "Entered fast balancing");
    TRebalanceActions result;

    // The even-load gate (WorkerLoadUneven) also gates the fast (count-based) rebalancing: when
    // worker CPU loads are even enough, skip the overcount kick so an already-even pipeline is not
    // churned. A stray (jobless) partition re-enables the kick (it must be placed anyway).
    // DistributeStrayPartitions always runs. Same predicate as the slow gate in ShouldApplySlowActionsNow.
    if (HasStrayPartitions() || WorkerLoadUneven()) {
        result.Merge(KickPartitionsFromOvercountedWorkers());
    } else {
        YT_LOG_EVENT(
            NController::BalancerLogger,
            NLogging::ELogLevel::Info,
            "Skipping overcount kick: worker load is even and no stray partitions (RelativeDeviation: %v)",
            Emulation_.GetWorkerDistributionOverall().RelativeDeviation());
    }
    result.Merge(DistributeStrayPartitions());
    AlreadyApplied_.Merge(result);
    YT_LOG_EVENT(NController::BalancerLogger, NLogging::ELogLevel::Info, "Passed fast balancing (Transactions: %v)", result.Transactions.size());

    return result;
}

TRebalanceActions TBalancer::DoSlowBalancing(const TInstant& until)
{
    YT_LOG_EVENT(NController::BalancerLogger, NLogging::ELogLevel::Info, "Entered slow balancing");

    PersistentManager_->ActionsBuffer = Verifier_.VerifyWithPreapplied(AlreadyApplied_, PersistentManager_->ActionsBuffer);
    PersistentManager_->ActionBufferScore = std::numeric_limits<double>::infinity();
    if (PersistentManager_->GetLoopContext().Computation.has_value()) {
        PersistentManager_->ActionBufferScore = AssessScore(PersistentManager_->ActionsBuffer, PersistentManager_->GetLoopContext().Computation.value().Id);
    }

    TRebalanceActions result;

    auto finishedComputation = [&] (const TComputationId&) {
        YT_LOG_EVENT(NController::BalancerLogger, NLogging::ELogLevel::Debug, GenerateInterimReport());

        Emulation_.ApplyAll(PersistentManager_->ActionsBuffer, Data_);
        AlreadyApplied_.Merge(PersistentManager_->ActionsBuffer);
        result.Merge(PersistentManager_->ActionsBuffer);
        PersistentManager_->ActionsBuffer = TRebalanceActions();
        PersistentManager_->ActionBufferScore = std::numeric_limits<double>::infinity();
    };

    if (Emulation_.ComputationInfos().empty() || Emulation_.Workers().empty()) {
        return result;
    }

    while (TInstant::Now() < until) {
        // If we've used up more than max time for one action, we remove all the remaining workers from the queue, which will wrap up action selection process.
        if (PersistentManager_->GetLoopContext().Computation.has_value() && TInstant::Now() > PersistentManager_->GetLoopContext().Computation.value().StartTime + ManagerSpec_->RebalanceActionMaxTime) {
            PersistentManager_->GetLoopContext().WorkersRemaining.clear();
        }

        if (!ProceedWithWorker(PersistentManager_->GetLoopContext().WorkersRemaining)) {
            TComputationId computationId;

            if (PersistentManager_->GetLoopContext().Computation.has_value()) {
                // Waiting for the RebalanceActionMinTime to pass since the start of this computation's processing.
                TDuration waitTime = PersistentManager_->GetLoopContext().Computation.value().StartTime + ManagerSpec_->RebalanceActionMinTime - TInstant::Now();
                waitTime = std::min(waitTime, until - TInstant::Now());
                if (waitTime > TDuration::Zero()) {
                    NConcurrency::TDelayedExecutor::WaitForDuration(waitTime);
                }

                // If we're stopped by "until", we should continue waiting again on the next iteration of RebalanceJobs.
                if (TInstant::Now() >= until) {
                    break;
                }

                finishedComputation(PersistentManager_->GetLoopContext().Computation.value().Id);
            }

            auto advanceResult = AdvanceContextComputation();
            if (!advanceResult.has_value()) {
                YT_LOG_EVENT(NController::BalancerLogger, NLogging::ELogLevel::Info, "Cannot advance context computation at the moment");
                NConcurrency::TDelayedExecutor::WaitForDuration(until - TInstant::Now());
                return result;
            }
            computationId = advanceResult.value();
            YT_LOG_EVENT(NController::BalancerLogger, NLogging::ELogLevel::Info, "Selected computation for slow balancing (Computation: %s)", computationId);

            std::vector<std::pair<std::string, double>> overallData;
            for (const auto& [workerAddress, worker] : Emulation_.Workers()) {
                if (Emulation_.GetInfo(worker, computationId).Executing.Count == 0) {
                    continue;
                }
                overallData.push_back({workerAddress, Emulation_.GetInfo(worker, computationId).Executing.CpuUsage});
            }

            std::ranges::sort(overallData, {}, &std::pair<std::string, double>::second);
            std::ranges::copy(std::views::transform(overallData, [] (const auto& a) {
                return a.first;
            }),
                std::back_inserter(PersistentManager_->GetLoopContext().WorkersRemaining));
        }
    }

    YT_LOG_EVENT(NController::BalancerLogger, NLogging::ELogLevel::Info, "Slow balancing iteration terminated (NewTransactions: %v)", result.Transactions.size());

    return result;
}

void TBalancer::ApplyAll(const TRebalanceActions& actions)
{
    auto checkedActions = Verifier_.VerifyWithPreapplied(AlreadyApplied_, actions);
    Emulation_.ApplyAll(checkedActions, Data_);
    AlreadyApplied_.Merge(checkedActions);

    PersistentManager_->ActionsBuffer = Verifier_.VerifyWithPreapplied(AlreadyApplied_, PersistentManager_->ActionsBuffer);

    if (PersistentManager_->GetLoopContext().Computation.has_value()) {
        PersistentManager_->ActionBufferScore = AssessScore(PersistentManager_->ActionsBuffer, PersistentManager_->GetLoopContext().Computation.value().Id);
    }
}

double TBalancer::GetTotalScore() const
{
    double result = Emulation_.GetRelativeDeviation();
    double totalSize = Emulation_.GetInfo().Executing.Count;

    for (const auto& [computationId, computationInfo] : Emulation_.ComputationInfos()) {
        double computationSize = Emulation_.GetInfo(computationId).Executing.Count;
        result += Emulation_.GetRelativeDeviation(computationId) * computationSize / totalSize;
    }

    return result;
}

TRebalanceActions TBalancer::ValidateDeferredActions(const TRebalanceActions& deferredActions)
{
    TRebalanceActions result = TRebalanceActions::NewSequencedAs(deferredActions);
    TRebalanceActionsVerifier::TPartitionLocations knownLocations = Verifier_.BuildPartitionLocations(AlreadyApplied_);
    for (const auto& transaction : deferredActions.Transactions) {
        if (transaction.IsEmpty()) {
            continue;
        }

        auto computationId = transaction.Actions.front().Info.ComputationId;
        TRebalanceActions action = TRebalanceActions::NewSequencedAs(deferredActions);
        action.AddTransaction(transaction);

        if (Emulation_.GetInfo().Executing.Count == 0) {
            continue;
        }

        action = Verifier_.VerifyWithKnownLocations(action, knownLocations, true);
        // To allow the action to be added, it should give at least as much improvement,
        // as the minimal average improvement for the deferred jobs to merge (target deviation / total count of partitions).
        double requiredImprovement = ManagerSpec_->RebalanceTargetDeviation / Emulation_.GetInfo().Executing.Count;
        if (GetScore(computationId) - AssessScore(action, computationId) >= requiredImprovement) {
            result.Merge(action);
            Emulation_.ApplyAll(action, Data_);
            auto doer = [&] (const TRebalanceActions::TRebalanceAction& action) {
                if (action.Type == ERebalanceActionType::Add) {
                    knownLocations[action.PartitionId] = action.WorkerAddress;
                } else {
                    knownLocations.erase(action.PartitionId);
                }
            };
            action.TransactionalApply(doer);
        }
    }
    Emulation_.ApplyAll(result.MakeReverted(), Data_);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

//! Returns fast actions (as diff) and slow actions (full history, since that can be amended later on).
std::pair<TRebalanceActions, TRebalanceActions> RebalanceJobs(
    const TFlowViewPtr& flowView,
    const TControllersMap& controllers,
    const TDynamicJobBalancerSpecPtr& balancerSpec,
    const TWorkerGroupId& workerGroup,
    const TInstant& until,
    const TPersistentBalanceManagerPtr& persistentManager,
    const TRebalanceActions& alreadyApplied,
    const TRebalanceActions& alreadyAppliedDeferred)
{
    TBalancer balancer(flowView, controllers, balancerSpec, workerGroup, persistentManager);
    TRebalanceActionsVerifier verifier(flowView);

    balancer.ApplyAll(alreadyApplied);
    auto fastActions = TRebalanceActions::NewSequencedAs(alreadyApplied);
    fastActions.Merge(balancer.DoFastBalancing());

    auto allFastActions = alreadyApplied;
    allFastActions = verifier.Verify(allFastActions);
    allFastActions.Merge(fastActions);

    auto deferredVerified = verifier.VerifyWithPreapplied(allFastActions, alreadyAppliedDeferred);
    auto deferredValidated = balancer.ValidateDeferredActions(deferredVerified);
    balancer.ApplyAll(deferredValidated);
    auto slowActions = balancer.DoSlowBalancing(until);
    deferredValidated.Merge(slowActions);
    return {fastActions, deferredValidated};
}

bool ShouldApplySlowActionsNow(
    const TFlowViewPtr& flowView,
    const TControllersMap& controllers,
    const TDynamicJobBalancerSpecPtr& balancerSpec,
    const TWorkerGroupId& workerGroup,
    const TPersistentBalanceManagerPtr& persistentManager,
    const TRebalanceActions& alreadyApplied,
    const TRebalanceActions& alreadyAppliedDeferred)
{
    TBalancer balancer(flowView, controllers, balancerSpec, workerGroup, persistentManager);
    balancer.ApplyAll(alreadyApplied);

    if (!balancer.WorkerLoadUneven()) {
        YT_LOG_EVENT(
            NController::BalancerLogger,
            NLogging::ELogLevel::Info,
            "Skipping deferred merge: worker load is even (RelativeDeviation: %v)",
            balancer.GetWorkerDistributionOverall().RelativeDeviation());
        return false;
    }

    double currentScore = balancer.GetTotalScore();
    balancer.ApplyAll(alreadyAppliedDeferred);
    double deferredScore = balancer.GetTotalScore();
    YT_LOG_EVENT(NController::BalancerLogger, NLogging::ELogLevel::Info, "Calculated scores (Current: %v, Deferred: %v)", currentScore, deferredScore);
    return deferredScore < currentScore - balancerSpec->RebalanceTargetDeviation;
}

THashMap<std::string, double> GetWorkerCoefs(
    const TFlowViewPtr& flowView,
    const TControllersMap& controllers,
    const TDynamicJobBalancerSpecPtr&,
    const TWorkerGroupId& workerGroup)
{
    TPartitionDistributionData data(flowView, controllers, workerGroup);
    THashMap<std::string, double> result;
    for (const auto& [workerAddress, _] : flowView->State->Workers) {
        result[workerAddress] = data.GetWorkerCoef(workerAddress);
    }
    return result;
}

THashMap<std::string, double> GetWorkerQueueSizes(
    const TFlowViewPtr& flowView,
    const TWorkerGroupId& workerGroup)
{
    THashMap<std::string, double> result;
    for (const auto& [workerAddress, worker] : flowView->State->Workers) {
        if (!WorkerBelongsToGroup(worker, workerGroup)) {
            continue;
        }
        double totalQueueSize = 0.;
        auto statusIt = flowView->Feedback->WorkerStatuses.find(workerAddress);
        if (statusIt != flowView->Feedback->WorkerStatuses.end()) {
            for (const auto& [resourceId, resourceStatus] : statusIt->second->ResourceStatuses) {
                totalQueueSize += resourceStatus->QueueSize10m.value_or(
                    resourceStatus->QueueSize30s.value_or(0.));
            }
        }
        result[workerAddress] = totalQueueSize;
    }
    return result;
}

std::pair<THashMap<TComputationId, TDistributionStat>, TDistributionStat> GetBalancerIncomingData(
    const TFlowViewPtr& flowView,
    const TControllersMap& controllers,
    const TDynamicJobBalancerSpecPtr& balancerSpec,
    const TWorkerGroupId& workerGroup)
{
    TPersistentBalanceManagerPtr temporaryPersistentManager = New<TPersistentBalanceManager>();
    TBalancer balancer(flowView, controllers, balancerSpec, workerGroup, temporaryPersistentManager);

    return {balancer.GetWorkerDistributionByComputations(), balancer.GetWorkerDistributionOverall()};
}

////////////////////////////////////////////////////////////////////////////////

class TBalanceAsyncSynchronizer
    : public IBalanceAsyncSynchronizer
{
    class TWorkerCoefMetrics
    {
    public:
        TWorkerCoefMetrics(const NProfiling::TProfiler& profiler)
            : Profiler_(profiler)
        { }

        void Update(const THashMap<std::string, double>& workerCoefs, const TFlowViewPtr& flowView)
        {
            for (const auto& [workerAddress, workerCoef] : workerCoefs) {
                const auto& gauge = GetOrInsert(Gauges_, workerAddress, [&] {
                    // Use worker name if available, otherwise fall back to address.
                    auto worker = GetOrDefault(flowView->State->Workers, workerAddress, nullptr);
                    const std::string& metricKey = worker && !worker->Name.empty() ? worker->Name : workerAddress;
                    return NProfiling::TGauge(Profiler_.Gauge("/" + metricKey));
                });
                gauge.Update(workerCoef);
            }

            DropMissingKeys(Gauges_, workerCoefs);
        }

    private:
        NProfiling::TProfiler Profiler_;
        THashMap<std::string, NProfiling::TGauge> Gauges_;
    };

    class TWorkerQueueMetrics
    {
    public:
        TWorkerQueueMetrics(const NProfiling::TProfiler& profiler)
            : Profiler_(profiler)
        { }

        void Update(const THashMap<std::string, double>& workerQueueSizes, const TFlowViewPtr& flowView)
        {
            for (const auto& [workerAddress, queueSize] : workerQueueSizes) {
                const auto& gauge = GetOrInsert(Gauges_, workerAddress, [&] {
                    auto worker = GetOrDefault(flowView->State->Workers, workerAddress, nullptr);
                    const std::string& metricKey = worker && !worker->Name.empty() ? worker->Name : workerAddress;
                    return NProfiling::TGauge(Profiler_.Gauge("/" + metricKey));
                });
                gauge.Update(queueSize);
            }

            DropMissingKeys(Gauges_, workerQueueSizes);
        }

    private:
        NProfiling::TProfiler Profiler_;
        THashMap<std::string, NProfiling::TGauge> Gauges_;
    };

    class TBalancerIncomingMetrics
    {
    public:
        TBalancerIncomingMetrics(const NProfiling::TProfiler& profiler)
            : Profiler_(profiler)
        { }

        void Update(const THashMap<TComputationId, TDistributionStat>& distributionByComputation, const TDistributionStat& distributionOverall, const TFlowViewPtr& flowView)
        {
            const auto& computationsRange = distributionByComputation | std::views::keys;
            THashSet<std::optional<TComputationId>> computations(computationsRange.begin(), computationsRange.end());
            computations.insert(std::nullopt);
            const auto& workersRange = distributionOverall.Set | std::views::values;
            THashSet<std::string> workers(workersRange.begin(), workersRange.end());

            auto updateForComputation = [&] (const std::optional<const TComputationId>& computationId, const TDistributionStat& distributionStat) {
                auto& workerGauges = Gauges_[computationId];

                for (const auto& [value, workerAddress] : distributionStat.Set) {
                    const auto& gauge = GetOrInsert(workerGauges, workerAddress, [&] {
                        // Use worker name if available, otherwise fall back to address.
                        auto worker = GetOrDefault(flowView->State->Workers, workerAddress, nullptr);
                        const std::string& metricKey = worker && !worker->Name.empty() ? worker->Name : workerAddress;
                        const auto& profiler = computationId ? Profiler_.WithTag("computation_id", computationId->Underlying()) : Profiler_;
                        return profiler.Gauge("/" + metricKey);
                    });
                    gauge.Update(value);
                }

                DropMissingKeys(workerGauges, workers);
            };

            for (const auto& [computationId, distributionStat] : distributionByComputation) {
                updateForComputation(computationId, distributionStat);
            }
            updateForComputation(std::nullopt, distributionOverall);
            DropMissingKeys(Gauges_, computations);
        }

    private:
        NProfiling::TProfiler Profiler_;
        THashMap<std::optional<TComputationId>, THashMap<std::string, NProfiling::TGauge>> Gauges_;
    };

    //! Stores all the data that should be passed from JobManager to balancer instance.
    struct TStartData
    {
        TFlowViewPtr FlowView;
        THashMap<TComputationId, IComputationControllerPtr> Controllers;
        TDynamicJobBalancerSpecPtr BalancerSpec;
    };

    TPersistentBalanceManagerPtr PersistentManager_;
    TWorkerCoefMetrics WorkerCoefMetrics_;
    TWorkerQueueMetrics WorkerQueueMetrics_;
    TBalancerIncomingMetrics IncomingMetrics_;
    TSequenceIdGeneratorPtr SequenceIdGenerator_ = New<TSequenceIdGenerator>();
    TSequenceIdGeneratorPtr DeferredSequenceIdGenerator_ = New<TSequenceIdGenerator>();
    TWorkerGroupId WorkerGroup_;

    // Start data section.
    TStartData StartData_;
    size_t FlowViewEpoch_ = 0;
    bool IsRunning_ = false;
    bool IsStopping_ = false;
    TFuture<void> StoppedFuture_;
    // Mutex of the section above.
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, StartDataLock_);

    // Section that stores actions that are already planned, but not yet consumed by JobManager.
    TRebalanceActions AppliedActions_;
    TRebalanceActions DeferredAppliedActions_;
    // Mutex of the section above.
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, AppliedActionsLock_);

public:
    TBalanceAsyncSynchronizer(const NProfiling::TProfiler& profiler, const TWorkerGroupId& workerGroup)
        : PersistentManager_(New<TPersistentBalanceManager>())
        , WorkerCoefMetrics_(profiler.WithPrefix("/worker_coefs"))
        , WorkerQueueMetrics_(profiler.WithPrefix("/worker_queue_sizes"))
        , IncomingMetrics_(profiler.WithPrefix("/incoming_metrics"))
        , WorkerGroup_(workerGroup)
        , AppliedActions_(SequenceIdGenerator_)
        , DeferredAppliedActions_(DeferredSequenceIdGenerator_)
    { }

    //! Emits worker_coefs / worker_queue_sizes / incoming_metrics gauges for this worker group.
    //! Called synchronously from DoBalance so the metrics are emitted regardless of balancer
    //! type. Previously these lived inside the async balancing loop, which is only awakened
    //! by Push() — and only the CpuAware async branch calls Push(), so non-CpuAware groups
    //! never saw any of these metrics.
    void UpdateMetrics(
        const TFlowViewPtr& flowView,
        const THashMap<TComputationId, IComputationControllerPtr>& controllers,
        const TDynamicJobBalancerSpecPtr& balancerSpec)
    {
        auto workerCoefsUpdate = GetWorkerCoefs(flowView, controllers, balancerSpec, WorkerGroup_);
        WorkerCoefMetrics_.Update(workerCoefsUpdate, flowView);
        auto workerQueueSizes = GetWorkerQueueSizes(flowView, WorkerGroup_);
        WorkerQueueMetrics_.Update(workerQueueSizes, flowView);
        auto incomingMetrics = GetBalancerIncomingData(flowView, controllers, balancerSpec, WorkerGroup_);
        IncomingMetrics_.Update(incomingMetrics.first, incomingMetrics.second, flowView);
    }

    void Push(const TFlowViewPtr& flowView,
        const THashMap<TComputationId, IComputationControllerPtr>& controllers,
        const TDynamicJobBalancerSpecPtr& balancerSpec)
    {
        auto startDataLock = Guard(StartDataLock_);
        auto appliedActionsLock = Guard(AppliedActionsLock_);
        StartData_ = {flowView->CopyPtr(), controllers, balancerSpec};
        StartData_.FlowView->State = flowView->State->Clone();
        // Create snapshot for consistent data. Pass committed=false so the snapshot also reflects
        // the uncommitted changes of the in-flight mutation (e.g. removed jobs), keeping it consistent
        // with the shared live feedback/ephemeral state the balancer reads (YTFLOW-625).
        StartData_.FlowView->State->CreateSnapshot(/*committed*/ false);
        StartData_.FlowView->EphemeralState = CloneYsonStruct(flowView->EphemeralState);
        FlowViewEpoch_++;
        size_t originalCount = AppliedActions_.Transactions.size();
        auto maxAppliedSequenceId = GetOrDefault(flowView->EphemeralState->MaxAppliedBalancerSequenceIds, WorkerGroup_, TSequenceId(0));
        AppliedActions_.DropAlreadyApplied(maxAppliedSequenceId);
        YT_LOG_EVENT(
            NController::BalancerLogger,
            NLogging::ELogLevel::Info,
            "FlowView pushed (SequenceId: %v, Dropped: %v)",
            maxAppliedSequenceId.Underlying(),
            originalCount - AppliedActions_.Transactions.size());
        TRebalanceActionsVerifier verifier(flowView);
        AppliedActions_ = verifier.Verify(AppliedActions_);
        DeferredAppliedActions_ = verifier.VerifyWithPreapplied(AppliedActions_, DeferredAppliedActions_);

        YT_LOG_INFO("Updated StartData (Epoch: %lu)", FlowViewEpoch_);
    }

    void StartBalancing(const IInvokerPtr& invoker) override
    {
        // Guarded section.
        {
            auto startDataLock = Guard(StartDataLock_);
            if (IsRunning_) {
                return;
            } else {
                IsRunning_ = true;
                IsStopping_ = false;
            }
        }

        // Main always-running loop for balancing.
        // We pass the actual flowView etc. to the new instance of the balancer using RebalanceJobs.
        // Each iteration (RebalanceJobs run) will last constant (dynamic spec-defined) number of seconds.
        // If some computation's processing had not been terminated by that time, the intermediate data will be saved using TPersistentBalanceManager.
        auto balanceProcedure = [this, weakThis = MakeWeak(this)] () {
            size_t epoch = 0;
            TStartData startData;
            TRebalanceActions alreadyApplied;
            TRebalanceActions alreadyAppliedDeferred;

            while (auto strongThis = weakThis.Lock()) {
                // A failing iteration (e.g. a THROW_ERROR deep in RebalanceJobs) must not kill the
                // background fiber: it would stay dead until the next Reconfigure while IsRunning_
                // stays true, stalling the pipeline. Catch, log and retry on the next iteration so
                // the balancer self-heals once fresh data (a new push) arrives. Fiber cancellation
                // throws a non-std::exception type, so shutdown still propagates.
                try {
                    // Updating local copies of start data if needed.
                    {
                        auto startDataLock = Guard(StartDataLock_);
                        auto appliedActionsLock = Guard(AppliedActionsLock_);

                        if (IsStopping_) {
                            IsStopping_ = false;
                            IsRunning_ = false;
                            return;
                        }

                        if (this->FlowViewEpoch_ != epoch) {
                            startData = StartData_;
                            epoch = FlowViewEpoch_;
                        }

                        alreadyApplied = AppliedActions_;
                        alreadyAppliedDeferred = DeferredAppliedActions_;
                    }

                    // If epoch is 0, we have never received any flowView to process, thus can't start now.
                    if (epoch == 0) {
                        NConcurrency::TDelayedExecutor::WaitForDuration(TDuration::Seconds(1));
                        continue;
                    }

                    YT_LOG_EVENT(
                        NController::BalancerLogger,
                        NLogging::ELogLevel::Debug,
                        "Going to balancing with already applied actions (FastActions: %v, DeferredActions: %v)",
                        alreadyApplied.Transactions.size(),
                        alreadyAppliedDeferred.Transactions.size());

                    auto [fastActions, slowActions] = RebalanceJobs(startData.FlowView, startData.Controllers, startData.BalancerSpec, strongThis->WorkerGroup_, TInstant::Now() + startData.BalancerSpec->RebalanceSyncPeriod, PersistentManager_, alreadyApplied, alreadyAppliedDeferred);
                    YT_LOG_EVENT(
                        NController::BalancerLogger,
                        NLogging::ELogLevel::Debug,
                        "Returned from balancing (FastActions: %v, DeferredActions: %v)",
                        fastActions.Transactions.size(),
                        slowActions.Transactions.size());

                    auto appliedActionsLock = Guard(AppliedActionsLock_);
                    AppliedActions_.Merge(fastActions);
                    YT_LOG_EVENT(
                        NController::BalancerLogger,
                        NLogging::ELogLevel::Debug,
                        "Fast actions merge completed (SequenceId: %v)",
                        AppliedActions_.GetSequenceId());
                    DeferredAppliedActions_ = slowActions;
                    TRebalanceActionsVerifier verifier(startData.FlowView);
                    DeferredAppliedActions_ = verifier.VerifyWithPreapplied(AppliedActions_, DeferredAppliedActions_);
                    if (ShouldApplySlowActionsNow(startData.FlowView, startData.Controllers, startData.BalancerSpec, strongThis->WorkerGroup_, PersistentManager_, AppliedActions_, DeferredAppliedActions_)) {
                        AppliedActions_.Merge(DeferredAppliedActions_);
                        DeferredAppliedActions_ = TRebalanceActions(DeferredSequenceIdGenerator_);
                        YT_LOG_EVENT(
                            NController::BalancerLogger,
                            NLogging::ELogLevel::Debug,
                            "Merged deferred actions (SequenceId: %v)",
                            AppliedActions_.GetSequenceId());
                    }
                    YT_LOG_EVENT(
                        NController::BalancerLogger,
                        NLogging::ELogLevel::Debug,
                        "Total merged size after balancing updated (Size: %v)",
                        AppliedActions_.Transactions.size());
                    YT_LOG_EVENT(
                        NController::BalancerLogger,
                        NLogging::ELogLevel::Debug,
                        "Total deferred size after balancing updated (Size: %v)",
                        DeferredAppliedActions_.Transactions.size());

                    appliedActionsLock.Release();
                } catch (const std::exception& ex) {
                    YT_LOG_EVENT(
                        NController::BalancerLogger,
                        NLogging::ELogLevel::Error,
                        "Async balancer iteration failed; the fiber stays alive and retries (Error: %v)",
                        TError(ex));
                    NConcurrency::TDelayedExecutor::WaitForDuration(TDuration::Seconds(1));
                }
                NConcurrency::Yield();
            }
        };

        StoppedFuture_ = std::move(BIND(balanceProcedure)
                .AsyncVia(invoker)
                .Run());
    }

    void StopBalancing() override
    {
        auto guard = Guard(StartDataLock_);
        if (IsRunning_) {
            IsStopping_ = true;
            guard.Release();
            WaitUntilSet(StoppedFuture_);
        }
    }

    TRebalanceActions PullActionsUnverified()
    {
        YT_LOG_INFO("Pulling actions from synchronizer");
        auto appliedActionsLock = Guard(AppliedActionsLock_);
        auto actions = AppliedActions_;
        return actions;
    }

    TRebalanceResult PrepareResult(const TRebalanceActions& rebalanceActions)
    {
        TRebalanceResult result;
        for (const auto& transaction : rebalanceActions.Transactions) {
            for (auto& [type, partitionId, workerAddress, _] : transaction.Actions) {
                result.Actions.push_back(TRebalanceResultAction{
                    .Type = type,
                    .PartitionId = partitionId,
                    .WorkerAddress = workerAddress});
            }
        }
        result.SequenceId = rebalanceActions.GetSequenceId();
        return result;
    }

    TRebalanceResult PullActionsVerify(const TFlowViewPtr& flowView)
    {
        auto verifier = TRebalanceActionsVerifier(flowView);
        return PrepareResult(verifier.Verify(PullActionsUnverified()));
    }

    TRebalanceResult DoBalance(
        const TFlowViewPtr& flowView,
        const THashMap<TComputationId, IComputationControllerPtr>& controllers,
        const TDynamicJobBalancerSpecPtr& balancerSpec,
        std::optional<TDuration> timeSinceSynced,
        EPipelineState targetState) override
    {
        UpdateMetrics(flowView, controllers, balancerSpec);

        const auto& layout = flowView->State->ExecutionSpec->Layout;
        NBalancer::TRebalanceResult rebalanceResult;

        if (balancerSpec->BalancerType == EJobBalancerType::Greedy) {
            rebalanceResult = DoBalanceGreedy(flowView, controllers, WorkerGroup_);
        } else if (balancerSpec->BalancerType == EJobBalancerType::CpuAware) {
            if (balancerSpec->AsyncBalancing) {
                // The pushed snapshot reflects the in-flight mutation (CreateSnapshot(committed=false)),
                // so it is always consistent with the live feedback/ephemeral state the balancer reads.
                // There is no longer any layout/spec combination that needs deferring (YTFLOW-625):
                // previously a mid-spec-change snapshot could reference a dropped computation and crash
                // the balancer, now the snapshot carries the mutation that interrupts those partitions.
                Push(flowView, controllers, balancerSpec);

                bool foundStrayPartitions = false;
                for (const auto& [_, partition] : layout->Partitions) {
                    if ((partition->State == EPartitionState::Executing || partition->State == EPartitionState::Completing || partition->State == EPartitionState::Interrupting) && !partition->CurrentJobId.has_value()) {
                        foundStrayPartitions = true;
                        break;
                    }
                }

                bool shouldDoPull = false;
                if (foundStrayPartitions) {
                    YT_LOG_INFO("Found stray partitions, applying rebalance actions immediately");
                    shouldDoPull = true;
                } else if (!timeSinceSynced.has_value()) {
                    YT_LOG_INFO("Pipeline is not in sync yet, delaying rebalance");
                } else if (targetState == EPipelineState::Stopped || targetState == EPipelineState::Paused) {
                    YT_LOG_INFO("Pipeline is stopping, will not balance if possible");
                } else if (timeSinceSynced < balancerSpec->RebalanceDelayAfterPipelineSync) {
                    YT_LOG_INFO("Pipeline is in sync not long enough, delaying rebalance");
                } else {
                    YT_LOG_INFO("Pipeline is deemed stable, applying rebalance buffer");
                    shouldDoPull = true;
                }

                if (shouldDoPull) {
                    rebalanceResult = PullActionsVerify(flowView);
                    YT_LOG_INFO("Pulled rebalance actions (Count: %v, SequenceId: %v)",
                        rebalanceResult.Actions.size(),
                        rebalanceResult.SequenceId);
                }
            } else {
                rebalanceResult = DoBalanceSync(flowView, controllers, balancerSpec);
            }
        } else if (balancerSpec->BalancerType == EJobBalancerType::ResourceQueue) {
            rebalanceResult = DoBalanceResourceQueue(flowView, balancerSpec, WorkerGroup_);
        } else {
            THROW_ERROR_EXCEPTION("Unknown balancer type: %v", balancerSpec->BalancerType);
        }

        return rebalanceResult;
    }

    TRebalanceResult DoBalanceSync(
        const TFlowViewPtr& flowView,
        const THashMap<TComputationId, IComputationControllerPtr>& controllers,
        const TDynamicJobBalancerSpecPtr& balancerSpec)
    {
        YT_ASSERT(!IsRunning_);
        AppliedActions_ = TRebalanceActions(SequenceIdGenerator_);
        auto [fastActions, slowActions] =
            RebalanceJobs(flowView, controllers, balancerSpec, WorkerGroup_, TInstant::Now() + balancerSpec->RebalanceSyncPeriod, PersistentManager_, AppliedActions_, TRebalanceActions(DeferredSequenceIdGenerator_));
        auto verifier = TRebalanceActionsVerifier(flowView);
        auto resultActions = verifier.Verify(fastActions);
        auto slowActionsVerified = verifier.VerifyWithPreapplied(resultActions, slowActions);
        if (ShouldApplySlowActionsNow(flowView, controllers, balancerSpec, WorkerGroup_, PersistentManager_, resultActions, slowActionsVerified)) {
            resultActions.Merge(slowActionsVerified);
        }
        return PrepareResult(verifier.Verify(resultActions));
    }
};

////////////////////////////////////////////////////////////////////////////////

TPersistentBalanceManager::TPersistentBalanceManager()
    : Timestamp_(TInstant::Now())
{
    YT_LOG_INFO("Persistent balance manager created");
}

TBalancerLoopContext& TPersistentBalanceManager::GetLoopContext()
{
    return LoopContext_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

IBalanceAsyncSynchronizerPtr CreateBalanceAsyncSynchronizer(const NProfiling::TProfiler& profiler, const TWorkerGroupId& workerGroup)
{
    return New<TBalanceAsyncSynchronizer>(profiler, workerGroup);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NBalancer

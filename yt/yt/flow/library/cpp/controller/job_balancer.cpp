#include "private.h"

#include "job_balancer.h"
#include "job_balancer_common.h"
#include "job_balancer_greedy.h"
#include "job_balancer_resource_queue.h"

#include <util/generic/map.h>

#include <yt/yt/flow/library/cpp/common/computation_controller.h>
#include <yt/yt/flow/library/cpp/common/flow_view.h>

#include <yt/yt/flow/library/cpp/misc/weighted_random.h>

#include <library/cpp/yt/containers/enum_indexed_array.h>

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
static constexpr TDuration EmptyIterationBackoff = TDuration::Seconds(1);
//! Hard cap of resource-relief moves per resource per slow-balancing round: the memory metric is
//! noisy, and a single round must not flood the pipeline with relocations.
static constexpr int MaxReliefMovesPerResource = 100;

//! Defaults of the even-load gate thresholds; overridable per resource via the
//! rebalance_even_load_thresholds spec map. The spread is in the resource's own units.
static constexpr double DefaultEvenLoadRatio = 1.2;

double DefaultEvenLoadSpread(EBalanceResource resource)
{
    switch (resource) {
        case EBalanceResource::Cpu:
            return 1.;
        case EBalanceResource::Memory:
            return 1_GB;
    }
    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

//! Per-resource vector of values (usages, weights, coefficients).
using TResourceVector = TEnumIndexedArray<EBalanceResource, double>;

//! Normalized (summing to 1) resource weights from the balancer spec.
TResourceVector NormalizeBalanceWeights(const THashMap<EBalanceResource, double>& weights)
{
    TResourceVector result;
    double sum = 0.;
    for (const auto& [resource, weight] : weights) {
        sum += weight;
    }
    // The spec validates that the sum is positive.
    for (const auto& [resource, weight] : weights) {
        result[resource] = weight / sum;
    }
    return result;
}

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
    //! Memory usage in bytes from the status of an active job, if there's one.
    std::optional<double> InputMemoryUsage;
    //! Calculated complexity (Cp from the formula above).
    double Complexity{};
    //! Calculated normalized CPU usage (Kc *  Wp * Cp).
    double NormalizedCpuUsage{};
    //! Calculated normalized memory usage: the raw usage when known, otherwise the computation
    //! average per weight unit times the weight. The memory worker coefficient is always 1.
    double NormalizedMemoryUsage{};
    //! Time a job has been active.
    TDuration TimeSinceStart;
};

//! Normalized per-resource usage of a partition as a vector.
TResourceVector GetNormalizedUsage(const TPartitionDistributionInfo& info)
{
    TResourceVector result;
    result[EBalanceResource::Cpu] = info.NormalizedCpuUsage;
    result[EBalanceResource::Memory] = info.NormalizedMemoryUsage;
    return result;
}

//! The bottleneck resource of a demand: the weighted resource with the largest demand relative to
//! the given per-resource scale (e.g. the computation's per-worker target usage). Falls back to
//! the heaviest-weighted resource when no weighted resource has a usable scale.
EBalanceResource PickBottleneckResource(const TResourceVector& weights, const TResourceVector& demand, const TResourceVector& scale)
{
    // The fallback must not resurrect CPU routing under a CPU-unweighted config such as
    // {cpu: 0, memory: 1}; the spec guarantees at least one positive weight.
    auto result = EBalanceResource::Cpu;
    for (auto resource : TEnumTraits<EBalanceResource>::GetDomainValues()) {
        if (weights[resource] > weights[result]) {
            result = resource;
        }
    }
    double resultShare = std::numeric_limits<double>::lowest();
    for (auto resource : TEnumTraits<EBalanceResource>::GetDomainValues()) {
        if (weights[resource] <= 0. || scale[resource] <= 0.) {
            continue;
        }
        double share = weights[resource] * demand[resource] / scale[resource];
        if (share > resultShare) {
            resultShare = share;
            result = resource;
        }
    }
    return result;
}

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

    //! How trustworthy the memory inputs of this round are.
    struct TMemoryMetricQuality
    {
        //! Partitions whose memory usage came from live job metrics.
        int MeasuredPartitions = 0;
        //! Partitions estimated from their computation's average (no metrics of their own yet).
        int EstimatedPartitions = 0;
        //! Partitions of computations with no memory data at all: invisible to memory balancing.
        int UnmeteredPartitions = 0;
    };

    const TMemoryMetricQuality& GetMemoryMetricQuality() const
    {
        return MemoryMetricQuality_;
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
    //! Filled by FinalizeMemoryUsage.
    TMemoryMetricQuality MemoryMetricQuality_;

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
    //! Finalize memory usage model: raw usage when known, computation average otherwise.
    void FinalizeMemoryUsage();
    //! Calculate the coefficient of AvgJobInterval of the workers' numbers.
    //! Is considered to be proportional to the average interval of the jobs currently executed on the worker (with upper limit on a single job interval).
    void CalculateWorkerAvgJobIntervals();
};

////////////////////////////////////////////////////////////////////////////////

//! Wise set of partitions with their resource usage (doesn't matter normalized or not).
//! Allows searching by usage of any single resource.
class TEmulationPartitionSet
{
public:
    int Count = 0;
    TResourceVector Usage;
    TEnumIndexedArray<EBalanceResource, std::set<std::pair<double, TPartitionId>>> Spectres;
    std::set<TPartitionId> Partitions;

    void Add(const TPartitionId& id, const TResourceVector& usage, const TEnumIndexedArray<EBalanceResource, bool>& activeResources)
    {
        Partitions.insert(id);
        Count++;
        for (auto resource : TEnumTraits<EBalanceResource>::GetDomainValues()) {
            Usage[resource] += usage[resource];
            // Spectres are std::set's — the expensive part; maintain them only for resources
            // somebody reads (CPU always, weighted resources otherwise).
            if (activeResources[resource]) {
                Spectres[resource].emplace(usage[resource], id);
            }
        }
    }

    void Del(const TPartitionId& id, const TResourceVector& usage, const TEnumIndexedArray<EBalanceResource, bool>& activeResources)
    {
        Partitions.erase(id);
        Count--;
        for (auto resource : TEnumTraits<EBalanceResource>::GetDomainValues()) {
            Usage[resource] -= usage[resource];
            if (activeResources[resource]) {
                Spectres[resource].erase(std::pair(usage[resource], id));
            }
        }
    }

    TPartitionId FindClosest(EBalanceResource resource, double usage) const
    {
        const auto& spectre = Spectres[resource];
        std::pair<double, TPartitionId> key{usage, {}};
        auto it = spectre.lower_bound(key);
        if (it == spectre.end()) {
            it = std::prev(it);
        } else if (it != spectre.begin()) {
            double found2 = it->first;
            it = std::prev(it);
            double found1 = it->first;
            if (std::abs(usage - found1) > std::abs(usage - found2)) {
                it = std::next(it);
            }
        }
        return it->second;
    }
};

//! More complex set of partitions with their resource usage.
//! Internally has separate sets for different partition statuses.
class TEmulationInfo
{
public:
    TEmulationPartitionSet All;
    TEmulationPartitionSet Executing;
    TEmulationPartitionSet Interrupting;

    void Add(const TPartitionId& id, const TPartitionDistributionInfo& info, const TResourceVector& usage, const TEnumIndexedArray<EBalanceResource, bool>& activeResources)
    {
        All.Add(id, usage, activeResources);
        if (info.State == EPartitionState::Executing) {
            Executing.Add(id, usage, activeResources);
        }
        if (info.State == EPartitionState::Interrupting || info.State == EPartitionState::Completing) {
            Interrupting.Add(id, usage, activeResources);
        }
    }

    void Del(const TPartitionId& id, const TPartitionDistributionInfo& info, const TResourceVector& usage, const TEnumIndexedArray<EBalanceResource, bool>& activeResources)
    {
        All.Del(id, usage, activeResources);
        if (info.State == EPartitionState::Executing) {
            Executing.Del(id, usage, activeResources);
        }
        if (info.State == EPartitionState::Interrupting || info.State == EPartitionState::Completing) {
            Interrupting.Del(id, usage, activeResources);
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

//! Approximately calculated desired number of jobs and resource usage of a worker.
struct TEmulationTarget
{
    double Count = 0;
    TResourceVector Usage;
    TResourceVector AvgUsage;
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

    //! Per-resource worker coefficient: only CPU has a per-worker speed coefficient,
    //! memory shares are uniform by design.
    double GetCoef(EBalanceResource resource) const
    {
        return resource == EBalanceResource::Cpu ? WorkerCoef : 1.;
    }

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

//!  Distribution statistics of some value (actually a resource usage) over some keys (actually workers).
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
        if (avg == 0.) {
            // All values are zero (e.g. a resource nobody reports): the distribution is even.
            return 0.;
        }
        return Deviation() / avg;
    }
};

////////////////////////////////////////////////////////////////////////////////

//! Wise collection of workers, their partitions, stray partitions.
//! Per-resource set of distribution statistics.
using TDistributionStats = TEnumIndexedArray<EBalanceResource, TDistributionStat>;

////////////////////////////////////////////////////////////////////////////////

class TDistributionEmulation
{
public:
    TDistributionEmulation(
        const TFlowViewPtr& flowView,
        const TPartitionDistributionData& partitionData,
        const TWorkerGroupId& workerGroup,
        const TResourceVector& balanceWeights);
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

    //! Get weighted relative deviation of resource usage distribution between workers.
    double GetRelativeDeviation() const
    {
        return WeightedRelativeDeviation(WorkerStat_);
    }

    //! Get weighted relative deviation of resource usage distribution of given computation between workers.
    double GetRelativeDeviation(const TComputationId& computationId) const
    {
        if (!WorkerStatByComputations_.contains(computationId)) {
            return 0;
        }
        return WeightedRelativeDeviation(WorkerStatByComputations_.at(computationId));
    }

    //! Get computations, ordered by weighted deviation of resource usage distribution over workers.
    const std::set<std::pair<double, TComputationId>>& ComputationsByDeviation() const
    {
        return ComputationsByDeviation_;
    }

    //! Get current worker of a partition. The partition must be not stray!.
    std::string PartitionWorker(const TPartitionId& id) const
    {
        return PartitionWorker_.at(id);
    }

    //! Get saved normalized resource usage of a partition. The partition must be not stray!.
    const TResourceVector& PartitionNormalizedUsage(const TPartitionId& id) const
    {
        return PartitionNormalizedUsage_.at(id);
    }

    //! Normalized (summing to 1) resource weights the emulation scores by.
    const TResourceVector& BalanceWeights() const
    {
        return BalanceWeights_;
    }

    //! Get actions that were made during rebalancing.
    const THashMap<TPartitionId, std::vector<TEmulationAction>>& Actions() const
    {
        return Actions_;
    }

    //! Obtain the distribution of the performance of workers (overall).
    const TDistributionStats& GetWorkerDistributionOverall() const&
    {
        return WorkerStat_;
    }

    TDistributionStats&& GetWorkerDistributionOverall() &&
    {
        return std::move(WorkerStat_);
    }

    //! Obtain the distribution of the performance of workers (by computation).
    const THashMap<TComputationId, TDistributionStats>& GetWorkerDistributionByComputations() const&
    {
        return WorkerStatByComputations_;
    }

    THashMap<TComputationId, TDistributionStats>&& GetWorkerDistributionByComputations() &&
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
    //! Emulation stores a number of PartitionSet by different datapoints. PartitionSet holds information about resource usage.
    //! If a PartitionSet belongs to a particular worker, it uses real usage; otherwise it used normalized usage.
    //! Normalized usage is usage divided by the per-resource worker coef to be independent from worker power.

    //! Saved normalized resource usage.
    THashMap<TPartitionId, TResourceVector> PartitionNormalizedUsage_;
    //! Saved resource usage. Is set only if a partition belongs to some worker.
    THashMap<TPartitionId, TResourceVector> PartitionUsage_;
    //! Current worker of a partitions.
    THashMap<TPartitionId, std::string> PartitionWorker_;

    //! Normalized (summing to 1) resource weights, see the balance_weights spec parameter.
    TResourceVector BalanceWeights_;

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

    //! Statistics of per-resource usage distribution over workers.
    TDistributionStats WorkerStat_;
    //! Statistics of per-resource usage distribution over workers per each computation.
    THashMap<TComputationId, TDistributionStats> WorkerStatByComputations_;
    //! Computations, ordered by weighted deviation of resource usage distribution over workers.
    std::set<std::pair<double, TComputationId>> ComputationsByDeviation_;
    //! The exact ordering keys currently stored in ComputationsByDeviation_.
    THashMap<TComputationId, double> ComputationDeviationKeys_;
    //! Whether more than one resource has a positive weight.
    bool MultiResource_ = false;
    //! Resources whose spectres and worker statistics are maintained: CPU always (some search
    //! paths read the CPU spectre unconditionally) plus every positively weighted resource.
    TEnumIndexedArray<EBalanceResource, bool> ActiveResources_;

    //! Action that were made during rebalancing.
    THashMap<TPartitionId, std::vector<TEmulationAction>> Actions_;
    int ActionCount_ = 0;

    void CollectWorkers(const TFlowViewPtr& flowView, const TPartitionDistributionData& partitionData, const TWorkerGroupId& workerGroup);
    void CollectPartitions(const TPartitionDistributionData& partitionData);
    void CalculateTargetValues();

    //! Weighted sum of the given per-resource statistic; zero-weight resources do not contribute.
    double WeightedStat(const TDistributionStats& stats, double (TDistributionStat::*measure)() const) const
    {
        double result = 0.;
        for (auto resource : TEnumTraits<EBalanceResource>::GetDomainValues()) {
            if (BalanceWeights_[resource] <= 0.) {
                continue;
            }
            result += BalanceWeights_[resource] * (stats[resource].*measure)();
        }
        return result;
    }

    //! Weighted sum of per-resource relative deviations (unitless, so resources mix cleanly).
    double WeightedRelativeDeviation(const TDistributionStats& stats) const
    {
        return WeightedStat(stats, &TDistributionStat::RelativeDeviation);
    }

    //! The key of ComputationsByDeviation_. With a single weighted resource this is exactly the
    //! legacy absolute deviation. With several resources the units differ (bytes vs cores), so
    //! each resource's deviation is normalized by its global average per-worker usage — a factor
    //! common to all computations, making the values comparable across resources while preserving
    //! the relative proportions within one resource.
    double ComputationOrderingKey(const TDistributionStats& stats) const
    {
        if (!MultiResource_) {
            return WeightedStat(stats, &TDistributionStat::Deviation);
        }
        double result = 0.;
        for (auto resource : TEnumTraits<EBalanceResource>::GetDomainValues()) {
            if (BalanceWeights_[resource] <= 0. || WorkerStat_[resource].Count == 0) {
                continue;
            }
            double globalAverage = WorkerStat_[resource].Sum / WorkerStat_[resource].Count;
            if (globalAverage <= 0.) {
                continue;
            }
            result += BalanceWeights_[resource] * stats[resource].Deviation() / globalAverage;
        }
        return result;
    }

    //! The normalization factor above drifts as the global stats change, so the erase key must be
    //! remembered exactly rather than recomputed.
    void EraseComputationOrderingEntry(const TComputationId& computationId)
    {
        if (auto it = ComputationDeviationKeys_.find(computationId); it != ComputationDeviationKeys_.end()) {
            ComputationsByDeviation_.erase(std::pair(it->second, computationId));
        }
    }

    void EmplaceComputationOrderingEntry(const TComputationId& computationId, const TDistributionStats& stats)
    {
        double key = ComputationOrderingKey(stats);
        ComputationsByDeviation_.emplace(key, computationId);
        ComputationDeviationKeys_[computationId] = key;
    }
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
    //! Weighted relative deviation of resource usage distribution between workers.
    double GetRelativeDeviation() const;

    const TDistributionStats& GetWorkerDistributionOverall() const&;
    TDistributionStats&& GetWorkerDistributionOverall() &&;
    const THashMap<TComputationId, TDistributionStats>& GetWorkerDistributionByComputations() const&;
    THashMap<TComputationId, TDistributionStats>&& GetWorkerDistributionByComputations() &&;

    //! Normalized (summing to 1) resource weights the emulation scores by.
    const TResourceVector& BalanceWeights() const;

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
    //! The legacy exact fill by one resource: every under-target worker pulls the stray partition
    //! closest to its per-slot deficit. Used when exactly one resource is weighted, reproducing
    //! the pre-multiresource behavior.
    TRebalanceActions DistributeStrayPartitionsPhase1SingleResource(EPartitionState partitionState, EBalanceResource resource);
    //! Shape-aware placement for several weighted resources: bottleneck routing + top-sqrt(N)
    //! shortlist by headroom + minimal weighted bottleneck utilization.
    TRebalanceActions DistributeStrayPartitionsPhase1MultiResource(EPartitionState partitionState);
    //! Distribute the remaining stray partitions.
    TRebalanceActions DistributeStrayPartitionsPhase2(EPartitionState partitionState);

    //! Try to move and exchange partitions to get better balance.
    TRebalanceActions RelieveWorker(const TComputationId& computationId, const std::string& myWorkerAddress);

    //! Cross-computation relief for weighted resources (e.g. memory) that per-computation balancing
    //! cannot even out: move the heaviest partition off the most-loaded worker onto the least-loaded
    //! one while that narrows the spread. Reaches single-partition computations that the per-computation
    //! deviation ordering never selects and the count-based overcount kick cannot see.
    //! Bounded by |until| and a per-resource move cap.
    TRebalanceActions RelieveResourceOverloadedWorkers(TInstant until);

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
    FinalizeMemoryUsage();
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
            // Unlike the CPU counterparts, the memory fields are plain integers: present iff positive.
            if (currentJobStatus->PerformanceMetrics->MemoryUsage10m > 0) {
                info.InputMemoryUsage = currentJobStatus->PerformanceMetrics->MemoryUsage10m;
            } else if (currentJobStatus->PerformanceMetrics->MemoryUsage30s > 0) {
                info.InputMemoryUsage = currentJobStatus->PerformanceMetrics->MemoryUsage30s;
            } else if (currentJobStatus->PerformanceMetrics->MemoryUsageCurrent > 0) {
                info.InputMemoryUsage = currentJobStatus->PerformanceMetrics->MemoryUsageCurrent;
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
        YT_TLOG_EVENT(NController::BalancerLogger, NLogging::ELogLevel::Debug, "Worker coef calculated")
            .With("Worker", address)
            .With("Safe", "1")
            .With("Unsafe", avgCoef)
            .With("AvgInterval", avgJobInterval)
            .With("WorkerCoef", WorkerCoefs_[address]);
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

void TPartitionDistributionData::FinalizeMemoryUsage()
{
    // Per-computation average memory per weight unit, to estimate partitions with no metrics yet.
    // The memory worker coefficient is always 1, so no per-worker normalization is applied.
    THashMap<TComputationId, double> sumMemoryPerWeight;
    THashMap<TComputationId, int> numMemoryPerWeight;
    for (const auto& [partitionId, info] : PartitionInfos_) {
        if (info.InputMemoryUsage.has_value()) {
            sumMemoryPerWeight[info.ComputationId] += info.InputMemoryUsage.value() / info.Weight;
            numMemoryPerWeight[info.ComputationId]++;
        }
    }

    MemoryMetricQuality_ = {};
    for (auto& [_, info] : PartitionInfos_) {
        if (info.InputMemoryUsage.has_value()) {
            info.NormalizedMemoryUsage = info.InputMemoryUsage.value();
            MemoryMetricQuality_.MeasuredPartitions++;
        } else if (auto it = numMemoryPerWeight.find(info.ComputationId); it != numMemoryPerWeight.end()) {
            info.NormalizedMemoryUsage = sumMemoryPerWeight.at(info.ComputationId) / it->second * info.Weight;
            MemoryMetricQuality_.EstimatedPartitions++;
        } else {
            // No memory data for the whole computation: its partitions weigh nothing memory-wise.
            info.NormalizedMemoryUsage = 0.;
            MemoryMetricQuality_.UnmeteredPartitions++;
        }
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

TDistributionEmulation::TDistributionEmulation(
    const TFlowViewPtr& flowView,
    const TPartitionDistributionData& partitionData,
    const TWorkerGroupId& workerGroup,
    const TResourceVector& balanceWeights)
    : BalanceWeights_(balanceWeights)
{
    int weightedResources = 0;
    for (auto resource : TEnumTraits<EBalanceResource>::GetDomainValues()) {
        ActiveResources_[resource] = resource == EBalanceResource::Cpu || BalanceWeights_[resource] > 0.;
        if (BalanceWeights_[resource] > 0.) {
            ++weightedResources;
        }
    }
    MultiResource_ = weightedResources > 1;

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
        ComputationDeviationKeys_[computationId] = 0.;
    }
    for (const auto& [workerAddress, worker] : Workers_) {
        for (auto resource : TEnumTraits<EBalanceResource>::GetDomainValues()) {
            if (!ActiveResources_[resource]) {
                continue;
            }
            WorkerStat_[resource].Add(0, workerAddress);
            for (const auto& computationId : computations) {
                WorkerStatByComputations_[computationId][resource].Add(0, workerAddress);
            }
        }
    }
    for (const auto& [partitionId, info] : partitionInfos) {
        PartitionNormalizedUsage_[partitionId] = GetNormalizedUsage(info);
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
    // Per-resource might sums: for CPU the worker coef makes faster workers mightier,
    // for memory all coefs are 1, so the shares are uniform.
    TResourceVector workerMightSums;
    for (const auto& [address, worker] : Workers_) {
        for (auto resource : TEnumTraits<EBalanceResource>::GetDomainValues()) {
            workerMightSums[resource] += 1. / worker.GetCoef(resource);
        }
    }

    for (auto& [address, worker] : Workers_) {
        // The count target follows the CPU might share.
        double workerMight = 1. / worker.WorkerCoef;
        double coef = workerMight / workerMightSums[EBalanceResource::Cpu];
        for (const auto& [computationId, info] : InfoByComputations_) {
            auto apply = [&coef, &workerMightSums] (TEmulationTarget& target, const TEmulationPartitionSet& info) {
                target.Count = info.Count * coef;
                for (auto resource : TEnumTraits<EBalanceResource>::GetDomainValues()) {
                    target.Usage[resource] = info.Usage[resource] / workerMightSums[resource];
                    target.AvgUsage[resource] = target.Usage[resource] / target.Count;
                }
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
    const auto& normalizedUsage = PartitionNormalizedUsage_[id];
    TResourceVector usage;
    for (auto resource : TEnumTraits<EBalanceResource>::GetDomainValues()) {
        usage[resource] = normalizedUsage[resource] * worker.GetCoef(resource);
    }
    YT_VERIFY(!InfoOverall_.Contains(id));

    PartitionUsage_[id] = usage;
    PartitionWorker_[id] = workerAddress;

    auto& workerStatByComputation = WorkerStatByComputations_[info.ComputationId];
    const auto& infoOverall = worker.InfoOverall;
    const auto& infoByComputations = worker.InfoByComputations[info.ComputationId];

    EraseComputationOrderingEntry(info.ComputationId);
    for (auto resource : TEnumTraits<EBalanceResource>::GetDomainValues()) {
        if (!ActiveResources_[resource]) {
            continue;
        }
        WorkerStat_[resource].Del(infoOverall.Executing.Usage[resource], workerAddress);
        workerStatByComputation[resource].Del(infoByComputations.Executing.Usage[resource], workerAddress);
    }

    InfoOverall_.Add(id, info, normalizedUsage, ActiveResources_);
    InfoByComputations_[info.ComputationId].Add(id, info, normalizedUsage, ActiveResources_);
    worker.InfoOverall.Add(id, info, usage, ActiveResources_);
    worker.InfoByComputations[info.ComputationId].Add(id, info, usage, ActiveResources_);

    for (auto resource : TEnumTraits<EBalanceResource>::GetDomainValues()) {
        if (!ActiveResources_[resource]) {
            continue;
        }
        WorkerStat_[resource].Add(infoOverall.Executing.Usage[resource], workerAddress);
        workerStatByComputation[resource].Add(infoByComputations.Executing.Usage[resource], workerAddress);
    }
    EmplaceComputationOrderingEntry(info.ComputationId, workerStatByComputation);

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
    const auto normalizedUsage = PartitionNormalizedUsage_[id];
    const auto usage = PartitionUsage_[id];
    YT_VERIFY(InfoOverall_.Contains(id));

    PartitionUsage_.erase(id);
    PartitionWorker_.erase(id);

    auto& workerStatByComputation = WorkerStatByComputations_[info.ComputationId];
    const auto& infoOverall = worker.InfoOverall;
    const auto& infoByComputations = worker.InfoByComputations[info.ComputationId];

    EraseComputationOrderingEntry(info.ComputationId);
    for (auto resource : TEnumTraits<EBalanceResource>::GetDomainValues()) {
        if (!ActiveResources_[resource]) {
            continue;
        }
        WorkerStat_[resource].Del(infoOverall.Executing.Usage[resource], workerAddress);
        workerStatByComputation[resource].Del(infoByComputations.Executing.Usage[resource], workerAddress);
    }

    InfoOverall_.Del(id, info, normalizedUsage, ActiveResources_);
    InfoByComputations_[info.ComputationId].Del(id, info, normalizedUsage, ActiveResources_);
    worker.InfoOverall.Del(id, info, usage, ActiveResources_);
    worker.InfoByComputations[info.ComputationId].Del(id, info, usage, ActiveResources_);

    for (auto resource : TEnumTraits<EBalanceResource>::GetDomainValues()) {
        if (!ActiveResources_[resource]) {
            continue;
        }
        WorkerStat_[resource].Add(infoOverall.Executing.Usage[resource], workerAddress);
        workerStatByComputation[resource].Add(infoByComputations.Executing.Usage[resource], workerAddress);
    }
    EmplaceComputationOrderingEntry(info.ComputationId, workerStatByComputation);

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
    const auto& normalizedUsage = PartitionNormalizedUsage_[id];
    YT_VERIFY(!StrayInfoOverall_.Contains(id));

    InfoOverall_.Add(id, info, normalizedUsage, ActiveResources_);
    InfoByComputations_[info.ComputationId].Add(id, info, normalizedUsage, ActiveResources_);
    StrayInfoOverall_.Add(id, info, normalizedUsage, ActiveResources_);
    StrayInfoByComputations_[info.ComputationId].Add(id, info, normalizedUsage, ActiveResources_);
}

void TDistributionEmulation::DelStrayPartition(const TPartitionId& id, const TPartitionDistributionInfo& info)
{
    const auto& normalizedUsage = PartitionNormalizedUsage_[id];
    YT_VERIFY(StrayInfoOverall_.Contains(id));

    InfoOverall_.Del(id, info, normalizedUsage, ActiveResources_);
    InfoByComputations_[info.ComputationId].Del(id, info, normalizedUsage, ActiveResources_);
    StrayInfoOverall_.Del(id, info, normalizedUsage, ActiveResources_);
    StrayInfoByComputations_[info.ComputationId].Del(id, info, normalizedUsage, ActiveResources_);
}

void TDistributionEmulation::ApplyAll(const TRebalanceActions& actions, const TPartitionDistributionData& partitionData)
{
    auto applier = [&] (const TRebalanceActions::TRebalanceAction& action) {
        const auto& [type, partitionId, workerAddress, info] = action;

        if (!GetInfo().All.Partitions.contains(partitionId)) {
            YT_TLOG_ERROR("Requested applying partition, while it is not present among executing partitions")
                .With("Partition", partitionId)
                .With("Worker", workerAddress);
            return;
        }
        if (!Workers().contains(workerAddress)) {
            YT_TLOG_ERROR("Requested applying partition, while it the worker is not present")
                .With("Partition", partitionId)
                .With("Worker", workerAddress);
            return;
        }

        if (type == ERebalanceActionType::Del) {
            if (GetStrayInfo().All.Partitions.contains(partitionId)) {
                YT_TLOG_ERROR("Requested deleting partition, while it is not already assigned to a worker")
                    .With("Partition", partitionId)
                    .With("Worker", workerAddress);
                return;
            }
            DelPartition(partitionId, partitionData.PartitionInfos().at(partitionId), workerAddress);
            AddStrayPartition(partitionId, partitionData.PartitionInfos().at(partitionId));
        }

        if (type == ERebalanceActionType::Add) {
            if (!GetStrayInfo().All.Partitions.contains(partitionId)) {
                YT_TLOG_ERROR("Requested adding partition, while it is already assigned to a worker")
                    .With("Partition", partitionId)
                    .With("Worker", workerAddress);
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
    , Emulation_(flowView, Data_, workerGroup, NormalizeBalanceWeights(balancerSpec->BalanceWeights))
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

    // The count trigger below is resource-independent, but the choice of WHICH partition to evict
    // is made along the worker's most overloaded weighted resource: on a memory-choked worker the
    // memory monsters go first, not whatever happens to match the CPU fit. With only CPU weighted
    // this is always CPU, matching the previous behavior.
    const auto& balanceWeights = Emulation_.BalanceWeights();
    // The eviction is routed along the worker's most overloaded weighted resource; the overload
    // vector (usage minus target) plays the role of the demand in the bottleneck routing.
    auto pickEvictionResource = [&balanceWeights] (const TEmulationPartitionSet& workerInfo, const TEmulationTarget& target) {
        TResourceVector overload;
        for (auto resource : TEnumTraits<EBalanceResource>::GetDomainValues()) {
            overload[resource] = workerInfo.Usage[resource] - target.Usage[resource];
        }
        return PickBottleneckResource(balanceWeights, overload, target.Usage);
    };

    // Whether to run this overcount kick at all is decided by the caller (DoFastBalancing) — it is
    // gated by the even-load thresholds, like deep rebalance.
    // Do not account that moves in the limit since it's a bit different rebalance.
    for (const auto& [computationId, computationInfo] : Emulation_.ComputationInfos()) {
        for (const auto& [workerAddress, worker] : workers) {
            const auto& workerInfo = Emulation_.GetInfo(worker, computationId).Executing;
            const auto& targets = worker.Targets.at(computationId);
            int maxCount = std::floor(targets.Executing.Count * ManagerSpec_->RebalanceCountExceedAllowed) + 1;
            while (workerInfo.Count > maxCount) {
                int countBeforeKick = workerInfo.Count;
                int plannedToRemove = workerInfo.Count - maxCount;
                auto resource = pickEvictionResource(workerInfo, targets.Executing);
                double targetUsagePerJob = targets.Executing.AvgUsage[resource];
                double targetUsagePerWorker = maxCount * targetUsagePerJob;
                double removeUsage = (workerInfo.Usage[resource] - targetUsagePerWorker) / plannedToRemove + targetUsagePerJob;
                TPartitionId partitionId = workerInfo.FindClosest(resource, removeUsage);
                const auto& info = partitionInfos.at(partitionId);
                Emulation_.DelPartition(partitionId, info, workerAddress);
                Emulation_.AddStrayPartition(partitionId, info);
                result.EmplaceAsTransaction(ERebalanceActionType::Del, partitionId, workerAddress, info);
                YT_TLOG_EVENT(NController::BalancerLogger, NLogging::ELogLevel::Info, "Job was kicked because worker is overloaded")
                    .With("JobId", info.JobId)
                    .With("Partition", partitionId)
                    .With("Computation", computationId)
                    .With("Worker", workerAddress)
                    .With("Count", countBeforeKick)
                    .With("MaxCount", maxCount);
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
    // With a single weighted resource the legacy exact fill is both cheaper and reproduces the
    // historical placement of pure-CPU pipelines byte for byte; the shortlist heuristic is only
    // needed when several resources must be traded against each other.
    const auto& balanceWeights = Emulation_.BalanceWeights();
    std::optional<EBalanceResource> singleResource;
    for (auto resource : TEnumTraits<EBalanceResource>::GetDomainValues()) {
        if (balanceWeights[resource] > 0.) {
            if (singleResource) {
                singleResource.reset();
                break;
            }
            singleResource = resource;
        }
    }
    if (singleResource) {
        return DistributeStrayPartitionsPhase1SingleResource(partitionState, *singleResource);
    }
    return DistributeStrayPartitionsPhase1MultiResource(partitionState);
}

TRebalanceActions TBalancer::DistributeStrayPartitionsPhase1SingleResource(EPartitionState partitionState, EBalanceResource resource)
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
                double targetUsagePerJob = (targets.*targetsType).AvgUsage[resource];
                const auto& executingInfo = Emulation_.GetInfo(worker, computationId).*infoType;
                int plannedToAdd = std::floor(targetCount) - executingInfo.Count;
                if (plannedToAdd <= 0) {
                    workerNoMoreCandidates.insert(workerAddress);
                    break;
                }
                double targetUsagePerWorker = std::floor(targetCount) * targetUsagePerJob;
                double addUsage = (targetUsagePerWorker - executingInfo.Usage[resource]) / plannedToAdd;
                double addNormalized = addUsage / worker.GetCoef(resource);
                TPartitionId partitionId = (strayInfo.*infoType).FindClosest(resource, addNormalized);
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

TRebalanceActions TBalancer::DistributeStrayPartitionsPhase1MultiResource(EPartitionState partitionState)
{
    const auto& workers = Emulation_.Workers();
    const auto& partitionInfos = Data_.PartitionInfos();
    const auto& balanceWeights = Emulation_.BalanceWeights();
    auto targetsType = partitionState == EPartitionState::Executing ? &TEmulationTargets::Executing : &TEmulationTargets::Interrupting;
    auto infoType = partitionState == EPartitionState::Executing ? &TEmulationInfo::Executing : &TEmulationInfo::Interrupting;
    TRebalanceActions result;

    // The bulk of the stray partitions is placed shape-aware in three cheap steps per partition:
    //   1. take the stray with the largest weighted demand share and its bottleneck resource d*;
    //   2. shortlist the top-sqrt(N) candidate workers by remaining normalized d* headroom;
    //   3. among the shortlist, pick the worker minimizing the resulting weighted bottleneck
    //      utilization max_d(w_d * used_d / fairShare_d).
    // The per-computation count target still caps every worker, so the count spread is unchanged.
    // (The single-weighted-resource case never reaches this method — see the dispatcher.)

    // Per-worker fair share of the overall normalized usage (in normalized units, so it is
    // comparable across workers): fairShare_d(w) = totalNormalized_d * (1/coef_d(w)) / mightSum_d.
    TResourceVector workerMightSums;
    for (const auto& [workerAddress, worker] : workers) {
        for (auto resource : TEnumTraits<EBalanceResource>::GetDomainValues()) {
            workerMightSums[resource] += 1. / worker.GetCoef(resource);
        }
    }

    for (const auto& [computationId, strayInfo] : Emulation_.StrayComputationInfos()) {
        // Remaining count capacity and normalized usage per candidate worker; per-resource
        // candidate sets ordered by normalized headroom.
        THashMap<std::string, int> slots;
        THashMap<std::string, TResourceVector> usedNorm;
        THashMap<std::string, TResourceVector> fairNorm;
        TEnumIndexedArray<EBalanceResource, std::set<std::pair<double, std::string>>> headroomSets;

        for (const auto& [workerAddress, worker] : workers) {
            const auto& targets = worker.Targets.at(computationId);
            const auto& executingInfo = Emulation_.GetInfo(worker, computationId).*infoType;
            int workerSlots = std::floor((targets.*targetsType).Count) - executingInfo.Count;
            if (workerSlots <= 0) {
                continue;
            }
            slots[workerAddress] = workerSlots;
            auto& used = usedNorm[workerAddress];
            auto& fair = fairNorm[workerAddress];
            for (auto resource : TEnumTraits<EBalanceResource>::GetDomainValues()) {
                double coef = worker.GetCoef(resource);
                used[resource] = (worker.InfoOverall.*infoType).Usage[resource] / coef;
                fair[resource] = (Emulation_.GetInfo().*infoType).Usage[resource] / coef / workerMightSums[resource];
                headroomSets[resource].emplace(fair[resource] - used[resource], workerAddress);
            }
        }

        // The per-worker target usage of this computation (worker-independent), used to rank the
        // partition's per-resource demands against each other.
        TResourceVector computationTargetUsage;
        if (!workers.empty()) {
            computationTargetUsage = (workers.begin()->second.Targets.at(computationId).*targetsType).Usage;
        }

        while ((strayInfo.*infoType).Count != 0 && !slots.empty()) {
            // Step 1: the most demanding stray partition and its bottleneck resource.
            auto bottleneckResource = EBalanceResource::Cpu;
            TPartitionId partitionId = (strayInfo.*infoType).Spectres[EBalanceResource::Cpu].rbegin()->second;
            double partitionDemandShare = std::numeric_limits<double>::lowest();
            for (auto resource : TEnumTraits<EBalanceResource>::GetDomainValues()) {
                if (balanceWeights[resource] <= 0. || computationTargetUsage[resource] <= 0.) {
                    continue;
                }
                const auto& top = *(strayInfo.*infoType).Spectres[resource].rbegin();
                double demandShare = balanceWeights[resource] * top.first / computationTargetUsage[resource];
                if (demandShare > partitionDemandShare) {
                    partitionDemandShare = demandShare;
                    bottleneckResource = resource;
                    partitionId = top.second;
                }
            }
            const auto& info = partitionInfos.at(partitionId);
            const auto& demand = Emulation_.PartitionNormalizedUsage(partitionId);

            // Step 2: shortlist workers by remaining headroom of the bottleneck resource.
            int shortlistSize = std::max<int>(1, std::ceil(std::sqrt(std::ssize(slots))));
            // Step 3: the worker with the smallest resulting weighted bottleneck utilization.
            std::optional<std::string> bestWorkerAddress;
            double bestCost = std::numeric_limits<double>::max();
            auto it = headroomSets[bottleneckResource].rbegin();
            for (int i = 0; i < shortlistSize; ++i, ++it) {
                const auto& workerAddress = it->second;
                double cost = 0.;
                for (auto resource : TEnumTraits<EBalanceResource>::GetDomainValues()) {
                    if (balanceWeights[resource] <= 0. || fairNorm[workerAddress][resource] <= 0.) {
                        continue;
                    }
                    double utilization = (usedNorm[workerAddress][resource] + demand[resource]) / fairNorm[workerAddress][resource];
                    cost = std::max(cost, balanceWeights[resource] * utilization);
                }
                if (cost < bestCost) {
                    bestCost = cost;
                    bestWorkerAddress = workerAddress;
                }
            }
            YT_VERIFY(bestWorkerAddress);

            Emulation_.DelStrayPartition(partitionId, info);
            Emulation_.AddPartition(partitionId, info, *bestWorkerAddress);
            result.EmplaceAsTransaction(ERebalanceActionType::Add, partitionId, *bestWorkerAddress, info);

            // Account the placement in the candidate structures.
            auto& used = usedNorm[*bestWorkerAddress];
            for (auto resource : TEnumTraits<EBalanceResource>::GetDomainValues()) {
                headroomSets[resource].erase(std::pair(fairNorm[*bestWorkerAddress][resource] - used[resource], *bestWorkerAddress));
                used[resource] += demand[resource];
            }
            if (--slots[*bestWorkerAddress] > 0) {
                for (auto resource : TEnumTraits<EBalanceResource>::GetDomainValues()) {
                    headroomSets[resource].emplace(fairNorm[*bestWorkerAddress][resource] - used[resource], *bestWorkerAddress);
                }
            } else {
                slots.erase(*bestWorkerAddress);
                usedNorm.erase(*bestWorkerAddress);
                fairNorm.erase(*bestWorkerAddress);
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
        auto mostLoaded = (Emulation_.GetStrayInfo().*infoType).Spectres[EBalanceResource::Cpu].rbegin();
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

    const auto& balanceWeights = Emulation_.BalanceWeights();
    // The computation's per-worker target usage (worker-independent) scales the demands of its
    // partitions against each other when picking a partition's bottleneck resource.
    const auto& computationTargetUsage = myWorker.Targets.at(computationId).Executing.Usage;

    std::vector<TPartitionId> partitions;

    partitions.clear();
    partitions.reserve(Emulation_.GetInfo(myWorker, computationId).Executing.Partitions.size());
    for (const auto& [_, partitionId] : Emulation_.GetInfo(myWorker, computationId).Executing.Spectres[EBalanceResource::Cpu]) {
        partitions.push_back(partitionId);
    }

    for (const auto& [peerWorkerAddress, peerWorker] : workers) {
        if (myWorkerAddress == peerWorkerAddress) {
            continue;
        }
        // How much of each resource should flow from this worker to the peer to even them out
        // (in normalized units). The swap partner search below is done along the moved partition's
        // bottleneck resource, by analogy with the phase-1 stray placement.
        TResourceVector needMoveNormalized;
        for (auto resource : TEnumTraits<EBalanceResource>::GetDomainValues()) {
            needMoveNormalized[resource] =
                (myWorker.InfoOverall.Executing.Usage[resource] - peerWorker.InfoOverall.Executing.Usage[resource]) /
                (myWorker.GetCoef(resource) + peerWorker.GetCoef(resource));
        }

        TStringStream finegrainedReports;
        finegrainedReports << "Finegrained report for worker " << myWorkerAddress << " begins\n\n";

        for (const auto& partitionId : partitions) {
            const auto& demand = Emulation_.PartitionNormalizedUsage(partitionId);
            auto bottleneckResource = PickBottleneckResource(balanceWeights, demand, computationTargetUsage);
            double recvNormalized = demand[bottleneckResource] - needMoveNormalized[bottleneckResource];
            std::optional<TPartitionId> peerPartitionId;
            if (Emulation_.GetInfo(peerWorker, computationId).Executing.Count > 0) {
                const auto& peerEmulation = Emulation_.GetInfo(peerWorker, computationId).Executing;
                peerPartitionId = peerEmulation.FindClosest(bottleneckResource, recvNormalized * peerWorker.GetCoef(bottleneckResource));
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
        YT_TLOG_EVENT(NController::BalancerLogger, NLogging::ELogLevel::Trace, finegrainedReports.Str());
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

TRebalanceActions TBalancer::RelieveResourceOverloadedWorkers(TInstant until)
{
    TRebalanceActions result;

    // Per-computation balancing (the slow-path computation ordering and the count-based overcount
    // kick) evens CPU well, but it is blind to the per-worker imbalance of a weighted resource that
    // does not track partition count — notably memory of single-partition computations: each such
    // computation is trivially "balanced" (one partition, one worker), so its deviation is ~0 and it
    // is never selected, while the worker holding several of them piles up memory. Here we relieve it
    // worker-centrically and cross-computation: for each weighted non-CPU resource, move the heaviest
    // partition off the most-loaded worker onto the least-loaded one, as long as that strictly narrows
    // the spread. Emitted as ordinary Del+Add moves (graceful downstream), not overcount kicks.
    const auto& weights = Emulation_.BalanceWeights();
    const auto& workers = Emulation_.Workers();
    const auto& partitionInfos = Data_.PartitionInfos();

    for (auto resource : TEnumTraits<EBalanceResource>::GetDomainValues()) {
        // CPU is handled by the count kick and the per-computation slow path; only the extra weighted
        // resources (e.g. memory) need this cross-computation relief.
        if (resource == EBalanceResource::Cpu || weights[resource] <= 0.) {
            continue;
        }

        // Bounded: the slow-balancing round deadline applies here too, and the hard cap keeps one
        // round from emitting an unbounded batch of relocations on a large pipeline.
        int maxMoves = std::min(static_cast<int>(Emulation_.GetInfo().Executing.Count), MaxReliefMovesPerResource);
        for (int moves = 0; moves < maxMoves && TInstant::Now() < until; ++moves) {
            const auto& stat = Emulation_.GetWorkerDistributionOverall()[resource];
            if (stat.Set.size() < 2 || stat.Count == 0) {
                break;
            }
            const auto [maxUsage, overWorker] = *stat.Set.rbegin();
            const auto [minUsage, underWorker] = *stat.Set.begin();
            const double mean = stat.Sum / stat.Count;
            const double gap = maxUsage - minUsage;

            // Stop once the two extremes are within the even-load tolerance.
            if (mean <= 0. || gap <= 2.0 * ManagerSpec_->RebalanceTargetDeviation * mean) {
                break;
            }

            // Respect the destination's per-computation count cap (mirrors RelieveWorker), so we do
            // not pile one computation onto a single worker.
            auto receiverAccepts = [&] (const TComputationId& computationId) {
                const auto& targets = workers.at(underWorker).Targets.at(computationId);
                const double maxComputationCountOnWorker =
                    std::max(1.0, std::floor(targets.Executing.Count * ManagerSpec_->RebalanceCountExceedAllowed));
                return Emulation_.GetInfo(workers.at(underWorker), computationId).Executing.Count < maxComputationCountOnWorker;
            };

            // Walk the over-loaded worker's partitions downwards from the largest one that still fits
            // under the gap (so the move strictly narrows the spread instead of shifting it to the
            // other worker) and take the first candidate the destination accepts: a computation capped
            // on the destination must not block partitions of other computations from moving.
            const auto& spectre = workers.at(overWorker).InfoOverall.Executing.Spectres[resource];
            std::optional<TPartitionId> foundPartitionId;
            for (auto it = spectre.lower_bound(std::pair(gap, TPartitionId())); it != spectre.begin();) {
                --it;
                const auto& [candidateUsage, candidateId] = *it;
                if (candidateUsage <= 0.) {
                    break; // Zero-usage partitions cannot narrow the spread.
                }
                if (receiverAccepts(partitionInfos.at(candidateId).ComputationId)) {
                    foundPartitionId = candidateId;
                    break;
                }
            }
            if (!foundPartitionId) {
                break; // No movable partition small enough to help.
            }

            const auto& partitionId = *foundPartitionId;
            const auto& info = partitionInfos.at(partitionId);

            Emulation_.DelPartition(partitionId, info, overWorker);
            Emulation_.AddPartition(partitionId, info, underWorker);
            auto& transaction = result.StartTransaction();
            transaction.Emplace(ERebalanceActionType::Del, partitionId, overWorker, info);
            transaction.Emplace(ERebalanceActionType::Add, partitionId, underWorker, info);
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
        YT_TLOG_EVENT(NController::BalancerLogger, NLogging::ELogLevel::Info, "All computations are balanced");
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
    YT_TLOG_EVENT(NController::BalancerLogger, NLogging::ELogLevel::Debug, "ProceedWithWorker started")
        .With("Worker", workerAddress);
    auto actions = RelieveWorker(computationId, workerAddress);

    if (double score = AssessScore(actions, computationId); score < PersistentManager_->ActionBufferScore) {
        PersistentManager_->ActionBufferScore = score;
        PersistentManager_->ActionsBuffer = std::move(actions);
    }

    return true;
}

double TBalancer::GetRelativeDeviation() const
{
    return Emulation_.GetRelativeDeviation();
}

const TDistributionStats& TBalancer::GetWorkerDistributionOverall() const&
{
    return Emulation_.GetWorkerDistributionOverall();
}

TDistributionStats&& TBalancer::GetWorkerDistributionOverall() &&
{
    return std::move(Emulation_).GetWorkerDistributionOverall();
}

const THashMap<TComputationId, TDistributionStats>& TBalancer::GetWorkerDistributionByComputations() const&
{
    return Emulation_.GetWorkerDistributionByComputations();
}

THashMap<TComputationId, TDistributionStats>&& TBalancer::GetWorkerDistributionByComputations() &&
{
    return std::move(Emulation_).GetWorkerDistributionByComputations();
}

const TResourceVector& TBalancer::BalanceWeights() const
{
    return Emulation_.BalanceWeights();
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

    const auto& weights = Emulation_.BalanceWeights();
    for (auto resource : TEnumTraits<EBalanceResource>::GetDomainValues()) {
        if (weights[resource] <= 0.) {
            continue;
        }
        const auto& stat = Emulation_.GetWorkerDistributionOverall()[resource];
        if (stat.Set.size() < 2) {
            continue;
        }

        double minSpread = DefaultEvenLoadSpread(resource);
        double minRatio = DefaultEvenLoadRatio;
        if (auto it = ManagerSpec_->RebalanceEvenLoadThresholds.find(resource);
            it != ManagerSpec_->RebalanceEvenLoadThresholds.end())
        {
            minSpread = it->second->Spread.value_or(minSpread);
            minRatio = it->second->Ratio.value_or(minRatio);
        }

        const double min = stat.Set.begin()->first;
        const double max = stat.Set.rbegin()->first;

        const double spread = max - min;
        const double relativeDeviation = stat.RelativeDeviation();

        // The load of a resource is uneven only when it is uneven by ALL measures. With a zero
        // minimum (e.g. a worker whose metrics are absent) the ratio is undefined, so the ratio
        // criterion is considered vacuously satisfied and the absolute spread threshold governs.
        const bool ratioUneven = min > 0.0 ? max / min >= minRatio : true;
        if (spread >= minSpread && ratioUneven && relativeDeviation > 2.0 * ManagerSpec_->RebalanceTargetDeviation) {
            // The open gate is the answer to "why is the balancer rebalancing right now", so spell
            // out the measures against their thresholds and the extreme workers; the even case is
            // reported by the callers ("Skipping overcount kick" / "Skipping deferred merge").
            YT_TLOG_EVENT(NController::BalancerLogger, NLogging::ELogLevel::Info, "Worker load uneven")
                .With("Resource", resource)
                .With("Spread", spread)
                .With("MinSpread", minSpread)
                .With("Ratio", min > 0.0 ? max / min : std::numeric_limits<double>::infinity())
                .With("MinRatio", minRatio)
                .With("RelativeDeviation", relativeDeviation)
                .With("DeviationThreshold", 2.0 * ManagerSpec_->RebalanceTargetDeviation)
                .With("MinWorker", stat.Set.begin()->second)
                .With("MaxWorker", stat.Set.rbegin()->second);
            return true;
        }
    }
    return false;
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
        out << "Worker: " << workerAddress << " count of tasks: " << worker.InfoOverall.All.Count << ", CPU load: " << worker.InfoOverall.All.Usage[EBalanceResource::Cpu] << ", memory load: " << worker.InfoOverall.All.Usage[EBalanceResource::Memory] << ", Coefficient: " << Emulation_.Workers().at(workerAddress).WorkerCoef << "\n";
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
    YT_TLOG_EVENT(NController::BalancerLogger, NLogging::ELogLevel::Info, "Entered fast balancing");
    TRebalanceActions result;

    // The record that tells bad balancing from bad inputs. A low RelativeDeviation here combined
    // with uneven real consumption means the inputs do not reflect reality; a persistently high one
    // means the balancer fails to even out what it sees. Many estimated or unmetered partitions
    // mean the memory inputs are guesswork to begin with.
    const auto& balanceWeights = Emulation_.BalanceWeights();
    for (auto resource : TEnumTraits<EBalanceResource>::GetDomainValues()) {
        if (balanceWeights[resource] <= 0.) {
            continue;
        }
        const auto& stat = Emulation_.GetWorkerDistributionOverall()[resource];
        if (stat.Count == 0) {
            continue;
        }
        YT_TLOG_EVENT(NController::BalancerLogger, NLogging::ELogLevel::Info, "Balancer resource view")
            .With("Resource", resource)
            .With("Workers", stat.Count)
            .With("Mean", stat.Sum / stat.Count)
            .With("Min", stat.Set.begin()->first)
            .With("Max", stat.Set.rbegin()->first)
            .With("RelativeDeviation", stat.RelativeDeviation())
            .With("MinWorker", stat.Set.begin()->second)
            .With("MaxWorker", stat.Set.rbegin()->second);
    }
    if (balanceWeights[EBalanceResource::Memory] > 0.) {
        const auto& quality = Data_.GetMemoryMetricQuality();
        YT_TLOG_EVENT(NController::BalancerLogger, NLogging::ELogLevel::Info, "Memory metric quality")
            .With("MeasuredPartitions", quality.MeasuredPartitions)
            .With("EstimatedPartitions", quality.EstimatedPartitions)
            .With("UnmeteredPartitions", quality.UnmeteredPartitions);
    }

    // The even-load gate (WorkerLoadUneven) also gates the fast (count-based) rebalancing: when
    // worker CPU loads are even enough, skip the overcount kick so an already-even pipeline is not
    // churned. A stray (jobless) partition re-enables the kick (it must be placed anyway).
    // DistributeStrayPartitions always runs. Same predicate as the slow gate in ShouldApplySlowActionsNow.
    if (HasStrayPartitions() || WorkerLoadUneven()) {
        result.Merge(KickPartitionsFromOvercountedWorkers());
    } else {
        YT_TLOG_EVENT(NController::BalancerLogger, NLogging::ELogLevel::Info, "Skipping overcount kick: worker load is even and no stray partitions")
            .With("RelativeDeviation", Emulation_.GetRelativeDeviation());
    }
    result.Merge(DistributeStrayPartitions());
    AlreadyApplied_.Merge(result);
    YT_TLOG_EVENT(NController::BalancerLogger, NLogging::ELogLevel::Info, "Passed fast balancing")
        .With("Transactions", result.Transactions.size());

    return result;
}

TRebalanceActions TBalancer::DoSlowBalancing(const TInstant& until)
{
    YT_TLOG_EVENT(NController::BalancerLogger, NLogging::ELogLevel::Info, "Entered slow balancing");

    PersistentManager_->ActionsBuffer = Verifier_.VerifyWithPreapplied(AlreadyApplied_, PersistentManager_->ActionsBuffer);
    PersistentManager_->ActionBufferScore = std::numeric_limits<double>::infinity();
    if (PersistentManager_->GetLoopContext().Computation.has_value()) {
        PersistentManager_->ActionBufferScore = AssessScore(PersistentManager_->ActionsBuffer, PersistentManager_->GetLoopContext().Computation.value().Id);
    }

    TRebalanceActions result;

    auto finishedComputation = [&] (const TComputationId&) {
        YT_TLOG_EVENT(NController::BalancerLogger, NLogging::ELogLevel::Debug, GenerateInterimReport());

        Emulation_.ApplyAll(PersistentManager_->ActionsBuffer, Data_);
        AlreadyApplied_.Merge(PersistentManager_->ActionsBuffer);
        result.Merge(PersistentManager_->ActionsBuffer);
        PersistentManager_->ActionsBuffer = TRebalanceActions();
        PersistentManager_->ActionBufferScore = std::numeric_limits<double>::infinity();
    };

    if (Emulation_.ComputationInfos().empty() || Emulation_.Workers().empty()) {
        NConcurrency::TDelayedExecutor::WaitForDuration(EmptyIterationBackoff);
        return result;
    }

    // Worker-centric relief of weighted resources the per-computation loop below cannot address
    // (memory of single-partition computations). Applied to the emulation and tracked in
    // AlreadyApplied_ so the per-computation search sees the relieved state and does not re-apply it.
    if (auto reliefActions = RelieveResourceOverloadedWorkers(until); !reliefActions.Transactions.empty()) {
        AlreadyApplied_.Merge(reliefActions);
        result.Merge(reliefActions);
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
                YT_TLOG_EVENT(NController::BalancerLogger, NLogging::ELogLevel::Info, "Cannot advance context computation at the moment");
                NConcurrency::TDelayedExecutor::WaitForDuration(until - TInstant::Now());
                return result;
            }
            computationId = advanceResult.value();
            YT_TLOG_EVENT(NController::BalancerLogger, NLogging::ELogLevel::Info, "Selected computation for slow balancing")
                .With("Computation", computationId);

            std::vector<std::pair<std::string, double>> overallData;
            for (const auto& [workerAddress, worker] : Emulation_.Workers()) {
                if (Emulation_.GetInfo(worker, computationId).Executing.Count == 0) {
                    continue;
                }
                overallData.push_back({workerAddress, Emulation_.GetInfo(worker, computationId).Executing.Usage[EBalanceResource::Cpu]});
            }

            std::ranges::sort(overallData, {}, &std::pair<std::string, double>::second);
            std::ranges::copy(std::views::transform(overallData, [] (const auto& a) {
                return a.first;
            }),
                std::back_inserter(PersistentManager_->GetLoopContext().WorkersRemaining));
        }
    }

    YT_TLOG_EVENT(NController::BalancerLogger, NLogging::ELogLevel::Info, "Slow balancing iteration terminated")
        .With("NewTransactions", result.Transactions.size());

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
        YT_TLOG_EVENT(NController::BalancerLogger, NLogging::ELogLevel::Info, "Skipping deferred merge: worker load is even")
            .With("RelativeDeviation", balancer.GetRelativeDeviation());
        return false;
    }

    double currentScore = balancer.GetTotalScore();
    // Per-resource relative deviations before the deferred actions, so a low-weight resource's
    // improvement (e.g. memory relief of single-partition computations) can be judged on its own
    // merit instead of being diluted by its weight in the scalar total score below.
    TResourceVector currentDeviation;
    for (auto resource : TEnumTraits<EBalanceResource>::GetDomainValues()) {
        currentDeviation[resource] = balancer.GetWorkerDistributionOverall()[resource].RelativeDeviation();
    }

    balancer.ApplyAll(alreadyAppliedDeferred);
    double deferredScore = balancer.GetTotalScore();
    YT_TLOG_EVENT(NController::BalancerLogger, NLogging::ELogLevel::Info, "Calculated scores")
        .With("Current", currentScore)
        .With("Deferred", deferredScore);
    bool apply = deferredScore < currentScore - balancerSpec->RebalanceTargetDeviation;
    if (!apply) {
        // Undiluted per-resource acceptance: apply when any weighted resource's per-worker relative
        // deviation drops by at least the target deviation. Without this, memory relief (weighted
        // low, so it barely moves the scalar total) would never clear the threshold and never be
        // applied.
        const auto& weights = balancer.BalanceWeights();
        for (auto resource : TEnumTraits<EBalanceResource>::GetDomainValues()) {
            if (weights[resource] <= 0.) {
                continue;
            }
            double deferredDeviation = balancer.GetWorkerDistributionOverall()[resource].RelativeDeviation();
            if (currentDeviation[resource] - deferredDeviation >= balancerSpec->RebalanceTargetDeviation) {
                apply = true;
                break;
            }
        }
    }
    // Log the verdict explicitly: the rejected case used to leave no trace, making "why did (not)
    // the balancer act" undiagnosable from logs.
    if (apply) {
        YT_TLOG_EVENT(NController::BalancerLogger, NLogging::ELogLevel::Info, "Applying deferred actions")
            .With("Actions", alreadyAppliedDeferred.Transactions.size())
            .With("ScoreImprovement", currentScore - deferredScore)
            .With("Threshold", balancerSpec->RebalanceTargetDeviation);
    } else {
        YT_TLOG_EVENT(NController::BalancerLogger, NLogging::ELogLevel::Info, "Keeping deferred actions: score improvement below threshold")
            .With("Actions", alreadyAppliedDeferred.Transactions.size())
            .With("ScoreImprovement", currentScore - deferredScore)
            .With("Threshold", balancerSpec->RebalanceTargetDeviation);
    }
    return apply;
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

    // These feed per-worker CPU gauges, so project the CPU component of the stats.
    THashMap<TComputationId, TDistributionStat> cpuByComputations;
    for (const auto& [computationId, stats] : balancer.GetWorkerDistributionByComputations()) {
        cpuByComputations[computationId] = stats[EBalanceResource::Cpu];
    }
    return {std::move(cpuByComputations), balancer.GetWorkerDistributionOverall()[EBalanceResource::Cpu]};
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
        YT_TLOG_EVENT(NController::BalancerLogger, NLogging::ELogLevel::Info, "FlowView pushed")
            .With("SequenceId", maxAppliedSequenceId.Underlying())
            .With("Dropped", originalCount - AppliedActions_.Transactions.size());
        TRebalanceActionsVerifier verifier(flowView);
        AppliedActions_ = verifier.Verify(AppliedActions_);
        DeferredAppliedActions_ = verifier.VerifyWithPreapplied(AppliedActions_, DeferredAppliedActions_);

        YT_TLOG_INFO("Updated StartData")
            .With("Epoch", FlowViewEpoch_);
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

                    YT_TLOG_EVENT(NController::BalancerLogger, NLogging::ELogLevel::Debug, "Going to balancing with already applied actions")
                        .With("FastActions", alreadyApplied.Transactions.size())
                        .With("DeferredActions", alreadyAppliedDeferred.Transactions.size());

                    auto [fastActions, slowActions] = RebalanceJobs(startData.FlowView, startData.Controllers, startData.BalancerSpec, strongThis->WorkerGroup_, TInstant::Now() + startData.BalancerSpec->RebalanceSyncPeriod, PersistentManager_, alreadyApplied, alreadyAppliedDeferred);
                    YT_TLOG_EVENT(NController::BalancerLogger, NLogging::ELogLevel::Debug, "Returned from balancing")
                        .With("FastActions", fastActions.Transactions.size())
                        .With("DeferredActions", slowActions.Transactions.size());

                    auto appliedActionsLock = Guard(AppliedActionsLock_);
                    AppliedActions_.Merge(fastActions);
                    YT_TLOG_EVENT(NController::BalancerLogger, NLogging::ELogLevel::Debug, "Fast actions merge completed")
                        .With("SequenceId", AppliedActions_.GetSequenceId());
                    DeferredAppliedActions_ = slowActions;
                    TRebalanceActionsVerifier verifier(startData.FlowView);
                    DeferredAppliedActions_ = verifier.VerifyWithPreapplied(AppliedActions_, DeferredAppliedActions_);
                    if (ShouldApplySlowActionsNow(startData.FlowView, startData.Controllers, startData.BalancerSpec, strongThis->WorkerGroup_, PersistentManager_, AppliedActions_, DeferredAppliedActions_)) {
                        AppliedActions_.Merge(DeferredAppliedActions_);
                        DeferredAppliedActions_ = TRebalanceActions(DeferredSequenceIdGenerator_);
                        YT_TLOG_EVENT(NController::BalancerLogger, NLogging::ELogLevel::Debug, "Merged deferred actions")
                            .With("SequenceId", AppliedActions_.GetSequenceId());
                    }
                    YT_TLOG_EVENT(NController::BalancerLogger, NLogging::ELogLevel::Debug, "Total merged size after balancing updated")
                        .With("Size", AppliedActions_.Transactions.size());
                    YT_TLOG_EVENT(NController::BalancerLogger, NLogging::ELogLevel::Debug, "Total deferred size after balancing updated")
                        .With("Size", DeferredAppliedActions_.Transactions.size());

                    appliedActionsLock.Release();
                } catch (const std::exception& ex) {
                    YT_TLOG_EVENT(NController::BalancerLogger, NLogging::ELogLevel::Error, "Async balancer iteration failed; the fiber stays alive and retries")
                        .With("Error", TError(ex));
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
        YT_TLOG_INFO("Pulling actions from synchronizer");
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
                    YT_TLOG_INFO("Found stray partitions, applying rebalance actions immediately");
                    shouldDoPull = true;
                } else if (!timeSinceSynced.has_value()) {
                    YT_TLOG_INFO("Pipeline is not in sync yet, delaying rebalance");
                } else if (targetState == EPipelineState::Stopped || targetState == EPipelineState::Paused) {
                    YT_TLOG_INFO("Pipeline is stopping, will not balance if possible");
                } else if (timeSinceSynced < balancerSpec->RebalanceDelayAfterPipelineSync) {
                    YT_TLOG_INFO("Pipeline is in sync not long enough, delaying rebalance");
                } else {
                    YT_TLOG_INFO("Pipeline is deemed stable, applying rebalance buffer");
                    shouldDoPull = true;
                }

                if (shouldDoPull) {
                    rebalanceResult = PullActionsVerify(flowView);
                    YT_TLOG_INFO("Pulled rebalance actions")
                        .With("Count", rebalanceResult.Actions.size())
                        .With("SequenceId", rebalanceResult.SequenceId);
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
    YT_TLOG_INFO("Persistent balance manager created");
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

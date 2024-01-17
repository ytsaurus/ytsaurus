#include "private.h"

#include "task.h"

#include <yt/yt/core/actions/invoker_pool.h>

namespace NYT::NControllerAgent::NControllers {

////////////////////////////////////////////////////////////////////////////////

struct IAlertManagerHost
    : public virtual TRefCounted
    , public IPersistent
{
    virtual const std::vector<TTaskPtr>& GetTasks() const = 0;
    virtual const TControllerAgentConfigPtr& GetConfig() const = 0;
    virtual EOperationType GetOperationType() const = 0;
    virtual TInstant GetStartTime() const = 0;
    virtual NLogging::TLogger GetLogger() const = 0;

    virtual void SetOperationAlert(EOperationAlertType type, const TError& alert) = 0;

    virtual IInvokerPtr GetCancelableInvoker(EOperationControllerQueue queue = EOperationControllerQueue::Default) const = 0;

    virtual std::vector<EJobType> GetSupportedJobTypesForJobsDurationAnalyzer() const = 0;
    virtual TStringBuf GetDataWeightParameterNameForJob(EJobType jobType) const = 0;

    virtual bool IsCompleted() const = 0;
    virtual int GetTotalJobCount() const = 0;
    virtual const TProgressCounterPtr& GetTotalJobCounter() const = 0;
    virtual i64 GetUnavailableInputChunkCount() const = 0;

    virtual const TScheduleAllocationStatisticsPtr& GetScheduleAllocationStatistics() const = 0;
    virtual const TAggregatedJobStatistics& GetAggregatedFinishedJobStatistics() const = 0;
    virtual const TAggregatedJobStatistics& GetAggregatedRunningJobStatistics() const = 0;

    virtual std::unique_ptr<IHistogram> ComputeFinalPartitionSizeHistogram() const = 0;

    virtual IDiagnosableInvokerPool::TInvokerStatistics GetInvokerStatistics(
        EOperationControllerQueue queue = EOperationControllerQueue::Default) const = 0;

    virtual void Persist(const TPersistenceContext& context) = 0;
};

DEFINE_REFCOUNTED_TYPE(IAlertManagerHost)

////////////////////////////////////////////////////////////////////////////////

struct IAlertManager
    : public virtual TRefCounted
    , public IPersistent
{
    virtual void StartPeriodicActivity() = 0;

    virtual void Analyze() = 0;
};

DEFINE_REFCOUNTED_TYPE(IAlertManager)

////////////////////////////////////////////////////////////////////////////////

IAlertManagerPtr CreateAlertManager(IAlertManagerHost* host);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers

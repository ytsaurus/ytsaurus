#include "parse_executor_resource.h"

#include "messages.h"

#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/net/local_address.h>

namespace NYT::NFlow::NFileParsingExample {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

namespace {

//! Stands for a real parse of |task| by |worker|.
TParseResultPtr ParseStub(
    TDuration timePerSizeUnit,
    const std::string& worker,
    const TParseTaskPtr& task)
{
    THROW_ERROR_EXCEPTION_IF(task->Size < 0,
        "File %v has negative size %v",
        task->Url,
        task->Size);

    // The parse time saturates rather than overflows.
    auto size = static_cast<ui64>(task->Size);
    auto fitsDuration = timePerSizeUnit == TDuration::Zero() ||
        size <= TDuration::Max().GetValue() / timePerSizeUnit.GetValue();
    auto parseTime = fitsDuration ? timePerSizeUnit * size : TDuration::Max();
    TDelayedExecutor::WaitForDuration(parseTime);

    auto result = New<TParseResult>();
    result->RowCount = task->Size;
    result->Worker = worker;
    return result;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void TParseExecutorResourceParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("parse_time_per_size_unit", &TThis::ParseTimePerSizeUnit)
        .Default(TDuration::MilliSeconds(100));
}

////////////////////////////////////////////////////////////////////////////////

TParseExecutorResource::~TParseExecutorResource()
{
    if (ThreadPool_) {
        ThreadPool_->Shutdown();
    }
}

TFuture<void> TParseExecutorResource::Load(const THashMap<TResourceId, IResourcePtr>& dependencies)
{
    auto parameters = GetParameters();
    ThreadPool_ = CreateThreadPool(parameters->SlotCount, "Parse");
    // Several workers may share a host; the instance id tells them apart.
    auto worker = Format("%v/%v", NNet::GetLocalHostName(), GetContext()->ResourceInstanceId);
    Executor_ = New<TParseExecutor>(
        parameters,
        ThreadPool_->GetInvoker(),
        BIND(&ParseStub, parameters->ParseTimePerSizeUnit, std::move(worker)));

    return TResourceBase::Load(dependencies);
}

TFuture<TParseResultPtr> TParseExecutorResource::Parse(TParseTaskPtr task)
{
    return Executor_->Submit(std::move(task));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NFileParsingExample

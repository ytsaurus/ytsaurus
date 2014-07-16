#include "stdafx.h"
#include "disk_health_checker.h"
#include "private.h"
#include "config.h"

#include <core/actions/future.h>

#include <core/misc/fs.h>

#include <util/random/random.h>

namespace NYT {
namespace NDataNode {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = DataNodeLogger;
static auto& Profiler = DataNodeProfiler;

static const char* TestFileName = "health_check~";

////////////////////////////////////////////////////////////////////////////////

TDiskHealthChecker::TDiskHealthChecker(
    TDiskHealthCheckerConfigPtr config,
    const Stroka& path,
    IInvokerPtr invoker)
    : Config(config)
    , Path(path)
    , CheckInvoker(invoker)
    , PeriodicExecutor(New<TPeriodicExecutor>(
        invoker,
        BIND(&TDiskHealthChecker::OnCheck, Unretained(this)),
        Config->CheckPeriod,
        EPeriodicExecutorMode::Manual))
    , FailedLock(0)
    , CheckCallback(BIND(&TDiskHealthChecker::DoRunCheck, Unretained(this)))
{ }

void TDiskHealthChecker::Start()
{
    PeriodicExecutor->Start();
}

TAsyncError TDiskHealthChecker::RunCheck()
{
    auto asyncError = NewPromise<TError>();

    CheckCallback.AsyncVia(CheckInvoker).Run().Subscribe(
        Config->Timeout,
        BIND([=] (TError error) mutable {
            asyncError.Set(error);
        }),
        BIND(&TDiskHealthChecker::OnCheckTimeout, MakeWeak(this), asyncError));
    return asyncError;
}

void TDiskHealthChecker::OnCheck()
{
    RunCheck().Subscribe(BIND(&TDiskHealthChecker::OnCheckCompleted, MakeWeak(this)));;
}

void TDiskHealthChecker::OnCheckCompleted(TError error)
{
    if (error.IsOK()) {
        PeriodicExecutor->ScheduleNext();
    } else if (AtomicIncrement(FailedLock) == 1) {
        Failed_.Fire();
    }
}

void TDiskHealthChecker::OnCheckTimeout(TAsyncErrorPromise result)
{
    auto error = TError("Disk health check timed out: %s", ~Path);
    LOG_ERROR(error);
    result.Set(error);
}

TError TDiskHealthChecker::DoRunCheck()
{
    LOG_DEBUG("Disk health check started: %s", ~Path);

    std::vector<ui8> writeData(Config->TestSize);
    std::vector<ui8> readData(Config->TestSize);

    for (int i = 0; i < Config->TestSize; ++i) {
        writeData[i] = RandomNumber<ui8>();
    }

    Stroka fileName = NFS::CombinePaths(Path, TestFileName);

    try {

        {
            TFile file(fileName, CreateAlways|WrOnly|Seq|Direct);
            file.Write(writeData.data(), Config->TestSize);
        }

        {
            TFile file(fileName, OpenExisting|RdOnly|Seq|Direct);
            if (file.GetLength() != Config->TestSize) {
                THROW_ERROR_EXCEPTION("Wrong test file size: %" PRId64 " instead of %" PRId64,
                    file.GetLength(),
                    Config->TestSize);
            }
            file.Read(readData.data(), Config->TestSize);
        }

        NFS::Remove(fileName);

        if (memcmp(readData.data(), writeData.data(), Config->TestSize) != 0) {
            THROW_ERROR_EXCEPTION("Test file is corrupt");
        }

        LOG_DEBUG("Disk health check finished: %s", ~Path);

        return TError();
    } catch (const std::exception& ex) {
        auto wrappedError = TError("Disk health check failed at %s", ~Path) << ex;
        LOG_ERROR(wrappedError);
        return wrappedError;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT

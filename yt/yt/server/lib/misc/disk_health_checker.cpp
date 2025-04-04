#include "disk_health_checker.h"
#include "private.h"
#include "config.h"

#include <yt/yt/server/node/data_node/config.h>

#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/misc/fs.h>

#include <util/random/random.h>

namespace NYT::NServer {

using namespace NConcurrency;
using namespace NProfiling;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

TDiskHealthChecker::TDiskHealthChecker(
    TDiskHealthCheckerConfigPtr config,
    const TString& path,
    IInvokerPtr invoker,
    NClusterNode::TClusterNodeDynamicConfigManagerPtr dynamicConfigManager,
    TLogger logger,
    const TProfiler& profiler)
    : DynamicConfigManager_(std::move(dynamicConfigManager))
    , Config_(std::move(config))
    , Path_(path)
    , CheckInvoker_(std::move(invoker))
    , Logger(logger)
    , TotalTimer_(profiler.Timer("/disk_health_check/total_time"))
    , ReadTimer_(profiler.Timer("/disk_health_check/read_time"))
    , WriteTimer_(profiler.Timer("/disk_health_check/write_time"))
    , PeriodicExecutor_(New<TPeriodicExecutor>(
        CheckInvoker_,
        BIND(&TDiskHealthChecker::OnCheck, MakeWeak(this)),
        Config_->CheckPeriod))
{
    Logger.AddTag("Path: %v", Path_);
}

TDuration TDiskHealthChecker::GetWaitTimeout() const
{
    auto waitTimeout = DynamicConfigManager_->GetConfig()->DataNode->DiskHealthChecker->WaitTimeout;
    return waitTimeout.value_or(Config_->WaitTimeout);
}

TDuration TDiskHealthChecker::GetExecTimeout() const
{
    auto execTimeout = DynamicConfigManager_->GetConfig()->DataNode->DiskHealthChecker->ExecTimeout;
    return execTimeout.value_or(Config_->ExecTimeout);
}

i64 TDiskHealthChecker::GetTestSize() const
{
    auto testSize = DynamicConfigManager_->GetConfig()->DataNode->DiskHealthChecker->TestSize;
    return testSize.value_or(Config_->TestSize);
}

void TDiskHealthChecker::Start()
{
    PeriodicExecutor_->Start();
}

TFuture<void> TDiskHealthChecker::Stop()
{
    return PeriodicExecutor_->Stop();
}

void TDiskHealthChecker::RunCheck()
{
    YT_VERIFY(!PeriodicExecutor_->IsStarted());
    return RunCheckWithDeadline()
        .ThrowOnError();
}

TError TDiskHealthChecker::RunCheckWithDeadline()
{
    return WaitFor(BIND_NO_PROPAGATE(&TDiskHealthChecker::RunCheckWithTimeout, MakeStrong(this))
        .AsyncVia(CheckInvoker_)
        .Run()
        .WithDeadline(TInstant::Now() + GetWaitTimeout() + GetExecTimeout()));
}

void TDiskHealthChecker::RunCheckWithTimeout()
{
    TWallTimer timer;

    DoRunCheck();

    if (timer.GetElapsedTime() > GetExecTimeout()) {
        THROW_ERROR_EXCEPTION(NChunkClient::EErrorCode::DiskHealthCheckFailed, "Disk health check timed out at %v", Path_);
    }
}


void TDiskHealthChecker::OnCheck()
{
    OnCheckCompleted(RunCheckWithDeadline());
}

void TDiskHealthChecker::OnCheckCompleted(const TError& error)
{
    if (error.IsOK()) {
        return;
    }

    YT_UNUSED_FUTURE(PeriodicExecutor_->Stop());

    auto actualError = error.GetCode() == NYT::EErrorCode::Timeout
        ? TError("Disk health check timed out at %v", Path_)
        : error;
    YT_LOG_ERROR(actualError);

    Failed_.Fire(actualError);
}

void TDiskHealthChecker::DoRunCheck()
{
    YT_LOG_DEBUG("Disk health check started");

    if (auto lockFilePath = NFS::CombinePaths(Path_, DisabledLockFileName); NFS::Exists(lockFilePath)) {
        TError lockFileError("Empty lock file found");
        try {
            if (
                auto error = NYTree::ConvertTo<TError>(NYson::TYsonString(TFileInput(lockFilePath).ReadAll()));
                !error.IsOK())
            {
                lockFileError = std::move(error);
            }
        } catch (const std::exception& ex) {
            YT_LOG_INFO(ex, "Failed to extract error from location lock file");
            lockFileError = TError("Failed to extract error from location lock file")
                << ex;
        }

        THROW_ERROR_EXCEPTION(NChunkClient::EErrorCode::LockFileIsFound, "Lock file is found") << std::move(lockFileError);
    }

    auto testSize = GetTestSize();
    std::vector<ui8> writeData(testSize);
    std::vector<ui8> readData(testSize);
    for (int i = 0; i < testSize; ++i) {
        writeData[i] = RandomNumber<ui8>();
    }

    try {
        auto fileName = NFS::CombinePaths(Path_, HealthCheckFileName);

        TEventTimerGuard totalGuard(TotalTimer_);
        {
            TEventTimerGuard totalGuard(WriteTimer_);
            try {
                TFile file(fileName, CreateAlways | WrOnly | Seq | Direct);
                file.Write(writeData.data(), testSize);
            } catch (const TSystemError& ex) {
                if (ex.Status() == ENOSPC) {
                    YT_LOG_WARNING(ex, "Disk health check ignored");
                    return;
                } else {
                    throw;
                }
            }
        }
        {
            TEventTimerGuard totalGuard(ReadTimer_);
            TFile file(fileName, OpenExisting | RdOnly | Seq | Direct);
            if (file.GetLength() != testSize) {
                THROW_ERROR_EXCEPTION("Wrong test file size: %v instead of %v",
                    file.GetLength(),
                    testSize);
            }
            file.Read(readData.data(), testSize);
        }

        NFS::Remove(fileName);

        if (memcmp(readData.data(), writeData.data(), testSize) != 0) {
            THROW_ERROR_EXCEPTION("Test file is corrupt");
        }
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION(NChunkClient::EErrorCode::DiskHealthCheckFailed, "Disk health check failed at %v", Path_)
            << ex;
    }

    YT_LOG_DEBUG("Disk health check finished");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NServer

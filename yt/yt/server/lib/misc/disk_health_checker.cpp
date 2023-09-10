#include "disk_health_checker.h"
#include "private.h"
#include "config.h"

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/misc/fs.h>

#include <util/random/random.h>

namespace NYT {

using namespace NConcurrency;
using namespace NProfiling;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

TDiskHealthChecker::TDiskHealthChecker(
    TDiskHealthCheckerConfigPtr config,
    const TString& path,
    IInvokerPtr invoker,
    TLogger logger,
    const TProfiler& profiler)
    : Config_(config)
    , Path_(path)
    , CheckInvoker_(invoker)
    , Logger(logger)
    , TotalTimer_(profiler.Timer("/disk_health_check/total_time"))
    , ReadTimer_(profiler.Timer("/disk_health_check/read_time"))
    , WriteTimer_(profiler.Timer("/disk_health_check/write_time"))
{
    Logger.AddTag("Path: %v", Path_);
}

void TDiskHealthChecker::Start()
{
    TDelayedExecutor::Submit(
        BIND(&TDiskHealthChecker::OnCheck, MakeWeak(this)),
        Config_->CheckPeriod);
}

TFuture<void> TDiskHealthChecker::RunCheck()
{
    return BIND(&TDiskHealthChecker::DoRunCheck, MakeStrong(this))
        .AsyncVia(CheckInvoker_)
        .Run()
        .WithTimeout(Config_->Timeout);
}

void TDiskHealthChecker::OnCheck()
{
    RunCheck().Subscribe(BIND(&TDiskHealthChecker::OnCheckCompleted, MakeWeak(this)));
}

void TDiskHealthChecker::OnCheckCompleted(const TError& error)
{
    if (error.IsOK()) {
        TDelayedExecutor::Submit(
            BIND(&TDiskHealthChecker::OnCheck, MakeWeak(this)),
            Config_->CheckPeriod);

        return;
    }

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
        TError lockFileError;
        try {
            lockFileError = NYTree::ConvertTo<TError>(NYson::TYsonString(TFileInput(lockFilePath).ReadAll()));
        } catch (const std::exception& ex) {
            YT_LOG_INFO(ex, "Failed to extract error from location lock file");
        }
        auto error = TError("Lock file is found");
        if (!lockFileError.IsOK()) {
            error.MutableInnerErrors()->push_back(std::move(lockFileError));
        }
        THROW_ERROR(error);
    }

    std::vector<ui8> writeData(Config_->TestSize);
    std::vector<ui8> readData(Config_->TestSize);
    for (int i = 0; i < Config_->TestSize; ++i) {
        writeData[i] = RandomNumber<ui8>();
    }

    try {
        auto fileName = NFS::CombinePaths(Path_, HealthCheckFileName);

        TEventTimerGuard totalGuard(TotalTimer_);
        {
            TEventTimerGuard totalGuard(WriteTimer_);
            try {
                TFile file(fileName, CreateAlways | WrOnly | Seq | Direct);
                file.Write(writeData.data(), Config_->TestSize);
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
            if (file.GetLength() != Config_->TestSize) {
                THROW_ERROR_EXCEPTION("Wrong test file size: %v instead of %v",
                    file.GetLength(),
                    Config_->TestSize);
            }
            file.Read(readData.data(), Config_->TestSize);
        }

        NFS::Remove(fileName);

        if (memcmp(readData.data(), writeData.data(), Config_->TestSize) != 0) {
            THROW_ERROR_EXCEPTION("Test file is corrupt");
        }

    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Disk health check failed at %v", Path_)
            << ex;
    }

    YT_LOG_DEBUG("Disk health check finished");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

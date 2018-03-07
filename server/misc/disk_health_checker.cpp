#include "disk_health_checker.h"
#include "private.h"
#include "config.h"

#include <yt/core/actions/future.h>

#include <yt/core/misc/fs.h>

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
    , PeriodicExecutor_(New<TPeriodicExecutor>(
        invoker,
        BIND(&TDiskHealthChecker::OnCheck, Unretained(this)),
        Config_->CheckPeriod,
        EPeriodicExecutorMode::Manual))
    , Logger(logger)
    , Profiler(profiler)
{
    Logger.AddTag("Path: %v", Path_);
}

void TDiskHealthChecker::Start()
{
    PeriodicExecutor_->Start();
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
    RunCheck().Subscribe(BIND(&TDiskHealthChecker::OnCheckCompleted, MakeWeak(this)));;
}

void TDiskHealthChecker::OnCheckCompleted(const TError& error)
{
    if (error.IsOK()) {
        PeriodicExecutor_->ScheduleNext();
        return;
    }

    auto actualError = error.GetCode() == NYT::EErrorCode::Timeout
        ? TError("Disk health check timed out at %v", Path_)
        : error;
    LOG_ERROR(actualError);

    Failed_.Fire(actualError);
}

void TDiskHealthChecker::DoRunCheck()
{
    LOG_DEBUG("Disk health check started");

    if (NFS::Exists(NFS::CombinePaths(Path_, DisabledLockFileName))) {
        THROW_ERROR_EXCEPTION("Lock file is found");
    }

    std::vector<ui8> writeData(Config_->TestSize);
    std::vector<ui8> readData(Config_->TestSize);

    for (int i = 0; i < Config_->TestSize; ++i) {
        writeData[i] = RandomNumber<ui8>();
    }

    auto fileName = NFS::CombinePaths(Path_, HealthCheckFileName);

    try {
        PROFILE_TIMING("/disk_health_check/total") {
            PROFILE_TIMING("/disk_health_check/write") {
                TFile file(fileName, CreateAlways | WrOnly | Seq | Direct);
                file.Write(writeData.data(), Config_->TestSize);
            }
            PROFILE_TIMING("/disk_health_check/read") { 
                TFile file(fileName, OpenExisting | RdOnly | Seq | Direct);
                if (file.GetLength() != Config_->TestSize) {
                    THROW_ERROR_EXCEPTION("Wrong test file size: %v instead of %v",
                        file.GetLength(),
                        Config_->TestSize);
                }
                file.Read(readData.data(), Config_->TestSize);
            }
        }

        NFS::Remove(fileName);

        if (memcmp(readData.data(), writeData.data(), Config_->TestSize) != 0) {
            THROW_ERROR_EXCEPTION("Test file is corrupt");
        }
            
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Disk health check failed at %v", Path_)
            << ex;
    }

    LOG_DEBUG("Disk health check finished");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

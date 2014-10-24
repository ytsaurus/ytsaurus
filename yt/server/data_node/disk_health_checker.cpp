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

static const Stroka TestFileName("health_check~");
static const Stroka DisabledLockFileName("disabled");

////////////////////////////////////////////////////////////////////////////////

TDiskHealthChecker::TDiskHealthChecker(
    TDiskHealthCheckerConfigPtr config,
    const Stroka& path,
    IInvokerPtr invoker)
    : Config_(config)
    , Path_(path)
    , CheckInvoker_(invoker)
    , PeriodicExecutor_(New<TPeriodicExecutor>(
        invoker,
        BIND(&TDiskHealthChecker::OnCheck, Unretained(this)),
        Config_->CheckPeriod,
        EPeriodicExecutorMode::Manual))
    , Logger(DataNodeLogger)
{
    Logger.AddTag("Path: %v", Path_);
    FailedLock_.clear();
}

void TDiskHealthChecker::Start()
{
    PeriodicExecutor_->Start();
}

TAsyncError TDiskHealthChecker::RunCheck()
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

void TDiskHealthChecker::OnCheckCompleted(TError error)
{
    if (error.IsOK()) {
        PeriodicExecutor_->ScheduleNext();
        return;
    }

    if (!FailedLock_.test_and_set())
        return;

    auto actualError = error.GetCode() == NYT::EErrorCode::Timeout
        ? TError("Disk health check timed out at %v", Path_)
        : error;
    LOG_ERROR(actualError);
    Failed_.Fire(actualError);
}

TError TDiskHealthChecker::DoRunCheck()
{
    LOG_DEBUG("Disk health check started");

    auto lockFileName = NFS::CombinePaths(Path_, DisabledLockFileName);
    if (NFS::Exists(lockFileName)) {
        LOG_INFO("Lock file found");
        return TError("Location is disabled by lock file");
    }

    std::vector<ui8> writeData(Config_->TestSize);
    std::vector<ui8> readData(Config_->TestSize);

    for (int i = 0; i < Config_->TestSize; ++i) {
        writeData[i] = RandomNumber<ui8>();
    }

    auto testFileName = NFS::CombinePaths(Path_, TestFileName);

    try {
        {
            TFile file(testFileName, CreateAlways|WrOnly|Seq|Direct);
            file.Write(writeData.data(), Config_->TestSize);
        }

        {
            TFile file(testFileName, OpenExisting|RdOnly|Seq|Direct);
            if (file.GetLength() != Config_->TestSize) {
                THROW_ERROR_EXCEPTION("Wrong test file size: %v instead of %v",
                    file.GetLength(),
                    Config_->TestSize);
            }
            file.Read(readData.data(), Config_->TestSize);
        }

        NFS::Remove(testFileName);

        if (memcmp(readData.data(), writeData.data(), Config_->TestSize) != 0) {
            THROW_ERROR_EXCEPTION("Test file is corrupt");
        }
    } catch (const std::exception& ex) {
        auto wrappedError = TError("Disk health check failed at %v", Path_)
            << ex;
        LOG_ERROR(wrappedError);
        return wrappedError;
    }

    LOG_DEBUG("Disk health check finished");

    return TError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT

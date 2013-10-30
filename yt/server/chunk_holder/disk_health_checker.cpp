#include "stdafx.h"
#include "disk_health_checker.h"
#include "private.h"
#include "config.h"

#include <ytlib/misc/fs.h>

#include <util/random/random.h>

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = DataNodeLogger;
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
    , PeriodicInvoker(New<TPeriodicInvoker>(
        invoker,
        BIND(&TDiskHealthChecker::OnCheck, Unretained(this)),
        Config->CheckPeriod,
        EPeriodicInvokerMode::Manual))
    , FailedLock(0)
    , CheckCallback(BIND(&TDiskHealthChecker::RunCheck, Unretained(this)))
{ }

void TDiskHealthChecker::Start()
{
    PeriodicInvoker->Start();
}

void TDiskHealthChecker::OnCheck()
{
    auto this_ = MakeStrong(this);
    CheckCallback.AsyncVia(CheckInvoker).Run().Subscribe(
        Config->Timeout,
        BIND(&TDiskHealthChecker::OnCheckCompleted, MakeStrong(this)),
        BIND(&TDiskHealthChecker::OnCheckTimeout, MakeStrong(this)));
}

void TDiskHealthChecker::OnCheckCompleted(TError error)
{
    if (error.IsOK()) {
        PeriodicInvoker->ScheduleNext();
    }
}

void TDiskHealthChecker::OnCheckTimeout()
{
    LOG_ERROR("Disk health check timed out: %s", ~Path);
    RaiseFailed();
}

TError TDiskHealthChecker::RunCheck()
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

        if (!NFS::Remove(fileName)) {
            THROW_ERROR_EXCEPTION("Error removing test file");
        }

        if (memcmp(readData.data(), writeData.data(), Config->TestSize) != 0) {
            THROW_ERROR_EXCEPTION("Test file is corrupt");
        }

        LOG_DEBUG("Disk health check finished: %s", ~Path);

        return TError();
    } catch (const std::exception& ex) {
        LOG_ERROR(ex, "Disk health check failed: %s", ~Path);
        RaiseFailed();

        return TError(ex);
    }
}

void TDiskHealthChecker::RaiseFailed()
{
    if (AtomicIncrement(FailedLock) == 1) {
        Failed_.Fire();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT

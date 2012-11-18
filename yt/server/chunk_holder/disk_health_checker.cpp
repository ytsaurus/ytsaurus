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
static NProfiling::TProfiler& Profiler = DataNodeProfiler;
static const char* TestFileName = "health_check.tmp";

////////////////////////////////////////////////////////////////////////////////

TDiskHealthChecker::TDiskHealthChecker(
    TDiskHealthCheckerConfigPtr config,
    const Stroka& path,
    IInvokerPtr invoker)
    : Config(config)
    , Path(path)
    , PeriodicInvoker(New<TPeriodicInvoker>(
        invoker,
        BIND(&TDiskHealthChecker::OnCheck, Unretained(this)),
        Config->CheckPeriod))
    , FailedLock(0)
{ }

void TDiskHealthChecker::Start()
{
    PeriodicInvoker->Start();
}

void TDiskHealthChecker::OnCheck()
{
    auto this_ = MakeStrong(this);
    RunCheck().Subscribe(
        Config->Timeout,
        BIND(&TDiskHealthChecker::OnCheckSuccess, MakeStrong(this)),
        BIND(&TDiskHealthChecker::OnCheckTimeout, MakeStrong(this)));
}


void TDiskHealthChecker::OnCheckSuccess()
{
    PeriodicInvoker->ScheduleNext();
}

void TDiskHealthChecker::OnCheckTimeout()
{
    LOG_ERROR("Disk health check timed out: %s", ~Path);
    RaiseFailed();
}

TFuture<void> TDiskHealthChecker::RunCheck()
{
    LOG_DEBUG("Running disk health check: %s", ~Path);

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

        if (memcmp(readData.data(), writeData.data(), Config->TestSize) != 0) {
            THROW_ERROR_EXCEPTION("Test file is corrupt");
        }
    } catch (const std::exception& ex) {
        LOG_ERROR(ex, "Disk health check failed: %s", ~Path);
        RaiseFailed();
    }

    return MakeFuture();
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

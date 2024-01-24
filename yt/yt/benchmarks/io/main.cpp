#include "config.h"
#include "configuration.h"
#include "driver.h"
#include "iotest.h"
#include "operation.h"
#include "output.h"
#include "rusage.h"
#include "statistics.h"
#include "uname.h"
#include "worker.h"

#include <yt/yt/core/misc/public.h>
#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/core/yson/string.h>

#include <library/cpp/getopt/last_getopt.h>

#include <util/folder/pathsplit.h>
#include <util/random/fast.h>
#include <util/stream/printf.h>

#include <util/system/fs.h>
#include <util/system/file.h>
#include <util/system/fstat.h>

#include <fcntl.h>
#include <unistd.h>

#ifndef FALLOC_FL_NO_HIDE_STALE
#define FALLOC_FL_NO_HIDE_STALE 0x04
#endif

namespace NYT::NIOTest {

using namespace NYT;
using namespace NYson;
using namespace NYTree;
using namespace NConcurrency;
using namespace NProfiling;
using namespace NLastGetopt;

////////////////////////////////////////////////////////////////////////////////

struct TTestOptions
{
    bool Validate = false;
    bool Recreate = false;
    bool ForceFileSize = false;
};


class TTestEpoch
{
public:
    TTestEpoch(TEpochConfigPtr config, const TTestOptions& options)
        : Config_(std::move(config))
        , Options_(options)
    { }

    TFormattedEpochPtr Run()
    {
        auto configuration = New<TConfiguration>();
        configuration->Validate = Options_.Validate;
        configuration->TimeLimit = Config_->TimeLimit;
        configuration->TransferLimit = Config_->TransferLimit;

        std::vector<TDriverDescription> iterateDriver;
        for (const auto& driverConfig : Config_->IterateDriver) {
            iterateDriver.push_back({driverConfig->Type, driverConfig->Config});
        }

        // TODO(savrus) add Dsync (need to patch util)
        std::vector<IConfigurationGeneratorPtr> generators{
            CreateDriverConfigurationGenerator(configuration, iterateDriver),
            CreateThreadsConfigurationGenerator(configuration, Config_->IterateThreads),
            CreateBlockSizeLogConfigurationGenerator(configuration, Config_->IterateBlockSizeLog),
            CreateZoneConfigurationGenerator(configuration, Config_->IterateZone),
            CreateReadPercentageConfigurationGenerator(configuration, Config_->IterateReadPercentage),
            CreateDirectConfigurationGenerator(configuration, Config_->IterateDirect),
            CreateSyncConfigurationGenerator(configuration, Config_->IterateSync),
            CreateFallocateConfigurationGenerator(configuration, Config_->IterateFallocate),
            CreateOneshotConfigurationGenerator(configuration, Config_->IterateOneshot),
            CreateLoopConfigurationGenerator(configuration, Config_->IterateLoop),
            CreatePatternConfigurationGenerator(configuration, Config_->IteratePattern),
            CreateThrottlerConfigurationGenerator(configuration, Config_->IterateThrottler),
            CreateShotCountConfigurationGenerator(configuration, Config_->IterateShotCount)
        };

        FormattedEpoch_ = New<TFormattedEpoch>();
        FormattedEpoch_->EpochName = Config_->EpochName;
        FormattedEpoch_->Config = Config_;

        Execute(configuration);

        for (int index = std::ssize(generators) - 1; index >= 0;) {
            if (!generators[index]->IsLast()) {
                generators[index]->Next();

                while (index + 1 < std::ssize(generators)) {
                    ++index;
                    generators[index]->Reset();
                }

                Execute(configuration);
            } else {
                --index;
            }
        }

        return FormattedEpoch_;
    }

private:
    const TEpochConfigPtr Config_;
    const TTestOptions Options_;
    TFormattedEpochPtr FormattedEpoch_;

    void Execute(TConfigurationPtr configuration)
    {
        if (!configuration->Direct) {
            DropFilesystemCaches();
        }

        auto formattedGeneration = New<TFormattedGeneration>();
        formattedGeneration->Configuration = TFormattedConfiguration::From(configuration);

        OpenFiles(configuration);
        Warmup(configuration);

        TStatistics allIterationStatistics(Now());

        for (int i = 0; i < configuration->Loop; ++i) {
            configuration->Start = Now();

            TStatistics statistics(configuration->Start);
            auto formattedIteration = New<TFormattedIteration>();
            formattedIteration->Start = configuration->Start;

            auto workers = CreateWorkers(configuration);

            auto watcher = CreateRusageWatcher(configuration->Start);
            watcher->Run();

            for (auto& worker : workers) {
                worker->Run();
            }

            for (auto& worker : workers) {
                worker->Join();
            }

            watcher->Stop();
            watcher->Join();

            auto watcherStatistics = watcher->Statistics();

            formattedIteration->ProcessRusage = TFormattedRusageTimeSeries::From(watcherStatistics.RusageTimeSeries);
            formattedIteration->Duration = CpuInstantToInstant(GetCpuInstant()) - configuration->Start;

            for (auto& worker : workers) {
                auto workerStatistics = worker->Statistics();

                statistics += workerStatistics;

                auto formattedStatistics = TFormattedStatistics::From(workerStatistics);
                formattedIteration->Threads.push_back(formattedStatistics);
            }

            formattedIteration->ThreadsAggregated = TFormattedStatistics::From(statistics);
            formattedGeneration->Iterations.push_back(formattedIteration);

            allIterationStatistics += statistics;
        }

        formattedGeneration->IterationCount = configuration->Loop;
        formattedGeneration->AllIterationStatistics = TFormattedStatistics::From(allIterationStatistics);
        FormattedEpoch_->Generations.push_back(formattedGeneration);

        CloseFiles(configuration);
    }

    std::vector<IWorkerPtr> CreateWorkers(TConfigurationPtr configuration)
    {
        std::vector<IWorkerPtr> workers;
        for (int i = 0; i < configuration->Threads; ++i) {
            workers.push_back(CreateWorker(configuration, i));
        }

        return workers;
    }

    void OpenFiles(TConfigurationPtr configuration)
    {
        std::vector<TFile> files;

        for (auto filename : Config_->Files) {
            CreateIfNotExists(filename, configuration);
            auto file = Open(filename, configuration);
            files.push_back(file);
        }

        configuration->Files = std::move(files);
    }

    void CloseFiles(TConfigurationPtr configuration)
    {
        for (auto& file : configuration->Files) {
            file.File.Close();
        }

        configuration->Files.clear();
    }

    TFile Open(TString filename, TConfigurationPtr configuration)
    {
        auto file = ::TFile(filename, GetOpenMode(configuration));
        PrepareContent(file, configuration);
        auto size = Options_.ForceFileSize
            ? Config_->FileSize
            : GetFileLength(file.GetHandle());
        posix_fadvise(file.GetHandle(), 0, 0, POSIX_FADV_DONTNEED);
        file.EvictCache(0, size);
        fdatasync(file.GetHandle());
        return {file, size};
    }

    void CreateIfNotExists(TString filename, TConfigurationPtr configuration)
    {
        if (Options_.Recreate) {
            if (NFs::Exists(filename) && !NFs::Remove(filename)) {
                THROW_ERROR_EXCEPTION("Error removing file %Qv: %Qv",
                    filename,
                    LastSystemErrorText());
            }
        }

        try {
            auto file = ::TFile(filename, EOpenModeFlag::RdWr | CreateNew);
            PrepareContent(file, configuration);
        } catch (const std::exception& ex) {
        }
    }

    EOpenModeFlag GetOpenMode(TConfigurationPtr configuration)
    {
        auto mode = ToUnderlying(EOpenModeFlag::RdWr);
        if (configuration->Direct) {
            mode |= ToUnderlying(EOpenModeFlag::DirectAligned);
        }
        if (configuration->Sync == ESyncMode::Sync) {
            mode |= ToUnderlying(EOpenModeFlag::Sync);
        }
#if 0
        // FIXME(savrus) enable when util is ready
        if (configuration->Sync == ESyncMode::Dsync) {
            mode |= ToUnderlying(EOpenModeFlag::Dsync);
        }
#endif
        return static_cast<EOpenModeFlag>(mode);
    }

    void PrepareContent(::TFile file, TConfigurationPtr configuration)
    {
        i64 size = GetFileLength(file.GetHandle());
        if (size >= Config_->FileSize) {
            return;
        }

        switch (configuration->Fallocate) {
            case EFallocateMode::None:
                break;
            case EFallocateMode::Fallocate:
                Fallocate(file);
                break;
            case EFallocateMode::Zero:
                Fill(file, true);
                break;
            case EFallocateMode::Fill:
                Fill(file, false);
                break;
        }
    }

    void Fallocate(::TFile file)
    {
        int result;

        do {
            //result = fallocate(file.GetHandle(), 0, 0, Config_->FileSize);
            result = fallocate(file.GetHandle(), FALLOC_FL_NO_HIDE_STALE, 0, Config_->FileSize);
        } while (result == -1 && errno == EINTR);

        if (result == -1) {
            THROW_ERROR_EXCEPTION("Unable to fallocate file %Qv: %Qv",
                result,
                LastSystemErrorText());
        }

        return;
    }


    void Fill(::TFile file, bool zero)
    {
        auto fileSize = Config_->FileSize;
        int pageSize = std::min((i64) 128 * 4096, fileSize);
        int count = pageSize / sizeof(i64);
        std::vector<i64> page(count);

        i64 size = GetFileLength(file.GetHandle());
        size &= ~(4095L);
        file.Seek(size, SeekDir::sSet);
        size /= sizeof(i64);

        while (size < static_cast<ssize_t>(fileSize / sizeof(i64))) {
            if (zero) {
                size += count;
            } else {
                for (int j = 0; j < count; ++j) {
                    page[j] = size++;
                }
            }

            file.Write(&page[0], count * sizeof(i64));
        }
    }

    void DropFilesystemCaches()
    {
        sync();
        auto file = ::TFile("/proc/sys/vm/drop_caches", EOpenModeFlag::WrOnly);
        file.Write("3", 1);
    }

    void Warmup(TConfigurationPtr configuration)
    {
        std::vector<char> buf(512);
        for (auto& file : configuration->Files) {
            file.File.Pread(&buf[0], 512, 0);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TTest
{
public:
    TTest(const TTestConfigPtr config, TString info, const TTestOptions& options)
        : Config_(std::move(config))
        , Options_(options)
        , Info_(info)
    { }

    void Run()
    {
        FormattedOutput_ = New<TFormattedOutput>();
        FormattedOutput_->Name = Config_->Name;
        FormattedOutput_->Info = Info_;
        FormattedOutput_->Uname = Uname();
        FormattedOutput_->Config = Config_;

        for (auto& epochConfig : Config_->Epochs) {
            auto epoch = TTestEpoch(epochConfig, Options_);
            FormattedOutput_->Epochs.push_back(epoch.Run());
        }

        Cout << ConvertToYsonString(FormattedOutput_, EYsonFormat::Pretty).ToString() << Endl;
    }

private:
    const TTestConfigPtr Config_;
    const TTestOptions Options_;
    const TString Info_;
    TFormattedOutputPtr FormattedOutput_;
};

////////////////////////////////////////////////////////////////////////////////

void RunTest(TTestConfigPtr config, TString info, const TTestOptions& options) {
    TTest test(std::move(config), info, options);
    test.Run();
}

void GuardedMain(int argc, char** argv) {
    TOpts opts;
    TString configFileName;
    TString info;
    TTestOptions options;
    auto config = New<TTestConfig>();

    opts.AddLongOption("config", "Config")
        .Required()
        .StoreResult(&configFileName);
    opts.AddLongOption("info", "Info")
        .Optional()
        .StoreResult(&info);
    opts.AddLongOption("validate", "Validate")
        .Optional()
        .StoreResult(&options.Validate);
    opts.AddLongOption("recreate", "Recreate files")
        .Optional()
        .StoreResult(&options.Recreate);
    opts.AddLongOption("force-file-size", "Force file size")
        .Optional()
        .StoreResult(&options.ForceFileSize);

    TOptsParseResult results(&opts, argc, argv);
    ::TFile file(configFileName, EOpenModeFlag::RdOnly);
    auto data = TSharedMutableRef::Allocate(file.GetLength(), {.InitializeStorage = false});
    file.Load(data.Begin(), data.Size());

    auto node = ConvertTo<INodePtr>(TYsonString(TStringBuf(data.Begin(), data.Size()), EYsonType::Node));
    config->Load(node);

    RunTest(std::move(config), info, options);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIOTest

int main(int argc, char* argv[])
{
    try {
        NYT::NIOTest::GuardedMain(argc, argv);
    } catch (const std::exception& ex) {
        Cout << ToString(NYT::TError(ex)) << Endl;
    }
}

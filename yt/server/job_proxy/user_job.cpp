#include "stdafx.h"

#include "user_job.h"

#include "config.h"
#include "job_detail.h"
#include "helpers.h"
#include "private.h"
#include "stderr_output.h"
#include "table_output.h"
#include "user_job_io.h"

#include <server/exec_agent/public.h>

#include <ytlib/chunk_client/public.h>

#include <ytlib/table_client/table_producer.h>
#include <ytlib/table_client/table_consumer.h>
#include <ytlib/table_client/sync_reader.h>
#include <ytlib/table_client/sync_writer.h>

#include <ytlib/scheduler/statistics.h>

#include <ytlib/formats/format.h>
#include <ytlib/formats/parser.h>

#include <ytlib/transaction_client/public.h>

#include <ytlib/cgroup/cgroup.h>

#include <ytlib/pipes/async_reader.h>
#include <ytlib/pipes/async_writer.h>

#include <core/logging/log.h>

#include <core/misc/error.h>
#include <core/misc/fs.h>
#include <core/misc/proc.h>
#include <core/misc/process.h>
#include <core/misc/pattern_formatter.h>

#include <core/concurrency/periodic_executor.h>
#include <core/concurrency/action_queue.h>

#include <util/folder/dirut.h>

#include <util/stream/null.h>

#include <util/system/execpath.h>

namespace NYT {
namespace NJobProxy {

using namespace NYTree;
using namespace NYson;
using namespace NTableClient;
using namespace NFormats;
using namespace NScheduler;
using namespace NScheduler::NProto;
using namespace NTransactionClient;
using namespace NConcurrency;
using namespace NCGroup;
using namespace NJobAgent;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NPipes;

using NJobTrackerClient::NProto::TJobResult;
using NScheduler::NProto::TUserJobSpec;

using NPipes::TAsyncReader;
using NPipes::TAsyncReaderPtr;
using NPipes::TAsyncWriter;
using NPipes::TAsyncWriterPtr;

////////////////////////////////////////////////////////////////////////////////

#ifdef _linux_

static const int JobStatisticsFD = 5;
static const char* CGroupPrefix = "user_jobs/yt-job-";

static TNullOutput NullOutput;

////////////////////////////////////////////////////////////////////////////////

struct TPipe
{
    int ReadFD;
    int WriteFD;
};

////////////////////////////////////////////////////////////////////////////////

class TUserJob
    : public TJob
{
public:
    TUserJob(
        IJobHost* host,
        const TUserJobSpec& userJobSpec,
        const TJobId& jobId,
        std::unique_ptr<IUserJobIO> userJobIO)
        : TJob(host)
        , JobIO_(std::move(userJobIO))
        , UserJobSpec_(userJobSpec)
        , JobId_(jobId)
        , Config_(host->GetConfig())
        , JobErrorPromise_(NewPromise<TError>())
        , MemoryUsage_(UserJobSpec_.memory_reserve())
        , PeriodicQueue_(New<TActionQueue>("UserJobPeriodic"))
        , Process_(GetExecPath(), false)
        , CpuAccounting_(CGroupPrefix + ToString(jobId))
        , BlockIO_(CGroupPrefix + ToString(jobId))
        , Memory_(CGroupPrefix + ToString(jobId))
        , Freezer_(CGroupPrefix + ToString(jobId))
        , Logger(host->GetLogger())
    {
        Logger.AddTag("JobId: %v", jobId);

        MemoryWatchdogExecutor_ = New<TPeriodicExecutor>(
            PeriodicQueue_->GetInvoker(),
            BIND(&TUserJob::CheckMemoryUsage, MakeWeak(this)),
            Config_->MemoryWatchdogPeriod);

        BlockIOWatchdogExecutor_ = New<TPeriodicExecutor>(
            PeriodicQueue_->GetInvoker(),
            BIND(&TUserJob::CheckBlockIOUsage, MakeWeak(this)),
            Config_->BlockIOWatchdogPeriod);
    }

    virtual TJobResult Run() override
    {
        LOG_DEBUG("Starting job process");

        Prepare();

        Process_.Spawn();
        LOG_INFO("Job process started");

        MemoryWatchdogExecutor_->Start();
        BlockIOWatchdogExecutor_->Start();

        DoJobIO();

        auto jobExitError = Process_.Wait();
        LOG_INFO(jobExitError, "Job process completed");

        AddStatistic(
            "/user_job/builtin/time", 
            TSummary(static_cast<i64>(GetElapsedTime().MilliSeconds())));

        WaitFor(BlockIOWatchdogExecutor_->Stop());
        WaitFor(MemoryWatchdogExecutor_->Stop());

        if (jobExitError.IsOK()) {
            FinalizeJobIO();
        }

        JobErrorPromise_.TrySet(jobExitError);

        const auto& jobResultError = JobErrorPromise_.Get();

        CleanupCGroups();

        TJobResult result;
        ToProto(result.mutable_error(), jobResultError.IsOK() 
            ? TError() 
            : TError("User job failed") << jobResultError);
        auto* schedulerResultExt = result.MutableExtension(TSchedulerJobResultExt::scheduler_job_result_ext);

        SaveErrorChunkId(schedulerResultExt);

        if (jobResultError.IsOK()) {
            JobIO_->PopulateResult(schedulerResultExt);
        } else if (UserJobSpec_.has_stderr_transaction_id()) {
            SaveFailContexts(schedulerResultExt);
        }

        return result;
    }

    virtual double GetProgress() const override
    {
        i64 total = 0;
        i64 current = 0;

        for (const auto& reader : JobIO_->GetReaders()) {
            total += reader->GetSessionRowCount();
            current += reader->GetSessionRowIndex();
        }

        if (total == 0) {
            return 0.0;
        }

        return std::min(current / static_cast<double>(total), 1.0);
    }

    virtual std::vector<TChunkId> GetFailedChunkIds() const override
    {
        std::vector<TChunkId> failedChunks;
        for (const auto& reader : JobIO_->GetReaders()) {
            auto chunks = reader->GetFailedChunkIds();
            failedChunks.insert(failedChunks.end(), chunks.begin(), chunks.end());
        }
        return failedChunks;
    }

private:
    std::unique_ptr<IUserJobIO> JobIO_;

    const TUserJobSpec& UserJobSpec_;
    TJobId JobId_;

    TJobProxyConfigPtr Config_;

    TAsyncErrorPromise JobErrorPromise_;

    i64 MemoryUsage_;

    TActionQueuePtr PeriodicQueue_;
    TPeriodicExecutorPtr MemoryWatchdogExecutor_;
    TPeriodicExecutorPtr BlockIOWatchdogExecutor_;

    std::vector<std::unique_ptr<TOutputStream>> TableOutputs_;
    std::unique_ptr<TErrorOutput> ErrorOutput_;
    std::unique_ptr<TTableOutput> StatisticsOutput_;

    std::vector<TAsyncReaderPtr> TablePipeReaders_;
    std::vector<TAsyncWriterPtr> TablePipeWriters_;

    std::vector<TContextPreservingInputPtr> ContextPreservingInputs_;

    std::vector<TCallback<TError()>> IOActions_;
    std::vector<TCallback<void()>> FinalizeActions_;

    TProcess Process_;

    TCpuAccounting CpuAccounting_;
    TBlockIO BlockIO_;
    TMemory Memory_;
    TFreezer Freezer_;

    std::vector<TBlockIO::TStatisticsItem> LastServicedIOs_;

    TSpinLock StatisticsLock_;
    TStatistics Statistics_;

    NLog::TLogger Logger;


    void Prepare()
    {
        PrepareCGroups();

        PreparePipes();

        Process_.AddArgument("--executor");
        Process_.AddArguments({"--command", UserJobSpec_.shell_command()});
        Process_.AddArguments({"--config", NFS::CombinePaths(GetCwd(), NExecAgent::ProxyConfigFileName)});
        Process_.AddArguments({"--working-dir", Config_->SandboxName});

        if (UserJobSpec_.enable_core_dump()) {
            Process_.AddArgument("--enable-core-dumps");
        }

        if (Config_->UserId) {
            Process_.AddArguments({"--uid", ::ToString(*Config_->UserId)});

            // ToDo(psushin): remove, use cgroups limit instead.
            if (UserJobSpec_.enable_io_prio()) {
                Process_.AddArgument("--enable-io-prio");
            }
        }

        // Init environment variables.
        TPatternFormatter formatter;
        formatter.AddProperty("SandboxPath", GetCwd());

        for (int i = 0; i < UserJobSpec_.environment_size(); ++i) {
            Process_.AddEnvVar(formatter.Format(UserJobSpec_.environment(i)));
        }
    }

    void CleanupCGroups()
    {
        if (!UserJobSpec_.enable_accounting() && !Config_->ForceEnableAccounting) {
            return;
        }

        try {
            // Kill everything for sanity reasons: main user process completed,
            // but its children may be still alive.
            RunKiller(Freezer_.GetFullPath());
        } catch (const std::exception& ex) {
            LOG_FATAL(ex, "Failed to clean up user processes");
        }

        auto cpuAccountingStats = CpuAccounting_.GetStatistics();
        AddStatistic("/user_job/builtin/cpu", cpuAccountingStats);

        auto blockIOStats = BlockIO_.GetStatistics();
        AddStatistic("/user_job/builtin/block_io", blockIOStats);

        Freezer_.Destroy();
        CpuAccounting_.Destroy();
        BlockIO_.Destroy();

        if (Config_->EnableCGroupMemoryHierarchy) {
            Memory_.ForceEmpty();
            Memory_.Destroy();
        }
    }

    TOutputStream* CreateStatisticsOutput()
    {
        auto consumer = std::make_unique<TStatisticsConsumer>(
            BIND(&TUserJob::ConsumeStatistics, Unretained(this)),
            "/user_job/custom");
        auto parser = CreateParserForFormat(TFormat(EFormatType::Yson), EDataType::Tabular, consumer.get());
        StatisticsOutput_.reset(new TTableOutput(std::move(parser), std::move(consumer)));
        return StatisticsOutput_.get();
    }

    TOutputStream* CreateErrorOutput()
    {
        if (!UserJobSpec_.has_stderr_transaction_id()) {
            return &NullOutput;
        }

        auto host = Host.Lock();
        YCHECK(host);

        ErrorOutput_.reset(new TErrorOutput(
            Config_->JobIO->ErrorFileWriter,
            host->GetMasterChannel(),
            FromProto<TTransactionId>(UserJobSpec_.stderr_transaction_id()),
            UserJobSpec_.max_stderr_size()));

        return ErrorOutput_.get();
    }

    void SaveErrorChunkId(TSchedulerJobResultExt* schedulerResultExt)
    {
        if (!ErrorOutput_) {
            return;
        }

        auto errorChunkId = ErrorOutput_->GetChunkId();
        if (errorChunkId != NullChunkId) {
            ToProto(schedulerResultExt->mutable_stderr_chunk_id(), errorChunkId);
            LOG_INFO("Stderr chunk generated (ChunkId: %v)", errorChunkId);
        }
    }

    void SaveFailContexts(TSchedulerJobResultExt* schedulerResultExt)
    {
        if (!UserJobSpec_.has_stderr_transaction_id()) {
            return;
        }

        auto host = Host.Lock();
        YCHECK(host);

        auto transactionId = FromProto<TTransactionId>(UserJobSpec_.stderr_transaction_id());
        for (int inputIndex = 0; inputIndex < ContextPreservingInputs_.size(); ++inputIndex) {
            auto input = ContextPreservingInputs_[inputIndex];

            TErrorOutput contextOutput(
                Config_->JobIO->ErrorFileWriter,
                host->GetMasterChannel(),
                transactionId);

            auto context = input->GetContext();
            contextOutput.Write(context.Begin(), context.Size());
            contextOutput.Finish();

            auto contextChunkId = contextOutput.GetChunkId();
            ToProto(schedulerResultExt->add_fail_context_chunk_ids(), contextChunkId);
            LOG_INFO("Fail context chunk generated (ChunkId: %v, InputIndex: %v)",
                contextChunkId,
                inputIndex);
        }
    }

    int GetMaxReservedDescriptor() const
    {
        int outputCount = JobIO_->GetWriters().size();
        int inputCount = JobIO_->GetReaders().size();

        if (UserJobSpec_.use_yamr_descriptors()) {
            return 2 + outputCount;
        }

        int result = 3 * std::max(inputCount, outputCount);
        return std::max(result, JobStatisticsFD + 1);
    }

    void PrepareOutputTablePipes(std::function<TPipe()> createPipe)
    {
        auto format = ConvertTo<TFormat>(TYsonString(UserJobSpec_.output_format()));
        
        auto& writers = JobIO_->GetWriters();
        TableOutputs_.resize(writers.size());
        for (int i = 0; i < writers.size(); ++i) {
            std::unique_ptr<IYsonConsumer> consumer(new TLegacyTableConsumer(writers, i));
            auto parser = CreateParserForFormat(format, EDataType::Tabular, consumer.get());
            TableOutputs_[i].reset(new TTableOutput(
                std::move(parser),
                std::move(consumer)));

            int jobDescriptor = UserJobSpec_.use_yamr_descriptors()
                ? 3 + i
                : 3 * i + 1;

            auto reader = PrepareOutputPipe(createPipe(), jobDescriptor, TableOutputs_[i].get());
            TablePipeReaders_.push_back(reader);
        }

        FinalizeActions_.push_back(BIND([=] () {
            for (auto& writer : JobIO_->GetWriters()) {
                writer->Close();
            }
        }));
    }

    TAsyncReaderPtr PrepareOutputPipe(TPipe pipe, int jobDescriptor, TOutputStream* output)
    {
        Process_.AddCloseFileAction(pipe.ReadFD);
        Process_.AddCloseFileAction(jobDescriptor);
        Process_.AddDup2FileAction(pipe.WriteFD, jobDescriptor);
        Process_.AddCloseFileAction(pipe.WriteFD);

        Process_.AddArguments({ "--prepare-pipe", ::ToString(jobDescriptor) });

        SafeMakeNonblocking(pipe.ReadFD);

        auto asyncInput = New<TAsyncReader>(pipe.ReadFD);

        IOActions_.push_back(
            BIND(
                &TUserJob::ReadFromOutputPipe,
                MakeWeak(this),
                pipe,
                asyncInput,
                output)
            .Guarded());

        return asyncInput;
    }

    void ReadFromOutputPipe(TPipe pipe, IAsyncInputStreamPtr asyncInput, TOutputStream* output)
    {
        SafeClose(pipe.WriteFD);
        auto input = CreateSyncAdapter(asyncInput);
        PipeInputToOutput(input.get(), output);
    }

    void PrepareInputTablePipe(TPipe pipe, int jobDescriptor, TContextPreservingInputPtr input)
    {
        Process_.AddCloseFileAction(pipe.WriteFD);
        Process_.AddCloseFileAction(jobDescriptor);
        Process_.AddDup2FileAction(pipe.ReadFD, jobDescriptor);
        Process_.AddCloseFileAction(pipe.ReadFD);

        Process_.AddArguments({ "--prepare-pipe", ::ToString(jobDescriptor) });

        SafeMakeNonblocking(pipe.WriteFD);
        auto asyncOutput = New<TAsyncWriter>(pipe.WriteFD);
        TablePipeWriters_.push_back(asyncOutput);

        IOActions_.push_back(
            BIND([=] () {
                auto output = CreateSyncAdapter(asyncOutput);
                input->PipeReaderToOutput(output.get());
                auto error = WaitFor(asyncOutput->Close());
                if (!error.IsOK()) {
                    THROW_ERROR TError("Table input pipe failed")
                        << TErrorAttribute("fd", jobDescriptor)
                        << error;
                }
            })
            .Guarded());

        if (!UserJobSpec_.check_input_fully_consumed()) {
            return;
        }

        FinalizeActions_.push_back(BIND([=] () {
            char buffer;
            // Try to read some data from the pipe.
            ssize_t res = ::read(pipe.ReadFD, &buffer, 1);
            if (res > 0) {
                THROW_ERROR_EXCEPTION("Input stream was not fully consumed by user process")
                    << TErrorAttribute("fd", jobDescriptor);
            }
        }));
    }

    void PrepareInputTablePipes(std::function<TPipe()> createPipe)
    {
        auto format = ConvertTo<TFormat>(TYsonString(UserJobSpec_.input_format()));
        const auto& readers = JobIO_->GetReaders();

        YCHECK(!UserJobSpec_.use_yamr_descriptors() || readers.size() == 1);

        for (int i = 0; i < readers.size(); ++i) {
            auto input = New<TContextPreservingInput>(
                readers[i], 
                format, 
                Config_->JobIO->TableReader->EnableTableIndex);

            ContextPreservingInputs_.push_back(input);
            PrepareInputTablePipe(createPipe(), 3 * i, input);
        }
    }

    void PreparePipes()
    {
        LOG_DEBUG("Initializing pipes");

        // We use the following convention for designating input and output file descriptors
        // in job processes:
        // fd == 3 * (N - 1) for the N-th input table (if exists)
        // fd == 3 * (N - 1) + 1 for the N-th output table (if exists)
        // fd == 2 for the error stream
        // e. g.
        // 0 - first input table
        // 1 - first output table
        // 2 - error stream
        // 3 - second input
        // 4 - second output
        // etc.
        //
        // A special option (ToDo(psushin): which one?) enables concatenating
        // all input streams into fd == 0.

        int maxReservedDescriptor = GetMaxReservedDescriptor();
        YASSERT(maxReservedDescriptor > 0);

        // To avoid descriptor collisions between pipes on this, proxy side,
        // and "standard" descriptor numbers in forked job (see comments above)
        // we ensure that enough lower descriptors are allocated before creating pipes.

        std::vector<int> reservedDescriptors;
        auto createPipe = [&] () -> TPipe {
            while (true) {
                int fd[2];
                SafePipe(fd);
                if (fd[0] < maxReservedDescriptor || fd[1] < maxReservedDescriptor) {
                    reservedDescriptors.push_back(fd[0]);
                    reservedDescriptors.push_back(fd[1]);
                } else {
                    TPipe pipe = {fd[0], fd[1]};
                    return pipe;
                }
            }
        };

        // Configure stderr pipe.
        PrepareOutputPipe(createPipe(), STDERR_FILENO, CreateErrorOutput());

        PrepareOutputTablePipes(createPipe);

        if (UserJobSpec_.use_yamr_descriptors()) {
            // This hack is to work around the fact that usual output pipe accepts a
            // single job descriptor, whilst yamr convention requires fds 1 and 3 to be the same.
            Process_.AddDup2FileAction(3, 1);
        } else {
            // Configure statistics output pipe.
            PrepareOutputPipe(createPipe(), JobStatisticsFD, CreateStatisticsOutput());
        }

        PrepareInputTablePipes(createPipe);

        // Close reserved descriptors.
        for (int fd : reservedDescriptors) {
            SafeClose(fd);
        }

        LOG_DEBUG("Pipes initialized");
    }

    void PrepareCGroups()
    {
        if (!UserJobSpec_.enable_accounting() && !Config_->ForceEnableAccounting) {
            return;
        }

        try {
            Freezer_.Create();
            Process_.AddArguments({ "--cgroup", Freezer_.GetFullPath() });

            CpuAccounting_.Create();
            Process_.AddArguments({ "--cgroup", CpuAccounting_.GetFullPath() });

            BlockIO_.Create();
            Process_.AddArguments({ "--cgroup", BlockIO_.GetFullPath() });

            if (Config_->EnableCGroupMemoryHierarchy) {
                Memory_.Create();
                Process_.AddArguments({ "--cgroup", Memory_.GetFullPath() });
            }
        } catch (const std::exception& ex) {
            LOG_FATAL(ex, "Unable to create required cgroups");
        }
    }

    void ConsumeStatistics(const TStatistics& statistics)
    {
        TGuard<TSpinLock> guard(StatisticsLock_);
        Statistics_.Merge(statistics);
    }

    template <class T>
    void AddStatistic(const TYPath& path, const T& statistic)
    {
        TGuard<TSpinLock> guard(StatisticsLock_);
        Statistics_.Add(path, statistic);
    }

    template <class T>
    TDataStatistics GetDataStatistics(const std::vector<T>& sources) const
    {
        auto statistics = ZeroDataStatistics();
        for (const auto& source : sources) {
            statistics += source->GetDataStatistics();
        }

        return statistics;
    }

    virtual NJobTrackerClient::NProto::TJobStatistics GetStatistics() const override
    {
        NJobTrackerClient::NProto::TJobStatistics result;
        result.set_time(GetElapsedTime().MilliSeconds());

        ToProto(result.mutable_input(), GetDataStatistics(JobIO_->GetReaders()));
        ToProto(result.mutable_output(), GetDataStatistics(JobIO_->GetWriters()));

        {
            TGuard<TSpinLock> guard(StatisticsLock_);
            ToProto(result.mutable_statistics(), ConvertToYsonString(Statistics_).Data());
        }

        return result;
    }

    void DoJobIO()
    {
        auto onIOError = BIND([=] (TError error) {
            if (error.IsOK()) {
                return;
            }
            
            if (!JobErrorPromise_.TrySet(error)) {
                return;
            }

            LOG_ERROR(error, "Job input/output error, aboring");

            for (auto& reader : TablePipeReaders_) {
                reader->Abort();
            }

            for (auto& writer : TablePipeWriters_) {
                writer->Abort();
            }
        });

        auto queue = New<TActionQueue>("PipesIO");

        auto awaiter = New<TParallelAwaiter>(queue->GetInvoker());
        for (auto& action : IOActions_) {
            auto asyncError = action
                .AsyncVia(queue->GetInvoker())
                .Run();
            awaiter->Await(asyncError, onIOError);
        }

        WaitFor(awaiter->Complete());
    }

    void FinalizeJobIO()
    {
        for (auto& action : FinalizeActions_) {
            try {
                action.Run();
            } catch (const std::exception& ex) {
                JobErrorPromise_.TrySet(ex);
            }
        }
    }

    i64 GetMemoryUsageByUid(int uid) const
    {
        auto pids = GetPidsByUid(uid);

        i64 rss = 0;
        for (int pid : pids) {
            try {
                i64 processRss = GetProcessRss(pid);
                // ProcessId itself is skipped since it's always 'sh'.
                // This also helps to prevent taking proxy's own RSS into account
                // when it has fork-ed but not exec-uted the child process yet.
                bool skip = (pid == Process_.GetProcessId());
                LOG_DEBUG("PID: %v, RSS: %v %v",
                    pid,
                    processRss,
                    skip ? " (skipped)" : "");
                if (!skip) {
                    rss += processRss;
                }
            } catch (const std::exception& ex) {
                LOG_DEBUG(ex, "Failed to get RSS for PID %v", pid);
            }
        }
        return rss;
    }

    void UpdateMemoryUsage(i64 rss)
    {
        auto host = Host.Lock();
        if (!host)
            return;

        i64 delta = rss - MemoryUsage_;
        LOG_DEBUG("Memory usage increased by %v", delta);

        MemoryUsage_ = rss;

        auto resourceUsage = host->GetResourceUsage();
        resourceUsage.set_memory(resourceUsage.memory() + delta);
        host->SetResourceUsage(resourceUsage);
    }

    void CheckMemoryUsage()
    {
        if (!Config_->UserId) {
            LOG_DEBUG("Memory usage control is disabled");
            return;
        }

        i64 rss = GetMemoryUsageByUid(*Config_->UserId);

        if (Memory_.IsCreated()) {
            auto statistics = Memory_.GetStatistics();
            AddStatistic("/user_job/builtin/memory", statistics);

            i64 uidRss = rss;
            rss = statistics.Rss + statistics.MappedFile;

            if (rss > 1.05 * uidRss && uidRss > 0) {
                LOG_ERROR("Memory usage measured by cgroup is much greater than via procfs: %v > %v",
                    rss,
                    uidRss);
            }
        }

        i64 memoryLimit = UserJobSpec_.memory_limit();
        LOG_DEBUG("Check memory usage (Rss: %v, MemoryLimit: %v)",
            rss,
            memoryLimit);

        if (rss > MemoryUsage_) {
            UpdateMemoryUsage(rss);
        }

        if (rss > memoryLimit) {
            JobErrorPromise_.TrySet(TError(EErrorCode::MemoryLimitExceeded, "Memory limit exceeded")
                << TErrorAttribute("time_since_start", GetElapsedTime().MilliSeconds())
                << TErrorAttribute("rss", rss)
                << TErrorAttribute("limit", memoryLimit));

            YCHECK(Freezer_.IsCreated());

            try {
                RunKiller(Freezer_.GetFullPath());
            } catch (const std::exception& ex) {
                LOG_FATAL(ex, "Failed to clean up user processes");
            }
        }
    }

    void CheckBlockIOUsage()
    {
        if (!BlockIO_.IsCreated() || !Config_->IopsThreshold) {
            return;
        }

        auto period = Config_->BlockIOWatchdogPeriod;
        auto iopsThreshold = *Config_->IopsThreshold;

        auto servicedIOs = BlockIO_.GetIOServiced();

        for (const auto& item : servicedIOs) {
            LOG_DEBUG("%v %v operation for %v device", item.Value, item.Type, item.DeviceId);

            auto previousItemIt = std::find_if(
                LastServicedIOs_.begin(),
                LastServicedIOs_.end(),
                [=] (const TBlockIO::TStatisticsItem& other) {
                    return item.DeviceId == other.DeviceId  && item.Type == other.Type;
                });

            if (previousItemIt == LastServicedIOs_.end()) {
                continue;
            }

            i64 deltaOperations = item.Value - previousItemIt->Value;
            if (deltaOperations < 0) {
                LOG_WARNING("%v < 0 operations were serviced for %v device since the last check",
                    deltaOperations,
                    item.DeviceId);
            }

            if (deltaOperations > iopsThreshold * period.Seconds()) {
                LOG_INFO("Woodpecker detected; limiting it to %v IOPS", iopsThreshold);
                BlockIO_.ThrottleOperations(item.DeviceId, iopsThreshold);
            }
        }

        LastServicedIOs_ = servicedIOs;
    }

};

////////////////////////////////////////////////////////////////////////////////

IJobPtr CreateUserJob(
    IJobHost* host,
    const NScheduler::NProto::TUserJobSpec& UserJobSpec_,
    std::unique_ptr<IUserJobIO> userJobIO,
    const TJobId& jobId)
{
    return New<TUserJob>(
        host,
        UserJobSpec_,
        jobId,
        std::move(userJobIO));
}

#else

IJobPtr CreateUserJob(
    IJobHost* host,
    const NScheduler::NProto::TUserJobSpec& UserJobSpec_,
    std::unique_ptr<IUserJobIO> userJobIO,
    const TJobId& jobId)
{
    THROW_ERROR_EXCEPTION("Streaming jobs are supported only under Linux");
}

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT

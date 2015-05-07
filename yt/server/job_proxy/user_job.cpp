#include "stdafx.h"

#include "user_job.h"

#include "config.h"
#include "job_detail.h"
#include "private.h"
#include "stderr_output.h"
#include "table_output.h"
#include "user_job_io.h"
#include "stracer.h"

#include <server/exec_agent/public.h>

#include <ytlib/chunk_client/public.h>

#include <ytlib/new_table_client/helpers.h>
#include <ytlib/new_table_client/table_consumer.h>
#include <ytlib/new_table_client/schemaless_chunk_reader.h>
#include <ytlib/new_table_client/schemaless_chunk_writer.h>

#include <ytlib/scheduler/statistics.h>

#include <ytlib/formats/parser.h>

#include <ytlib/transaction_client/public.h>

#include <ytlib/cgroup/cgroup.h>

#include <core/pipes/async_reader.h>
#include <core/pipes/async_writer.h>

#include <core/misc/fs.h>
#include <core/misc/proc.h>
#include <core/misc/process.h>
#include <core/misc/subprocess.h>
#include <core/misc/pattern_formatter.h>
#include <core/misc/finally.h>

#include <core/tools/tools.h>

#include <core/concurrency/action_queue.h>

#include <util/folder/dirut.h>

#include <util/system/execpath.h>

namespace NYT {
namespace NJobProxy {

using namespace NYTree;
using namespace NYson;
using namespace NVersionedTableClient;
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

////////////////////////////////////////////////////////////////////////////////

#ifdef _linux_

static const int JobStatisticsFD = 5;
static const char* CGroupPrefix = "user_jobs/yt-job-";

static const int BufferSize = 1024 * 1024;

static const size_t MaxCustomStatisticNameLength = 512;

static TNullOutput NullOutput;

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
        , Config_(host->GetConfig())
        , JobErrorPromise_(NewPromise<void>())
        , MemoryUsage_(UserJobSpec_.memory_reserve())
        , PipeIOQueue_(New<TActionQueue>("PipeIO"))
        , PeriodicQueue_(New<TActionQueue>("UserJobPeriodic"))
        , JobProberQueue_(New<TActionQueue>("JobProber"))
        , Process_(GetExecPath(), false)
        , CpuAccounting_(CGroupPrefix + ToString(jobId))
        , BlockIO_(CGroupPrefix + ToString(jobId))
        , Memory_(CGroupPrefix + ToString(jobId))
        , Freezer_(CGroupPrefix + ToString(jobId))
        , Logger(host->GetLogger())
    {
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

        Prepared_ = true;

        Process_.Spawn();
        LOG_INFO("Job process started");

        MemoryWatchdogExecutor_->Start();
        BlockIOWatchdogExecutor_->Start();

        DoJobIO();

        WaitFor(BlockIOWatchdogExecutor_->Stop());
        WaitFor(MemoryWatchdogExecutor_->Stop());

        if (!JobErrorPromise_.IsSet())  {
            FinalizeJobIO();
        }

        {
            // One do not need to get a lock here
            TGuard<TSpinLock> guard(StatisticsLock_);
            FillCurrentDataStatistics(Statistics_);
        }

        CleanupCGroups();

        JobErrorPromise_.TrySet(TError());
        const auto& jobResultError = JobErrorPromise_.Get();

        TJobResult result;
        ToProto(result.mutable_error(), jobResultError.IsOK()
            ? TError()
            : TError("User job failed") << jobResultError);
        auto* schedulerResultExt = result.MutableExtension(TSchedulerJobResultExt::scheduler_job_result_ext);

        SaveErrorChunkId(schedulerResultExt);

        if (jobResultError.IsOK()) {
            JobIO_->PopulateResult(schedulerResultExt);
        } else {
            DumpFailContexts(schedulerResultExt);
        }

        return result;
    }

    virtual double GetProgress() const override
    {
        i64 total = 0;
        i64 current = 0;

        for (const auto& reader : JobIO_->GetReaders()) {
            total += reader->GetTotalRowCount();
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

    TJobProxyConfigPtr Config_;

    TPromise<void> JobErrorPromise_;

    std::atomic<bool> Prepared_ = { false };

    std::atomic_flag Stracing_ = ATOMIC_FLAG_INIT;

    i64 MemoryUsage_;

    TActionQueuePtr PipeIOQueue_;

    TActionQueuePtr PeriodicQueue_;
    TPeriodicExecutorPtr MemoryWatchdogExecutor_;
    TPeriodicExecutorPtr BlockIOWatchdogExecutor_;

    std::vector<std::unique_ptr<TOutputStream>> TableOutputs_;
    std::vector<TWritingValueConsumerPtr> WritingValueConsumers_;

    std::unique_ptr<TErrorOutput> ErrorOutput_;
    std::unique_ptr<TTableOutput> StatisticsOutput_;

    std::vector<TAsyncReaderPtr> TablePipeReaders_;
    std::vector<TAsyncWriterPtr> TablePipeWriters_;

    std::vector<ISchemalessFormatWriterPtr> FormatWriters_;

    std::vector<TCallback<void()>> InputActions_;
    std::vector<TCallback<void()>> OutputActions_;
    std::vector<TCallback<void()>> FinalizeActions_;

    TActionQueuePtr JobProberQueue_;

    TProcess Process_;

    TCpuAccounting CpuAccounting_;
    TBlockIO BlockIO_;
    TMemory Memory_;
    TFreezer Freezer_;

    TSpinLock FreezerLock_;

    std::vector<TBlockIO::TStatisticsItem> LastServicedIOs_;

    TSpinLock StatisticsLock_;
    TStatistics Statistics_;

    NLogging::TLogger Logger;


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
        if (!Config_->ForceEnableAccounting) {
            return;
        }

        try {
            // Kill everything for sanity reasons: main user process completed,
            // but its children may still be alive.
            Stroka freezerFullPath;
            {
                TGuard<TSpinLock> guard(FreezerLock_);
                freezerFullPath = Freezer_.GetFullPath();
            }

            RunKiller(freezerFullPath);
        } catch (const std::exception& ex) {
            LOG_FATAL(ex, "Failed to clean up user processes");
        }

        auto cpuAccountingStats = CpuAccounting_.GetStatistics();
        AddStatistic("/user_job/cpu", cpuAccountingStats);

        auto blockIOStats = BlockIO_.GetStatistics();
        AddStatistic("/user_job/block_io", blockIOStats);

        AddStatistic("/user_job/max_memory", Memory_.GetMaxMemoryUsage());

        {
            TGuard<TSpinLock> guard(FreezerLock_);
            Freezer_.Destroy();
        }

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
            "/custom");
        auto parser = CreateParserForFormat(TFormat(EFormatType::Yson), EDataType::Tabular, consumer.get());
        StatisticsOutput_.reset(new TTableOutput(std::move(parser), std::move(consumer)));
        return StatisticsOutput_.get();
    }

    TOutputStream* CreateErrorOutput()
    {
        auto host = Host.Lock();
        YCHECK(host);

        ErrorOutput_.reset(new TErrorOutput(
            Config_->JobIO->ErrorFileWriter,
            host->GetMasterChannel(),
            FromProto<TTransactionId>(UserJobSpec_.async_scheduler_transaction_id()),
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

    void DumpFailContexts(TSchedulerJobResultExt* schedulerResultExt)
    {
        auto contexts = DoGetInputContexts();
        auto contextChunkIds = DoDumpInputContexts(contexts);

        for (const auto& contextChunkId : contextChunkIds) {
            ToProto(schedulerResultExt->add_fail_context_chunk_ids(), contextChunkId);
        }
    }

    virtual std::vector<TChunkId> DumpInputContext() override
    {
        if (!Prepared_) {
            THROW_ERROR_EXCEPTION("Cannot dump input context: job pipes are not prepared yet");
        }

        auto asyncContexts = BIND(&TUserJob::DoGetInputContexts, MakeStrong(this))
                .AsyncVia(PipeIOQueue_->GetInvoker())
                .Run();
        auto contexts = WaitFor(asyncContexts)
            .ValueOrThrow();

        return DoDumpInputContexts(contexts);
    }

    std::vector<TChunkId> DoDumpInputContexts(const std::vector<TBlob>& contexts)
    {
        auto host = Host.Lock();
        YCHECK(host);

        std::vector<TChunkId> result;

        auto transactionId = FromProto<TTransactionId>(UserJobSpec_.async_scheduler_transaction_id());
        for (int index = 0; index < contexts.size(); ++index) {
            TErrorOutput contextOutput(
                Config_->JobIO->ErrorFileWriter,
                host->GetMasterChannel(),
                transactionId);

            const auto& context = contexts[index];
            contextOutput.Write(context.Begin(), context.Size());
            contextOutput.Finish();

            auto contextChunkId = contextOutput.GetChunkId();
            LOG_INFO("Input context chunk generated (ChunkId: %v, InputIndex: %v)",
                contextChunkId,
                index);

            result.push_back(contextChunkId);
        }

        return result;
    }

    std::vector<TBlob> DoGetInputContexts()
    {
        std::vector<TBlob> result;

        for (const auto& input : FormatWriters_) {
            result.push_back(input->GetContext());
        }

        return result;
    }

    virtual TYsonString Strace() override
    {
        std::vector<int> pids;

        {
            TGuard<TSpinLock> guard(FreezerLock_);
            if (!Freezer_.IsCreated()) {
                THROW_ERROR_EXCEPTION("Cannot determine user job processes: freezer cgoup is not created");
            }

            pids = Freezer_.GetTasks();
        }

        if (Stracing_.test_and_set()) {
            THROW_ERROR_EXCEPTION("Cannot strace while other stracing routing is active");
        }

        TFinallyGuard guard([this] () {
            Stracing_.clear();
        });

        auto asyncTraces = WaitFor(BIND([&] () {
            return RunTool<TStraceTool>(pids);
        })
            .AsyncVia(JobProberQueue_->GetInvoker())
            .Run());

        if (!asyncTraces.IsOK()) {
            THROW_ERROR_EXCEPTION("Failed to strace")
                << asyncTraces;
        }

        return ConvertToYsonString(asyncTraces.Value());
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

    std::vector<IValueConsumerPtr> CreateValueConsumers()
    {
        std::vector<IValueConsumerPtr> valueConsumers;
        for (const auto& writer : JobIO_->GetWriters()) {
            WritingValueConsumers_.push_back(New<TWritingValueConsumer>(writer));
            valueConsumers.push_back(WritingValueConsumers_.back());
        }
        return valueConsumers;
    }

    void PrepareOutputTablePipes(TPipeFactory* pipeFactory)
    {
        YCHECK(pipeFactory);
        auto format = ConvertTo<TFormat>(TYsonString(UserJobSpec_.output_format()));

        const auto& writers = JobIO_->GetWriters();

        TableOutputs_.resize(writers.size());
        for (int i = 0; i < writers.size(); ++i) {
            auto valueConsumers = CreateValueConsumers();
            std::unique_ptr<IYsonConsumer> consumer(new TTableConsumer(valueConsumers, i));
            auto parser = CreateParserForFormat(format, EDataType::Tabular, consumer.get());
            TableOutputs_[i].reset(new TTableOutput(
                std::move(parser),
                std::move(consumer)));

            int jobDescriptor = UserJobSpec_.use_yamr_descriptors()
                ? 3 + i
                : 3 * i + 1;

            auto reader = PrepareOutputPipe(pipeFactory->Create(), jobDescriptor, TableOutputs_[i].get());
            TablePipeReaders_.push_back(reader);
        }

        FinalizeActions_.push_back(BIND([=] () {
            for (auto valueConsumer : WritingValueConsumers_) {
                valueConsumer->Flush();
            }

            for (auto writer : JobIO_->GetWriters()) {
                auto error = WaitFor(writer->Close());
                THROW_ERROR_EXCEPTION_IF_FAILED(error, "Error closing table output");
            }
        }));
    }

    TAsyncReaderPtr PrepareOutputPipe(TPipe pipe, int jobDescriptor, TOutputStream* output)
    {
        Process_.AddDup2FileAction(pipe.WriteFD, jobDescriptor);

        Process_.AddArguments({ "--prepare-pipe", ::ToString(jobDescriptor) });

        SafeMakeNonblocking(pipe.ReadFD);

        auto asyncInput = New<TAsyncReader>(pipe.ReadFD);

        OutputActions_.push_back(BIND([=] () {
            SafeClose(pipe.WriteFD);
            auto input = CreateSyncAdapter(asyncInput);
            PipeInputToOutput(input.get(), output, BufferSize);
        }));

        return asyncInput;
    }

    void PrepareInputTablePipe(
        TPipe pipe,
        int jobDescriptor,
        ISchemalessMultiChunkReaderPtr reader,
        const TFormat& format)
    {

        Process_.AddDup2FileAction(pipe.ReadFD, jobDescriptor);

        Process_.AddArguments({ "--prepare-pipe", ::ToString(jobDescriptor) });

        SafeMakeNonblocking(pipe.WriteFD);
        auto asyncOutput = New<TAsyncWriter>(pipe.WriteFD);
        TablePipeWriters_.push_back(asyncOutput);
        auto output = CreateSyncAdapter(asyncOutput);
        auto bufferRowCount = Config_->JobIO->BufferRowCount;

        auto writer = CreateSchemalessWriterForFormat(
            format,
            reader->GetNameTable(),
            std::move(output),
            true,
            Config_->JobIO->EnableInputTableIndex,
            JobIO_->IsKeySwitchEnabled(),
            reader->GetKeyColumns().size());

        FormatWriters_.push_back(writer);

        InputActions_.push_back(BIND([=] () {
            PipeReaderToWriter(reader, writer, bufferRowCount);

            auto error = WaitFor(asyncOutput->Close());
            if (!error.IsOK()) {
                THROW_ERROR_EXCEPTION("Table input pipe failed")
                    << TErrorAttribute("fd", jobDescriptor);
            }
        }));

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
            // close pipe.ReadFD?
        }));
    }

    void PrepareInputTablePipes(TPipeFactory* pipeFactory)
    {
        YCHECK(pipeFactory);
        auto format = ConvertTo<TFormat>(TYsonString(UserJobSpec_.input_format()));
        const auto& readers = JobIO_->GetReaders();

        YCHECK(!UserJobSpec_.use_yamr_descriptors() || readers.size() == 1);

        for (int i = 0; i < readers.size(); ++i) {
            PrepareInputTablePipe(pipeFactory->Create(), 3 * i, readers[i], format);
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

        TPipeFactory pipeFactory(maxReservedDescriptor + 1);

        // Configure stderr pipe.
        PrepareOutputPipe(pipeFactory.Create(), STDERR_FILENO, CreateErrorOutput());

        PrepareOutputTablePipes(&pipeFactory);

        if (UserJobSpec_.use_yamr_descriptors()) {
            // This hack is to work around the fact that usual output pipe accepts a
            // single job descriptor, whilst yamr convention requires fds 1 and 3 to be the same.
            Process_.AddDup2FileAction(3, 1);
        } else {
            // Configure statistics output pipe.
            PrepareOutputPipe(pipeFactory.Create(), JobStatisticsFD, CreateStatisticsOutput());
        }

        PrepareInputTablePipes(&pipeFactory);

        // Close reserved descriptors.
        pipeFactory.Clear();

        LOG_DEBUG("Pipes initialized");
    }

    void PrepareCGroups()
    {
        if (!Config_->ForceEnableAccounting) {
            return;
        }

        try {
            {
                TGuard<TSpinLock> guard(FreezerLock_);
                Freezer_.Create();
                Process_.AddArguments({ "--cgroup", Freezer_.GetFullPath() });
            }

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

        size_t customStatisticsCount = 0;
        for (const auto& pair : Statistics_) {
            if (pair.first.has_prefix("/custom")) {
                if (pair.first.size() > MaxCustomStatisticNameLength)
                {
                    THROW_ERROR_EXCEPTION(
                        "Custom statistics name is too long: %v > %v",
                        pair.first.size(),
                        MaxCustomStatisticNameLength);
                }
                ++customStatisticsCount;
            }
        }

        if (customStatisticsCount > UserJobSpec_.custom_statistics_count_limit()) {
            THROW_ERROR_EXCEPTION(
                "Custom statistics count exceeded: %v > %v. Increate custom statistics count limit in the job spec",
                customStatisticsCount,
                UserJobSpec_.custom_statistics_count_limit());
        }
    }

    template <class T>
    void AddStatistic(const TYPath& path, const T& statistic)
    {
        TGuard<TSpinLock> guard(StatisticsLock_);
        Statistics_.AddComplex(path, statistic);
    }

    void FillCurrentDataStatistics(TStatistics& statistics) const
    {
        statistics.Add(
            "/user_job/time",
            static_cast<i64>(GetElapsedTime().MilliSeconds()));

        statistics.AddComplex("/data/input", GetDataStatistics(JobIO_->GetReaders()));

        int i = 0;
        for (const auto& writer : JobIO_->GetWriters()) {
            statistics.AddComplex(
                "/data/output/" + NYPath::ToYPathLiteral(i),
                writer->GetDataStatistics());
            ++i;
        }

        statistics.Add("/data/time", GetElapsedTime().MilliSeconds());
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

    virtual TStatistics GetStatistics() const override
    {
        TStatistics result;
        {
            TGuard<TSpinLock> guard(StatisticsLock_);
            result = Statistics_;
        }
        FillCurrentDataStatistics(result);
        return std::move(result);
    }

    void DoJobIO()
    {
        auto onIOError = BIND([=] (const TError& error) {
            if (error.IsOK() || error.FindMatching(NPipes::EErrorCode::Aborted)) {
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

        auto runActions = [&] (std::vector<TCallback<void()>>& actions) {
            std::vector<TFuture<void>> result;
            for (auto& action : actions) {
                auto asyncError = action
                    .AsyncVia(PipeIOQueue_->GetInvoker())
                    .Run();
                asyncError.Subscribe(onIOError);
                result.emplace_back(std::move(asyncError));
            }
            return result;
        };

        auto inputFutures = runActions(InputActions_);
        auto outputFutures = runActions(OutputActions_);

        // First, wait for job output pipes.
        // If job successfully completes or dies prematurely, they close automatically.
        // ToDo(psushin): extract into separate function (e.g. CombineAll?  )
        for (const auto& future : outputFutures) {
            WaitFor(future);
        }

        // Then, wait for job process to finish.
        // Theoretically, process may have closed its output pipes,
        // but still doing some computations.
        auto jobExitError = Process_.Wait();
        LOG_INFO(jobExitError, "Job process completed");
        onIOError.Run(jobExitError);

        // Abort input pipes unconditionally.
        // If job didn't read input to the end, pipe writer could be blocked,
        // because we didn't close the reader end.
        for (auto& writer : TablePipeWriters_) {
            writer->Abort();
        }

        // Make sure, that input pipes are also completed.
        for (const auto& future : inputFutures) {
            WaitFor(future);
        }
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
        // Warning: we can account here a ytserver process in executor mode memory consumption.
        // But this is not a problem because it does not consume much.
        for (int pid : pids) {
            try {
                i64 processRss = GetProcessRss(pid);
                LOG_DEBUG("PID: %v, RSS: %v",
                    pid,
                    processRss);
                rss += processRss;
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
            AddStatistic("/user_job/current_memory", statistics);

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
                Stroka freezerFullPath;
                {
                    TGuard<TSpinLock> guard(FreezerLock_);
                    freezerFullPath = Freezer_.GetFullPath();
                }

                RunKiller(freezerFullPath);
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
    const TUserJobSpec& UserJobSpec_,
    const TJobId& jobId,
    std::unique_ptr<IUserJobIO> userJobIO)
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
    const TUserJobSpec& UserJobSpec_,
    const TJobId& jobId,
    std::unique_ptr<IUserJobIO> userJobIO)
{
    THROW_ERROR_EXCEPTION("Streaming jobs are supported only under Linux");
}

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT

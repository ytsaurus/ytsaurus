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

#include <ytlib/table_client/helpers.h>
#include <ytlib/table_client/table_consumer.h>
#include <ytlib/table_client/schemaless_chunk_reader.h>
#include <ytlib/table_client/schemaless_chunk_writer.h>
#include <ytlib/table_client/schemaful_reader_adapter.h>
#include <ytlib/table_client/schemaful_writer_adapter.h>

#include <ytlib/scheduler/statistics.h>

#include <ytlib/formats/parser.h>

#include <ytlib/transaction_client/public.h>

#include <ytlib/cgroup/cgroup.h>

#include <ytlib/query_client/public.h>
#include <ytlib/query_client/evaluator.h>
#include <ytlib/query_client/plan_fragment.h>
#include <ytlib/query_client/query_statistics.h>

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
#include <core/concurrency/periodic_executor.h>

#include <core/misc/public.h>

#include <util/folder/dirut.h>

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
using namespace NQueryClient;
using namespace NExecAgent;

using NJobTrackerClient::NProto::TJobResult;
using NJobTrackerClient::NProto::TJobSpec;
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
        , CumulativeMemoryUsageMbSec_(0)
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

        JobIO_->Init();

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

        FillCurrentDataStatistics(Statistics_);
        AddStatistic("/user_job/woodpecker", IsWoodpecker_ ? 1 : 0);
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
        const auto& reader = JobIO_->GetReader();
        if (!reader) {
            return 0;
        }

        i64 total = reader->GetTotalRowCount();
        i64 current = reader->GetSessionRowIndex();

        if (total == 0) {
            return 0.0;
        }

        return std::min(current / static_cast<double>(total), 1.0);
    }

    virtual std::vector<TChunkId> GetFailedChunkIds() const override
    {
        std::vector<TChunkId> failedChunks;
        const auto& reader = JobIO_->GetReader();
        if (reader) {
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
    std::atomic<bool> IsWoodpecker_ = { false };

    std::atomic_flag Stracing_ = ATOMIC_FLAG_INIT;

    i64 MemoryUsage_;
    i64 CumulativeMemoryUsageMbSec_;

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
        Process_.AddArguments({"--working-dir", SandboxDirectoryNames[ESandboxIndex::User]});

        if (UserJobSpec_.enable_core_dump()) {
            Process_.AddArgument("--enable-core-dump");
        }

        if (Config_->UserId) {
            Process_.AddArguments({"--uid", ::ToString(*Config_->UserId)});
        }

        // Init environment variables.
        TPatternFormatter formatter;
        formatter.AddProperty("SandboxPath", NFS::CombinePaths(GetCwd(), SandboxDirectoryNames[ESandboxIndex::User]));

        for (int i = 0; i < UserJobSpec_.environment_size(); ++i) {
            Process_.AddEnvVar(formatter.Format(UserJobSpec_.environment(i)));
        }
    }

    void CleanupCGroups()
    {
        if (!Config_->EnableCGroups) {
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

        if (Config_->IsCGroupSupported(TCpuAccounting::Name)) {
            auto cpuAccountingStats = CpuAccounting_.GetStatistics();
            AddStatistic("/user_job/cpu", cpuAccountingStats);
            CpuAccounting_.Destroy();
        }

        if (Config_->IsCGroupSupported(TBlockIO::Name)) {
            auto blockIOStats = BlockIO_.GetStatistics();
            AddStatistic("/user_job/block_io", blockIOStats);
            BlockIO_.Destroy();
        }

        if (Config_->IsCGroupSupported(TMemory::Name)) {
            AddStatistic("/user_job/max_memory", Memory_.GetMaxMemoryUsage());
            Memory_.ForceEmpty();
            Memory_.Destroy();
        }

        AddStatistic("/user_job/cumulative_memory_mb_sec", CumulativeMemoryUsageMbSec_);

        {
            TGuard<TSpinLock> guard(FreezerLock_);
            Freezer_.Destroy();
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
            host->GetClient(),
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
        if (errorChunkId) {
            ToProto(schedulerResultExt->mutable_stderr_chunk_id(), errorChunkId);
            LOG_INFO("Stderr chunk generated (ChunkId: %v)", errorChunkId);
        }
    }

    void DumpFailContexts(TSchedulerJobResultExt* schedulerResultExt)
    {
        auto contexts = DoGetInputContexts();
        auto contextChunkIds = DoDumpInputContexts(contexts);

        YCHECK(contextChunkIds.size() <= 1);
        if (!contextChunkIds.empty()) {
            ToProto(schedulerResultExt->mutable_fail_context_chunk_id(), contextChunkIds.front());
        }
    }

    virtual std::vector<TChunkId> DumpInputContext() override
    {
        if (!Prepared_) {
            THROW_ERROR_EXCEPTION("Cannot dump job context: job pipes haven't been prepared yet");
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
                host->GetClient(),
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
        if (!Prepared_) {
            THROW_ERROR_EXCEPTION("Job has not started yet");
        }

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
        int inputCount = 1;

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

    TAsyncReaderPtr PrepareOutputPipe(TPipe&& pipe, int jobDescriptor, TOutputStream* output)
    {
        Process_.AddDup2FileAction(pipe.GetWriteFD(), jobDescriptor);

        Process_.AddArguments({ "--prepare-pipe", ::ToString(jobDescriptor) });

        auto asyncInput = pipe.CreateAsyncReader();

        OutputActions_.push_back(BIND([=] (int writeFD) {
            SafeClose(writeFD, false);
            auto input = CreateSyncAdapter(asyncInput);
            PipeInputToOutput(input.get(), output, BufferSize);
        }, pipe.ReleaseWriteFD()));

        return asyncInput;
    }

    void PrepareInputActionsPassthrough(
        int jobDescriptor,
        const TFormat& format,
        TAsyncWriterPtr asyncOutput)
    {
        JobIO_->CreateReader();
        const auto& reader = JobIO_->GetReader();
        auto writer = CreateSchemalessWriterForFormat(
            format,
            reader->GetNameTable(),
            asyncOutput,
            true,
            Config_->JobIO->ControlAttributes->EnableKeySwitch,
            JobIO_->GetReduceKeyColumnCount());

        FormatWriters_.push_back(writer);

        auto bufferRowCount = Config_->JobIO->BufferRowCount;

        InputActions_.push_back(BIND([=] () {
            try {
                PipeReaderToWriter(
                    reader,
                    writer,
                    Config_->JobIO->ControlAttributes,
                    bufferRowCount);

                WaitFor(asyncOutput->Close())
                    .ThrowOnError();
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION("Table input pipe failed")
                        << TErrorAttribute("fd", jobDescriptor)
                        << ex;
            }
        }));
    }

    void PrepareInputActionsQuery(
        const TQuerySpec& spec,
        int jobDescriptor,
        const TFormat& format,
        TAsyncWriterPtr asyncOutput)
    {
        if (Config_->JobIO->ControlAttributes->EnableKeySwitch) {
            THROW_ERROR_EXCEPTION("enable_key_switch is not supported when query is set");
        }

        auto writerFactory = [=] (TNameTablePtr nameTable) {
            auto writer = CreateSchemalessWriterForFormat(
                format,
                nameTable,
                asyncOutput,
                true,
                false,
                0);

            FormatWriters_.push_back(writer);

            return writer;
        };

        auto readerFactory = JobIO_->GetReaderFactory();

        InputActions_.push_back(BIND([=] () {
            try {
                auto writer = CreateSchemafulWriterAdapter(writerFactory);
                auto query = FromProto(spec.query());
                std::vector<TUdfDescriptorPtr> descriptors;
                for (const auto& descriptor : FromProto<Stroka>(spec.udf_descriptors())) {
                    descriptors.push_back(ConvertTo<TUdfDescriptorPtr>(TYsonString(descriptor)));
                }
                auto registry = CreateJobFunctionRegistry(descriptors, SandboxDirectoryNames[ESandboxIndex::Udf]);
                auto evaluator = New<TEvaluator>(New<TExecutorConfig>());
                auto reader = WaitFor(CreateSchemafulReaderAdapter(readerFactory, query->TableSchema))
                    .ValueOrThrow();

                evaluator->Run(query, reader, writer, registry, true);
                WaitFor(asyncOutput->Close())
                    .ThrowOnError();
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION("Query evaluation failed")
                    << TErrorAttribute("fd", jobDescriptor)
                    << ex;
            }
        }));
    }

    void PrepareInputTablePipe(TPipeFactory* pipeFactory)
    {
        YCHECK(pipeFactory);
        auto pipe = pipeFactory->Create();
        int jobDescriptor = 0;

        Process_.AddDup2FileAction(pipe.GetReadFD(), jobDescriptor);
        Process_.AddArguments({ "--prepare-pipe", ::ToString(jobDescriptor) });

        auto format = ConvertTo<TFormat>(TYsonString(UserJobSpec_.input_format()));
        auto asyncOutput = pipe.CreateAsyncWriter();
        TablePipeWriters_.push_back(asyncOutput);

        // NB: we do not bother to close it. Anyway, job proxy process would not live long.
        auto readFD = pipe.ReleaseReadFD();

        auto host = Host.Lock();
        YCHECK(host);

        auto jobSpec = host->GetJobSpec().GetExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
        if (jobSpec.has_input_query_spec()) {
            PrepareInputActionsQuery(jobSpec.input_query_spec(), jobDescriptor, format, asyncOutput);
        } else {
            PrepareInputActionsPassthrough(jobDescriptor, format, asyncOutput);
        }

        if (!UserJobSpec_.check_input_fully_consumed()) {
            return;
        }

        FinalizeActions_.push_back(BIND([=] (int readFD) {
            char buffer;
            // Try to read some data from the pipe.
            ssize_t res = ::read(readFD, &buffer, 1);
            if (res > 0) {
                THROW_ERROR_EXCEPTION("Input stream was not fully consumed by user process")
                    << TErrorAttribute("fd", jobDescriptor);
            }
            YCHECK(TryClose(readFD, false));
        }, readFD));
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

        PrepareInputTablePipe(&pipeFactory);

        // Close reserved descriptors.
        pipeFactory.Clear();

        LOG_DEBUG("Pipes initialized");
    }

    void PrepareCGroups()
    {
        if (!Config_->EnableCGroups) {
            return;
        }

        try {
            {
                TGuard<TSpinLock> guard(FreezerLock_);
                Freezer_.Create();
                Process_.AddArguments({ "--cgroup", Freezer_.GetFullPath() });
            }

            if (Config_->IsCGroupSupported(TCpuAccounting::Name)) {
                CpuAccounting_.Create();
                Process_.AddArguments({ "--cgroup", CpuAccounting_.GetFullPath() });
                Process_.AddEnvVar(Format("YT_CGROUP_CPUACCT=%v", CpuAccounting_.GetFullPath()));
            }

            if (Config_->IsCGroupSupported(TBlockIO::Name)) {
                BlockIO_.Create();
                Process_.AddArguments({ "--cgroup", BlockIO_.GetFullPath() });
                Process_.AddEnvVar(Format("YT_CGROUP_BLKIO=%v", BlockIO_.GetFullPath()));
            }

            if (Config_->IsCGroupSupported(TMemory::Name)) {
                Memory_.Create();
                Process_.AddArguments({ "--cgroup", Memory_.GetFullPath() });
                Process_.AddEnvVar(Format("YT_CGROUP_MEMORY=%v", Memory_.GetFullPath()));
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
        const auto& reader = JobIO_->GetReader();
        if (reader) {
            statistics.AddComplex("/data/input", reader->GetDataStatistics());
        }

        int i = 0;
        for (const auto& writer : JobIO_->GetWriters()) {
            statistics.AddComplex(
                "/data/output/" + NYPath::ToYPathLiteral(i),
                writer->GetDataStatistics());
            ++i;
        }
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

        CumulativeMemoryUsageMbSec_ += (rss / (1024 * 1024)) * Config_->MemoryWatchdogPeriod.Seconds();

        i64 memoryLimit = UserJobSpec_.memory_limit();
        LOG_DEBUG("Check memory usage (Rss: %v, MemoryLimit: %v)",
            rss,
            memoryLimit);

        if (rss > MemoryUsage_) {
            UpdateMemoryUsage(rss);
        }

        if (rss > memoryLimit) {
            JobErrorPromise_.TrySet(TError(EErrorCode::MemoryLimitExceeded, "Memory limit exceeded")
                << TErrorAttribute("rss", rss)
                << TErrorAttribute("limit", memoryLimit));

            if (!Config_->EnableCGroups) {
                // TODO(psushin): If someone wanted to use
                // YT without cgroups in production than one need to
                // implement kill by uid here
                return;
            }

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
        if (!BlockIO_.IsCreated()) {
            return;
        }

        auto period = Config_->BlockIOWatchdogPeriod;
        auto servicedIOs = BlockIO_.GetIOServiced();

        for (const auto& item : servicedIOs) {
            LOG_DEBUG("%v %v operation for %v device", item.Value, item.Type, item.DeviceId);

            auto previousItemIt = std::find_if(
                LastServicedIOs_.begin(),
                LastServicedIOs_.end(),
                [=] (const TBlockIO::TStatisticsItem& other) {
                    return item.DeviceId == other.DeviceId  && item.Type == other.Type;
                });

            i64 deltaOperations = item.Value;
            if (previousItemIt != LastServicedIOs_.end()) {
                deltaOperations -= previousItemIt->Value;
            }

            if (deltaOperations < 0) {
                LOG_WARNING("%v < 0 operations were serviced for %v device since the last check",
                    deltaOperations,
                    item.DeviceId);
            }

            if (deltaOperations > UserJobSpec_.iops_threshold() * period.Seconds()) {
                LOG_DEBUG("Woodpecker detected (DeviceId: %v)", item.DeviceId);
                IsWoodpecker_ = true;
                if (Config_->EnableIopsThrottling) {
                    BlockIO_.ThrottleOperations(item.DeviceId, UserJobSpec_.iops_threshold());
                } 
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

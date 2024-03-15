
#include <library/cpp/yt/memory/new.h>

#include <library/cpp/yt/logging/logger.h>

#include <yt/cpp/mapreduce/interface/errors.h>
#include <yt/cpp/mapreduce/interface/operation.h>

#include <yt/cpp/mapreduce/interface/logging/logger.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/misc/config.h>

#include <yt/yt/core/bus/tcp/client.h>
#include <yt/yt/core/bus/tcp/config.h>
#include <yt/yt/core/rpc/bus/channel.h>

#include <yt/systest/retrier.h>
#include <yt/systest/sort_dataset.h>
#include <yt/systest/util.h>
#include <yt/systest/validator.h>
#include <yt/systest/validator_job.h>
#include <yt/systest/validator_service.h>

#include <yt/systest/table_dataset.h>

#include <util/stream/file.h>

#include <libgen.h>
#include <stdlib.h>
#include <unistd.h>

namespace NYT::NTest {

template <typename TResult>
class TRetrier;

template <typename TProtoResponse>
using TRpcResult = typename NRpc::TTypedClientResponse<TProtoResponse>::TResult;

template <typename TProtoResponse>
using TRpcRetrier = TRetrier<TRpcResult<TProtoResponse>>;

///////////////////////////////////////////////////////////////////////////////

static bool IsPermanentWorkerError(TError& error)
{
    // If connection refused, worker died and will not listen on that port again.
    return error.FindMatching(TErrorCode(4200 + ECONNREFUSED)) != std::nullopt;
}

static void AnnotateError(
    TError& error,
    const TString& name,
    const TString& hostport,
    const TString& path,
    int attempt,
    std::optional<int> intervalIndex = std::nullopt,
    std::optional<int> numIntervals = std::nullopt)
{
    auto* attrs = error.MutableAttributes();
    attrs->Set("name", name);
    attrs->Set("hostport", hostport);
    attrs->Set("path", path);
    attrs->Set("attempt", attempt);
    if (intervalIndex) {
        attrs->Set("interval_index", *intervalIndex);
    }
    if (numIntervals) {
        attrs->Set("num_intervals", *numIntervals);
    }
}

static void PopulateTableInterval(
    const TString& path,
    int64_t start,
    int64_t limit,
    NProto::TTableInterval* output)
{
    output->set_table_path(path);
    output->set_start_row_index(start);
    output->set_limit_row_index(limit);
}

static int64_t GetInt64Node(NApi::IClientPtr rpcClient, const TString& path)
{
    auto value = NConcurrency::WaitFor(rpcClient->GetNode(path)).ValueOrThrow();
    return NYTree::ConvertTo<int64_t>(value);
}

///////////////////////////////////////////////////////////////////////////////

TFuture<TString> TValidator::StartMapInterval(
    const TString& targetName,
    int retryAttempt,
    int intervalIndex,
    int numIntervals,
    const TString& tablePath,
    int64_t start,
    int64_t limit,
    TDuration timeout,
    const TTable& table,
    const IMultiMapper& mapper)
{
    NYT::NLogging::TLogger Logger("test");

    TString outputPath = TestHome_.CreateIntervalPath(targetName, intervalIndex, retryAttempt);

    auto token = WorkerSet_.AcquireWorker();
    const TString& hostport = token.HostPort();

    YT_LOG_INFO("Start MapInterval (Worker: %v, SourcePath: %v, RowRange: [%v, %v), Interval: %v/%v, TargetPath: %v)",
        hostport, tablePath, start, limit, intervalIndex, numIntervals, outputPath);

    auto client = CreateBusClient(NBus::TBusClientConfig::CreateTcp(hostport));
    auto channel = NRpc::NBus::CreateBusChannel(client);

    TValidatorProxy proxy(channel);

    auto request = proxy.MapInterval();
    PopulateTableInterval(tablePath, start, limit, request->mutable_input());

    request->set_output_path(outputPath);
    ToProto(request->mutable_map_spec()->mutable_table(), table);
    mapper.ToProto(request->mutable_map_spec()->mutable_operation());

    return request->Invoke()
        .WithTimeout(timeout)
        .ApplyUnique(BIND(
        [=, token = std::move(token)] (TErrorOr<TRpcResult<NProto::TRspMapInterval>> &&result) mutable {
            YT_LOG_INFO("Done MapInterval (Worker: %v, SourcePath: %v, Interval: %v/%v, Status: %v)",
                hostport,
                tablePath,
                intervalIndex,
                numIntervals,
                result);
            if (!result.IsOK()) {
                token.MarkFailure(IsPermanentWorkerError(result));
                AnnotateError(
                    result,
                    "MapInterval",
                    hostport,
                    tablePath,
                    retryAttempt,
                    intervalIndex,
                    numIntervals);
                result.ThrowOnError();
            }
            return outputPath;
        }));
}

TFuture<TString> TValidator::StartReduceInterval(
    const TString& targetName,
    int retryAttempt,
    int intervalIndex,
    int numIntervals,
    const TString& tablePath,
    int64_t start,
    int64_t limit,
    TDuration timeout,
    const TTable& table,
    const TReduceOperation& operation)
{
    NYT::NLogging::TLogger Logger("test");
    auto token = WorkerSet_.AcquireWorker();
    const TString& hostport = token.HostPort();

    TString outputPath = TestHome_.CreateIntervalPath(targetName, intervalIndex, retryAttempt);

    YT_LOG_INFO("Start ReduceInterval (Worker: %v, SourcePath: %v, RowRange: [%v %v), Interval: %v/%v, TargetPath: %v)",
        hostport, tablePath, start, limit, intervalIndex, numIntervals, outputPath);

    auto client = CreateBusClient(NBus::TBusClientConfig::CreateTcp(hostport));
    auto channel = NRpc::NBus::CreateBusChannel(client);

    TValidatorProxy proxy(channel);

    auto request = proxy.ReduceInterval();
    PopulateTableInterval(tablePath, start, limit, request->mutable_input());

    request->set_output_path(outputPath);
    ToProto(request->mutable_reduce_spec()->mutable_table(), table);
    for (const TString& column : operation.ReduceBy) {
        request->mutable_reduce_spec()->add_reduce_by(column);
    }
    operation.Reducer->ToProto(request->mutable_reduce_spec()->mutable_operation());

    return request->Invoke()
        .WithTimeout(timeout)
        .ApplyUnique(BIND(
        [=, token = std::move(token)] (TErrorOr<TRpcResult<NProto::TRspReduceInterval>> &&result) mutable {
            YT_LOG_INFO("Done ReduceInterval (Worker: %v, SourcePath: %v, Interval: %v/%v, Status: %v)",
                hostport,
                tablePath,
                intervalIndex,
                numIntervals,
                result);
            if (!result.IsOK()) {
                token.MarkFailure(IsPermanentWorkerError(result));
                AnnotateError(
                    result,
                    "ReduceInterval",
                    hostport,
                    tablePath,
                    retryAttempt,
                    intervalIndex,
                    numIntervals);
                result.ThrowOnError();
            }
            return outputPath;
        }));
}

TFuture<TString> TValidator::StartSortInterval(
    const TString& targetName,
    int retryAttempt,
    int intervalIndex,
    int numIntervals,
    const TString& tablePath,
    int64_t start,
    int64_t limit,
    TDuration timeout,
    const TTable& table,
    const TSortOperation& operation)
{
    NYT::NLogging::TLogger Logger("test");
    auto token = WorkerSet_.AcquireWorker();
    const TString& hostport = token.HostPort();

    TString outputPath = TestHome_.CreateIntervalPath(targetName, intervalIndex, retryAttempt);

    YT_LOG_INFO("Start SortInterval (Worker: %v, SourcePath: %v, RowRange: [%v, %v), Interval: %v/%v, TargetPath: %v)",
        hostport, tablePath, start, limit, intervalIndex, numIntervals, outputPath);

    auto client = CreateBusClient(NBus::TBusClientConfig::CreateTcp(hostport));
    auto channel = NRpc::NBus::CreateBusChannel(client);

    TValidatorProxy proxy(channel);

    auto request = proxy.SortInterval();
    PopulateTableInterval(tablePath, start, limit, request->mutable_input());

    request->set_output_path(outputPath);
    ToProto(request->mutable_sort_spec()->mutable_table(), table);

    for (const auto& column : operation.SortBy) {
        request->mutable_sort_spec()->add_sort_by(column);
    }

    return request->Invoke()
        .WithTimeout(timeout)
        .ApplyUnique(BIND(
        [=, token = std::move(token)] (TErrorOr<TRpcResult<NProto::TRspSortInterval>> &&result) mutable {
            YT_LOG_INFO("Done SortInterval (Worker: %v, SourcePath: %v,Interval: %v/%v, Status: %v)",
                hostport,
                tablePath,
                intervalIndex,
                numIntervals,
                result);
            if (!result.IsOK()) {
                token.MarkFailure(IsPermanentWorkerError(result));
                AnnotateError(
                    result,
                    "SortInterval",
                    hostport,
                    tablePath,
                    retryAttempt,
                    intervalIndex,
                    numIntervals);
                result.ThrowOnError();
            }
            return outputPath;
        }));
}

TFuture<typename NRpc::TTypedClientResponse<NProto::TRspCompareInterval>::TResult>
TValidator::StartCompareInterval(
    int retryAttempt,
    const TString& targetPath,
    const TString& intervalPath,
    int64_t start,
    int64_t limit,
    int intervalIndex,
    int numIntervals,
    TDuration timeout)
{
    NYT::NLogging::TLogger Logger("test");
    auto token = WorkerSet_.AcquireWorker();
    const TString& hostport = token.HostPort();

    YT_LOG_INFO("Start CompareInterval (Worker: %v, IntervalPath: %v, RowRange: [%v, %v), Interval: %v/%v, TargetPath: %v)",
        hostport, intervalPath, start, limit, intervalIndex, numIntervals, targetPath);

    auto client = CreateBusClient(NBus::TBusClientConfig::CreateTcp(hostport));
    auto channel = NRpc::NBus::CreateBusChannel(client);

    TValidatorProxy proxy(channel);

    auto request = proxy.CompareInterval();
    request->set_target_path(targetPath);
    request->set_interval_path(intervalPath);
    request->set_start_row_index(start);
    request->set_limit_row_index(limit);

    return request->Invoke()
        .WithTimeout(timeout)
        .ApplyUnique(BIND(
        [=, token = std::move(token)] (TErrorOr<TRpcResult<NProto::TRspCompareInterval>> &&result) mutable {
            YT_LOG_INFO("Done CompareInterval (Worker: %v, Interval: %v/%v, Status: %v)",
                hostport,
                intervalIndex,
                numIntervals,
                result);
            if (!result.IsOK()) {
                token.MarkFailure(IsPermanentWorkerError(result));
                AnnotateError(
                    result,
                    "CompareInterval",
                    hostport,
                    intervalPath,
                    retryAttempt,
                    intervalIndex,
                    numIntervals);
                result.ThrowOnError();
            }
            return result.ValueOrThrow();
        }));
}

TFuture<typename NRpc::TTypedClientResponse<NProto::TRspMergeSortedAndCompare>::TResult>
TValidator::StartMergeSortedAndCompare(
    int retryAttempt,
    const std::vector<TString>& intervalPath,
    TDuration timeout,
    const TString& targetPath,
    const TTable& table)
{
    NYT::NLogging::TLogger Logger("test");
    auto token = WorkerSet_.AcquireWorker();
    const TString& hostport = token.HostPort();

    YT_LOG_INFO("Start MergeSortedAndCompare (Worker: %v, NumIntervals: %v, TargetPath: %v)",
        hostport, std::ssize(intervalPath), targetPath);

    auto busConfig = NBus::TBusClientConfig::CreateTcp(hostport);
    busConfig->ReadStallTimeout = TDuration::Seconds(10);

    auto client = CreateBusClient(busConfig);
    auto channel = NRpc::NBus::CreateBusChannel(client);

    TValidatorProxy proxy(channel);

    auto request = proxy.MergeSortedAndCompare();

    request->set_target_path(targetPath);
    for (const auto& path : intervalPath) {
        request->add_interval_path(path);
    }
    ToProto(request->mutable_table(), table);

    return request->Invoke()
        .WithTimeout(timeout)
        .ApplyUnique(BIND(
        [=, token = std::move(token)] (TErrorOr<TRpcResult<NProto::TRspMergeSortedAndCompare>> &&result) mutable {
            YT_LOG_INFO("Done MergeSortedAndCompare (Worker: %v, NumIntervals: %v, Status: %v)",
                hostport, std::ssize(intervalPath), result);
            if (!result.IsOK()) {
                token.MarkFailure(IsPermanentWorkerError(result));
                AnnotateError(
                    result,
                    "MergeSortedAndCompare",
                    hostport,
                    targetPath,
                    retryAttempt);
                result.ThrowOnError();
            }
            return result.ValueOrThrow();
        }));
}

///////////////////////////////////////////////////////////////////////////////

TValidator::TValidator(
    const TString& pool,
    TValidatorConfig config,
    IClientPtr client,
    NApi::IClientPtr rpcClient,
    TTestHome& testHome)
    : Pool_(pool)
    , Config_(config)
    , Client_(client)
    , RpcClient_(rpcClient)
    , TestHome_(testHome)
    , ThreadPool_(NConcurrency::CreateThreadPool(1, "validator_service_poller"))
    , WorkerSet_(
        ThreadPool_->GetInvoker(),
        [this]() { return PollWorkers(); },
        config.PollDelay,
        config.WorkerFailureBackoffDelay
      )
    , Logger("test")
{
}

std::vector<TString> TValidator::PollWorkers()
{
    auto dir = TestHome_.ValidatorsDir();
    auto listResult = NConcurrency::WaitFor(RpcClient_->ListNode(dir)).ValueOrThrow();
    auto hostports = NYTree::ConvertTo<std::vector<NYTree::IStringNodePtr>>(listResult);

    std::vector<TString> result;
    for (auto& hostport : hostports) {
        result.push_back(hostport->GetValue());
    }

    return result;
}

void TValidator::Start()
{
    auto dir = TestHome_.ValidatorsDir();
    TDuration backoffTime = TDuration::MicroSeconds(0);

    WorkerSet_.Start();

    bool success = false;
    for (int iteration = 0; !success ; ++iteration) {
        NConcurrency::TDelayedExecutor::WaitForDuration(backoffTime);
        try {
            StartValidatorOperation();
            success = true;
        } catch (const TErrorResponse& exception) {
            if (IsRetriableError(exception)) {
                backoffTime = std::min(TDuration::Seconds(2) * iteration, TDuration::Seconds(30));
            }
            YT_LOG_ERROR("Failed to start validator operation, will backoff "
                "(Error: %v, Backoff: %v)", yexception(exception), backoffTime);
        }
    }
}

TStoredDataset TValidator::CompareIntervals(
    const TString& targetPath,
    std::vector<TFuture<TString>>& intervalResult)
{
    const int numIntervals = std::ssize(intervalResult);

    std::vector<TFuture<TRpcResult<NProto::TRspCompareInterval>>> compareIntervalResult;
    std::vector<std::unique_ptr<TRpcRetrier<NProto::TRspCompareInterval>>> compareRetrier;

    int64_t startInterval = 0;
    for (int index = 0; index < numIntervals; ++index) {
        TString path = NConcurrency::WaitFor(intervalResult[index]).ValueOrThrow();

        int64_t intervalRowCount = GetInt64Node(RpcClient_, path + "/@row_count");

        YT_LOG_INFO("Interval generated (Index: %v, IntervalRowCount: %v, NumIntervals: %v, Path: %v)",
            index, intervalRowCount, numIntervals, path);

        const auto rowStart = startInterval;
        const auto rowLimit = startInterval + intervalRowCount;
        const auto timeout = Config_.BaseTimeout + Config_.IntervalTimeout;
        compareRetrier.push_back(std::make_unique<TRpcRetrier<NProto::TRspCompareInterval>>(
             [=, this] (int attempt) {
                return StartCompareInterval(
                    attempt,
                    targetPath,
                    path,
                    rowStart,
                    rowLimit,
                    index,
                    numIntervals,
                    timeout);
            }));

        compareIntervalResult.push_back(compareRetrier.back()->Run());
        startInterval = rowLimit;
    }

    for (int index = 0; index < std::ssize(compareIntervalResult); ++index) {
        auto result = NConcurrency::WaitFor(compareIntervalResult[index]).ValueOrThrow();
    }

    int64_t totalSize = GetInt64Node(RpcClient_, targetPath + "/@uncompressed_data_size");

    return TStoredDataset{
        targetPath,
        startInterval,
        totalSize
    };
}

TStoredDataset TValidator::VerifyMap(
    const TString& targetName,
    const TString& sourcePath,
    const TString& targetPath,
    const TTable& sourceTable,
    const IMultiMapper& mapper)
{
    const auto intervalInfo = GetMapIntervalBoundaries(sourcePath, Config_.IntervalBytes);
    const int numIntervals = std::ssize(intervalInfo.Boundaries) - 1;

    YT_LOG_INFO("Will validate map operation (SourcePath: %v, TargetPath: %v, NumIntervals: %v)",
        sourcePath,
        targetPath,
        numIntervals);

    std::vector<TFuture<TString>> mapIntervalResult;
    std::vector<std::unique_ptr<TRetrier<TString>>> mapRetrier;
    for (int index = 0; index < numIntervals; ++index) {
        const auto rowStart = intervalInfo.Boundaries[index];
        const auto rowLimit = intervalInfo.Boundaries[index + 1];
        const auto timeout = Config_.BaseTimeout + Config_.IntervalTimeout;
        mapRetrier.push_back(std::make_unique<TRetrier<TString>>(
            [=, &sourceTable, &mapper, this](int attempt) {
                return StartMapInterval(
                    targetName,
                    attempt,
                    index,
                    numIntervals,
                    sourcePath,
                    rowStart,
                    rowLimit,
                    timeout,
                    sourceTable,
                    mapper);
                }));

        mapIntervalResult.push_back(mapRetrier.back()->Run());
    }

    auto storedDataset = CompareIntervals(targetPath, mapIntervalResult);

    YT_LOG_INFO("Validated map operation (SourcePath: %v, TargetPath: %v, NumIntervals: %v, "
        "NumRecords: %v, Bytes: %v)",
        sourcePath,
        targetPath,
        numIntervals,
        storedDataset.TotalRecords,
        storedDataset.TotalBytes);

    return storedDataset;
}

TStoredDataset TValidator::VerifyReduce(
    const TString& targetName,
    const TString& sourcePath,
    const TString& targetPath,
    const TTable& sourceTable,
    const TReduceOperation& reduceOperation)
{
    auto intervalInfo = GetReduceIntervalBoundaries(sourceTable, sourcePath, Config_.IntervalBytes);
    const int numIntervals = std::ssize(intervalInfo.Boundaries) - 1;

    YT_LOG_INFO("Will validate reduce operation (SourcePath: %v, TargetPath: %v, NumIntervals: %v, "
        "ReduceColumnCount: %v",
        sourcePath,
        targetPath,
        numIntervals,
        std::ssize(reduceOperation.ReduceBy));

    std::vector<TFuture<TString>> reduceIntervalResult;
    std::vector<std::unique_ptr<TRetrier<TString>>> reduceRetrier;

    for (int index = 0; index < numIntervals; ++index) {
        const auto rowStart = intervalInfo.Boundaries[index];
        const auto rowLimit = intervalInfo.Boundaries[index + 1];
        const auto timeout = Config_.BaseTimeout + Config_.IntervalTimeout;
        reduceRetrier.push_back(std::make_unique<TRetrier<TString>>(
            [=, &sourceTable, &reduceOperation, this](int attempt) {
                return StartReduceInterval(
                    targetName,
                    attempt,
                    index,
                    numIntervals,
                    sourcePath,
                    rowStart,
                    rowLimit,
                    timeout,
                    sourceTable,
                    reduceOperation
                );
            }));
        reduceIntervalResult.push_back(reduceRetrier.back()->Run());
    }

    auto storedDataset = CompareIntervals(targetPath, reduceIntervalResult);

    YT_LOG_INFO("Validated reduce operation (SourcePath: %v, TargetPath: %v, NumIntervals: %v, "
        "NumRecords: %v, Bytes: %v)",
        sourcePath,
        targetPath,
        numIntervals,
        storedDataset.TotalRecords,
        storedDataset.TotalBytes);

    return storedDataset;
}

TStoredDataset TValidator::VerifySort(
    const TString& targetName,
    const TString& sourcePath,
    const TString& targetPath,
    const TTable& sourceTable,
    const TSortOperation& operation)
{
    const auto intervalInfo = GetMapIntervalBoundaries(sourcePath, Config_.IntervalBytes);
    if (intervalInfo.RowCount == 0) {
        int64_t targetRowCount = GetInt64Node(RpcClient_, targetPath + "/@row_count");
        if (targetRowCount != 0) {
            THROW_ERROR_EXCEPTION("Sort source %v is empty, but target %v contains %v rows",
                sourcePath, targetPath, targetRowCount);
        }
        return TStoredDataset{
            targetPath,
            0,
            0
        };
    }
    YT_VERIFY(std::ssize(intervalInfo.Boundaries) > 1);

    const int numIntervals = std::ssize(intervalInfo.Boundaries) - 1;

    if (intervalInfo.TotalBytes > Config_.SortVerificationLimit) {
        YT_LOG_INFO("Skip validating sort operation (SourcePath: %v, TargetPath: %v, TotalBytes: %v)",
            sourcePath,
            targetPath,
            intervalInfo.TotalBytes);

        return TStoredDataset{
            targetPath,
            intervalInfo.Boundaries[numIntervals],
            intervalInfo.TotalBytes
        };
    }

    YT_LOG_INFO("Will validate sort operation (SourcePath: %v, TargetPath: %v, NumIntervals: %v)",
        sourcePath,
        targetPath,
        numIntervals);

    std::vector<TFuture<TString>> sortIntervalResult;
    std::vector<std::unique_ptr<TRetrier<TString>>> sortRetrier;

    for (int index = 0; index < numIntervals; index++) {
        const auto rowStart = intervalInfo.Boundaries[index];
        const auto rowLimit = intervalInfo.Boundaries[index + 1];
        const auto timeout = Config_.BaseTimeout + Config_.IntervalTimeout;
        sortRetrier.push_back(std::make_unique<TRetrier<TString>>(
            [=, &sourceTable, &operation, this](int attempt) {
                return StartSortInterval(
                    targetName,
                    attempt,
                    index,
                    numIntervals,
                    sourcePath,
                    rowStart,
                    rowLimit,
                    timeout,
                    sourceTable,
                    operation);
            }));
        sortIntervalResult.push_back(sortRetrier.back()->Run());
    }

    std::vector<TString> intervalPath =
        NConcurrency::WaitFor(AllSucceeded(sortIntervalResult)).ValueOrThrow();

    TTable sortedTable;
    ApplySortOperation(sourceTable, operation, &sortedTable);

    const auto mergeTimeout = Config_.BaseTimeout +
        numIntervals * trunc(log(numIntervals) + 1 + 1e-9) * Config_.IntervalTimeout;
    TRpcRetrier<NProto::TRspMergeSortedAndCompare> mergeRetrier(
        [=, &sortedTable, this](int attempt) {
            return StartMergeSortedAndCompare(
                attempt,
                intervalPath,
                mergeTimeout,
                targetPath,
                sortedTable);
        });

    NConcurrency::WaitFor(mergeRetrier.Run()).ThrowOnError();

    return TStoredDataset{
        targetPath,
        intervalInfo.Boundaries[numIntervals],
        intervalInfo.TotalBytes
    };

    return TStoredDataset{
        targetPath,
        intervalInfo.Boundaries[numIntervals],
        intervalInfo.TotalBytes
    };
}

void TValidator::Stop()
{
    YT_LOG_INFO("Stopping operation %v", Operation_->GetId().AsUuidString());
    Operation_->AbortOperation();
    WorkerSet_.Stop();
    YT_LOG_INFO("Validator stopped");
}

void TValidator::StartValidatorOperation()
{
    auto dir = TestHome_.ValidatorsDir();

    auto userSpec = TUserJobSpec()
        .AddEnvironment("YT_PROXY", getenv("YT_PROXY"))
        .AddEnvironment("YT_LOG_LEVEL", "info")
        .MemoryLimit(Config_.MemoryLimit);

    auto secureEnv = TNode::CreateMap({
        {"YT_TOKEN", NYT::TConfig::Get()->Token}
    });

    TOperationOptions options;
    options.StartOperationMode(TOperationOptions::EStartOperationMode::SyncStart);
    options.SecureVault(secureEnv);

    auto spec = TVanillaOperationSpec()
            .Pool(Pool_)
            .CoreTablePath(TestHome_.CoreTable())
            .StderrTablePath(TestHome_.StderrTable("validator"))
            .MaxFailedJobCount(10000)
            .AddTask(
                TVanillaTask()
                    .Name("Validator")
                    .Spec(userSpec)
                    .Job(new TValidatorJob(dir))
                    .JobCount(Config_.NumJobs));

    Operation_ = Client_->RunVanilla(spec, options);

    YT_LOG_INFO("Started vanilla validator operation");
}

TValidator::TableIntervalInfo TValidator::GetMapIntervalBoundaries(
    const TString& tablePath,
    int64_t intervalBytes)
{
    TableIntervalInfo result;
    result.RowCount = GetInt64Node(RpcClient_, tablePath + "/@row_count");

    if (result.RowCount == 0) {
        return result;
    }

    result.TotalBytes = GetInt64Node(RpcClient_, tablePath + "/@uncompressed_data_size");
    const int64_t numIntervals = (result.TotalBytes + intervalBytes - 1) / intervalBytes;
    const int64_t intervalRows = (result.RowCount + numIntervals - 1) / numIntervals;

    for (int index = 0; index < numIntervals; ++index) {
        result.Boundaries.push_back(index * intervalRows);
    }
    result.Boundaries.push_back(result.RowCount);

    return result;
}

TValidator::TableIntervalInfo TValidator::GetReduceIntervalBoundaries(
    const TTable& table, const TString& tablePath, int64_t intervalBytes)
{
    TableIntervalInfo result;
    auto columnIndex = BuildColumnIndex(table.DataColumns);

    result.RowCount = GetInt64Node(RpcClient_, tablePath + "/@row_count");
    if (result.RowCount == 0) {
        return {};
    }

    result.TotalBytes = GetInt64Node(RpcClient_, tablePath + "/@uncompressed_data_size");

    const int64_t numIntervals = (result.TotalBytes + intervalBytes - 1) / intervalBytes;
    const int64_t intervalRows = (result.RowCount + numIntervals - 1) / numIntervals;

    int64_t intervalStart = intervalRows;

    result.Boundaries.push_back(0);
    while (intervalStart < result.RowCount) {
        TRichYPath readerPath;
        readerPath.Path(tablePath);
        readerPath.AddRange(TReadRange::FromRowIndices(intervalStart, result.RowCount));

        auto reader = Client_->CreateTableReader<TNode>(readerPath);

        if (!reader->IsValid()) {
            THROW_ERROR_EXCEPTION("Reading row %v / %v from table %v, hit end",
                intervalStart, result.RowCount, tablePath);
        }

        auto key = ArrangeValuesToIndex(columnIndex, reader->GetRow().AsMap());
        key.resize(table.SortColumns);

        int64_t skipRows = 0;
        std::vector<TNode> currentKey;
        while (reader->IsValid()) {
            currentKey = ArrangeValuesToIndex(columnIndex, reader->GetRow().AsMap());
            currentKey.resize(table.SortColumns);

            if (currentKey != key) {
                break;
            }

            reader->Next();
            ++skipRows;
        }

        int64_t boundary = intervalStart + skipRows;
        YT_LOG_DEBUG("Added a boundary at %v after skipping %v rows", boundary, skipRows);

        result.Boundaries.push_back(boundary);
        intervalStart = std::min(boundary + intervalRows, result.RowCount);
    }
    result.Boundaries.push_back(result.RowCount);

    return result;
}

}  // namespace NYT::NTest


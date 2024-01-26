
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

#include <yt/systest/proto/validator.pb.h>

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

static TFuture<typename NRpc::TTypedClientResponse<NProto::TRspMapInterval>::TResult>
StartMapInterval(
    const TString& hostport,
    const TString& tablePath,
    const TString& outputPath,
    int64_t start,
    int64_t limit,
    const TTable& table,
    const IMultiMapper& mapper,
    TWorkerSet::TWorkerGuard token)
{
    NYT::NLogging::TLogger Logger("test");
    YT_LOG_INFO("StartMapInterval (Worker: %v, SourcePath: %v, Start: %v, Limit: %v, TargetPath: %v)",
        hostport, tablePath, start, limit, outputPath);

    auto client = CreateBusClient(NBus::TBusClientConfig::CreateTcp(hostport));
    auto channel = NRpc::NBus::CreateBusChannel(client);

    TValidatorProxy proxy(channel);

    auto request = proxy.MapInterval();
    PopulateTableInterval(tablePath, start, limit, request->mutable_input());

    request->set_output_path(outputPath);
    ToProto(request->mutable_map_spec()->mutable_table(), table);
    mapper.ToProto(request->mutable_map_spec()->mutable_operation());

    return request->Invoke().ApplyUnique(BIND(
        [token = std::move(token)] (TErrorOr<typename NRpc::TTypedClientResponse<NProto::TRspMapInterval>::TResult> &&result) {
            return result.ValueOrThrow();
        }));
}

static TFuture<typename NRpc::TTypedClientResponse<NProto::TRspReduceInterval>::TResult>
StartReduceInterval(
    const TString& hostport,
    const TString& tablePath,
    const TString& outputPath,
    int64_t start,
    int64_t limit,
    const TTable& table,
    const TReduceOperation& operation,
    TWorkerSet::TWorkerGuard token)
{
    NYT::NLogging::TLogger Logger("test");
    YT_LOG_INFO("StartReduceInterval (Worker: %v, SourcePath: %v, Start: %v, Limit: %v, TargetPath: %v)",
        hostport, tablePath, start, limit, outputPath);

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

    return request->Invoke().ApplyUnique(BIND(
        [token = std::move(token)] (TErrorOr<typename NRpc::TTypedClientResponse<NProto::TRspReduceInterval>::TResult> &&result) {
            return result.ValueOrThrow();
        }));
}

static TFuture<typename NRpc::TTypedClientResponse<NProto::TRspSortInterval>::TResult>
StartSortInterval(
    const TString& hostport,
    const TString& tablePath,
    const TString& outputPath,
    int64_t start,
    int64_t limit,
    const TTable& table,
    const TSortOperation& operation,
    TWorkerSet::TWorkerGuard token)
{
    NYT::NLogging::TLogger Logger("test");
    YT_LOG_INFO("StartSortInterval (Worker: %v, SourcePath: %v, Start: %v, Limit: %v, TargetPath: %v)",
        hostport, tablePath, start, limit, outputPath);

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

    return request->Invoke().ApplyUnique(BIND(
        [token = std::move(token)] (TErrorOr<typename NRpc::TTypedClientResponse<NProto::TRspSortInterval>::TResult> &&result) {
            return result.ValueOrThrow();
        }));
}

static TFuture<typename NRpc::TTypedClientResponse<NProto::TRspCompareInterval>::TResult>
StartCompareInterval(
    const TString& hostport,
    const TString& targetPath,
    const TString& intervalPath,
    int64_t start,
    int64_t limit,
    TWorkerSet::TWorkerGuard token)
{
    NYT::NLogging::TLogger Logger("test");
    YT_LOG_INFO("StartCompareInterval (Worker: %v, IntervalPath: %v, Start: %v, Limit: %v, TargetPath: %v)",
        hostport, intervalPath, start, limit, targetPath);

    auto client = CreateBusClient(NBus::TBusClientConfig::CreateTcp(hostport));
    auto channel = NRpc::NBus::CreateBusChannel(client);

    TValidatorProxy proxy(channel);

    auto request = proxy.CompareInterval();
    request->set_target_path(targetPath);
    request->set_interval_path(intervalPath);
    request->set_start_row_index(start);
    request->set_limit_row_index(limit);

    return request->Invoke().ApplyUnique(BIND(
        [token = std::move(token)] (TErrorOr<typename NRpc::TTypedClientResponse<NProto::TRspCompareInterval>::TResult> &&result) {
            return result.ValueOrThrow();
        }));
}

static TFuture<typename NRpc::TTypedClientResponse<NProto::TRspMergeSortedAndCompare>::TResult>
StartMergeSortedAndCompare(
    const TString& hostport,
    const std::vector<TString>& intervalPath,
    const TString& targetPath,
    const TTable& table,
    TWorkerSet::TWorkerGuard token)
{
    NYT::NLogging::TLogger Logger("test");
    YT_LOG_INFO("StartMergeSortedAndCompare (Worker: %v, NumIntervals: %v, TargetPath: %v)",
        hostport, std::ssize(intervalPath), targetPath);

    auto client = CreateBusClient(NBus::TBusClientConfig::CreateTcp(hostport));
    auto channel = NRpc::NBus::CreateBusChannel(client);

    TValidatorProxy proxy(channel);

    auto request = proxy.MergeSortedAndCompare();

    request->set_target_path(targetPath);
    for (const auto& path : intervalPath) {
        request->add_interval_path(path);
    }
    ToProto(request->mutable_table(), table);

    return request->Invoke().ApplyUnique(BIND(
        [token = std::move(token)] (TErrorOr<typename NRpc::TTypedClientResponse<NProto::TRspMergeSortedAndCompare>::TResult> &&result) {
            return result.ValueOrThrow();
        }));
}

///////////////////////////////////////////////////////////////////////////////

void TValidatorConfig::RegisterOptions(NLastGetopt::TOpts* opts)
{
    opts->AddLongOption("validator-jobs")
        .StoreResult(&NumJobs)
        .DefaultValue(4);

    opts->AddLongOption("validator-interval-bytes")
        .StoreResult(&IntervalBytes)
        .DefaultValue(64 << 20);

    opts->AddLongOption("validator-poll-delay")
        .StoreResult(&PollDelay)
        .DefaultValue(TDuration::Seconds(5));

    opts->AddLongOption("validator-worker-failure-backoff-delay")
        .StoreResult(&WorkerFailureBackoffDelay)
        .DefaultValue(TDuration::Seconds(30));
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

template <typename T>
TStoredDataset TValidator::CompareIntervals(
    const TString& targetPath,
    std::vector<TFuture<T>>& intervalResult,
    const std::vector<TString>& intervalPath)
{
    const int numIntervals = std::ssize(intervalPath);

    std::vector<TFuture<typename NRpc::TTypedClientResponse<NProto::TRspCompareInterval>::TResult>>
        compareIntervalResult;

    int64_t startInterval = 0;
    for (int index = 0; index < numIntervals; ++index) {
        intervalResult[index].Get().ThrowOnError();

        int64_t intervalRowCount = Client_->Get(intervalPath[index] + "/@row_count").AsInt64();

        YT_LOG_INFO("Interval generated (Index: %v, IntervalRowCount: %v, NumIntervals: %v, Path: %v)",
            index, intervalRowCount, numIntervals, intervalPath[index]);

        auto token = WorkerSet_.AcquireWorker();
        const TString& hostPort = token.HostPort();
        auto result = StartCompareInterval(
            hostPort,
            targetPath,
            intervalPath[index],
            startInterval,
            startInterval + intervalRowCount,
            std::move(token));

        compareIntervalResult.push_back(result);
        startInterval += intervalRowCount;
    }

    for (int index = 0; index < std::ssize(compareIntervalResult); ++index) {
        YT_LOG_INFO("Interval compared (Index: %v, NumIntervals: %v, Path: %v)",
            index, numIntervals, intervalPath[index]);

        auto result = compareIntervalResult[index].Get().ValueOrThrow();
    }

    int64_t totalSize = Client_->Get(targetPath + "/@uncompressed_data_size").AsInt64();

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

    std::vector<TString> intervalPath;
    std::vector<TFuture<typename NRpc::TTypedClientResponse<NProto::TRspMapInterval>::TResult>>
        mapIntervalResult;

    YT_LOG_INFO("Will validate map operation (SourcePath: %v, TargetPath: %v, NumIntervals: %v)",
        sourcePath,
        targetPath,
        numIntervals);

    for (int index = 0; index < numIntervals; ++index) {
        auto tempOutputPath = TestHome_.CreateIntervalPath(targetName, index);
        intervalPath.push_back(tempOutputPath);

        auto token = WorkerSet_.AcquireWorker();
        const TString& hostPort = token.HostPort();
        auto result = StartMapInterval(
            hostPort,
            sourcePath,
            tempOutputPath,
            intervalInfo.Boundaries[index],
            intervalInfo.Boundaries[index + 1],
            sourceTable,
            mapper,
            std::move(token));

        mapIntervalResult.push_back(result);
    }

    auto storedDataset = CompareIntervals(targetPath, mapIntervalResult, intervalPath);

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

    std::vector<TFuture<typename NRpc::TTypedClientResponse<NProto::TRspReduceInterval>::TResult>>
        reduceIntervalResult;
    std::vector<TString> intervalPath;

    for (int index = 0; index < numIntervals; ++index) {
        auto tempOutputPath = TestHome_.CreateIntervalPath(targetName, index);
        intervalPath.push_back(tempOutputPath);

        auto token = WorkerSet_.AcquireWorker();
        const TString& hostPort = token.HostPort();
        auto result = StartReduceInterval(
            hostPort,
            sourcePath,
            tempOutputPath,
            intervalInfo.Boundaries[index],
            intervalInfo.Boundaries[index + 1],
            sourceTable,
            reduceOperation,
            std::move(token)
        );
        reduceIntervalResult.push_back(result);
    }

    auto storedDataset = CompareIntervals(targetPath, reduceIntervalResult, intervalPath);

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
    const int numIntervals = std::ssize(intervalInfo.Boundaries) - 1;

    YT_LOG_INFO("Will validate sort operation (SourcePath: %v, TargetPath: %v, NumIntervals: %v)",
        sourcePath,
        targetPath,
        numIntervals);

    std::vector<TFuture<typename NRpc::TTypedClientResponse<NProto::TRspSortInterval>::TResult>>
        sortIntervalResult;
    std::vector<TString> intervalPath;

    for (int index = 0; index < numIntervals; index++) {
        auto tempOutputPath = TestHome_.CreateIntervalPath(targetName, index);
        intervalPath.push_back(tempOutputPath);

        YT_LOG_INFO("Will produce sorted table %v for interval %v", tempOutputPath, index);

        auto token = WorkerSet_.AcquireWorker();
        const TString& hostPort = token.HostPort();
        auto result = StartSortInterval(
            hostPort,
            sourcePath,
            tempOutputPath,
            intervalInfo.Boundaries[index],
            intervalInfo.Boundaries[index + 1],
            sourceTable,
            operation,
            std::move(token));
        sortIntervalResult.push_back(result);
    }

    AllSucceeded(sortIntervalResult).Get().ThrowOnError();

    TTable sortedTable;
    ApplySortOperation(sourceTable, operation, &sortedTable);

    auto mergeToken = WorkerSet_.AcquireWorker();
    const TString& hostPort = mergeToken.HostPort();
    StartMergeSortedAndCompare(
            hostPort,
            intervalPath,
            targetPath,
            sortedTable,
            std::move(mergeToken))
        .Get().ThrowOnError();

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
        .AddEnvironment("YT_LOG_LEVEL", "error");

    auto secureEnv = TNode::CreateMap({
        {"YT_TOKEN", TConfig::Get()->Token}
    });

    TOperationOptions options;
    options.StartOperationMode(TOperationOptions::EStartOperationMode::SyncStart);
    options.SecureVault(secureEnv);

    auto spec = TVanillaOperationSpec()
            .Pool(Pool_)
            .CoreTablePath(TestHome_.CoreTable())
            .TimeLimit(TDuration::Hours(48))
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
    result.RowCount = Client_->Get(tablePath + "/@row_count").AsInt64();
    if (result.RowCount == 0) {
        return {};
    }

    result.TotalBytes = Client_->Get(tablePath + "/@uncompressed_data_size").AsInt64();
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

    result.RowCount = Client_->Get(tablePath + "/@row_count").AsInt64();
    if (result.RowCount == 0) {
        return {};
    }

    result.TotalBytes = Client_->Get(tablePath + "/@uncompressed_data_size").AsInt64();

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



#include <library/cpp/yt/logging/logger.h>
#include <yt/cpp/mapreduce/interface/logging/logger.h>
#include <yt/cpp/mapreduce/interface/client.h>

#include <yt/systest/test_home.h>
#include <yt/systest/map_dataset.h>
#include <yt/systest/reduce_dataset.h>
#include <yt/systest/sort_dataset.h>
#include <yt/systest/table_dataset.h>
#include <yt/systest/util.h>
#include <yt/systest/validator_service.h>

#include <yt/yt/core/bus/bus.h>
#include <yt/yt/core/bus/tcp/config.h>
#include <yt/yt/core/bus/private.h>
#include <yt/yt/core/bus/server.h>
#include <yt/yt/core/bus/tcp/client.h>
#include <yt/yt/core/bus/tcp/server.h>

#include <yt/yt/core/rpc/bus/channel.h>
#include <yt/yt/core/rpc/bus/server.h>
#include <yt/yt/core/rpc/client.h>
#include <yt/yt/core/rpc/server.h>
#include <yt/yt/core/rpc/service_detail.h>

#include <util/digest/numeric.h>

namespace NYT::NTest {

class TValidatorService
    : public NRpc::TServiceBase {
public:
    TValidatorService(IClientPtr Client, IInvokerPtr invoker, NLogging::TLogger& logger);

    DECLARE_RPC_SERVICE_METHOD(NProto, MapInterval);
    DECLARE_RPC_SERVICE_METHOD(NProto, ReduceInterval);
    DECLARE_RPC_SERVICE_METHOD(NProto, SortInterval);
    DECLARE_RPC_SERVICE_METHOD(NProto, MergeSortedAndCompare);
    DECLARE_RPC_SERVICE_METHOD(NProto, CompareInterval);

private:
    IClientPtr Client_;
};

///////////////////////////////////////////////////////////////////////////////

static int64_t CountAndHashValues(int keyLength, IDatasetIterator* iterator, size_t* hashValue)
{
    std::vector<TNode> key(iterator->Values().begin(), iterator->Values().begin() + keyLength);
    int64_t count = 0;
    // Compute sum for hashes for all rows in the interval.
    *hashValue = 0;
    while (!iterator->Done() && CompareRowPrefix(keyLength, key, iterator->Values()) == 0) {
        size_t rowHash = RowHash(TRange<TNode>(iterator->Values().begin() + keyLength, iterator->Values().end()));
        *hashValue += rowHash;
        iterator->Next();
        ++count;
    }
    return count;
}

static TRichYPath CreateRichYPath(const NProto::TTableInterval& interval)
{
    TRichYPath result;
    result.Path(interval.table_path());
    result.AddRange(TReadRange::FromRowIndices(
        interval.start_row_index(), interval.limit_row_index()));
    return result;
}

static TString DebugString(const TString& path, int64_t start, int64_t limit)
{
    return path + "#[" + std::to_string(start) + "," + std::to_string(limit) + ")";
}

///////////////////////////////////////////////////////////////////////////////

TValidatorService::TValidatorService(IClientPtr client, IInvokerPtr invoker, NLogging::TLogger& logger)
    : TServiceBase(
        invoker,
        TValidatorProxy::GetDescriptor(),
        logger)
    , Client_(client)
{
    const int queueSizeLimit = 100000;
    RegisterMethod(RPC_SERVICE_METHOD_DESC(MapInterval)
        .SetCancelable(true)
        .SetQueueSizeLimit(queueSizeLimit));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(ReduceInterval)
        .SetCancelable(true)
        .SetQueueSizeLimit(queueSizeLimit));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(SortInterval)
        .SetCancelable(true)
        .SetQueueSizeLimit(queueSizeLimit));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(MergeSortedAndCompare)
        .SetCancelable(true)
        .SetQueueSizeLimit(queueSizeLimit));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(CompareInterval)
        .SetCancelable(true)
        .SetQueueSizeLimit(queueSizeLimit));
}

///////////////////////////////////////////////////////////////////////////////

DEFINE_RPC_SERVICE_METHOD(TValidatorService, MapInterval)
{
    NLogging::TLogger Logger("validator_service");
    YT_LOG_INFO("MapInterval <- %v", request->ShortDebugString());

    TTable table;
    FromProto(&table, request->map_spec().table());

    std::unique_ptr<IDataset> dataset = std::make_unique<TTableDataset>(
        table, Client_, CreateRichYPath(request->input()));

    auto operation = CreateFromProto(table, request->map_spec().operation());
    std::unique_ptr<TMapDataset> mapDataset = std::make_unique<TMapDataset>(*dataset, *operation);

    MaterializeIgnoringStableNames(Client_, request->output_path(), *mapDataset);

    YT_LOG_INFO("MapInterval -> %v", request->output_path());
    context->Reply();
}

DEFINE_RPC_SERVICE_METHOD(TValidatorService, ReduceInterval)
{
    NLogging::TLogger Logger("validator_service");
    YT_LOG_INFO("ReduceInterval <- %v", request->ShortDebugString());

    TTable table;
    FromProto(&table, request->reduce_spec().table());

    std::unique_ptr<IDataset> dataset = std::make_unique<TTableDataset>(
        table, Client_, CreateRichYPath(request->input()));

    std::vector<TString> reduceBy(
        request->reduce_spec().reduce_by().begin(),
        request->reduce_spec().reduce_by().end());

    TReduceOperation reduceOperation{
        CreateFromProto(table, request->reduce_spec().operation()),
        reduceBy
    };

    auto reduceDataset = std::make_unique<TReduceDataset>(*dataset, reduceOperation);
    MaterializeIgnoringStableNames(Client_, request->output_path(), *reduceDataset);

    YT_LOG_INFO("ReduceInterval -> %v", request->output_path());
    context->Reply();
}

DEFINE_RPC_SERVICE_METHOD(TValidatorService, SortInterval)
{
    NLogging::TLogger Logger("validator_service");
    YT_LOG_INFO("SortInterval <- %v", request->ShortDebugString());

    TTable table;
    TSortOperation sortOperation;

    FromProto(&table, &sortOperation, request->sort_spec());

    std::unique_ptr<IDataset> dataset = std::make_unique<TTableDataset>(
        table, Client_, CreateRichYPath(request->input()));

    std::unique_ptr<IDataset> sortDataset = std::make_unique<TSortDataset>(
        *dataset, sortOperation);

    MaterializeIgnoringStableNames(Client_, request->output_path(), *sortDataset);

    YT_LOG_INFO("SortInterval -> %v", request->output_path());
    context->Reply();
}

static void CompareSortedDatasets(
    const TString& targetPath,
    const TTable& table,
    std::unique_ptr<IDatasetIterator> iteratorA,
    std::unique_ptr<IDatasetIterator> iteratorB)
{
    NLogging::TLogger Logger("validator_service");

    const int LogInterval = 1000000;
    const int keyLength = table.SortColumns;
    int index = 0;
    while (!iteratorA->Done() && !iteratorB->Done()) {
        if (CompareRowPrefix(keyLength, iteratorA->Values(), iteratorB->Values()) != 0) {
            THROW_ERROR_EXCEPTION("Sort keys differ at position %v", index);
        }
        size_t hashA, hashB;
        int64_t countA = CountAndHashValues(keyLength, iteratorA.get(), &hashA);
        int64_t countB = CountAndHashValues(keyLength, iteratorB.get(), &hashB);
        if (countA != countB) {
            THROW_ERROR_EXCEPTION("Different value count, %v != %v at position %v",
                countA, countB,
                index);
        }
        if (hashA != hashB) {
            THROW_ERROR_EXCEPTION("Different content hash at position %v, value count %v",
                index, countA);
        }

        if (index % LogInterval + countA >= LogInterval) {
            YT_LOG_INFO("CompareSortedDatasets progress (TargetTable: %v, NumProcessed: %v)",
                targetPath, index + countA);
        }
        index += countA;
    }

    if (!iteratorA->Done()) {
        THROW_ERROR_EXCEPTION("Side A contains more records after reading all %v records "
            "from side B", index);
    }

    if (!iteratorB->Done()) {
        THROW_ERROR_EXCEPTION("Side B contains more records after reading all %v records "
            "from side Z", index);
    }
    YT_LOG_INFO("CompareSortedDatasets done (TargetTable: %v, NumRows: %v)", targetPath, index);
}

DEFINE_RPC_SERVICE_METHOD(TValidatorService, MergeSortedAndCompare)
{
    NLogging::TLogger Logger("validator_service");
    YT_LOG_INFO("MergeSortedAndCompare <- %v", request->ShortDebugString());

    if (request->interval_path_size() == 0) {
        auto rowCount = Client_->Get(request->target_path() + "/@row_count").AsInt64();
        if (rowCount != 0) {
            THROW_ERROR_EXCEPTION("List of intervals is empty, but target table %v contains %v rows",
             request->target_path(), rowCount);
        }
        context->Reply();
    }

    std::vector<std::unique_ptr<TTableDataset>> inputDataset;
    std::vector<const IDataset*> inner;

    TTable table;
    FromProto(&table, request->table());
    for (const auto& intervalPath : request->interval_path()) {
        inputDataset.push_back(std::make_unique<TTableDataset>(
            table, Client_, intervalPath));
        inner.push_back(inputDataset.back().get());
    }
    auto mergeDataset = std::make_unique<TMergeSortedDataset>(std::move(inner));

    TTableDataset target(table, Client_, request->target_path());

    CompareSortedDatasets(
        request->target_path(),
        table,
        mergeDataset->NewIterator(),
        target.NewIterator());

    YT_LOG_INFO("MergeSortedAndCompare -> %v", request->target_path());
    context->Reply();
}

DEFINE_RPC_SERVICE_METHOD(TValidatorService, CompareInterval)
{
    NLogging::TLogger Logger("validator_service");
    YT_LOG_INFO("CompareInterval <- %v", request->ShortDebugString());
    TRichYPath tablePath;
    tablePath.Path(request->target_path());
    tablePath.AddRange(TReadRange::FromRowIndices(
        request->start_row_index(), request->limit_row_index()));

    auto left = Client_->CreateTableReader<TNode>(tablePath);
    auto right = Client_->CreateTableReader<TNode>(request->interval_path());

    int index = 0;
    while (left->IsValid() && right->IsValid()) {
        auto leftRow = left->GetRow();
        auto rightRow = right->GetRow();
        if (leftRow != rightRow) {
            THROW_ERROR_EXCEPTION("Comparison failed at position %v, "
                "IntervalTable: %v, targetTable: %v",
                index,
                request->interval_path(),
                DebugString(request->target_path(), request->start_row_index(), request->limit_row_index()));
        }
        ++index;
        left->Next();
        right->Next();
    }

    if (left->IsValid()) {
        THROW_ERROR_EXCEPTION("Target table %v has rows left after reading "
            "%v rows from the interval table %v", tablePath.Path_, index, request->interval_path());
    }
    if (right->IsValid()) {
        THROW_ERROR_EXCEPTION("Interval table %v has rows left after reading "
            "%v rows from the target table %v", request->interval_path(), index, request->interval_path());
    }

    YT_LOG_INFO("CompareInterval -> %v", request->interval_path());
    context->Reply();
}

void RunValidatorService(IClientPtr client, int port)
{
    auto busServer = CreateBusServer(NBus::TBusServerConfig::CreateTcp(port));

    auto rpcServer = NRpc::NBus::CreateBusServer(busServer);

    auto queue = New<NConcurrency::TActionQueue>("RPC");

    NLogging::TLogger Logger;
    rpcServer->RegisterService(New<TValidatorService>(client, queue->GetInvoker(), Logger));
    rpcServer->Start();

    TString str;
    Cin >> str;
}

}  // namespace NYT::NTest

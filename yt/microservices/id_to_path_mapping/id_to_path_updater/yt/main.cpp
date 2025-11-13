#include <yt/microservices/id_to_path_mapping/id_to_path_updater/lib/id_to_path_updater.h>

#include <yt/cpp/mapreduce/interface/client.h>

#include <yt/cpp/roren/yt/yt.h>

#include <yt/yt/core/net/address.h>
#include <yt/yt/core/net/config.h>

#include <library/cpp/getopt/last_getopt.h>

#include <util/datetime/base.h>

#include <util/system/tempfile.h>
#include <util/system/user.h>

using namespace NYT;

static auto Logger = NLogging::TLogger("IdToPathUpdater");

static const TStringBuf IdToPathProcessingTimeAttribute = "id_to_path_processed_at";
static const TStringBuf TmpTablePrefix = "static_";
static const auto OutputSchema = TTableSchema()
    .AddColumn(TColumnSchema().Name("cluster").Type(VT_STRING).SortOrder(SO_ASCENDING))
    .AddColumn(TColumnSchema().Name("node_id").Type(VT_STRING).SortOrder(SO_ASCENDING))
    .AddColumn(TColumnSchema().Name("path").Type(VT_STRING))
    .UniqueKeys(true);

void ConstructPipeline(NRoren::TPipeline& pipeline, const TString& inputTable, const TString& outputTable, TString forceCluster, THashSet<TString> clusterFilter)
{
    auto parsed = pipeline | NRoren::YtRead<TNode>(inputTable)
        | NRoren::ParDo([](const TNode& input, NRoren::TOutput<NJson::TJsonValue>& output) {
                auto lines = input["value"].AsString() | std::views::split('\n');
                for (const auto& line : lines) {
                    try {
                        TMemoryInput memoryInput(TStringBuf(line.begin(), line.end()));
                        output.Add(NJson::ReadJsonTree(&memoryInput));
                    } catch (const std::exception& ex) {
                        YT_LOG_WARNING("Cannot parse json value: %v", ex.what());
                    }
                }
        })
        | MakeUpdateItem(std::move(forceCluster));

    NRoren::TPCollection<TIdToPathRow> filtered = parsed;
    if (clusterFilter.empty()) {
        // No filter
    } else {
        filtered = parsed | FilterClusters(std::move(clusterFilter));
    }

    filtered
        | NRoren::ParDo([](TIdToPathRow&& row, NRoren::TOutput<NRoren::TKV<TString, TIdToPathRow>>& output){
            output.Add({row.GetCluster() + row.GetNodeId(), std::move(row)});
        })
        | LogOnceInAWhile("intermediate proto")
        | NRoren::GroupByKey()
        | NRoren::ParDo([](const NRoren::TKV<TString, NRoren::TInputPtr<TIdToPathRow>>& input, NRoren::TOutput<TIdToPathRow>& output) {
            for (const auto& row : input.Value()) {
                output.Add(row);
                return; // just send one row
            }
        })
        | NRoren::YtSortedWrite(outputTable, OutputSchema);
}

void MergeToStaticTable(const IClientBasePtr& client, const std::vector<TString>& inTables, const TString& outTable)
{
    TMergeOperationSpec spec;
    for (const TString& inTable : inTables) {
        spec.AddInput(inTable);
    }
    spec.Output(outTable);
    auto operation = client->Merge(spec);
    auto operationFuture = operation->Watch();
    operationFuture.Wait();
    operationFuture.TryRethrow();
}

void MergeToDynTable(const IClientBasePtr& client, const TString& inTable, const TString& outTable)
{
    TMergeOperationSpec spec;
    spec.Mode(EMergeMode::MM_ORDERED);
    spec.ForceTransform(true);
    spec.AddInput(inTable);
    spec.Output(outTable);
    auto operation = client->Merge(spec);
    auto operationFuture = operation->Watch();
    operationFuture.Wait();
    operationFuture.TryRethrow();
}

void MarkTablesAsProcessed(const IClientPtr& client, const std::vector<TString>& tables)
{
    auto batchRequest = client->CreateBatchRequest();
    TVector<::NThreading::TFuture<void>> batchResults;
    batchResults.reserve(tables.size());
    for (const auto& table : tables) {
        auto batchResult = batchRequest->Set(Format("%v/@%v", table, IdToPathProcessingTimeAttribute), TInstant::Now().ToString());
        batchResults.push_back(std::move(batchResult));
        YT_LOG_DEBUG("Adding table to batch for marking as processed (Path: %v)", table);
    }
    batchRequest->ExecuteBatch();
    for (size_t tableIndex = 0; tableIndex < tables.size(); ++tableIndex) {
        try {
            batchResults[tableIndex].GetValue();
        } catch (const TErrorResponse& ex) {
            YT_LOG_ERROR(ex, "Failed to mark table as processed (Path: %v)", tables[tableIndex]);
        }
    }
}

void Process(TString cluster, TString tmpPath, std::vector<TString> inputTables, TString outputTable, TString forceCluster, THashSet<TString> clusterFilter)
{
    NRoren::TYtPipelineConfig config;
    config.SetCluster(cluster);
    config.SetWorkingDir(tmpPath);
    auto pipeline = NRoren::MakeYtPipeline(config);
    auto client = CreateClient(cluster);
    auto staticInputTable = TTempTable(client, TString(TmpTablePrefix), tmpPath, TCreateOptions().Recursive(true));
    auto staticOutputTable = TTempTable(client, TString(TmpTablePrefix), tmpPath, TCreateOptions().Recursive(true));

    if (!client->Exists(outputTable)) {
        YT_LOG_ERROR("Output table does not exist (OutputTablePath: %v)", outputTable);
        throw yexception() << "Output table does not exist: " << outputTable;
    }

    MergeToStaticTable(client, inputTables, staticInputTable.Name());
    ConstructPipeline(pipeline, staticInputTable.Name(), staticOutputTable.Name(), std::move(forceCluster), std::move(clusterFilter));
    pipeline.Run();
    MergeToDynTable(client, staticOutputTable.Name(), outputTable);
    MarkTablesAsProcessed(client, inputTables);
}

std::vector<TString> GetTablesToProcess(const TString& cluster, const TString& inputDirectory)
{
    auto client = CreateClient(cluster);
    auto listOptions = TListOptions()
        .AttributeFilter(TAttributeFilter()
            .AddAttribute(TString(IdToPathProcessingTimeAttribute)));
    auto allTables = client->List(inputDirectory, listOptions);
    YT_LOG_INFO("Found tables (TableCount: %v, InputDirectory: %v)", allTables.size(), inputDirectory);

    std::vector<TString> result;
    for (const auto& table : allTables) {
        if (!table.GetAttributes().HasKey(IdToPathProcessingTimeAttribute)) {
            auto tableName = table.AsString();
            YT_LOG_DEBUG("Found table to process (TableName: %v)", tableName);
            result.push_back(Format("%v/%v", inputDirectory, tableName));
        }
    }
    std::sort(result.rbegin(), result.rend());
    return result;
}

int main(int argc, const char** argv)
{
    TConfig::Get()->LogLevel = "info";
    auto resolverConfig = NYT::New<NNet::TAddressResolverConfig>();
    resolverConfig->EnableIPv4 = true;
    NNet::TAddressResolver::Get()->Configure(resolverConfig);
    Initialize();

    TString cluster;
    TString tmpPath;
    TString inputDirectory;
    TString outputTable;
    TString forceCluster;
    THashSet<TString> clusterFilter;

    auto opts = NLastGetopt::TOpts::Default();
    opts.AddLongOption("cluster", "YT cluster").StoreResult(&cluster).Required();
    opts.AddLongOption("tmp-path", "Path for temporary files").StoreResult(&tmpPath).Required();
    opts.AddLongOption("input-directory", "Path to directory with input tables").StoreResult(&inputDirectory).Required();
    opts.AddLongOption("output-table", "Output table").StoreResult(&outputTable).Required();
    opts.AddLongOption("force-cluster", "Force cluster name for all rows").StoreResult(&forceCluster);
    opts.AddLongOption("cluster-filter", "Cluster to process").InsertTo(&clusterFilter);
    NLastGetopt::TOptsParseResult res(&opts, argc, argv);

    auto inputTables = GetTablesToProcess(cluster, inputDirectory);
    if (inputTables.empty()) {
        YT_LOG_INFO("No tables to process");
        return EXIT_SUCCESS;
    }
    YT_LOG_INFO("Processing input tables (InputTableCount: %v)", inputTables.size());

    Process(std::move(cluster), std::move(tmpPath), std::move(inputTables), std::move(outputTable), std::move(forceCluster), std::move(clusterFilter));
    return EXIT_SUCCESS;
}

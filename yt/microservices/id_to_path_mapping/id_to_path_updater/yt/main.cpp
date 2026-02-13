#include <yt/microservices/id_to_path_mapping/id_to_path_updater/lib/id_to_path_updater.h>
#include <yt/microservices/id_to_path_mapping/id_to_path_updater/lib/pipeline.h>

#include <yt/cpp/mapreduce/interface/client.h>

#include <yt/cpp/roren/yt/yt.h>

#include <yt/yt/core/net/address.h>
#include <yt/yt/core/net/config.h>

#include <yt/yt/library/auth/auth.h>

#include <library/cpp/getopt/last_getopt.h>

#include <util/datetime/base.h>

#include <util/system/env.h>
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

void ConstructPipeline(NRoren::TPipeline& pipeline, const TString& inputTable, const TString& outputTable, TString forceCluster, THashSet<std::string> allowClusters)
{
    auto input = pipeline | NRoren::YtRead<TNode>(inputTable)
        | NRoren::ParDo([](const TNode& input, NRoren::TOutput<std::string>& output) {
            auto lines = input["value"].AsString() | std::views::split('\n');
            for (const auto& line : lines) {
                output.Add(std::string(line.begin(), line.end()));
            }
        });

    auto filtered = ApplyMapper(input, std::move(forceCluster), std::move(allowClusters));

    filtered | LogOnceInAWhile("intermediate proto")
        | NRoren::GroupByKey()
        | NRoren::ParDo([](const NRoren::TKV<ui64, NRoren::TInputPtr<TIdToPathRow>>& input, NRoren::TOutput<TIdToPathRow>& output) {
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

void Process(TString cluster, TString tmpPath, std::vector<TString> inputTables, TString outputTable, TString forceCluster, THashSet<std::string> allowClusters)
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
    ConstructPipeline(pipeline, staticInputTable.Name(), staticOutputTable.Name(), std::move(forceCluster), std::move(allowClusters));
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

TString LoadIdToPathToken(TString tokenEnvVariable)
{
    auto ytIdToPathToken = GetEnv(tokenEnvVariable);
    YT_VERIFY(
        !ytIdToPathToken.empty(),
        ::TStringBuilder() << "Token environment variable " << tokenEnvVariable.c_str() << " is not set or empty");
    NAuth::ValidateToken(ytIdToPathToken);
    return ytIdToPathToken;
}

int main(int argc, const char** argv)
{
    TConfig::Get()->LogLevel = "info";
    auto resolverConfig = New<NNet::TAddressResolverConfig>();
    resolverConfig->EnableIPv4 = true;
    NNet::TAddressResolver::Get()->Configure(resolverConfig);
    Initialize();

    TString cluster;
    TString tmpPath;
    TString inputDirectory;
    TString outputTable;
    TString forceCluster;
    TString tokenEnvVariable;
    THashSet<std::string> allowClusters;

    auto opts = NLastGetopt::TOpts::Default();
    opts.AddLongOption("cluster", "YT cluster").StoreResult(&cluster).Required();
    opts.AddLongOption("tmp-path", "Path for temporary files").StoreResult(&tmpPath).Required();
    opts.AddLongOption("input-directory", "Path to directory with input tables").StoreResult(&inputDirectory).Required();
    opts.AddLongOption("output-table", "Output table").StoreResult(&outputTable).Required();
    opts.AddLongOption("force-cluster", "Force cluster name for all rows").StoreResult(&forceCluster);
    opts.AddLongOption("token-env-variable", "Environment variable that specifies the token used when accessing YT")
        .StoreResult(&tokenEnvVariable).DefaultValue("YT_ID_TO_PATH_TOKEN");
    opts.AddLongOption("allow-clusters", "Process only clusters in set").InsertTo(&allowClusters);
    NLastGetopt::TOptsParseResult res(&opts, argc, argv);

    TConfig::Get()->Token = LoadIdToPathToken(std::move(tokenEnvVariable));

    auto inputTables = GetTablesToProcess(cluster, inputDirectory);
    if (inputTables.empty()) {
        YT_LOG_INFO("No tables to process");
        return EXIT_SUCCESS;
    }
    YT_LOG_INFO("Processing input tables (InputTableCount: %v)", inputTables.size());

    Process(std::move(cluster), std::move(tmpPath), std::move(inputTables), std::move(outputTable), std::move(forceCluster), std::move(allowClusters));
    return EXIT_SUCCESS;
}

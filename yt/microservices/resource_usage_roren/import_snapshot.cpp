#include <yt/microservices/resource_usage_roren/data.pb.h>
#include <yt/microservices/resource_usage_roren/import_snapshot.h>

#include <yt/microservices/resource_usage_roren/lib/misc.h>

#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/roren/interface/roren.h>
#include <yt/cpp/roren/yt/yt.h>
#include <yt/yt/client/api/rpc_proxy/config.h>
#include <yt/yt/client/api/rpc_proxy/connection.h>
#include <yt/yt/core/concurrency/scheduler_api.h>
#include <yt/yt/core/https/client.h>
#include <yt/yt/core/https/config.h>
#include <yt/yt/library/auth/auth.h>
#include <yt/yt/library/named_value/named_value.h>

#include <library/cpp/yt/yson_string/convert.h>
#include <library/cpp/getopt/last_getopt.h>
#include <library/cpp/iterator/zip.h>

#include <util/datetime/base.h>
#include <util/generic/algorithm.h>
#include <util/generic/size_literals.h>
#include <util/system/env.h>

#include <ctime>
#include <iomanip>
#include <sstream>
#include <utility>
#include <variant>

using namespace NYT;
using namespace NRoren;

static NLogging::TLogger Logger("resource_usage");
static const auto TX_TOP_N = 100;
static const auto TX_LIMIT = 1000;
static_assert(TX_TOP_N < TX_LIMIT);

struct TDetailedMasterMemory
{
    i64 Attributes = 0;
    i64 Chunks = 0;
    i64 Nodes = 0;
    i64 Schemas = 0;
    i64 Tablets = 0;

    Y_SAVELOAD_DEFINE(
        Attributes,
        Chunks,
        Nodes,
        Schemas,
        Tablets);
};

void MergeDetailedMasterMemoryWith(TDetailedMasterMemory& to, const TDetailedMasterMemory& from)
{
    to.Attributes += from.Attributes;
    to.Chunks += from.Chunks;
    to.Nodes += from.Nodes;
    to.Schemas += from.Schemas;
    to.Tablets += from.Tablets;
}

using TDiskSpacePerMedium = TNode;

struct TResourceUsage
{
    i64 ChunkCount = 0;
    i64 DiskSpace = 0;
    i64 MasterMemory = 0;
    i64 NodeCount = 0;
    i64 TabletCount = 0;
    i64 ChunkHostCellMasterMemory = 0;
    i64 TabletStaticMemory = 0;

    TDetailedMasterMemory DetailedMasterMemory;
    TDiskSpacePerMedium DiskSpacePerMedium;

    Y_SAVELOAD_DEFINE(
        ChunkCount,
        DetailedMasterMemory,
        DiskSpace,
        DiskSpacePerMedium,
        MasterMemory,
        NodeCount,
        TabletCount,
        TabletStaticMemory);
};

void SetIntFromNode(i64& num, const TNode& node)
{
    if (node.IsInt64()) {
        num = node.AsInt64();
        return;
    }
    Y_ABORT_IF(!node.IsUint64());
    num = static_cast<i64>(node.AsUint64());
}


void GetDetailedMasterMemoryFromNode(TDetailedMasterMemory& masterMemory, const TNode& node)
{
    Y_ABORT_IF(!node.IsMap());

    SetIntFromNode(masterMemory.Attributes, node["attributes"]);
    SetIntFromNode(masterMemory.Chunks, node["chunks"]);
    SetIntFromNode(masterMemory.Nodes, node["nodes"]);
    SetIntFromNode(masterMemory.Schemas, node["schemas"]);
    SetIntFromNode(masterMemory.Tablets, node["tablets"]);
}

void GetResourceUsageFromNode(TResourceUsage& resourceUsage, const TNode& node)
{
    Y_ABORT_IF(!node.IsMap());

    SetIntFromNode(resourceUsage.ChunkCount, node["chunk_count"]);
    SetIntFromNode(resourceUsage.DiskSpace, node["disk_space"]);
    SetIntFromNode(resourceUsage.MasterMemory, node["master_memory"]);
    SetIntFromNode(resourceUsage.NodeCount, node["node_count"]);
    SetIntFromNode(resourceUsage.TabletCount, node["tablet_count"]);
    SetIntFromNode(resourceUsage.TabletStaticMemory, node["tablet_static_memory"]);
    SetIntFromNode(resourceUsage.ChunkHostCellMasterMemory, node["chunk_host_cell_master_memory"]);

    GetDetailedMasterMemoryFromNode(resourceUsage.DetailedMasterMemory, node["detailed_master_memory"]);
    resourceUsage.DiskSpacePerMedium = node["disk_space_per_medium"];
}

using TMedium = THashMap<TString, i64>;

bool IsNonZero(const TVersionedResourceUsage& vru)
{
    for (const auto& [key, node] : vru.VersionedResourceUsageNode.AsMap()) {
        if (node.IsInt64() && node.AsInt64() > 0) {
            return true;
        }
    }
    for (const auto& [key, node] : vru.DiskSpacePerMedium.AsMap()) {
        if (node.IsInt64() && node.AsInt64() > 0) {
            return true;
        }
    }
    return false;
}

TNode VersionedResourceUsageMapGetSummary(const TVersionedResourceUsageMap& vrum)
{
    if (vrum.empty()) {
        return TNode::CreateMap();
    }

    TNode node = TNode::CreateMap();
    TNode total = TNode::CreateMap();
    total["per_medium"] = TNode::CreateMap();
    TNode current = TNode::CreateMap();
    current["per_medium"] = TNode::CreateMap();
    TNode child = TNode::CreateMap();
    child["per_medium"] = TNode::CreateMap();
    for (const auto& [tx_id, vru] : vrum) {
        MergeIntNodeWith(total, vru.VersionedResourceUsageNode);
        MergeIntNodeWith(total["per_medium"], vru.DiskSpacePerMedium);
        if (vru.IsOriginal) {
            MergeIntNodeWith(current, vru.VersionedResourceUsageNode);
            MergeIntNodeWith(current["per_medium"], vru.DiskSpacePerMedium);
        } else {
            MergeIntNodeWith(child, vru.VersionedResourceUsageNode);
            MergeIntNodeWith(child["per_medium"], vru.DiskSpacePerMedium);
        }
    }
    node["total"] = total;
    node["current"] = current;
    node["child"] = child;
    return node;
}

TNode VersionedResourceUsageToNode(const TVersionedResourceUsageMap& vrum)
{
    TNode node = TNode::CreateMap();
    for (const auto& [tx_id, vru] : vrum) {
        node[tx_id] = vru.ToNode();
    }
    return node;
}

template <class TSetter>
void SetIfContains(TOutputMessage& out, TSetter setter, const THashMap<TString, TNode>& map, const TString& key)
{
    if (!map.contains(key)) {
        return;
    }
    (out.*setter)(map.at(key).AsInt64());
}

void SetVersionedResourceUsageMapInRow(TOutputMessage& out, const TNode& totalNode)
{
    if (totalNode.AsMap().empty()) {
        return;
    }
    // FIXME: double convert
    out.SetVersionedResourceUsage(NodeToYsonString(totalNode));

    SetIfContains(out, &TOutputMessage::SetVersionedChunkCount, totalNode["total"].AsMap(), "chunk_count");
    SetIfContains(out, &TOutputMessage::SetVersionedCompressedDataSize, totalNode["total"].AsMap(), "compressed_data_size");
    SetIfContains(out, &TOutputMessage::SetVersionedDataWeight, totalNode["total"].AsMap(), "data_weight");
    SetIfContains(out, &TOutputMessage::SetVersionedErasureDiskSpace, totalNode["total"].AsMap(), "erasure_disk_space");
    SetIfContains(out, &TOutputMessage::SetVersionedRegularDiskSpace, totalNode["total"].AsMap(), "regular_disk_space");
    SetIfContains(out, &TOutputMessage::SetVersionedRowCount, totalNode["total"].AsMap(), "row_count");
    SetIfContains(out, &TOutputMessage::SetVersionedUncompressedDataSize, totalNode["total"].AsMap(), "uncompressed_data_size");

    /*
     *  TODO:
     *
     * versioned:disk_space
     * versioned:master_memory
     * versioned:node_count
     * versioned:tablet_count
     * versioned:tablet_static_memory
     * versioned:direct_child_count
     * versioned:recursive_versioned_resource_usage
     *
     */
}


struct TRowAfterMap
{
    TString Account;
    TString Path;
    TString Type;
    TString OriginalPath;

    i64 AccessTime;
    i64 CreationTime;
    i64 ModificationTime;

    std::optional<TString> CypressTransactionId;
    std::optional<TString> CypressTransactionTitle;
    std::optional<TString> OriginatorCypressTransactionId;

    std::optional<TString> Id;
    std::optional<TString> Owner;
    std::optional<bool> Dynamic;
    std::optional<TString> PrimaryMedium;
    std::optional<i64> ApproximateRowCount;
    std::optional<bool> PathPatched;
    TResourceUsage ResourceUsage;
    TVersionedResourceUsage VersionedResourceUsage;

    bool IsOriginal;

    Y_SAVELOAD_DEFINE(
        Account,
        Path,
        Id,
        OriginalPath,
        ResourceUsage,
        VersionedResourceUsage,
        AccessTime,
        CreationTime,
        Dynamic,
        ModificationTime,
        Owner,
        Type,
        PrimaryMedium,
        ApproximateRowCount,
        PathPatched,
        CypressTransactionId,
        CypressTransactionTitle,
        OriginatorCypressTransactionId,
        IsOriginal);
};

// Call only if row has CypressTransactionId and VersionedResourceUsage
TVersionedResourceUsage VersionedResourceUsageFromRow(const TInputMessage& row)
{
    TString medium = row.HasPrimaryMedium() ? row.GetPrimaryMedium() : "default";
    TString title = row.GetCypressTransactionTitle();
    TNode versionedResourceUsageNode = NodeFromYsonString(row.GetVersionedResourceUsage());
    const auto& map = versionedResourceUsageNode.AsMap();

    TNode diskSpacePerMedium = TNode::CreateMap();
    const i64 erasure = map.contains("erasure_disk_space") ? map.at("erasure_disk_space").As<i64>() : 0;
    const i64 regular = map.contains("regular_disk_space") ? map.at("regular_disk_space").As<i64>() : 0;
    const i64 value = erasure ? erasure : regular;
    if (value > 0) {
        diskSpacePerMedium[medium] = value;
    }

    return TVersionedResourceUsage(title, versionedResourceUsageNode, diskSpacePerMedium, true);
}

void MergeDiskSpacePerMediumWith(TDiskSpacePerMedium& to, const TDiskSpacePerMedium& from)
{
    MergeIntNodeWith(to, from);
}

void MergeResourceUsageWith(TResourceUsage& to, const TResourceUsage& from)
{
    to.ChunkCount += from.ChunkCount;
    to.DiskSpace += from.DiskSpace;
    to.MasterMemory += from.MasterMemory;
    to.NodeCount += from.NodeCount;
    to.TabletCount += from.TabletCount;
    to.TabletStaticMemory += from.TabletStaticMemory;
    to.ChunkHostCellMasterMemory += from.ChunkHostCellMasterMemory;

    MergeDiskSpacePerMediumWith(to.DiskSpacePerMedium, from.DiskSpacePerMedium);
    MergeDetailedMasterMemoryWith(to.DetailedMasterMemory, from.DetailedMasterMemory);
}

TNode DetailedMasterMemoryToNode(const TDetailedMasterMemory& var)
{
    TNode result = TNode::CreateMap();
    auto& map = result.AsMap();
    map["attributes"] = var.Attributes;
    map["chunks"] = var.Chunks;
    map["nodes"] = var.Nodes;
    map["schemas"] = var.Schemas;
    map["tablets"] = var.Tablets;

    return result;
}

TNode ResourceUsageToNode(const TResourceUsage& resourceUsage)
{
    TNode result = TNode::CreateMap();
    auto& map = result.AsMap();
    map["chunk_count"] = resourceUsage.ChunkCount;
    map["disk_space"] = resourceUsage.DiskSpace;
    map["master_memory"] = resourceUsage.MasterMemory;
    map["node_count"] = resourceUsage.NodeCount;
    map["tablet_count"] = resourceUsage.TabletCount;
    map["tablet_static_memory"] = resourceUsage.TabletStaticMemory;
    map["chunk_host_cell_master_memory"] = resourceUsage.ChunkHostCellMasterMemory;

    if (!resourceUsage.DiskSpacePerMedium.IsUndefined() && !resourceUsage.DiskSpacePerMedium.AsMap().empty()) {
        map["disk_space_per_medium"] = resourceUsage.DiskSpacePerMedium;
    }
    map["detailed_master_memory"] = DetailedMasterMemoryToNode(resourceUsage.DetailedMasterMemory);

    return result;
}

void SetResourceUsageInRow(TOutputMessage& row, const TResourceUsage& resourceUsage)
{
    row.SetChunkCount(resourceUsage.ChunkCount);
    row.SetDiskSpace(resourceUsage.DiskSpace);
    row.SetMasterMemory(resourceUsage.MasterMemory);
    row.SetNodeCount(resourceUsage.NodeCount);
    row.SetTabletCount(resourceUsage.TabletCount);
    row.SetTabletStaticMemory(resourceUsage.TabletStaticMemory);
    row.SetChunkHostCellMasterMemory(resourceUsage.ChunkHostCellMasterMemory);

    /* FIXME: Double convert, should be fixed */
    TNode node = ResourceUsageToNode(resourceUsage);
    row.SetResourceUsage(NodeToYsonString(node));
}

const THashMap<TString, double> ERASURE_CODEC_AMP = {
    {"reed_solomon_3_3", 2},
    {"reed_solomon_6_3", 1.5},
    {"lrc_12_2_2", 4.0 / 3},
};

TStringBuf RemovePrefix(const TStringBuf text, const TStringBuf prefix)
{
    if (text.starts_with(prefix)) {
        return text.substr(prefix.size(), text.size() - prefix.size());
    }
    return text;
}

double GetFromMap(const TStringBuf key)
{
    auto it = ERASURE_CODEC_AMP.find(key);
    if (it != ERASURE_CODEC_AMP.end()) {
        return it->second;
    }
    return 0;
}

void SetInt64IfContains(const THashMap<TString, TNode>& map, const TStringBuf key, std::optional<i64>& opt)
{
    auto it = map.find(key);
    if (it != map.end()) {
        opt = it->second.AsInt64();
    }
}

bool HaveToPatchPath(const TStringBuf path)
{
    return !path.empty() && !path.starts_with("/");
}

i64 ParseTime(const TStringBuf s)
{
    std::tm timeParsed{};
    std::stringstream ss(s.data());
    ss >> std::get_time(&timeParsed, "%Y-%m-%dT%H:%M:%S");
    Y_ABORT_IF(ss.fail());
    return mktime(&timeParsed);
}

struct TResourceUsageMap;
using TResourceUsageMapPtr = TSimpleSharedPtr<TResourceUsageMap>;
using TResourceUsageVariant = std::variant<i64, TResourceUsageMapPtr>;

// This struct respents a resource usage object, see https://ytsaurus.tech/docs/en/user-guide/storage/transactions#attributes.
struct TResourceUsageMap
{
    TMap<TString, TResourceUsageVariant> Map;

    static TResourceUsageMapPtr ResourceUsageMapFromNode(const TNode& node)
    {
        if (node.IsUndefined()) {
            return nullptr;
        }
        Y_ABORT_IF(!node.IsMap());

        TResourceUsageMapPtr resourceUsageMapPtr = MakeSimpleShared<TResourceUsageMap>();
        for (const auto& [key, value] : node.AsMap()) {
            if (value.IsInt64()) {
                resourceUsageMapPtr->Map[key] = value.AsInt64();
                continue;
            }
            if (value.IsUint64()) {
                resourceUsageMapPtr->Map[key] = (i64)value.AsUint64();
                continue;
            }
            Y_ABORT_IF(!value.IsMap());
            resourceUsageMapPtr->Map[key] = ResourceUsageMapFromNode(value.AsMap());
        }

        return resourceUsageMapPtr;
    }

    static TNode NodeFromResourceUsageMap(const TResourceUsageMapPtr& resourceUsageMapPtr)
    {
        if (resourceUsageMapPtr == nullptr) {
            return TNode::CreateMap();
        }

        const TResourceUsageMap& resourceUsageMap = *resourceUsageMapPtr;
        auto result = TNode::CreateMap();
        for (const auto& [key, resourceUsageVariant] : resourceUsageMap.Map) {
            if (std::holds_alternative<i64>(resourceUsageVariant)) {
                result[key] = TNode(std::get<i64>(resourceUsageVariant));
            } else {
                result[key] = NodeFromResourceUsageMap(std::get<TResourceUsageMapPtr>(resourceUsageVariant));
            }
        }

        return result;
    }
};

struct TTransactionMap;
using TTransactionMapPtr = TSimpleSharedPtr<TTransactionMap>;
using TTransactions = TMap<TString, TTransactionMapPtr>;

// This struct stores the resource usage of transaction A and list of transactions B, where A is an originator of B.
struct TTransactionMap
{
    std::optional<TString> Title;
    TTransactions Transactions;
    TResourceUsageMapPtr ResourceUsage;

    static TNode NodeFromTransactionMap(const TTransactionMap& transactionMap)
    {
        auto result = TNode::CreateMap();
        result["title"] = transactionMap.Title.has_value() ? TNode(transactionMap.Title.value()) : TNode::CreateEntity();
        result["transactions"] = NodeFromTransactions(transactionMap.Transactions);
        result["resource_usage"] = TResourceUsageMap::NodeFromResourceUsageMap(transactionMap.ResourceUsage);

        return result;
    }

    static TNode NodeFromTransactions(const TTransactions& transactions)
    {
        auto result = TNode::CreateMap();
        for (const auto& [transactionId, transactionMapPtr] : transactions) {
            result[transactionId] = NodeFromTransactionMap(*transactionMapPtr);
        }

        return result;
    }

    static TTransactionMapPtr CreateTransactionMap(std::optional<TString> title = {},
        const TTransactions& transactions = {},
        TResourceUsageMapPtr resourceUsage = nullptr)
    {
        return MakeSimpleShared<TTransactionMap>(
            title,
            transactions,
            resourceUsage);
    }

    static void InitializeIfKeyDoesntExist(TTransactions& transactions, const TString& key)
    {
        transactions.emplace(key, CreateTransactionMap());
    }
};

const TString emptyString;

// Called if and only if row has CypressTransactionId.
// Updates TTransactions from row: adds or updates the information about transaction with id row.CypressTransactionId (namely, the title and the resource_usage).
// Note: AddVersionedRow is called from the reduce. And it is called from the current row only if it
// wasn't called from another row with the same cypressTransactionId, which was more than the current row.
// And regardless of whether the AddVersionedRow was called, an edge octi->cti must be added (see the code below the calls of AddVersionedRow).
void AddVersionedRow(TTransactions& transactions, const TRowAfterMap& row)
{
    const auto& cypressTransactionId = row.CypressTransactionId.value();
    TTransactionMap::InitializeIfKeyDoesntExist(transactions, cypressTransactionId);
    // TODO: set transactions[CypressTransactionId] without NodeResourceUsage
}

// This function adds resource usage B to resource usage A.
// It is used only in BuildRecursiveVersionedResourceUsage to add children's resources to parent's resources
void IaddVersionedResources(TResourceUsageMapPtr& to, const TResourceUsageMapPtr& from)
{
    Y_ABORT_IF(from == nullptr);
    for (const auto& [key, fromVal] : from->Map) {
        if (to == nullptr) {
            to = MakeSimpleShared<TResourceUsageMap>();
        }
        auto& toMap = to->Map;
        auto [itToByKey, toMapContainsKey] = toMap.emplace(key, fromVal);
        if (toMapContainsKey) {
            continue;
        }
        if (std::holds_alternative<TResourceUsageMapPtr>(fromVal)) {
            Y_ABORT_IF(!std::holds_alternative<TResourceUsageMapPtr>(itToByKey->second));
            auto& toKey = std::get<TResourceUsageMapPtr>(itToByKey->second)->Map;
            for (const auto& [subkey, fromSubVal] : std::get<TResourceUsageMapPtr>(fromVal)->Map) {
                auto [itToKeyBySubkey, toKeyContainsSubkey] = toKey.emplace(subkey, fromSubVal);
                if (toKeyContainsSubkey) {
                    continue;
                }
                itToKeyBySubkey->second = std::get<i64>(itToKeyBySubkey->second) + std::get<i64>(fromSubVal);
            }
        } else {
            Y_ABORT_IF(!std::holds_alternative<i64>(itToByKey->second));
            Y_ABORT_IF(!std::holds_alternative<i64>(fromVal));
            itToByKey->second = std::get<i64>(itToByKey->second) + std::get<i64>(fromVal);
        }
    }
}

// This function is called one time, after transactions in reduce is ready.
// transactions is a forest of rooted trees, each of nodes stores a title and resource usage of this the transaction (see TTransactions).
// This function makes recursive traversal, starting from node root. After all children are visited, it calls IaddVersionedResources.
// As a result, each visited node's resource usage contains a sum of resource usage in all its subtree.
TTransactionMapPtr BuildRecursiveVersionedResourceUsage(TTransactions& transactions, const TString& root)
{
    TTransactionMap::InitializeIfKeyDoesntExist(transactions, root);
    TTransactionMapPtr rvru = transactions[root];
    for (const auto& [transactionId, transactionMapPtr] : rvru->Transactions) {
        Y_ABORT_IF(transactionMapPtr == nullptr);
        auto& transaction = rvru->Transactions[transactionId];
        transaction = BuildRecursiveVersionedResourceUsage(transactions, transactionId);
        IaddVersionedResources(rvru->ResourceUsage, transaction->ResourceUsage);
    }
    return rvru;
}

#define SET_FIELD_IF_DEFINED(protoTo, from, field) \
    if (from.field.has_value()) { \
        protoTo.Set##field(from.field.value()); \
    }

#define SET_VERSIONED_FIELD_IF_DEFINED(protoTo, from, field) \
    if (from.field.has_value()) { \
        protoTo.SetVersioned##field(from.field.value()); \
    }

void PlusIfDefined(std::optional<i64>& to, const std::optional<i64>& from)
{
    if (from.has_value()) {
        to = to.value_or(0) + from.value();
    }
}

template <class K, class V>
void MergeHashMap(THashMap<K, V>& dest, const THashMap<K, V>& src) {
    for (const auto& [key, value] : src) {
        dest[key] = value;
    }
}

class TPathPatching
    : public IDoFn<TRowAfterMap, TRowAfterMap>
{
public:
    TPathPatching() = default;

    TPathPatching(TString nodeIdDict, TString nodeIdDictCluster, TString clusterToLookup)
        : NodeIdDict_(std::move(nodeIdDict)),
        NodeIdDictCluster_(std::move(nodeIdDictCluster)),
        ClusterToLookup_(std::move(clusterToLookup))
    {
    }

    void Start(TOutput<TRowAfterMap>&) override
    {
        TString clusterUrl = CreateUrl(NodeIdDictCluster_);
        NApi::NRpcProxy::TConnectionConfigPtr connectionConfig = NApi::NRpcProxy::TConnectionConfig::CreateFromClusterUrl(clusterUrl);
        NApi::IConnectionPtr connection = NApi::NRpcProxy::CreateConnection(connectionConfig);
        NApi::TClientOptions clientOptions = NApi::TClientOptions();
        clientOptions.Token = GetEnv("YT_SECURE_VAULT_YT_TOKEN");
        Client_ = connection->CreateClient(clientOptions);
    }

    void Do(const TRowAfterMap& row, TOutput<TRowAfterMap>& output)
    {
        if (row.PathPatched.has_value() && row.PathPatched.value()) {
            OriginalRows_.push_back(row);
            if (OriginalRows_.size() == 1000) {
                ResolvePaths(output);
            }
        } else {
            output.Add(row);
        }
    }

    void Finish(TOutput<TRowAfterMap>& output) override
    {
        if (!OriginalRows_.empty()) {
            ResolvePaths(output);
        }
    }

private:
    static TStringBuf GetClearNodeId(TStringBuf path)
    {
        TStringBuf nodeId{path};
        size_t posSlash = nodeId.find("/");
        if (posSlash != TStringBuf::npos) {
            nodeId = nodeId.substr(0, posSlash);
        }
        if (nodeId.back() == '&') {
            nodeId.remove_suffix(1);
        }
        Y_ABORT_IF(nodeId.empty());
        if (nodeId[0] == '#') {
            nodeId.remove_prefix(1);
        }
        return nodeId;
    }

    static TString CreateUrl(const TString& cluster)
    {
        TString url = cluster;
        if (url.find('.') == TString::npos && url.find(':') == TString::npos && url.find("localhost") == TString::npos) {
            url += ".yt.yandex.net";
        }
        return url;
    }

    void ResolvePaths(TOutput<TRowAfterMap>& output)
    {
        std::vector<NTableClient::TUnversionedOwningRow> requestRows(OriginalRows_.size());
        std::vector<NYT::NTableClient::TLegacyKey> rowsToLookup;
        rowsToLookup.reserve(OriginalRows_.size());
        NYT::TIntrusivePtr<NYT::NTableClient::TNameTable> nameTable = NYT::New<NYT::NTableClient::TNameTable>();
        for (size_t i = 0; i < OriginalRows_.size(); ++i) {
            TStringBuf nodeId = GetClearNodeId(OriginalRows_[i].Path);
            requestRows[i] = NYT::NNamedValue::MakeRow(nameTable, {{"cluster", ClusterToLookup_}, {"node_id", nodeId}});
            rowsToLookup.emplace_back(std::move(requestRows[i]));
        }
        auto keys = MakeSharedRange(rowsToLookup, nameTable);
        NYT::TIntrusivePtr<NYT::NApi::IRowset<NYT::NTableClient::TUnversionedRow>> lookupResult;
        for (size_t i = 0;; ++i) {
            try {
                lookupResult = NYT::NConcurrency::WaitFor(Client_->LookupRows(NodeIdDict_, nameTable, keys)).ValueOrThrow().Rowset;
                break;
            } catch (const std::exception& e) {
                Cerr << e.what() << Endl;
                if (i < 12) {
                    Sleep(TDuration::Seconds(5));  // sleep and retry
                } else {
                    throw;  // throw and terminate
                }
            }
        }
        std::vector<NYT::NTableClient::TUnversionedRow> rows = lookupResult->GetRows().ToVector();

        size_t resultIndex = 0;
        for (auto& originalRow : OriginalRows_) {
            if (resultIndex < rows.size() && rows[resultIndex][0].AsString() == ClusterToLookup_ && rows[resultIndex][1].AsString() == GetClearNodeId(originalRow.Path)) {
                Y_ENSURE(rows[resultIndex].GetCount() >= 3);
                originalRow.Path = rows[resultIndex][2].AsString();
                ++resultIndex;
            } else {
                originalRow.Path = NYT::Format("//$unknown_nodes/%v", GetClearNodeId(originalRow.Path));
            }
            output.Add(originalRow);
        }
        Y_ABORT_IF(resultIndex < rows.size());
        OriginalRows_.clear();
    }

private:
    TString NodeIdDict_;
    TString NodeIdDictCluster_;
    TString ClusterToLookup_;
    std::vector<TRowAfterMap> OriginalRows_;
    NYT::NApi::IClientPtr Client_;

    Y_SAVELOAD_DEFINE_OVERRIDE(NodeIdDict_, NodeIdDictCluster_, ClusterToLookup_);
};

class TFinalParDo
    : public IDoFn<TKV<std::tuple<TString, TString, TString>, TInputPtr<TRowAfterMap>>, TOutputMessage>
{
public:
    TFinalParDo() = default;

    TFinalParDo(const std::vector<TString>& media)
        : Media_(media)
    {
    }

    void Do(const TKV<std::tuple<TString, TString, TString>, TInputPtr<TRowAfterMap>>& kv, TOutput<TOutputMessage>& output)
    {
        TTransactions transactions;

        TOutputMessage newRow;
        newRow.SetAccount(std::get<0>(kv.Key()));
        const TString& path = std::get<1>(kv.Key());
        newRow.SetPath(path);
        newRow.SetType(std::get<2>(kv.Key()));
        newRow.SetDepth(Count(path.cbegin(), path.cend(), '/'));

        using TStringSetter = void (TOutputMessage::*)(const TProtoStringType&);
        TMutiAggregator<TRowAfterMap, TOutputMessage> aggregator(newRow);
        aggregator.SetNonTxOrAny(&TOutputMessage::SetCreationTime, &TRowAfterMap::CreationTime);
        aggregator.SetNonTxOrAny(&TOutputMessage::SetModificationTime, &TRowAfterMap::ModificationTime);
        aggregator.SetNonTxOrAny(&TOutputMessage::SetAccessTime, &TRowAfterMap::AccessTime);
        aggregator.SetNonTxOrAny((TStringSetter)&TOutputMessage::SetOwner, &TRowAfterMap::Owner);
        aggregator.SetNonTxOrAny(&TOutputMessage::SetDynamic, &TRowAfterMap::Dynamic);
        aggregator.SetNonTxOrAny(&TOutputMessage::SetPathPatched, &TRowAfterMap::PathPatched);

        TMergeAggregator resourceAggregator(&TRowAfterMap::ResourceUsage, MergeResourceUsageWith);
        TVersionedResourceUsageMap versionedResourceUsageMap;
        i64 directChildCount = 0;

        for (const auto& row : kv.Value()) { // Rows are NOT sorted.
            ++directChildCount;

            if (row.IsOriginal) {
                aggregator.Update(row);
            }

            if (row.CypressTransactionId.has_value()) {
                const TString& id = row.CypressTransactionId.value();
                if (versionedResourceUsageMap.contains(id)) {
                    versionedResourceUsageMap[id].MergeWith(row.VersionedResourceUsage);
                } else if (IsNonZero(row.VersionedResourceUsage)) { // Zeros transactions are not wtitten to disk
                    versionedResourceUsageMap[id] = row.VersionedResourceUsage;
                }
            } else {
                resourceAggregator.Update(row);
            }
        }
        --directChildCount;

        // Set data in row
        newRow.SetDirectChildCount(directChildCount);
        resourceAggregator.Finish(newRow, SetResourceUsageInRow);
        aggregator.Finish();

        TNode versionedResourceUsageNode = versionedResourceUsageMap.size() > TX_LIMIT ? VersionedResourceUsageTop(versionedResourceUsageMap, TX_TOP_N) : VersionedResourceUsageToNode(versionedResourceUsageMap);
        auto vruSummary = VersionedResourceUsageMapGetSummary(versionedResourceUsageMap);
        MergeHashMap(versionedResourceUsageNode.AsMap(), vruSummary.AsMap());
        SetVersionedResourceUsageMapInRow(newRow, versionedResourceUsageNode);

        TNode mediumColumns = TNode::CreateMap();
        if (resourceAggregator.HasValue()) {
            SetMap(mediumColumns, resourceAggregator.GetResult().DiskSpacePerMedium.AsMap(), Media_, "medium:");
        }
        if (!vruSummary.AsMap().empty()) {
            SetMap(mediumColumns, vruSummary["total"]["per_medium"].AsMap(), Media_, "versioned:medium:");
        }
        newRow.SetOtherColumns(NodeToYsonString(mediumColumns));

        output.Add(newRow);
    }

private:
    static void SetMap(TNode& to, const THashMap<TString, TNode>& from, const std::vector<TString>& keys, const TString& keyPrefix)
    {
        for (const TString& key : keys) {
            auto itFromByKey = from.find(key);
            if (itFromByKey != from.end()) {
                to[keyPrefix + key] = itFromByKey->second;
            } else {
                to[keyPrefix + key] = TNode::CreateEntity();
            }
        }
    };

private:
    std::vector<TString> Media_;

    Y_SAVELOAD_DEFINE_OVERRIDE(Media_);
};

void SetZeroIfResourceUsageContains(std::optional<i64>& opt, const THashMap<TString, TNode>& map, TStringBuf field)
{
    if (!opt.has_value() && map.contains(field)) {
        opt = 0;
    }
}

std::vector<TString> GetMedia(IClientPtr client)
{
    std::vector<TString> media;
    auto mediaNode = client->Get("//sys/media");
    for (const auto& [a, b] : mediaNode.AsMap()) {
        media.push_back(a);
    }
    return media;
}

TPipeline CreateEmptyPipeline(TString cluster,
    TString tmpDir,
    std::optional<TString> pool,
    std::optional<TString> networkProject,
    i64 memoryLimit)
{
    TYtPipelineConfig config;
    config.SetCluster(cluster);
    config.SetWorkingDir(tmpDir);

    TNode specPatch;
    if (pool) {
        specPatch["pool"] = pool.value();
    }
    if (networkProject) {
        specPatch["mapper"]["network_project"] = *networkProject;
    }
    specPatch["mapper"]["envrionment"]["YT_LOG_LEVEL"] = "debug";
    specPatch["secure_vault"]["YT_TOKEN"] = NYT::TConfig::Get()->Token;
    specPatch["reducer"]["memory_limit"] = memoryLimit;
    (*config.MutableOperatinonConfig())["ResourceUsageParDo"].SetSpecPatch(NodeToYsonString(specPatch));

    return MakeYtPipeline(config);
}

TRowAfterMap MapDo(const TInputMessage& row)
{
    TRowAfterMap result;

    result.IsOriginal = true;

    result.Account = row.GetAccount();
    result.Path = row.GetPath();
    result.Type = row.GetType();

    result.AccessTime = ParseTime(row.GetAccessTime());
    result.ModificationTime = ParseTime(row.GetModificationTime());
    result.CreationTime = ParseTime(row.GetCreationTime());

    result.PathPatched = HaveToPatchPath(row.GetPath());
    result.OriginalPath = row.GetPath();

    result.Id = row.GetId();
    result.ApproximateRowCount = row.HasChunkRowCount() ? row.GetChunkRowCount() : 0;

    if (row.HasCypressTransactionId()) {
        result.CypressTransactionId = row.GetCypressTransactionId();
    }
    if (row.HasCypressTransactionTitle()) {
        result.CypressTransactionTitle = row.GetCypressTransactionTitle();
    }
    if (row.HasOriginatorCypressTransactionId()) {
        result.OriginatorCypressTransactionId = row.GetOriginatorCypressTransactionId();
    }
    if (row.HasDynamic()) {
        auto dynamic = NodeFromYsonString(row.GetDynamic());
        result.Dynamic = (dynamic.IsBool() && dynamic.AsBool()) || row.GetDynamic() == "true";
    }
    if (row.HasOwner()) {
        result.Owner = row.GetOwner();
    }

    if (row.HasResourceUsage()) {
        TNode NodeResourceUsage = NodeFromYsonString(row.GetResourceUsage());

        // Double convert
        // WILL BE FIXED
        GetResourceUsageFromNode(result.ResourceUsage, NodeResourceUsage);
    }
    if (row.HasVersionedResourceUsage()) {
        result.VersionedResourceUsage = VersionedResourceUsageFromRow(row);
    }

    return result;
}


void ExpandForEachPathLevel(const TRowAfterMap& row, TOutput<TRowAfterMap>& output)
{
    TRowAfterMap result(row);
    output.Add(result);
    result.IsOriginal = false;
    result.Type = "map_node";
    if (result.CypressTransactionId.has_value()) {
        result.VersionedResourceUsage.IsOriginal = false;
    }
    for (int depth = Count(result.Path.cbegin(), result.Path.cend(), '/'); depth > 1; --depth) {
        while (result.Path.back() != '/') {
            Y_ABORT_IF(result.Path.empty());
            result.Path.pop_back();
        }
        result.Path.pop_back();
        Y_ABORT_IF(result.Path.empty());
        output.Add(result);
    }
}


void AddImportSnapshotToPipeline(
    TString sourceTable,
    TString destinationTable,
    TString nodeIdDictCluster,
    TString nodeIdDict,
    TString clusterToLookup,
    const std::vector<TString>& media,
    const TTableSchema& outputSchema,
    TPipeline& pipeline)
{
    pipeline
    | YtRead<TInputMessage>(sourceTable)
    | "ResourceUsageParDo" >> ParDo(MapDo)
    | "PathPatching" >> MakeParDo<TPathPatching>(nodeIdDict, nodeIdDictCluster, clusterToLookup)
    | "ExpandForEachPathLevel" >> ParDo(ExpandForEachPathLevel)
    | "MadeKV" >> ParDo([] (const TRowAfterMap& row) -> TKV<std::tuple<TString, TString, TString>, TRowAfterMap> {
        TKV<std::tuple<TString, TString, TString>, TRowAfterMap> kv;
        kv.Key() = std::make_tuple(row.Account, row.Path, row.Type);
        kv.Value() = row;
        return kv;
    })
    | GroupByKey()
    | "AggregatePathStatistic" >> MakeParDo<TFinalParDo>(media)
    | YtSortedWrite(NYT::TRichYPath{destinationTable}.OptimizeFor(EOptimizeForAttr::OF_LOOKUP_ATTR), outputSchema);
}


std::optional<TString> ExtractSnapshotId(TString objectName)
{
    std::vector<TString> splitByColon = StringSplitter(objectName).Split(':');
    if (splitByColon.size() != 2) {
        return {};
    }
    const auto& snapshotData = splitByColon[1];
    std::vector<TString> splitByDot = StringSplitter(snapshotData).Split('.');
    if (splitByDot.size() != 3) {
        return {};
    }
    return NYT::Format("%v.%v", splitByDot[0], splitByDot[1]);
}

TString GetSourceTableName(const TString& source, const TString& snapshotId)
{
    return source + "/" + snapshotId + "_unified_export";
}


i64 GetSnapshotCreationTimestamp(IClientPtr client, const TString& source, const TString& snapshotId)
{
    try {
        const TNode snapshotCreationTime = client->Get(GetSourceTableName(source, snapshotId) + "/@snapshot_creation_time");
        Y_ABORT_IF(!snapshotCreationTime.IsString());
        return TInstant::ParseIso8601(snapshotCreationTime.AsString()).Seconds();
    } catch (...) {
        // just pass
    }
    const TNode primaryCellTag = client->Get("//sys/primary_masters/@native_cell_tag");
    const TString snapshotPath = NYT::Format("//sys/admin/snapshots/%v/snapshots/%v", primaryCellTag.AsUint64(), snapshotId);
    const TNode snapshotCreationTime = client->Get(snapshotPath + "/@creation_time");
    Y_ABORT_IF(!snapshotCreationTime.IsString());
    return TInstant::ParseIso8601(snapshotCreationTime.AsString()).Seconds();
}


void ImportSnapshotMain(int argc, const char** argv)
{
    NLastGetopt::TOpts opts;
    opts.AddHelpOption('h');

    TString cluster;
    opts.AddFreeArgBinding("cluster", cluster, "Name of cluster with source table. If is not provided, is taken from env var YT_PROXY (in this case it must be set)");
    opts.AddLongOption("source").DefaultValue("//sys/admin/snapshots/snapshot_exports");
    opts.AddLongOption("destination").DefaultValue("//sys/admin/yt-microservices/resource_usage");
    opts.AddLongOption("node-id-dict").DefaultValue("//sys/admin/yt-microservices/node_id_dict/data");
    opts.AddLongOption("node-id-dict-cluster").DefaultValue("hahn");
    opts.AddLongOption("snapshot-id").DefaultValue("latest");
    opts.AddLongOption("pool").DefaultValue("yt-microservices");
    opts.AddLongOption("network-project");
    opts.AddLongOption("force").NoArgument();
    opts.AddLongOption("cluster-to-lookup"); // This is only needed for testing
    opts.AddLongOption("memory-limit").DefaultValue(8_GB);

    NLastGetopt::TOptsParseResult r(&opts, argc, argv);

    if (cluster.empty()) {
        cluster = GetEnv("YT_PROXY", "");
        if (cluster.empty()) {
            opts.PrintUsage(argv[0], Cerr);
            exit(EXIT_FAILURE);
        }
    }
    TString source = r.Get("source");
    TString destination = r.Get("destination");
    TString nodeIdDict = r.Get("node-id-dict");
    TString nodeIdDictCluster = r.Get("node-id-dict-cluster");
    TString snapshotId = r.Get("snapshot-id");
    std::optional<TString> pool = r.Has("pool") ? r.Get("pool") : std::optional<TString>{};
    std::optional<TString> networkProject;
    if (r.Has("network-project")) {
        networkProject = r.Get("network-project");
    }
    bool force = r.Has("force");
    TString clusterToLookup = r.GetOrElse("cluster-to-lookup", cluster); // This is only needed for testing
    i64 memoryLimit = FromString<i64>(r.Get("memory-limit"));

    NYT::TConfig::Get()->Token = LoadResourceUsageToken();

    THashSet<TString> snapshots;

    const auto client = NYT::CreateClient(cluster);

    TStringBuf unifiedExportSuffix{"_unified_export"};
    if (snapshotId == "all") {
        for (const auto& exportIdNode : client->List(source)) {
            Y_ABORT_IF(!exportIdNode.IsString());
            TString exportId = exportIdNode.AsString();
            if (exportId.EndsWith(unifiedExportSuffix)) {
                exportId.remove(exportId.size() - unifiedExportSuffix.size());
                snapshots.insert(exportId);
            }
        }
    } else if (snapshotId == "latest") {
        auto exportIdNode = client->Get(source + "/latest/@key");
        Y_ABORT_IF(!exportIdNode.IsString());
        TString exportId = exportIdNode.AsString();
        if (exportId.EndsWith(unifiedExportSuffix)) {
            exportId.remove(exportId.size() - unifiedExportSuffix.size());
        }
        snapshots.insert(exportId);
    } else {
        snapshots.insert(snapshotId);
    }

    if (!force) {
        for (const auto& objectName : client->List(destination)) {
            Y_ABORT_IF(!objectName.IsString());
            auto snapshotToErase = ExtractSnapshotId(objectName.AsString());
            if (snapshotToErase) {
                snapshots.erase(snapshotToErase.value());
            }
        }
    }

    auto media = GetMedia(client);
    auto outputSchema = CreateTableSchema<TOutputMessage>({"account", "depth", "path", "type"}).Strict(true).UniqueKeys(true);
    for (const auto& prefix : {TString{"medium:"}, TString{"versioned:medium:"}}) {
        for (const auto& medium : media) {
            outputSchema.AddColumn(prefix + medium, NTi::Optional(NTi::Int64()));
        }
    }
    TString tmpDir = NYT::Format("%v/tmp", destination);
    auto pipeline = CreateEmptyPipeline(cluster, tmpDir, pool, networkProject, memoryLimit);
    std::vector<TString> tmpDestinationTablesNames;
    std::vector<TString> realDestinationTablesNames;
    tmpDestinationTablesNames.reserve(snapshots.size());
    realDestinationTablesNames.reserve(snapshots.size());
    for (const auto& snapshotId : snapshots) {
        auto snapshotCreationZuluTimestamp = GetSnapshotCreationTimestamp(client, source, snapshotId);
        TString tmpDestinationTable = NYT::Format("%v/tmp/%v:%v.resource_usage", destination, snapshotCreationZuluTimestamp, snapshotId);
        TString realDestinationTable = NYT::Format("%v/%v:%v.resource_usage", destination, snapshotCreationZuluTimestamp, snapshotId);

        TString sourceTable = GetSourceTableName(source, snapshotId);
        AddImportSnapshotToPipeline(std::move(sourceTable), tmpDestinationTable, nodeIdDictCluster, nodeIdDict, clusterToLookup, media, outputSchema, pipeline);

        tmpDestinationTablesNames.emplace_back(std::move(tmpDestinationTable));
        realDestinationTablesNames.emplace_back(std::move(realDestinationTable));
    }
    for (const auto& snapshotId : snapshots) {
        YT_LOG_INFO("Attempting import of [%v]", snapshotId);
    }
    pipeline.Run();

    for (const auto& [snapshotId, tmpDestinationTable, realDestinationTable] : Zip(snapshots, tmpDestinationTablesNames, realDestinationTablesNames)) {
        auto alterTableOptions = NYT::TAlterTableOptions().Dynamic(true);
        client->AlterTable(tmpDestinationTable, alterTableOptions);

        auto moveOptions = NYT::TMoveOptions().Force(true);
        client->Move(tmpDestinationTable, realDestinationTable, moveOptions);

        client->MountTable(realDestinationTable);
        client->Set(NYT::Format("%v/@_features", realDestinationTable), TNode::CreateMap()("recursive_versioned_resource_usage", 1)("type_in_key", 1));

        YT_LOG_INFO("Import of [%v] done.", snapshotId);
    }
}

#include "storages_yt_nodes.h"

#include "config.h"
#include "query_context.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/rpc_helpers.h>
#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/core/ytree/ypath_resolver.h>

#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>

#include <Storages/System/IStorageSystemOneBlock.h>

#include <Core/NamesAndTypes.h>

namespace NYT::NClickHouseServer {

using namespace NApi;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NYPath;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

DB::NamesAndTypesList MakeTypesNullable(DB::NamesAndTypesList list)
{
    for (auto& elem : list) {
        // Note: Always create new NameAndTypePair instead of modifying the old one,
        // because it contains some private fields calculated in constructor.
        elem = {std::move(elem.name), DB::makeNullable(std::move(elem.type))};
    }
    return list;
}

//! Returns a list of table paths from provided dirs with its attributes.
//! If any master request is failed (e.g. ResolveError), the error will be returned.
std::vector<TErrorOr<INodePtr>> ListDirs(
    const std::vector<TString>& dirPaths,
    const std::vector<TString>& attributesToFetch,
    TQueryContext* queryContext)
{
    // TODO(dakovalkov): Take it from config?
    NApi::TMasterReadOptions masterReadOptions;

    const auto& settings = queryContext->Settings->ListDir;

    const auto& client = queryContext->Client();
    const auto& connection = client->GetNativeConnection();
    TObjectServiceProxy proxy(client->GetMasterChannelOrThrow(masterReadOptions.ReadFrom));
    auto batchReq = proxy.ExecuteBatch();
    SetBalancingHeader(batchReq, connection, masterReadOptions);

    int index = 0;
    for (auto& path : dirPaths) {
        auto req = TYPathProxy::List(path);
        SetCachingHeader(req, connection, masterReadOptions);
        ToProto(req->mutable_attributes()->mutable_keys(), attributesToFetch);
        req->Tag() = index;
        ++index;
        if (settings->MaxSize) {
            req->set_limit(settings->MaxSize);
        }
        batchReq->AddRequest(req);
    }

    auto batchResponse = WaitFor(batchReq->Invoke())
        .ValueOrThrow();

    std::vector<TErrorOr<INodePtr>> result;

    for (const auto& [tag, rspOrError] : batchResponse->GetTaggedResponses<TYPathProxy::TRspList>()) {
        index = std::any_cast<int>(tag);
        if (rspOrError.IsOK()) {
            auto listNode = ConvertToNode(TYsonString(rspOrError.Value()->value()));

            if (listNode->Attributes().Get<bool>("incomplete", false)) {
                THROW_ERROR_EXCEPTION("Directory contains too many nodes, list request returned incomplete result");
            }

            auto nodes = listNode->AsList()->GetChildren();
            for (auto& node : nodes) {
                node->AsString()->SetValue(dirPaths[index] + "/" + node->GetValue<TString>());
                result.push_back(std::move(node));
            }
        } else {
            result.push_back(TError(rspOrError));
        }
    }

    return result;
}

//! Returns a list of object paths with its attributes.
std::vector<TErrorOr<INodePtr>> GetNodeAttributes(
    const std::vector<TString>& paths,
    const std::vector<TString>& attributesToFetch,
    TQueryContext* queryContext)
{
    // TODO(dakovalkov): Take it from config?
    NApi::TMasterReadOptions masterReadOptions;

    const auto& client = queryContext->Client();
    const auto& connection = DynamicPointerCast<NApi::NNative::IConnection>(client->GetConnection());
    TObjectServiceProxy proxy(client->GetMasterChannelOrThrow(masterReadOptions.ReadFrom));
    auto batchReq = proxy.ExecuteBatch();
    SetBalancingHeader(batchReq, connection, masterReadOptions);

    int index = 0;
    for (auto& path : paths) {
        auto req = TYPathProxy::Get(path + "/@");
        SetCachingHeader(req, connection, masterReadOptions);
        ToProto(req->mutable_attributes()->mutable_keys(), attributesToFetch);
        req->Tag() = index;
        ++index;
        batchReq->AddRequest(req);
    }

    auto batchResponse = WaitFor(batchReq->Invoke())
        .ValueOrThrow();

    std::vector<TErrorOr<INodePtr>> result(paths.size());

    for (const auto& [tag, rspOrError] : batchResponse->GetTaggedResponses<TYPathProxy::TRspGet>()) {
        index = std::any_cast<int>(tag);
        if (rspOrError.IsOK()) {
            auto ysonAttributes = TYsonString(rspOrError.Value()->value());
            auto node = BuildYsonNodeFluently().Value(paths[index]);
            auto* attributes = node->MutableAttributes();
            attributes->Clear();
            TAttributeConsumer consumer(attributes);
            Serialize(ysonAttributes, &consumer);

            result[index] = std::move(node);
        } else {
            result[index] = TError(rspOrError);
        }
    }

    YT_VERIFY(result.size() == paths.size());

    return result;
}

std::vector<INodePtr> ValuesOrThrow(const std::vector<TErrorOr<INodePtr>>& nodesOrErrors)
{
    std::vector<INodePtr> nodes;
    nodes.reserve(nodesOrErrors.size());
    for (const auto& nodeOrError : nodesOrErrors) {
        nodes.push_back(nodeOrError.ValueOrThrow());
    }
    return nodes;
}

bool IsLink(const INodePtr& node)
{
    return node->Attributes().Get<TString>("type") == "link";
}

bool IsTable(const INodePtr& node)
{
    return node->Attributes().Get<TString>("type") == "table";
}

//! Takes a list of node paths with its attributes and resolve all 'link' nodes to its target.
//! If the link is not resolvable (ResolveError), the link is retained untouched.
std::vector<INodePtr> ResolveLinks(
    std::vector<INodePtr> nodes,
    const std::vector<TString>& attributesToFetch,
    TQueryContext* queryContext)
{
    std::vector<TString> pathsToResolve;
    pathsToResolve.reserve(nodes.size());

    for (const auto& node : nodes) {
        if (IsLink(node)) {
            pathsToResolve.push_back(node->GetValue<TString>());
        }
    }

    // Nothing to resolve.
    if (pathsToResolve.empty()) {
        return nodes;
    }

    auto resolvedNodes = GetNodeAttributes(pathsToResolve, attributesToFetch, queryContext);

    int linkIndex = 0;
    for (auto& node : nodes) {
        if (IsLink(node)) {
            const auto& resolvedNodeOrError = resolvedNodes[linkIndex];
            if (resolvedNodeOrError.IsOK()) {
                auto resolvedNode = resolvedNodeOrError.Value();
                YT_VERIFY(node->GetValue<TString>() == resolvedNode->GetValue<TString>());
                node = std::move(resolvedNode);
            } else if (resolvedNodeOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
                // Link leads to nonexistent object. Retain link 'unresolved'.
            } else {
                // Errors other than ResolveError are not expected.
                THROW_ERROR_EXCEPTION("Failed to resolve links in directory")
                    << resolvedNodeOrError;
            }
            ++linkIndex;
        }
    }

    return nodes;
}

// TODO(dakovalkov): Copy pasted from table_functions_concat.cpp
TString BaseName(const TYPath& path)
{
    return TString(path.begin() + path.rfind('/') + 1, path.end());
}

TString DirPath(const TYPath& path)
{
    return TString(path.begin(), path.begin() + path.rfind('/'));
}

std::pair<TInstant, TInstant> GetLogTableTimeInterval(TString path)
{
    auto startTime = TInstant::ParseIso8601(BaseName(path));
    auto scaleString = BaseName(DirPath(path));
    if (scaleString.EndsWith("min")) {
        // util parses minuts with 'm' suffix.
        scaleString.resize(scaleString.size() - 2);
    }
    auto scale = TDuration::Parse(scaleString);
    return {startTime, startTime + scale};
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TStorageYtNodesBase
    : public DB::IStorage
{
public:
    TStorageYtNodesBase()
        : DB::IStorage({"YT", "nodes"})
    {
        DB::StorageInMemoryMetadata storageMetadata;
        storageMetadata.setColumns(DB::ColumnsDescription(ExplicitColumns));
        setInMemoryMetadata(storageMetadata);
    }

    std::string getName() const override
    {
        return "YtNodes";
    }

    DB::Pipe read(
        const DB::Names& columnNames,
        const DB::StorageMetadataPtr& metadataSnapshot,
        DB::SelectQueryInfo& /*queryInfo*/,
        DB::ContextPtr context,
        DB::QueryProcessingStage::Enum /*processingStage*/,
        size_t /*maxBlockSize*/,
        unsigned /*numStreams*/) override
    {
        auto* queryContext = GetQueryContext(context);
        // const auto& Logger = queryContext->Logger;
        auto client = queryContext->Client();

        THashSet<TString> uniqueAttributes;

        for (const auto& columnName : columnNames) {
            if (columnName.starts_with('$')) {
                // Builtin column, do not need to fetch anything.
            } else if (ResourceUsageAttributes.contains(columnName)) {
                uniqueAttributes.emplace("resource_usage");
            } else {
                uniqueAttributes.emplace(columnName);
            }
        }
        uniqueAttributes.emplace("type");

        std::vector<TString> attributesToFetch = {uniqueAttributes.begin(), uniqueAttributes.end()};

        auto nodes = FetchNodesWithAttributes(attributesToFetch, queryContext);

        auto header = metadataSnapshot->getSampleBlockForColumns(columnNames, getVirtuals());
        auto resultColumns = header.cloneEmptyColumns();

        for (const auto& node : nodes) {
            auto path = node->GetValue<TString>();

            const auto& attributes = node->Attributes();
            for (size_t index = 0; index < columnNames.size(); ++index) {
                const auto& columnName = columnNames[index];
                auto& column = resultColumns[index];

                DB::WhichDataType type = DB::removeNullable(header.getByPosition(index).type);

                DB::Field value;

                if (columnName == "$key") {
                    value = std::string(BaseName(path));
                } else if (columnName == "$path") {
                    value = std::string(path);
                } else if (ResourceUsageAttributes.contains(columnName)) {
                    if (auto resourceUsageYson = attributes.FindYson("resource_usage")) {
                        if (!type.isInt64()) {
                            // Only int64 type is supported for resource_usage attributes.
                            THROW_ERROR_EXCEPTION(
                                "Unexpected type %v of column %Qv in StorageYtDir",
                                header.getByPosition(index).type->getName(),
                                columnName);
                        }
                        if (auto attribute = TryGetInt64(resourceUsageYson.AsStringBuf(), "/" + columnName)) {
                            value = *attribute;
                        }
                    }
                } else {
                    if (type.isInt64()) {
                        if (auto attribute = attributes.Find<i64>(columnName)) {
                            value = *attribute;
                        }
                    } else if (type.isUInt64()) {
                        if (auto attribute = attributes.Find<ui64>(columnName)) {
                            value = *attribute;
                        }
                    } else if (type.isFloat64()) {
                        if (auto attribute = attributes.Find<double>(columnName)) {
                            value = *attribute;
                        }
                    } else if (type.isUInt8()) {
                        if (auto attribute = attributes.Find<bool>(columnName)) {
                            value = *attribute;
                        }
                    } else if (type.isString() && YsonAttributes.contains(columnName)) {
                        if (auto attribute = attributes.FindYson(columnName)) {
                            value = std::string(attribute.AsStringBuf());
                        }
                    } else if (type.isString()) {
                        if (auto attribute = attributes.Find<TString>(columnName)) {
                            value = std::string(*attribute);
                        }
                    } else {
                        THROW_ERROR_EXCEPTION(
                            "Unexpected type %v of column %Qv in StorageYtDir",
                            header.getByPosition(index).type->getName(),
                            columnName);
                    }
                }
                column->insert(value);
            }
        }

        auto rowCount = resultColumns.at(0)->size();
        DB::Chunk chunk(std::move(resultColumns), rowCount);

        return DB::Pipe(std::make_shared<DB::SourceFromSingleChunk>(std::move(header), std::move(chunk)));
    }

    DB::NamesAndTypesList getVirtuals() const override
    {
        return ImplicitColumns;
    }

protected:
    virtual std::vector<INodePtr> FetchNodesWithAttributes(
        const std::vector<TString>& attributesToFetch,
        TQueryContext* queryContext) = 0;

private:
    //! A set of columns which are part of "resource_usage" attribute.
    static inline const THashSet<TString> ResourceUsageAttributes {
        "disk_space",
        "tablet_count",
        "master_memory",
    };

    // TODO(dakovalkov): Eliminate this when YsonType is ready.
    //! A set of columns which store Yson values.
    static inline const THashSet<TString> YsonAttributes {
        "acl",
        "effective_acl",
        "user_attribute_keys",
        "opaque_attribute_keys"
        "user_attributes",
        "estimated_creation_time",
        "locks",
        "resource_usage",
        "recursive_resource_usage",
        "chunk_ids",
        "compression_statistics",
        "erasure_statistics",
        "multicell_statistics",
        "chunk_format_statistics",
        "media",
        "security_tags",
        "key_columns",
        "schema",
        "optimize_for_statistics",
        "table_chunk_format_statistics",
        "hunk_statistics",
    };

    //! A set of 'real' columns of the table in terms of CH.
    //! This set of columns appears in 'select *' expression.
    //! Only lightweight and frequently used columns should be here.
    static inline const DB::NamesAndTypesList ExplicitColumns = MakeTypesNullable({
        {"$key",                    std::make_shared<DB::DataTypeString>()}, // Builtin.
        {"$path",                   std::make_shared<DB::DataTypeString>()}, // Builtin.

        {"type",                    std::make_shared<DB::DataTypeString>()},
        {"dynamic",                 std::make_shared<DB::DataTypeUInt8>()},

        {"row_count",               std::make_shared<DB::DataTypeInt64>()},
        {"data_weight",             std::make_shared<DB::DataTypeInt64>()},
        {"compressed_data_size",    std::make_shared<DB::DataTypeInt64>()},
        {"uncompressed_data_size",  std::make_shared<DB::DataTypeInt64>()},
        {"disk_space",              std::make_shared<DB::DataTypeInt64>()}, // From resource_usage.
        {"chunk_count",             std::make_shared<DB::DataTypeInt64>()},

        {"optimize_for",            std::make_shared<DB::DataTypeString>()},
        {"erasure_codec",           std::make_shared<DB::DataTypeString>()},
        {"primary_medium",          std::make_shared<DB::DataTypeString>()},
        {"compression_codec",       std::make_shared<DB::DataTypeString>()},

        {"account",                 std::make_shared<DB::DataTypeString>()},
        {"owner",                   std::make_shared<DB::DataTypeString>()},

        {"creation_time",           std::make_shared<DB::DataTypeString>()},
        {"modification_time",       std::make_shared<DB::DataTypeString>()},
        {"access_time",             std::make_shared<DB::DataTypeString>()},
    });

    //! A set of 'virtual' columns of the table in terms of CH.
    //! They do not appear in 'select *' expression, but they can be used explicitly in query.
    static inline const DB::NamesAndTypesList ImplicitColumns = MakeTypesNullable({
        {"key",                     std::make_shared<DB::DataTypeString>()},
        {"path",                    std::make_shared<DB::DataTypeString>()},

        {"id",                      std::make_shared<DB::DataTypeString>()},
        {"builtin",                 std::make_shared<DB::DataTypeUInt8>()},
        {"ref_counter",             std::make_shared<DB::DataTypeInt64>()},
        {"ephemeral_ref_counter",   std::make_shared<DB::DataTypeInt64>()},
        {"weak_ref_counter",        std::make_shared<DB::DataTypeInt64>()},
        {"foreign",                 std::make_shared<DB::DataTypeUInt8>()},
        {"native_cell_tag",         std::make_shared<DB::DataTypeUInt64>()},
        {"inherit_acl",             std::make_shared<DB::DataTypeUInt8>()},
        {"acl",                     std::make_shared<DB::DataTypeString>()}, // YSON.
        {"effective_acl",           std::make_shared<DB::DataTypeString>()}, // YSON.
        {"user_attribute_keys",     std::make_shared<DB::DataTypeString>()}, // YSON.
        {"opaque_attribute_keys",   std::make_shared<DB::DataTypeString>()}, // YSON.
        {"user_attributes",         std::make_shared<DB::DataTypeString>()}, // YSON.
        {"life_stage",              std::make_shared<DB::DataTypeString>()},
        {"estimated_creation_time", std::make_shared<DB::DataTypeString>()}, // YSON.
        {"parent_id",               std::make_shared<DB::DataTypeString>()},
        {"external",                std::make_shared<DB::DataTypeUInt8>()},
        {"locks",                   std::make_shared<DB::DataTypeString>()}, // YSON.
        {"lock_count",              std::make_shared<DB::DataTypeUInt64>()},
        {"lock_mode",               std::make_shared<DB::DataTypeString>()},
        {"access_counter",          std::make_shared<DB::DataTypeInt64>()},
        {"revision",                std::make_shared<DB::DataTypeUInt64>()},
        {"attribute_revision",      std::make_shared<DB::DataTypeUInt64>()},
        {"content_revision",        std::make_shared<DB::DataTypeUInt64>()},
        {"native_content_revision", std::make_shared<DB::DataTypeUInt64>()},
        {"resource_usage",          std::make_shared<DB::DataTypeString>()}, // YSON.
        {"recursive_resource_usage", std::make_shared<DB::DataTypeString>()}, // YSON.
        {"opaque",                  std::make_shared<DB::DataTypeUInt8>()},
        {"shard_id",                std::make_shared<DB::DataTypeString>()},
        {"resolve_cached",          std::make_shared<DB::DataTypeUInt8>()},
        {"annotation",              std::make_shared<DB::DataTypeString>()},
        {"annotation_path",         std::make_shared<DB::DataTypeString>()},
        {"count",                   std::make_shared<DB::DataTypeInt64>()},

        {"target_path",             std::make_shared<DB::DataTypeString>()},
        {"broken",                  std::make_shared<DB::DataTypeUInt8>()},

        {"chunk_list_id",           std::make_shared<DB::DataTypeString>()},
        {"chunk_ids",               std::make_shared<DB::DataTypeString>()}, // YSON.
        {"compression_statistics",  std::make_shared<DB::DataTypeString>()}, // YSON.
        {"erasure_statistics",      std::make_shared<DB::DataTypeString>()}, // YSON.
        {"multicell_statistics",    std::make_shared<DB::DataTypeString>()}, // YSON.
        {"chunk_format_statistics", std::make_shared<DB::DataTypeString>()}, // YSON.
        {"compression_ratio",       std::make_shared<DB::DataTypeFloat64>()},
        {"update_mode",             std::make_shared<DB::DataTypeString>()},
        {"replication_factor",      std::make_shared<DB::DataTypeInt64>()},
        {"vital",                   std::make_shared<DB::DataTypeUInt8>()},
        {"media",                   std::make_shared<DB::DataTypeString>()}, // YSON.
        {"security_tags",           std::make_shared<DB::DataTypeString>()}, // YSON.
        {"chunk_merger_mode",       std::make_shared<DB::DataTypeString>()},
        {"is_being_merged",         std::make_shared<DB::DataTypeUInt8>()},
        {"enable_skynet_sharing",   std::make_shared<DB::DataTypeUInt8>()},

        {"chunk_row_count",         std::make_shared<DB::DataTypeInt64>()},
        {"sorted",                  std::make_shared<DB::DataTypeUInt8>()},
        {"key_columns",             std::make_shared<DB::DataTypeString>()}, // YSON.
        {"schema",                  std::make_shared<DB::DataTypeString>()}, // YSON.
        {"schema_id",               std::make_shared<DB::DataTypeString>()},
        {"schema_duplicate_count",  std::make_shared<DB::DataTypeInt64>()},
        {"tablet_cell_bundle",      std::make_shared<DB::DataTypeString>()},
        {"atomicity",               std::make_shared<DB::DataTypeString>()},
        {"commit_ordering",         std::make_shared<DB::DataTypeString>()},
        {"in_memory_mode",          std::make_shared<DB::DataTypeString>()},
        {"optimize_for_statistics", std::make_shared<DB::DataTypeString>()}, // YSON.
        {"schema_mode",             std::make_shared<DB::DataTypeString>()},
        {"table_chunk_format_statistics", std::make_shared<DB::DataTypeString>()}, // YSON.
        {"hunk_statistics",         std::make_shared<DB::DataTypeString>()}, // YSON.

        {"tablet_count",            std::make_shared<DB::DataTypeInt64>()}, // From resourse_usage.
        {"master_memory",           std::make_shared<DB::DataTypeInt64>()}, // From resourse_usage.
    });
};

////////////////////////////////////////////////////////////////////////////////

class TStorageYtDir
    : public TStorageYtNodesBase
{
public:
    TStorageYtDir(TString dirPath, TStorageYtDirOptions options)
        : DirPath_(std::move(dirPath))
        , Options_(std::move(options))
    { }

protected:
    std::vector<INodePtr> FetchNodesWithAttributes(
        const std::vector<TString>& attributesToFetch,
        TQueryContext* queryContext) override
    {
        auto nodes = ValuesOrThrow(ListDirs({DirPath_}, attributesToFetch, queryContext));

        std::erase_if(nodes, [this] (const INodePtr& node) {
            return !IsNodeKeySuitable(node);
        });

        if (Options_.ResolveLinks) {
            nodes = ResolveLinks(nodes, attributesToFetch, queryContext);
        }

        if (Options_.TablesOnly) {
            std::erase_if(nodes, [] (const INodePtr& node) {
                return !IsTable(node);
            });
        }

        return nodes;
    }

private:
    TString DirPath_;
    TStorageYtDirOptions Options_;

    bool IsNodeKeySuitable(const INodePtr& node) const
    {
        if (Options_.From && BaseName(node->GetValue<TString>()) < *Options_.From) {
            return false;
        }
        if (Options_.To && BaseName(node->GetValue<TString>()) > *Options_.To) {
            return false;
        }
        return true;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TStorageYtNodeAttributes
    : public TStorageYtNodesBase
{
public:
    explicit TStorageYtNodeAttributes(std::vector<TString> paths)
        : Paths_(std::move(paths))
    { }

protected:
    std::vector<INodePtr> FetchNodesWithAttributes(
        const std::vector<TString>& attributesToFetch,
        TQueryContext* queryContext) override
    {
        return ValuesOrThrow(GetNodeAttributes(Paths_, attributesToFetch, queryContext));
    }

private:
    std::vector<TString> Paths_;
};

////////////////////////////////////////////////////////////////////////////////

class TStorageYtLogTables
    : public TStorageYtNodesBase
{
public:
    TStorageYtLogTables(TString logPath, TStorageYtLogTablesOptions options)
        : LogPath_(std::move(logPath))
        , Options_(std::move(options))
    {
        // Transform relative path into absolute one.
        if (!LogPath_.StartsWith("//")) {
            LogPath_ = "//logs/" + LogPath_;
        }
    }

protected:
    std::vector<INodePtr> FetchNodesWithAttributes(
        const std::vector<TString>& attributesToFetch,
        TQueryContext* queryContext) override
    {
        std::vector<TString> paths;
        for (const auto& suffix : {"/1d", "/1h", "/30min", "/stream/5min"}) {
            paths.push_back(LogPath_ + suffix);
        }

        auto nodesOrErrors = ListDirs(paths, attributesToFetch, queryContext);

        std::vector<INodePtr> nodes;
        nodes.reserve(nodesOrErrors.size());

        for (const auto& nodeOrError : nodesOrErrors) {
            if (nodeOrError.IsOK()) {
                nodes.push_back(nodeOrError.Value());
            } else if (nodeOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
                // Some dirs may not exist, ignore them.
            } else {
                THROW_ERROR_EXCEPTION("Failed to list directory")
                    << nodeOrError;
            }
        }

        if (nodes.empty() && nodesOrErrors.size() == 4) {
            THROW_ERROR_EXCEPTION("Path %Qv does not exists or does not correspond to log directory",
                LogPath_);
        }

        std::erase_if(nodes, [this] (const INodePtr& node) {
            return !IsTable(node) || !IsNodeKeySuitable(node);
        });

        return UnifyTableData(std::move(nodes));
    }

private:
    TString LogPath_;
    TStorageYtLogTablesOptions Options_;

    bool IsNodeKeySuitable(const INodePtr& node)
    {
        auto [startTime, finishTime] = GetLogTableTimeInterval(node->GetValue<TString>());

        if (Options_.From && *Options_.From >= finishTime) {
            return false;
        }
        if (Options_.To && *Options_.To <= startTime) {
            return false;
        }
        return true;
    }

    std::vector<INodePtr> UnifyTableData(std::vector<INodePtr> nodes)
    {
        // Sort time intervals by (startTime ASC, finishTime DESC).
        // Example:
        // [----]
        // [-]
        //    [-]
        //       [----]
        std::sort(nodes.begin(), nodes.end(), [] (const INodePtr& lhs, const INodePtr& rhs) {
            auto [lhsStartTime, lhsFinishTime] = GetLogTableTimeInterval(lhs->GetValue<TString>());
            auto [rhsStartTime, rhsFinishTime] = GetLogTableTimeInterval(rhs->GetValue<TString>());

            if (lhsStartTime != rhsStartTime) {
                return lhsStartTime < rhsStartTime;
            }
            return lhsFinishTime > rhsFinishTime;
        });

        auto lastFinishTime = TInstant::Zero();

        std::vector<INodePtr> result;
        result.reserve(nodes.size());

        // Delete overlapping tables using 'scan-line' technique.
        for (const auto& node : nodes) {
            auto [startTime, finishTime] = GetLogTableTimeInterval(node->GetValue<TString>());
            if (lastFinishTime < finishTime) {
                if (startTime < lastFinishTime) {
                    THROW_ERROR_EXCEPTION("There are intersecting by time interval tables in log directory");
                }
                result.push_back(node);
                lastFinishTime = finishTime;
            }
        }

        return result;
    }
};

////////////////////////////////////////////////////////////////////////////////

DB::StoragePtr CreateStorageYtDir(TString dirPath, TStorageYtDirOptions options)
{
    return std::make_shared<TStorageYtDir>(std::move(dirPath), std::move(options));
}

////////////////////////////////////////////////////////////////////////////////

DB::StoragePtr CreateStorageYtNodeAttributes(std::vector<TString> paths)
{
    return std::make_shared<TStorageYtNodeAttributes>(std::move(paths));
}

////////////////////////////////////////////////////////////////////////////////

DB::StoragePtr CreateStorageYtLogTables(TString logPath, TStorageYtLogTablesOptions options)
{
    return std::make_shared<TStorageYtLogTables>(std::move(logPath), std::move(options));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer

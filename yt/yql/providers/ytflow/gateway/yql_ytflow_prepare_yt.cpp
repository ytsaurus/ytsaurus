#include "yql_ytflow_prepare.h"
#include "yql_ytflow_prepare_common.h"
#include "yql_ytflow_schema.h"
#include "yql_ytflow_yt_clients_cache.h"
#include "yql_ytflow_utils.h"
#include "yql_ytflow_worker_config.h"

#include <library/cpp/yt/misc/enum.h>
#include <library/cpp/yt/string/enum.h>

#include <yql/essentials/providers/common/schema/expr/yql_expr_schema.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/yql_panic.h>

#include <yt/yql/providers/ytflow/expr_nodes/yql_ytflow_expr_nodes.h>
#include <yt/yql/providers/ytflow/integration/interface/yql_ytflow_integration.h>
#include <yt/yql/providers/ytflow/integration/proto/yt.pb.h>
#include <yt/yql/providers/ytflow/provider/yql_ytflow_utils.h>
#include <yt/yt/client/api/client.h>
#include <yt/yt/client/tablet_client/public.h>
#include <yt/yt/core/actions/bind.h>
#include <yt/yt/core/actions/future.h>
#include <yt/yt/core/ytree/fluent.h>

#include <util/string/join.h>
#include <util/system/guard.h>
#include <util/system/spinlock.h>

#include <google/protobuf/any.pb.h>

#include <functional>


namespace NYql::NYtflow::NPrepare::NPrivate {

using namespace NNodes;


class TYtMixin
    : public virtual TConfigMixin
{
public:
    struct TField
    {
        TString Name;
        TString Type;
        TString Expression;

        bool IsKeyField = false;
        bool IsRequired = false;

        std::optional<i64> MaxInlineHunkSize;
    };

public:
    void Init(TContext& prepareCtx)
    {
        TConfigMixin::Init(prepareCtx);
        ConfigClusters = prepareCtx.ConfigClusters;

        auto rpcTimeout = GetConfig()->_RpcTimeout.Get();
        YQL_ENSURE(rpcTimeout, "Ytflow._RpcTimeout system setting is not set");

        RpcTimeout = *rpcTimeout;

        ClientsCache = CreateYtClientsCache(ConfigClusters);
    }

    TString GetYtConsumerPath() const
    {
        return GetConfig()->GetYtConsumerPath();
    }

    uint64_t GetYtConsumerVital() const
    {
        return GetConfig()->GetYtConsumerVital();
    }

    TString GetYtProducerPath() const
    {
        return GetConfig()->GetYtProducerPath();
    }

    uint64_t GetYtPartitionCount() const
    {
        auto value = GetConfig()->YtPartitionCount.Get();
        YQL_ENSURE(value, "Ytflow.YtPartitionCount pragma is not set");
        return *value;
    }

    NYT::TFuture<void> EnsureExpectedYtNode(
        TString path,
        NYT::NObjectClient::EObjectType type,
        NYT::NYTree::IAttributeDictionaryPtr extraAttributes,
        NYT::NApi::IClientPtr client,
        const std::pair<TString, TString>& logCtx,
        NYT::IInvokerPtr invoker) const
    {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(logCtx);

        YQL_CLOG(INFO, ProviderYtflow)
            << "Requesting attributes of yt node " << path << " ...";

        auto attributeKeyFilter = std::vector<std::string>{"type"};
        for (const auto& key : extraAttributes->ListKeys()) {
            attributeKeyFilter.push_back(key);
        }

        auto options = NYT::NApi::TGetNodeOptions();
        options.Attributes = NYT::NYTree::TAttributeFilter(
            std::move(attributeKeyFilter));

        options.Timeout = RpcTimeout;

        return client->GetNode(path, options)
            .Apply(BIND([=](const NYT::NYson::TYsonString& value) {
                YQL_LOG_CTX_ROOT_SESSION_SCOPE(logCtx);

                auto node = NYT::NYTree::ConvertTo<NYT::NYTree::INodePtr>(value);
                auto typeAttribute = node->Attributes().Get<TString>("type");
                auto decodedType = NYT::DecodeEnumValue(typeAttribute);

                auto realType = NYT::TEnumTraits<
                    NYT::NObjectClient::EObjectType>::FromString(decodedType);

                YQL_ENSURE(realType == type);

                auto updateAttributes = NYT::NYTree::CreateEphemeralAttributes();
                for (const auto& [key, value] : extraAttributes->ListPairs()) {
                    if (!node->Attributes().FindYson(key)) {
                        updateAttributes->SetYson(key, value);
                    }
                }

                if (updateAttributes->ListKeys().empty()) {
                    YQL_CLOG(INFO, ProviderYtflow)
                        << "Skipped updating attributes of yt node " << path;

                    return NYT::OKFuture;
                }

                YQL_CLOG(INFO, ProviderYtflow)
                    << "Updating attributes of yt node " << path << " ...";

                NYT::NApi::TMultisetAttributesNodeOptions multisetAttributesNodeOptions;
                multisetAttributesNodeOptions.Timeout = RpcTimeout;

                return client->MultisetAttributesNode(
                    NYT::Format("%v/@", path),
                    updateAttributes->ToMap(),
                    std::move(multisetAttributesNodeOptions))
                    .Apply(BIND([=] {
                        YQL_LOG_CTX_ROOT_SESSION_SCOPE(logCtx);

                        YQL_CLOG(INFO, ProviderYtflow)
                            << "Updated attributes of yt node " << path;

                    }).AsyncVia(invoker));
            }).AsyncVia(invoker));
    }

    NYT::TFuture<void> EnsureYtTableMounted(
        TString path,
        NYT::NApi::IClientPtr client,
        const std::pair<TString, TString>& logCtx,
        NYT::IInvokerPtr invoker) const
    {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(logCtx);

        auto options = NYT::NApi::TGetNodeOptions();
        options.Attributes = NYT::NYTree::TAttributeFilter({"tablet_state"});
        options.Timeout = RpcTimeout;

        return client->GetNode(path, std::move(options))
            .Apply(BIND([
                =,
                this,
                this_ = NYT::MakeStrong(this)
            ](const NYT::NYson::TYsonString& value) mutable {
                YQL_LOG_CTX_ROOT_SESSION_SCOPE(logCtx);

                auto node = NYT::NYTree::ConvertTo<NYT::NYTree::INodePtr>(value);
                auto tabletState = node->Attributes().Get<
                    NYT::NTabletClient::ETabletState
                >("tablet_state", NYT::NTabletClient::ETabletState::Unmounted);

                if (tabletState == NYT::NTabletClient::ETabletState::Mounted) {
                    YQL_CLOG(INFO, ProviderYtflow) << "Skipped mounting table " << path;

                    return NYT::OKFuture;
                }

                YQL_CLOG(INFO, ProviderYtflow)
                    << NYT::Format(
                        "Mounting table %v with tablet state %v ...",
                        path,
                        tabletState);

                NYT::NApi::TMountTableOptions mountTableOptions;
                mountTableOptions.Timeout = RpcTimeout;

                return client->MountTable(path, std::move(mountTableOptions))
                    .Apply(BIND([path, logCtx] {
                        YQL_LOG_CTX_ROOT_SESSION_SCOPE(logCtx);

                        YQL_CLOG(INFO, ProviderYtflow)
                            << "Mounted table " << path;
                    }).AsyncVia(invoker));
            }).AsyncVia(invoker));
    }

    NYT::TFuture<void> CreateYtNode(
        NYT::NObjectClient::EObjectType type,
        TString path,
        NYT::NYTree::IAttributeDictionaryPtr attributes,
        NYT::NYTree::IAttributeDictionaryPtr extraAttributes,
        TString cluster,
        const TYtflowSettings& config,
        bool force,
        const std::pair<TString, TString>& logCtx,
        NYT::IInvokerPtr invoker)
    {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(logCtx);

        if (auto tabletCellBundle = config.TabletCellBundle.Get()) {
            attributes->Set("tablet_cell_bundle", *tabletCellBundle);
        }

        if (auto account = config.Account.Get()) {
            attributes->Set("account", *account);
        }

        if (auto primaryMedium = config.PrimaryMedium.Get()) {
            attributes->Set("primary_medium", *primaryMedium);
        }

        auto client = GetClient(
            ConfigClusters->GetRealName(cluster),
            ::NYql::NYtflow::NPrivate::GetAuth(cluster, config, *ConfigClusters));

        path = ::NYql::NYtflow::NPrivate::CanonizeYtPath(
            std::move(path), config);

        YQL_CLOG(INFO, ProviderYtflow)
            << "Creating yt node " << path
            << " ...";

        NYT::NApi::TNodeExistsOptions nodeExistsOptions;
        nodeExistsOptions.Timeout = RpcTimeout;

        return client->NodeExists(path, std::move(nodeExistsOptions))
            .Apply(BIND([
                =,
                this,
                this_ = NYT::MakeStrong(this)
            ](bool value) mutable {
                YQL_LOG_CTX_ROOT_SESSION_SCOPE(logCtx);

                bool isDynamic = attributes->Get<bool>("dynamic", false);

                if (value && !force) {
                    return EnsureExpectedYtNode(
                        path,
                        type,
                        extraAttributes,
                        client,
                        logCtx,
                        invoker)
                        .Apply(BIND([
                            =,
                            this,
                            this_ = NYT::MakeStrong(this)
                        ]() mutable {
                            YQL_LOG_CTX_ROOT_SESSION_SCOPE(logCtx);

                            YQL_CLOG(INFO, ProviderYtflow)
                                << "Skipped creation of yt node " << path;

                            if (isDynamic) {
                                return EnsureYtTableMounted(
                                    std::move(path),
                                    std::move(client),
                                    std::move(logCtx),
                                    std::move(invoker));
                            }

                            return NYT::OKFuture;
                        }));
                }

                auto combinedAttributes = NYT::NYTree::CreateEphemeralAttributes();
                combinedAttributes->MergeFrom(*attributes);
                combinedAttributes->MergeFrom(*extraAttributes);

                auto createNodeOptions = NYT::NApi::TCreateNodeOptions();
                createNodeOptions.Attributes = std::move(combinedAttributes);
                createNodeOptions.Force = force;
                createNodeOptions.Recursive = true;
                createNodeOptions.Timeout = RpcTimeout;

                return client->CreateNode(path, type, std::move(createNodeOptions))
                    .Apply(BIND([
                        =,
                        this,
                        this_ = NYT::MakeStrong(this)
                    ](const NYT::NCypressClient::TNodeId& /*value*/) {
                        if (isDynamic) {
                            NYT::NApi::TMountTableOptions mountTableOptions;
                            mountTableOptions.Timeout = RpcTimeout;

                            return client->MountTable(path, std::move(mountTableOptions));
                        }

                        return NYT::OKFuture;
                    }).AsyncVia(invoker))
                    .Apply(BIND([=] {
                        YQL_LOG_CTX_ROOT_SESSION_SCOPE(logCtx);

                        YQL_CLOG(INFO, ProviderYtflow)
                            << "Created yt node " << path;
                    }).AsyncVia(invoker));
            }).AsyncVia(invoker));
    }

    static NYT::NYTree::IAttributeDictionaryPtr BuildTableAttributes(
        TVector<TField> fields,
        NYT::NYTree::INodePtr extraAttributes = {})
    {
        auto schemaBuilder = NYT::NYTree::BuildYsonNodeFluently()
            .BeginList();

        bool isSorted = false;

        for (const auto& field : fields) {
            auto listItem = schemaBuilder.Item();
            auto map = listItem.BeginMap()
                .Item("name").Value(field.Name)
                .Item("type").Value(field.Type)
                .Item("required").Value(field.IsRequired);

            if (field.IsKeyField) {
                map.Item("sort_order").Value("ascending");

                isSorted = true;
            }

            if (field.Expression) {
                map.Item("expression").Value(field.Expression);
            }

            if (field.MaxInlineHunkSize) {
                map.Item("max_inline_hunk_size").Value(*field.MaxInlineHunkSize);
            }

            map.EndMap();
        }

        auto schema = schemaBuilder
            .EndList();

        auto* schemaAttributes = schema->MutableAttributes();
        schemaAttributes->Set("strict", true);
        schemaAttributes->Set("unique_keys", isSorted);

        auto attributes = NYT::NYTree::CreateEphemeralAttributes();
        if (extraAttributes) {
            attributes->MergeFrom(extraAttributes->AsMap());
        }

        attributes->Set("dynamic", true);
        attributes->Set("schema", schema);

        return attributes;
    }

protected:
    NYT::NApi::IClientPtr GetClient(const TString& cluster, const TString& token)
    {
        return ClientsCache->GetClient(cluster, token);
    }

protected:
    TConfigClusters::TPtr ConfigClusters;
    TDuration RpcTimeout;

private:
    IYtClientsCachePtr ClientsCache;
};

class TOutputTablesAction
    : public IAction
    , public TYtMixin
{
public:
    TOutputTablesAction()
    { }

    void Init(TExprNode::TPtr node, TContext& prepareCtx) override
    {
        TYtMixin::Init(prepareCtx);

        VisitPersistentSinkSettings(node, prepareCtx, [this, &prepareCtx](const ::google::protobuf::Any& sinkSettings) {
            if (sinkSettings.Is<NProto::TQYTSinkMessage>()) {
                auto& settings = QYTSinkSettings.emplace_back();
                sinkSettings.UnpackTo(&settings);

                auto* rowType = ::NYql::NCommon::ParseTypeFromYson(
                    TStringBuf(settings.GetRowType()), prepareCtx.ExprContext);

                auto tableSchema = BuildTableSchema(rowType);
                QYTSinkSchemas.push_back(std::move(tableSchema));
            }
        });
    }

    NYT::TFuture<void> Run(NYT::IInvokerPtr invoker) override
    {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(GetSessionId());

        TVector<NYT::TFuture<void>> futures;

        YQL_CLOG(INFO, ProviderYtflow)
            << "Preparing output tables...";

        for (ssize_t index = 0; index < std::ssize(QYTSinkSettings); ++index) {
            auto future = BIND(
                &TOutputTablesAction::PrepareYtOutputTable,
                NYT::MakeStrong(this),
                index,
                invoker
            )
                .AsyncVia(invoker)
                .Run();

            futures.push_back(std::move(future));
        }

        return NYT::AllSucceeded(std::move(futures))
            .Apply(BIND([
                this,
                this_ = NYT::MakeStrong(this)
            ] {
                YQL_LOG_CTX_ROOT_SESSION_SCOPE(GetSessionId());

                YQL_CLOG(INFO, ProviderYtflow)
                    << "Prepared output tables";
            }).AsyncVia(invoker));
    }

private:
    NYT::TFuture<void> PrepareYtOutputTable(ssize_t sinkIndex, NYT::IInvokerPtr invoker)
    {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(GetSessionId());

        const auto& settings = QYTSinkSettings[sinkIndex];

        TVector<TString> keyColumns(
            settings.GetKeyColumns().begin(), settings.GetKeyColumns().end());

        const TStringBuf tableKind = keyColumns ? "sorted" : "ordered";

        YQL_CLOG(INFO, ProviderYtflow)
            << "Preparing output yt " << tableKind << " table " << settings.GetPath()
            << " ...";

        if (settings.GetDoesExist() && !settings.GetTruncate()) {
            YQL_CLOG(INFO, ProviderYtflow)
                << "Skipped prepare of output yt " << tableKind << " table " << settings.GetPath();

            auto client = GetClient(
                ConfigClusters->GetRealName(settings.GetCluster()),
                ::NYql::NYtflow::NPrivate::GetAuth(settings.GetCluster(), *GetConfig(), *ConfigClusters));

            auto path = ::NYql::NYtflow::NPrivate::CanonizeYtPath(
                settings.GetPath(), *GetConfig());

            return EnsureYtTableMounted(
                path,
                std::move(client),
                NYql::NLog::CurrentLogContextPath(),
                invoker);
        }

        auto schema = keyColumns
            ? ConvertToSortedTableCreateSchema(QYTSinkSchemas[sinkIndex], keyColumns)
            : ConvertToQueueCreateSchema(QYTSinkSchemas[sinkIndex]);

        auto attributes = NYT::NYTree::CreateEphemeralAttributes();
        attributes->Set("dynamic", true);
        attributes->Set("schema", schema);
        attributes->Set("tablet_count", GetYtPartitionCount());

        if (auto ttl = GetConfig()->YtTtl.Get()) {
            attributes->Set("max_data_versions", 1);
            attributes->Set("min_data_versions", 0);
            attributes->Set("max_data_ttl", ttl->MilliSeconds());
            attributes->Set("min_data_ttl", 0);
            attributes->Set(
                "auto_compaction_period",
                static_cast<ui64>(ttl->MilliSeconds() / 2));
        }

        return CreateYtNode(
            NYT::NObjectClient::EObjectType::Table,
            settings.GetPath(),
            std::move(attributes),
            NYT::NYTree::CreateEphemeralAttributes(),
            settings.GetCluster(),
            *GetConfig(),
            /*force*/ true,
            NYql::NLog::CurrentLogContextPath(),
            invoker)
            .Apply(BIND([
                this,
                this_ = NYT::MakeStrong(this),
                settings,
                tableKind,
                invoker
            ] {
                YQL_LOG_CTX_ROOT_SESSION_SCOPE(GetSessionId());

                YQL_CLOG(INFO, ProviderYtflow)
                    << "Prepared output yt " << tableKind << " table " << settings.GetPath();
            }).AsyncVia(invoker));
    }

private:
    TVector<NProto::TQYTSinkMessage> QYTSinkSettings;
    TVector<NYT::NTableClient::TTableSchemaPtr> QYTSinkSchemas;
};

class TPipelineNodeAction
    : public IAction
    , public TYtMixin
{
public:
    TPipelineNodeAction()
    { }

    void Init(TExprNode::TPtr /*node*/, TContext& prepareCtx) override
    {
        TYtMixin::Init(prepareCtx);
    }

    NYT::TFuture<void> Run(NYT::IInvokerPtr invoker) override
    {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(GetSessionId());

        YQL_CLOG(INFO, ProviderYtflow)
            << "Preparing pipeline node...";

        auto logCtx = NYql::NLog::CurrentLogContextPath();

        auto future = BIND([
            =,
            this,
            this_ = NYT::MakeStrong(this),
            path = GetPipelinePath(),
            cluster = GetCluster(),
            config = GetConfig()
        ]() mutable {
            auto extraAttributes = NYT::NYTree::CreateEphemeralAttributes();
            extraAttributes->Set("pipeline_format_version", 1);

            if (auto monitoringProject = config->_MonitoringProject.Get()) {
                extraAttributes->Set("monitoring_project", *monitoringProject);
            }

            if (auto monitoringCluster = config->_MonitoringCluster.Get()) {
                extraAttributes->Set("monitoring_cluster", *monitoringCluster);
            }

            return CreateYtNode(
                NYT::NObjectClient::EObjectType::MapNode,
                std::move(path),
                NYT::NYTree::CreateEphemeralAttributes(),
                std::move(extraAttributes),
                std::move(cluster),
                *config,
                /*force*/ false,
                logCtx,
                invoker);
        })
            .AsyncVia(invoker)
            .Run();

        return future
            .Apply(BIND([
                =,
                this,
                this_ = NYT::MakeStrong(this),
                path = GetPipelinePath(),
                cluster = GetCluster(),
                config = GetConfig()
            ] {
                bool createWorkerLogsTable = false;
                if (auto maybeWorkerWriteLogsToYT = config->_WorkerWriteLogsToYT.Get()) {
                    createWorkerLogsTable = *maybeWorkerWriteLogsToYT;
                }

                TVector<NYT::TFuture<void>> futures;

                for (auto& [table, attributes] : GetTableAttributesList(createWorkerLogsTable)) {
                    auto future = BIND([
                        =,
                        this,
                        this_ = NYT::MakeStrong(this),
                        table = std::move(table),
                        attributes = std::move(attributes)
                    ]() mutable {
                        return CreateYtNode(
                            NYT::NObjectClient::EObjectType::Table,
                            Join('/', path, table),
                            std::move(attributes),
                            NYT::NYTree::CreateEphemeralAttributes(),
                            cluster,
                            *config,
                            /*force*/ false,
                            logCtx,
                            invoker);
                    })
                        .AsyncVia(invoker)
                        .Run();

                    futures.push_back(std::move(future));
                }

                return NYT::AllSucceeded(std::move(futures));
            }).AsyncVia(invoker))
            .Apply(BIND([logCtx] {
                YQL_LOG_CTX_ROOT_SESSION_SCOPE(logCtx);

                YQL_CLOG(INFO, ProviderYtflow)
                    << "Prepared pipeline node";
            }).AsyncVia(invoker));
    }

    TVector<std::pair<TString, NYT::NYTree::IAttributeDictionaryPtr>>
    GetTableAttributesList(bool createWorkerLogsTable)
    {
        auto logsTableAttributes = BuildTableAttributes(
            {
                TField{
                    .Name = "host",
                    .Type = "string"
                },
                TField{
                    .Name = "data",
                    .Type = "string"
                },
                TField{
                    .Name = "codec",
                    .Type = "string"
                },
                TField{
                    .Name = "$timestamp",
                    .Type = "uint64"
                },
                TField{
                    .Name = "$cumulative_data_weight",
                    .Type = "int64"
                }
            },
            NYT::NYTree::BuildYsonNodeFluently()
                .BeginMap()
                    .Item("mount_config")
                        .BeginMap()
                            .Item("min_data_versions").Value(0)
                            .Item("min_data_ttl").Value(0)
                            .Item("max_data_ttl").Value(86400000)
                        .EndMap()
                    .Item("tablet_count").Value(1)
                .EndMap()
        );

        TVector<std::pair<TString, NYT::NYTree::IAttributeDictionaryPtr>> tableAttributesList = {
            {"input_messages", BuildTableAttributes(
                {
                    TField{
                        .Name = "computation_id",
                        .Type = "string",
                        .IsKeyField = true
                    },
                    TField{
                        .Name = "key",
                        .Type = "any",
                        .IsKeyField = true
                    },
                    TField{
                        .Name = "message_id",
                        .Type = "string",
                        .IsKeyField = true
                    },
                    TField{
                        .Name = "system_timestamp",
                        .Type = "uint64"
                    }
                },
                NYT::NYTree::BuildYsonNodeFluently()
                    .BeginMap()
                        .Item("mount_config")
                            .BeginMap()
                                .Item("min_data_versions").Value(0)
                                .Item("min_data_ttl").Value(0)
                                .Item("row_merger_type").Value("watermark")
                            .EndMap()
                    .EndMap()
            )},
            {"compact_input_messages", BuildTableAttributes(
                {
                    TField{
                        .Name = "deduplication_message_key",
                        .Type = "string",
                        .IsKeyField = true
                    },
                    TField{
                        .Name = "system_timestamp",
                        .Type = "uint64"
                    }
                },
                NYT::NYTree::BuildYsonNodeFluently()
                    .BeginMap()
                        .Item("mount_config")
                            .BeginMap()
                                .Item("min_data_versions").Value(0)
                                .Item("min_data_ttl").Value(0)
                                .Item("row_merger_type").Value("watermark")
                            .EndMap()
                    .EndMap()
            )},
            {"compact_output_messages", BuildTableAttributes({
                TField{
                    .Name = "computation_id",
                    .Type = "string",
                    .IsKeyField = true
                },
                TField{
                    .Name = "key",
                    .Type = "any",
                    .IsKeyField = true
                },
                TField{
                    .Name = "stream_id",
                    .Type = "string",
                    .IsKeyField = true
                },
                TField{
                    .Name = "chunk_id",
                    .Type = "int64",
                    .IsKeyField = true
                },
                TField{
                    .Name = "data",
                    .Type = "string",
                    .MaxInlineHunkSize = 128
                },
                TField{
                    .Name = "data_codec",
                    .Type = "int64"
                },
                TField{
                    .Name = "processed_mask",
                    .Type = "string"
                }
            })},
            {"compact_partition_output_messages", BuildTableAttributes({
                TField{
                    .Name = "hash",
                    .Type = "uint64",
                    .Expression = "farm_hash(partition_id)",
                    .IsKeyField = true
                },
                TField{
                    .Name = "partition_id",
                    .Type = "string",
                    .IsKeyField = true
                },
                TField{
                    .Name = "stream_id",
                    .Type = "string",
                    .IsKeyField = true
                },
                TField{
                    .Name = "chunk_id",
                    .Type = "int64",
                    .IsKeyField = true
                },
                TField{
                    .Name = "data",
                    .Type = "string",
                    .MaxInlineHunkSize = 128
                },
                TField{
                    .Name = "data_codec",
                    .Type = "int64"
                },
                TField{
                    .Name = "processed_mask",
                    .Type = "string"
                }
            })},
            {"states", BuildTableAttributes({
                TField{
                    .Name = "computation_id",
                    .Type = "string",
                    .IsKeyField = true
                },
                TField{
                    .Name = "key",
                    .Type = "any",
                    .IsKeyField = true
                },
                TField{
                    .Name = "name",
                    .Type = "string",
                    .IsKeyField = true
                },
                TField{
                    .Name = "state",
                    .Type = "any"
                },
                TField{
                    .Name = "compressed",
                    .Type = "string"
                },
                TField{
                    .Name = "compressed_patch",
                    .Type = "string"
                },
                TField{
                    .Name = "format",
                    .Type = "any"
                }
            })},
            {"partition_states", BuildTableAttributes({
                 TField{
                    .Name = "hash",
                    .Type = "uint64",
                    .Expression = "farm_hash(partition_id)",
                    .IsKeyField = true
                 },
                TField{
                    .Name = "partition_id",
                    .Type = "string",
                    .IsKeyField = true
                },
                TField{
                    .Name = "name",
                    .Type = "string",
                    .IsKeyField = true
                },
                TField{
                    .Name = "state",
                    .Type = "any"
                },
                TField{
                    .Name = "compressed",
                    .Type = "string"
                },
                TField{
                    .Name = "compressed_patch",
                    .Type = "string"
                },
                TField{
                    .Name = "format",
                    .Type = "any"
                }
            })},
            {"key_visitor_states", BuildTableAttributes({
                TField{
                    .Name = "computation_id",
                    .Type = "string",
                    .IsKeyField = true
                },
                TField{
                    .Name = "stream_id",
                    .Type = "string",
                    .IsKeyField = true
                },
                TField{
                    .Name = "key",
                    .Type = "any",
                    .IsKeyField = true
                },
                TField{
                    .Name = "is_lower",
                    .Type = "boolean",
                    .IsKeyField = true
                },
                TField{
                    .Name = "state",
                    .Type = "any"
                }
            })},
            {"timers", BuildTableAttributes({
                TField{
                    .Name = "computation_id",
                    .Type = "string",
                    .IsKeyField = true
                },
                TField{
                    .Name = "key",
                    .Type = "any",
                    .IsKeyField = true
                },
                TField{
                    .Name = "message_id",
                    .Type = "string",
                    .IsKeyField = true
                },
                TField{
                    .Name = "stream_id",
                    .Type = "string"
                },
                TField{
                    .Name = "system_timestamp",
                    .Type = "uint64"
                },
                TField{
                    .Name = "event_timestamp",
                    .Type = "uint64"
                },
                TField{
                    .Name = "trigger_timestamp",
                    .Type = "uint64"
                }
            })},
            {TString(CONTROLLER_LOGS_TABLE), logsTableAttributes},
            {"flow_state", BuildTableAttributes({
                TField{
                    .Name = "sequence_id",
                    .Type = "int64",
                    .IsKeyField = true
                },
                TField{
                    .Name = "flags",
                    .Type = "uint64"
                },
                TField{
                    .Name = "state_name",
                    .Type = "string"
                },
                TField{
                    .Name = "key_left",
                    .Type = "string"
                },
                TField{
                    .Name = "key_right",
                    .Type = "string"
                },
                TField{
                    .Name = "value",
                    .Type = "any"
                }
            })},
            {"flow_state_obsolete", BuildTableAttributes({
                TField{
                    .Name = "key",
                    .Type = "string",
                    .IsKeyField = true
                },
                TField{
                    .Name = "value",
                    .Type = "any"
                }
            })},
            {"flow_control", BuildTableAttributes({
                TField{
                    .Name = "key",
                    .Type = "string",
                    .IsKeyField = true
                },
                TField{
                    .Name = "value",
                    .Type = "any"
                }
            })},
            {"partition_transactions", BuildTableAttributes({
                TField{
                    .Name = "hash",
                    .Type = "uint64",
                    .Expression = "farm_hash(partition_id)",
                    .IsKeyField = true
                },
                TField{
                    .Name = "partition_id",
                    .Type = "string",
                    .IsKeyField = true
                },
                TField{
                    .Name = "last_transaction_start_timestamp",
                    .Type = "uint64"
                }
            })},
            {"leases", BuildTableAttributes({
                TField{
                    .Name = "hash",
                    .Type = "uint64",
                    .Expression = "farm_hash(key)",
                    .IsKeyField = true
                },
                TField{
                    .Name = "key",
                    .Type = "string",
                    .IsKeyField = true
                },
                TField{
                    .Name = "subkey",
                    .Type = "string",
                    .IsKeyField = true
                },
                TField{
                    .Name = "value",
                    .Type = "any"
                }
            })}
        };

        if (createWorkerLogsTable) {
            tableAttributesList.push_back({TString(WORKER_LOGS_TABLE), logsTableAttributes});
        }

        return tableAttributesList;
    }
};

class TYtConsumersAction
    : public IAction
    , public TYtMixin
{
public:
    TYtConsumersAction()
    { }

    void Init(TExprNode::TPtr node, TContext& prepareCtx) override
    {
        TYtMixin::Init(prepareCtx);

        VisitPersistentSourceSettings(node, prepareCtx, [this](const ::google::protobuf::Any& sourceSettings) {
            if (sourceSettings.Is<NProto::TQYTSourceMessage>()) {
                auto& settings = QYTSourceSettings.emplace_back();
                sourceSettings.UnpackTo(&settings);
            }
        });
    }

    NYT::TFuture<void> Run(NYT::IInvokerPtr invoker) override
    {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(GetSessionId());

        YQL_CLOG(INFO, ProviderYtflow)
            << "Preparing yt consumers...";

        auto logCtx = NYql::NLog::CurrentLogContextPath();

        auto future = BIND([
            this,
            this_ = NYT::MakeStrong(this),
            path = GetYtConsumerPath(),
            cluster = GetCluster(),
            config = GetConfig(),
            logCtx,
            invoker
        ]() mutable {
            return CreateYtNode(
                NYT::NObjectClient::EObjectType::Table,
                std::move(path),
                BuildTableAttributes(
                    {
                        TField{
                            .Name = "queue_cluster",
                            .Type = "string",
                            .IsKeyField = true,
                            .IsRequired = true
                        },
                        TField{
                            .Name = "queue_path",
                            .Type = "string",
                            .IsKeyField = true,
                            .IsRequired = true
                        },
                        TField{
                            .Name = "partition_index",
                            .Type = "uint64",
                            .IsKeyField = true,
                            .IsRequired = true
                        },
                        TField{
                            .Name = "offset",
                            .Type = "uint64",
                            .IsRequired = true
                        },
                        TField{
                            .Name = "meta",
                            .Type = "any"
                        }
                    },
                    NYT::NYTree::BuildYsonNodeFluently()
                        .BeginMap()
                            .Item("treat_as_queue_consumer").Value(true)
                        .EndMap()
                ),
                NYT::NYTree::CreateEphemeralAttributes(),
                std::move(cluster),
                *config,
                /*force*/ false,
                logCtx,
                invoker);
        })
            .AsyncVia(invoker)
            .Run();

        return future.Apply(BIND([
            =,
            this,
            this_ = NYT::MakeStrong(this)
        ] {
            TVector<NYT::TFuture<void>> futures;

            for (ssize_t index = 0; index < std::ssize(QYTSourceSettings); ++index) {
                auto future = BIND(
                    &TYtConsumersAction::RegisterYtConsumer,
                    NYT::MakeStrong(this),
                    index,
                    invoker
                )
                    .AsyncVia(invoker)
                    .Run();

                futures.push_back(std::move(future));
            }

            return NYT::AllSucceeded(std::move(futures));
        }).AsyncVia(invoker))
        .Apply(BIND([=] {
            YQL_LOG_CTX_ROOT_SESSION_SCOPE(logCtx);

            YQL_CLOG(INFO, ProviderYtflow)
                << "Prepared yt consumers";
        }).AsyncVia(invoker));
    }

private:
    NYT::TFuture<void> RegisterYtConsumer(ssize_t sourceIndex, NYT::IInvokerPtr invoker)
    {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(GetSessionId());

        const auto& settings = QYTSourceSettings[sourceIndex];

        auto cluster = settings.GetCluster();
        auto clusterRealName = ConfigClusters->GetRealName(cluster);

        auto sourcePath = ::NYql::NYtflow::NPrivate::CanonizeYtPath(
            settings.GetPath(), *GetConfig());

        auto richSourcePath = NYT::NYPath::TRichYPath(std::move(sourcePath));
        richSourcePath.SetCluster(clusterRealName);

        auto richConsumerPath = ::NYql::NYtflow::NPrivate::MakeYtConsumerRichPath(
            *GetConfig(), *ConfigClusters);

        YQL_CLOG(INFO, ProviderYtflow)
            << "Registering yt consumer " << richConsumerPath.GetPath()
            << " to queue " << richSourcePath.GetPath()
            << " ...";

        auto client = GetClient(
            clusterRealName,
            ::NYql::NYtflow::NPrivate::GetAuth(cluster, *GetConfig(), *ConfigClusters));

        NYT::NApi::TListQueueConsumerRegistrationsOptions listQueueConsumerRegistrationsOptions;
        listQueueConsumerRegistrationsOptions.Timeout = RpcTimeout;

        return client->ListQueueConsumerRegistrations(
            richSourcePath,
            richConsumerPath,
            std::move(listQueueConsumerRegistrationsOptions))
            .Apply(BIND([
                this,
                this_ = NYT::MakeStrong(this),
                richSourcePath = std::move(richSourcePath),
                richConsumerPath = std::move(richConsumerPath),
                vital = GetYtConsumerVital(),
                client,
                invoker,
                logCtx = NYql::NLog::CurrentLogContextPath()
            ](
                const std::vector<NYT::NApi::TListQueueConsumerRegistrationsResult>& value
            ) {
                YQL_LOG_CTX_ROOT_SESSION_SCOPE(logCtx);

                if (!value.empty()) {
                    YQL_CLOG(INFO, ProviderYtflow)
                        << "Skipped registration of yt consumer " << richConsumerPath.GetPath()
                        << " to queue " << richSourcePath.GetPath();

                    return NYT::OKFuture;
                }

                NYT::NApi::TRegisterQueueConsumerOptions registerQueueConsumerOptions;
                registerQueueConsumerOptions.Timeout = RpcTimeout;

                return client->RegisterQueueConsumer(
                    richSourcePath,
                    richConsumerPath,
                    vital,
                    std::move(registerQueueConsumerOptions))
                    .Apply(BIND([richSourcePath, richConsumerPath, logCtx] {
                        YQL_LOG_CTX_ROOT_SESSION_SCOPE(logCtx);

                        YQL_CLOG(INFO, ProviderYtflow)
                            << "Registered yt consumer " << richConsumerPath.GetPath()
                            << " to queue " << richSourcePath.GetPath();
                    }).AsyncVia(invoker));
            }).AsyncVia(invoker));
    }

private:
    TVector<NProto::TQYTSourceMessage> QYTSourceSettings;
};

class TYtProducersAction
    : public IAction
    , public TYtMixin
{
public:
    TYtProducersAction()
    { }

    void Init(TExprNode::TPtr /*node*/, TContext& prepareCtx) override
    {
        TYtMixin::Init(prepareCtx);
    }

    NYT::TFuture<void> Run(NYT::IInvokerPtr invoker) override
    {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(GetSessionId());

        YQL_CLOG(INFO, ProviderYtflow)
            << "Preparing yt producers...";

        auto logCtx = NYql::NLog::CurrentLogContextPath();

        auto future = BIND([
            =,
            this,
            this_ = NYT::MakeStrong(this),
            path = GetYtProducerPath(),
            cluster = GetCluster(),
            config = GetConfig()
        ]() mutable {
            return CreateYtNode(
                NYT::NObjectClient::EObjectType::Table,
                std::move(path),
                BuildTableAttributes(
                    {
                        TField{
                            .Name = "queue_cluster",
                            .Type = "string",
                            .IsKeyField = true
                        },
                        TField{
                            .Name = "queue_path",
                            .Type = "string",
                            .IsKeyField = true
                        },
                        TField{
                            .Name = "session_id",
                            .Type = "string",
                            .IsKeyField = true
                        },
                        TField{
                            .Name = "sequence_number",
                            .Type = "int64"
                        },
                        TField{
                            .Name = "epoch",
                            .Type = "int64"
                        },
                        TField{
                            .Name = "user_meta",
                            .Type = "any"
                        },
                        TField{
                            .Name = "system_meta",
                            .Type = "any"
                        }
                    },
                    NYT::NYTree::BuildYsonNodeFluently()
                        .BeginMap()
                            .Item("treat_as_queue_producer").Value(true)
                        .EndMap()
                ),
                NYT::NYTree::CreateEphemeralAttributes(),
                std::move(cluster),
                *config,
                /*force*/ false,
                logCtx,
                invoker);
        })
            .AsyncVia(invoker)
            .Run();

        return future
            .Apply(BIND([=] {
                YQL_LOG_CTX_ROOT_SESSION_SCOPE(logCtx);

                YQL_CLOG(INFO, ProviderYtflow)
                    << "Prepared yt producers";
            }).AsyncVia(invoker));
    }

private:
    IYtflowGateway::TRunOptions RunOptions;
};

} // namespace NYql::NYtflow::NPrepare::NPrivate

namespace NYql::NYtflow::NPrepare {

DEFINE_REFCOUNTED_TYPE(IAction);

IActionPtr CreateOutputTablesAction()
{
    return NYT::New<NPrivate::TOutputTablesAction>();
}

IActionPtr CreatePipelineNodeAction()
{
    return NYT::New<NPrivate::TPipelineNodeAction>();
}

IActionPtr CreateYtConsumersAction()
{
    return NYT::New<NPrivate::TYtConsumersAction>();
}

IActionPtr CreateYtProducersAction()
{
    return NYT::New<NPrivate::TYtProducersAction>();
}

} // namespace NYql::NYtflow::NPrepare

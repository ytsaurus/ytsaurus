#pragma once

#include <yql/tools/yqlworker/executor/common/task.h>

#include <yt/yql/providers/yt/provider/yql_yt_gateway.h>
#include <yt/yql/providers/yt/lib/secret_masker/secret_masker.h>

#include <yql/essentials/core/credentials/yql_credentials.h>
#include <yql/essentials/core/facade/yql_facade.h>
#include <yql/essentials/core/yql_data_provider.h>
#include <yql/essentials/core/file_storage/file_storage.h>
#include <yql/essentials/core/layers/remote_layer_provider.h>
#include <yql/essentials/core/qplayer/storage/interface/yql_qstorage.h>
#include <yql/essentials/core/url_lister/interface/url_lister.h>
#include <yql/essentials/core/user_data/yql_user_data.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/providers/common/metrics/metrics_registry.h>
#include <yql/essentials/providers/common/proto/gateways_config.pb.h>
#include <yql/essentials/providers/common/proto/static_gateways_config.pb.h>
#include <yql/essentials/sql/sql.h>

#include <contrib/ydb/library/yql/providers/common/token_accessor/client/factory.h>
#include <contrib/ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>

void ExtDqFactories(
    TVector<NKikimr::NMiniKQL::TComputationNodeFactory>& factories,
    const NYdb::TDriver& driver
);

void ExtUrlListers(
    TVector<NYql::IUrlListerPtr>& urlListers
);

NYql::NLayers::IRemoteLayerProviderPtr ExtCreateRemoteLayerProvider(
    const NYql::TGatewaysConfig& config,
    const NYql::TCredentials::TPtr& credentials
);

void ExtAddRemoteLayersProvider(
    const NYql::TProgramFactoryPtr& programFactory,
    NYql::NLayers::IRemoteLayerProviderPtr base,
    const NYql::TQContext* qContext = nullptr
);

NYql::ISecretMasker::TPtr ExtCreateSecretMasker();

void ExtUserMounts(
    NYql::TUserDataTable& userData
);

void ExtProviderSpecific(
    const NYql::TGatewaysConfig& gatewaysConfig,
    const NYql::TStaticGatewaysConfig& staticGatewaysConfig,
    const NKikimr::NMiniKQL::IFunctionRegistry* funcRegistry,
    TVector<TIntrusivePtr<TThrRefBase>>& gateways,
    TVector<NYql::TDataProviderInitializer>& dataProvidersInit,
    const NYql::IMetricsRegistryPtr& metrics,
    THashMap<TString, TString>& clusters,
    THashMap<TString, TString>& clusterPathPrefixes,
    bool disableAnonymousAccess,
    const NYql::TFileStoragePtr& fileStorage,
    const NYdb::TDriver& ydbDriver,
    NYql::IStructuredTokenCredentialsFactory::TPtr credentialsFactory
);

NYql::IYtGateway::TPtr CreateFmrGatewayFromYtIfNeeded(
    NYql::IYtGateway::TPtr ytGateway,
    const NYql::TGatewaysConfig& gatewaysConfig,
    const NYql::TStaticGatewaysConfig& staticGatewaysConfig,
    const NYql::TTaskDataPtr taskData,
    const NKikimr::NMiniKQL::IFunctionRegistry* funcRegistry,
    const NYql::TFileStoragePtr& fileStorage,
    bool isMixedGateway,
    const TString& fmrJobBinaryPath = {},
    const TString& fmrJobBinaryMd5 = {}
);

void ExtDownloaders(
    TVector<NYql::NFS::IDownloaderPtr>& downloaders,
    const NYql::TFileStorageConfig& config
);

void ExtAddTranslators(
    ui32 sparkParserPort,
    NSQLTranslation::TTranslatorsRegistry& translatorsRegistry
);

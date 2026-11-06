#include <yql/tools/yqlworker/executor/common/tasks/provider_load/interface.h>

#include <yt/yql/providers/yt/lib/secret_masker/dummy/dummy_secret_masker.h>

using namespace NYql;

void ExtProviderSpecific(
    const NYql::TGatewaysConfig& gatewaysConfig,
    const NYql::TStaticGatewaysConfig& staticGatewaysConfig,
    const NKikimr::NMiniKQL::IFunctionRegistry* funcRegistry,
    TVector<TIntrusivePtr<TThrRefBase>>& gateways,
    TVector<NYql::TDataProviderInitializer>& dataProvidersInit,
    const IMetricsRegistryPtr& metrics,
    THashMap<TString, TString>& clusters,
    THashMap<TString, TString>& clusterPathPrefixes,
    bool disableAnonymousAccess,
    const TFileStoragePtr& fileStorage,
    const NYdb::TDriver& ydbDriver,
    NYql::IStructuredTokenCredentialsFactory::TPtr credentialsFactory
) {
    Y_UNUSED(gatewaysConfig);
    Y_UNUSED(staticGatewaysConfig);
    Y_UNUSED(funcRegistry);
    Y_UNUSED(gateways);
    Y_UNUSED(dataProvidersInit);
    Y_UNUSED(metrics);
    Y_UNUSED(clusters);
    Y_UNUSED(clusterPathPrefixes);
    Y_UNUSED(disableAnonymousAccess);
    Y_UNUSED(fileStorage);
    Y_UNUSED(ydbDriver);
    Y_UNUSED(credentialsFactory);
}

IYtGateway::TPtr CreateFmrGatewayFromYtIfNeeded(
    IYtGateway::TPtr ytGateway,
    const TGatewaysConfig& gatewaysConfig,
    const TStaticGatewaysConfig& staticGatewaysConfig,
    const TTaskDataPtr taskData,
    const NKikimr::NMiniKQL::IFunctionRegistry* funcRegistry,
    const NYql::TFileStoragePtr& fileStorage,
    bool isMixedGateway,
    const TString& fmrJobBinaryPath,
    const TString& fmrJobBinaryMd5
) {
    Y_UNUSED(gatewaysConfig);
    Y_UNUSED(staticGatewaysConfig);
    Y_UNUSED(taskData);
    Y_UNUSED(funcRegistry);
    Y_UNUSED(fileStorage);
    Y_UNUSED(isMixedGateway);
    Y_UNUSED(fmrJobBinaryPath);
    Y_UNUSED(fmrJobBinaryMd5);
    return ytGateway;
}

void ExtDownloaders(
    TVector<NYql::NFS::IDownloaderPtr>& downloaders,
    const NYql::TFileStorageConfig& config
) {
    Y_UNUSED(downloaders);
    Y_UNUSED(config);
}

void ExtAddTranslators(
    ui32 sparkParserPort,
    NSQLTranslation::TTranslatorsRegistry& translatorsRegistry
) {
    Y_UNUSED(sparkParserPort);
    Y_UNUSED(translatorsRegistry);
}

void ExtDqFactories(
    TVector<NKikimr::NMiniKQL::TComputationNodeFactory>& factories,
    const NYdb::TDriver& driver
) {
    Y_UNUSED(factories);
    Y_UNUSED(driver);
}

void ExtUrlListers(
    TVector<IUrlListerPtr>& urlListers
) {
    Y_UNUSED(urlListers);
}

NLayers::IRemoteLayerProviderPtr ExtCreateRemoteLayerProvider(
    const TGatewaysConfig& config,
    const TCredentials::TPtr& credentials
) {
    Y_UNUSED(config);
    Y_UNUSED(credentials);
    return {};
}

void ExtAddRemoteLayersProvider(
    const TProgramFactoryPtr& programFactory,
    NLayers::IRemoteLayerProviderPtr base,
    const TQContext* qContext
) {
    Y_UNUSED(programFactory);
    Y_UNUSED(base);
    Y_UNUSED(qContext);
}

ISecretMasker::TPtr ExtCreateSecretMasker() {
    return CreateDummySecretMasker();
}

void ExtUserMounts(
    TUserDataTable& userData
) {
    Y_UNUSED(userData);
}

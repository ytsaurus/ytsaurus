# Configuration reference

This file contains descriptions of all specs and configs used for {{product-name}} Flow configuration. The file generation algorithm recursively searches for all subconfigs. It is not perfect.{% if audience == "internal" %} If something is missing, you can reach out to the [YT Flow Public](https://nda.ya.ru/t/hcJkQdBD7LNa9V) chat, and we will try to add it.{% endif %}

{% include [_](./NYT_NApi_EConnectionType.md) %}

{% include [_](./NYT_NApi_NRpcProxy_EAddressType.md) %}

{% include [_](./NYT_NApi_NRpcProxy_TConnectionConfig.md) %}

{% include [_](./NYT_NApi_TTableMountCacheConfig.md) %}

{% include [_](./NYT_NAuth_TTvmServiceConfig.md) %}

{% include [_](./NYT_NBus_EEncryptionMode.md) %}

{% include [_](./NYT_NBus_EVerificationMode.md) %}

{% include [_](./NYT_NBus_NTcp_TBusConfig.md) %}

{% include [_](./NYT_NBus_NTcp_TBusServerConfig.md) %}

{% include [_](./NYT_NBus_NTcp_TDispatcherConfig.md) %}

{% include [_](./NYT_NBus_NTcp_TDispatcherDynamicConfig.md) %}

{% include [_](./NYT_NBus_NTcp_TMultiplexingBandConfig.md) %}

{% include [_](./NYT_NChaosClient_TReplicationCardCacheConfig.md) %}

{% include [_](./NYT_NClient_NCache_TClientsCacheConfig.md) %}

{% include [_](./NYT_NCodegen_EOptimizationLevel.md) %}

{% include [_](./NYT_NCompression_ECodec.md) %}

{% include [_](./NYT_NConcurrency_EExecutionStackKind.md) %}

{% include [_](./NYT_NConcurrency_TFiberManagerConfig.md) %}

{% include [_](./NYT_NConcurrency_TFiberManagerDynamicConfig.md) %}

{% include [_](./NYT_NCoreDump_TCoreDumperConfig.md) %}

{% include [_](./NYT_NCrypto_TPemBlobConfig.md) %}

{% include [_](./NYT_NCrypto_TSslContextCommand.md) %}

{% include [_](./NYT_NFlow_EBacktraceEnricherLevel.md) %}

{% include [_](./NYT_NFlow_EDistributionOrdering.md) %}

{% include [_](./NYT_NFlow_EFetchType.md) %}

{% include [_](./NYT_NFlow_EFlowStateTarget.md) %}

{% include [_](./NYT_NFlow_EFramingFormat.md) %}

{% include [_](./NYT_NFlow_EJobBalancerType.md) %}

{% include [_](./NYT_NFlow_EMalformedBigRTQueueMessagePolicy.md) %}

{% include [_](./NYT_NFlow_EMalformedLogbrokerMessagePolicy.md) %}

{% include [_](./NYT_NFlow_EMoniumAuthMode.md) %}

{% include [_](./NYT_NFlow_EMoniumMismatchBehavior.md) %}

{% include [_](./NYT_NFlow_EPipelineState.md) %}

{% include [_](./NYT_NFlow_EProcessingMode.md) %}

{% include [_](./NYT_NFlow_ETimeType.md) %}

{% include [_](./NYT_NFlow_ETimestampFormat.md) %}

{% include [_](./NYT_NFlow_EUnavailableSourcePolicy.md) %}

{% include [_](./NYT_NFlow_NBigRTExtensions_TDynamicProfileJoinerSpec.md) %}

{% include [_](./NYT_NFlow_NBigRTExtensions_TDynamicProfileManagerSpec.md) %}

{% include [_](./NYT_NFlow_NBigRTExtensions_TProfileJoinerSpec.md) %}

{% include [_](./NYT_NFlow_NBigRTExtensions_TProfileManagerSpec.md) %}

{% include [_](./NYT_NFlow_NCompanion_TCompanionConfig.md) %}

{% include [_](./NYT_NFlow_NController_TControllerConfig.md) %}

{% include [_](./NYT_NFlow_NController_TControllerServiceConfig.md) %}

{% include [_](./NYT_NFlow_NController_TElectionManagerConfig.md) %}

{% include [_](./NYT_NFlow_NController_TLeaseManagerConfig.md) %}

{% include [_](./NYT_NFlow_NController_TPersistedStateManagerConfig.md) %}

{% include [_](./NYT_NFlow_NDeltaCodecs_ECodec.md) %}

{% include [_](./NYT_NFlow_NStaticTableConnector_TTableTimestampLocatorSpec.md) %}

{% include [_](./NYT_NFlow_NWorker_TWorkerConfig.md) %}

{% include [_](./NYT_NFlow_TAtMostOnceStrategyDynamicParameters.md) %}

{% include [_](./NYT_NFlow_TAtMostOnceStrategyParameters.md) %}

{% include [_](./NYT_NFlow_TAuthenticatorConfig.md) %}

{% include [_](./NYT_NFlow_TBacktraceEnricherDynamicSpec.md) %}

{% include [_](./NYT_NFlow_TBacktraceEnricherSpec.md) %}

{% include [_](./NYT_NFlow_TComputationSpec.md) %}

{% include [_](./NYT_NFlow_TDeleteStatesArg.md) %}

{% include [_](./NYT_NFlow_TDeleteStatesResponse.md) %}

{% include [_](./NYT_NFlow_TDynamicBufferStateManagerSpec.md) %}

{% include [_](./NYT_NFlow_TDynamicBufferStateManagerSpec_TOneSideBufferSpec.md) %}

{% include [_](./NYT_NFlow_TDynamicComputationSpec.md) %}

{% include [_](./NYT_NFlow_TDynamicControllerConnectorSpec.md) %}

{% include [_](./NYT_NFlow_TDynamicExpiringJobNamedStateCacheSpec.md) %}

{% include [_](./NYT_NFlow_TDynamicExternalStateJoinerSpec.md) %}

{% include [_](./NYT_NFlow_TDynamicExternalStateManagerSpec.md) %}

{% include [_](./NYT_NFlow_TDynamicInputStoreSpec.md) %}

{% include [_](./NYT_NFlow_TDynamicJobManagerGroupSpec.md) %}

{% include [_](./NYT_NFlow_TDynamicJobManagerSpec.md) %}

{% include [_](./NYT_NFlow_TDynamicJobTrackerSpec.md) %}

{% include [_](./NYT_NFlow_TDynamicKeyVisitorStreamSpec.md) %}

{% include [_](./NYT_NFlow_TDynamicMessageDistributorSpec.md) %}

{% include [_](./NYT_NFlow_TDynamicOutputStoreSpec.md) %}

{% include [_](./NYT_NFlow_TDynamicPartitionTracerSpec.md) %}

{% include [_](./NYT_NFlow_TDynamicPipelineSpec.md) %}

{% include [_](./NYT_NFlow_TDynamicResourceSpec.md) %}

{% include [_](./NYT_NFlow_TDynamicRetryableRequestSpec.md) %}

{% include [_](./NYT_NFlow_TDynamicSimpleExternalStateJoinerSpec.md) %}

{% include [_](./NYT_NFlow_TDynamicSimpleExternalStateManagerSpec.md) %}

{% include [_](./NYT_NFlow_TDynamicSinkSpec.md) %}

{% include [_](./NYT_NFlow_TDynamicSourceSpec.md) %}

{% include [_](./NYT_NFlow_TDynamicStateCacheSpec.md) %}

{% include [_](./NYT_NFlow_TDynamicStateFormatSpec.md) %}

{% include [_](./NYT_NFlow_TDynamicStateJoinerSpec.md) %}

{% include [_](./NYT_NFlow_TDynamicStateManagerSpec.md) %}

{% include [_](./NYT_NFlow_TDynamicStaticTableKeyVisitorJoinerSpec.md) %}

{% include [_](./NYT_NFlow_TDynamicTableRequestSpec.md) %}

{% include [_](./NYT_NFlow_TDynamicThrottlerSpec.md) %}

{% include [_](./NYT_NFlow_TDynamicTimerStoreSpec.md) %}

{% include [_](./NYT_NFlow_TDynamicUnitedParameters_NYT_NFlow_NCompanion_TSwiftMapCompanionComputation.md) %}

{% include [_](./NYT_NFlow_TDynamicUnitedParameters_NYT_NFlow_NCompanion_TSwiftOrderedSourceCompanionComputation.md) %}

{% include [_](./NYT_NFlow_TDynamicUnitedParameters_NYT_NFlow_NCompanion_TTransformCompanionComputation.md) %}

{% include [_](./NYT_NFlow_TDynamicUnitedParameters_NYT_NFlow_NSortedDynamicTable_TSyncSink.md) %}

{% include [_](./NYT_NFlow_TDynamicUnitedParameters_NYT_NFlow_NStaticTableConnector_TSource.md) %}

{% include [_](./NYT_NFlow_TDynamicUnitedParameters_NYT_NFlow_TAsyncBigRTQueueSink.md) %}

{% include [_](./NYT_NFlow_TDynamicUnitedParameters_NYT_NFlow_TAsyncMultiClusterQueueSink.md) %}

{% include [_](./NYT_NFlow_TDynamicUnitedParameters_NYT_NFlow_TAsyncQueueSink.md) %}

{% include [_](./NYT_NFlow_TDynamicUnitedParameters_NYT_NFlow_TAtLeastOnceLogbrokerFramingSink.md) %}

{% include [_](./NYT_NFlow_TDynamicUnitedParameters_NYT_NFlow_TAtLeastOnceLogbrokerSink.md) %}

{% include [_](./NYT_NFlow_TDynamicUnitedParameters_NYT_NFlow_TBigRTQueueSource.md) %}

{% include [_](./NYT_NFlow_TDynamicUnitedParameters_NYT_NFlow_TLogbrokerFramingSink.md) %}

{% include [_](./NYT_NFlow_TDynamicUnitedParameters_NYT_NFlow_TLogbrokerFramingSource.md) %}

{% include [_](./NYT_NFlow_TDynamicUnitedParameters_NYT_NFlow_TLogbrokerMultiLineSource.md) %}

{% include [_](./NYT_NFlow_TDynamicUnitedParameters_NYT_NFlow_TLogbrokerSink.md) %}

{% include [_](./NYT_NFlow_TDynamicUnitedParameters_NYT_NFlow_TLogbrokerSource.md) %}

{% include [_](./NYT_NFlow_TDynamicUnitedParameters_NYT_NFlow_TMoniumSink.md) %}

{% include [_](./NYT_NFlow_TDynamicUnitedParameters_NYT_NFlow_TMoniumSource.md) %}

{% include [_](./NYT_NFlow_TDynamicUnitedParameters_NYT_NFlow_TPassthroughComputation.md) %}

{% include [_](./NYT_NFlow_TDynamicUnitedParameters_NYT_NFlow_TQueueSource.md) %}

{% include [_](./NYT_NFlow_TDynamicUnitedParameters_NYT_NFlow_TRandomSource.md) %}

{% include [_](./NYT_NFlow_TDynamicUnitedParameters_NYT_NFlow_TServiceLogSource.md) %}

{% include [_](./NYT_NFlow_TDynamicUnitedParameters_NYT_NFlow_TSwiftPassthroughComputation.md) %}

{% include [_](./NYT_NFlow_TDynamicUnitedParameters_NYT_NFlow_TSwiftPassthroughOrderedSourceComputation.md) %}

{% include [_](./NYT_NFlow_TDynamicUnitedParameters_NYT_NFlow_TSyncBigRTQueueSink.md) %}

{% include [_](./NYT_NFlow_TDynamicUnitedParameters_NYT_NFlow_TSyncQueueSink.md) %}

{% include [_](./NYT_NFlow_TEventTimestampAssignerSpec.md) %}

{% include [_](./NYT_NFlow_TExternalStateJoinerSpec.md) %}

{% include [_](./NYT_NFlow_TExternalStateManagerSpec.md) %}

{% include [_](./NYT_NFlow_TFetcherInJoinerSpec.md) %}

{% include [_](./NYT_NFlow_TFlowNodeConfig.md) %}

{% include [_](./NYT_NFlow_THeavyHittersSpec.md) %}

{% include [_](./NYT_NFlow_TIdlePartitionsSpec.md) %}

{% include [_](./NYT_NFlow_TInputOrderingSpec.md) %}

{% include [_](./NYT_NFlow_TKeyStateRow.md) %}

{% include [_](./NYT_NFlow_TKeyVisitorStreamSpec.md) %}

{% include [_](./NYT_NFlow_TLateDataPartitionsSpec.md) %}

{% include [_](./NYT_NFlow_TLoadThroughputThrottlerSpec.md) %}

{% include [_](./NYT_NFlow_TMatchedStates.md) %}

{% include [_](./NYT_NFlow_TMatchedStatesBucket.md) %}

{% include [_](./NYT_NFlow_TMessageSerializer.md) %}

{% include [_](./NYT_NFlow_TMoniumDriverConfig.md) %}

{% include [_](./NYT_NFlow_TMoniumMetricParameters.md) %}

{% include [_](./NYT_NFlow_TMoniumQuerySpec.md) %}

{% include [_](./NYT_NFlow_TMoniumSinkControllerParameters.md) %}

{% include [_](./NYT_NFlow_TPartitionStateRow.md) %}

{% include [_](./NYT_NFlow_TPipelineSpec.md) %}

{% include [_](./NYT_NFlow_TPivotFinderSpec.md) %}

{% include [_](./NYT_NFlow_TReadStatesArg.md) %}

{% include [_](./NYT_NFlow_TReadStatesResponse.md) %}

{% include [_](./NYT_NFlow_TResourceDescription.md) %}

{% include [_](./NYT_NFlow_TResourceSpec.md) %}

{% include [_](./NYT_NFlow_TSimpleExternalStateJoinerSpec.md) %}

{% include [_](./NYT_NFlow_TSimpleExternalStateManagerSpec.md) %}

{% include [_](./NYT_NFlow_TSimpleRunnerConfig.md) %}

{% include [_](./NYT_NFlow_TSinkSpec.md) %}

{% include [_](./NYT_NFlow_TSourceSpec.md) %}

{% include [_](./NYT_NFlow_TStateJoinSpec.md) %}

{% include [_](./NYT_NFlow_TStateJoinerSpec.md) %}

{% include [_](./NYT_NFlow_TStaticTableKeyVisitorJoinerSpec.md) %}

{% include [_](./NYT_NFlow_TStreamSpec.md) %}

{% include [_](./NYT_NFlow_TTableFetcherSpec.md) %}

{% include [_](./NYT_NFlow_TTableJoinerSpec.md) %}

{% include [_](./NYT_NFlow_TTimerSerializer.md) %}

{% include [_](./NYT_NFlow_TTimerSpec.md) %}

{% include [_](./NYT_NFlow_TUnavailablePartitionGroupsSpec.md) %}

{% include [_](./NYT_NFlow_TUnitedParameters_NYT_NFlow_NCompanion_TSwiftMapCompanionComputation.md) %}

{% include [_](./NYT_NFlow_TUnitedParameters_NYT_NFlow_NCompanion_TSwiftOrderedSourceCompanionComputation.md) %}

{% include [_](./NYT_NFlow_TUnitedParameters_NYT_NFlow_NCompanion_TTransformCompanionComputation.md) %}

{% include [_](./NYT_NFlow_TUnitedParameters_NYT_NFlow_NSortedDynamicTable_TSyncSink.md) %}

{% include [_](./NYT_NFlow_TUnitedParameters_NYT_NFlow_NStaticTableConnector_TSource.md) %}

{% include [_](./NYT_NFlow_TUnitedParameters_NYT_NFlow_TAsyncBigRTQueueSink.md) %}

{% include [_](./NYT_NFlow_TUnitedParameters_NYT_NFlow_TAsyncMultiClusterQueueSink.md) %}

{% include [_](./NYT_NFlow_TUnitedParameters_NYT_NFlow_TAsyncQueueSink.md) %}

{% include [_](./NYT_NFlow_TUnitedParameters_NYT_NFlow_TAtLeastOnceLogbrokerFramingSink.md) %}

{% include [_](./NYT_NFlow_TUnitedParameters_NYT_NFlow_TAtLeastOnceLogbrokerSink.md) %}

{% include [_](./NYT_NFlow_TUnitedParameters_NYT_NFlow_TBigRTQueueSource.md) %}

{% include [_](./NYT_NFlow_TUnitedParameters_NYT_NFlow_TLogbrokerFramingSink.md) %}

{% include [_](./NYT_NFlow_TUnitedParameters_NYT_NFlow_TLogbrokerFramingSource.md) %}

{% include [_](./NYT_NFlow_TUnitedParameters_NYT_NFlow_TLogbrokerMultiLineSource.md) %}

{% include [_](./NYT_NFlow_TUnitedParameters_NYT_NFlow_TLogbrokerSink.md) %}

{% include [_](./NYT_NFlow_TUnitedParameters_NYT_NFlow_TLogbrokerSource.md) %}

{% include [_](./NYT_NFlow_TUnitedParameters_NYT_NFlow_TMoniumSink.md) %}

{% include [_](./NYT_NFlow_TUnitedParameters_NYT_NFlow_TMoniumSource.md) %}

{% include [_](./NYT_NFlow_TUnitedParameters_NYT_NFlow_TPassthroughComputation.md) %}

{% include [_](./NYT_NFlow_TUnitedParameters_NYT_NFlow_TQueueSource.md) %}

{% include [_](./NYT_NFlow_TUnitedParameters_NYT_NFlow_TRandomSource.md) %}

{% include [_](./NYT_NFlow_TUnitedParameters_NYT_NFlow_TServiceLogSource.md) %}

{% include [_](./NYT_NFlow_TUnitedParameters_NYT_NFlow_TSwiftPassthroughComputation.md) %}

{% include [_](./NYT_NFlow_TUnitedParameters_NYT_NFlow_TSwiftPassthroughOrderedSourceComputation.md) %}

{% include [_](./NYT_NFlow_TUnitedParameters_NYT_NFlow_TSyncBigRTQueueSink.md) %}

{% include [_](./NYT_NFlow_TUnitedParameters_NYT_NFlow_TSyncQueueSink.md) %}

{% include [_](./NYT_NFlow_TVanillaConfig.md) %}

{% include [_](./NYT_NFlow_TVanillaTaskConfig.md) %}

{% include [_](./NYT_NFlow_TWatermarkAlignmentSpec.md) %}

{% include [_](./NYT_NFlow_TWatermarkGeneratorSpec.md) %}

{% include [_](./NYT_NFlow_TWatermarkPercentileSpec.md) %}

{% include [_](./NYT_NFlow_TWatermarkStrategySpec.md) %}

{% include [_](./NYT_NHttp_TClientConfig.md) %}

{% include [_](./NYT_NHttps_TClientConfig.md) %}

{% include [_](./NYT_NHttps_TClientCredentialsConfig.md) %}

{% include [_](./NYT_NLogging_ELogFamily.md) %}

{% include [_](./NYT_NLogging_ELogLevel.md) %}

{% include [_](./NYT_NLogging_TLogManagerConfig.md) %}

{% include [_](./NYT_NLogging_TLogManagerDynamicConfig.md) %}

{% include [_](./NYT_NLogging_TRuleConfig.md) %}

{% include [_](./NYT_NNet_TAddressResolverConfig.md) %}

{% include [_](./NYT_NNet_TDialerConfig.md) %}

{% include [_](./NYT_NPipeIO_TPipeIODispatcherConfig.md) %}

{% include [_](./NYT_NPipeIO_TPipeIODispatcherDynamicConfig.md) %}

{% include [_](./NYT_NProfiling_ELabelSanitizationPolicy.md) %}

{% include [_](./NYT_NProfiling_TResourceTrackerConfig.md) %}

{% include [_](./NYT_NProfiling_TShardConfig.md) %}

{% include [_](./NYT_NProfiling_TSolomonExporterConfig.md) %}

{% include [_](./NYT_NProfiling_TSolomonProxyConfig.md) %}

{% include [_](./NYT_NProfiling_TSolomonRegistryConfig.md) %}

{% include [_](./NYT_NProfiling_TSolomonRegistryDynamicConfig.md) %}

{% include [_](./NYT_NQueryClient_EStatisticsAggregation.md) %}

{% include [_](./NYT_NQueryClient_TCodegenCacheConfig.md) %}

{% include [_](./NYT_NQueryClient_TCodegenCacheDynamicConfig.md) %}

{% include [_](./NYT_NQueryClient_TQueryEngineConfig.md) %}

{% include [_](./NYT_NQueryClient_TQueryEngineDynamicConfig.md) %}

{% include [_](./NYT_NRpc_EPeerPriorityStrategy.md) %}

{% include [_](./NYT_NRpc_ERequestTracingMode.md) %}

{% include [_](./NYT_NRpc_NGrpc_TChannelConfig.md) %}

{% include [_](./NYT_NRpc_NGrpc_TChannelCredentialsConfig.md) %}

{% include [_](./NYT_NRpc_NGrpc_TDispatcherConfig.md) %}

{% include [_](./NYT_NRpc_NGrpc_TDispatcherDynamicConfig.md) %}

{% include [_](./NYT_NRpc_NGrpc_TSslPemKeyCertPairConfig.md) %}

{% include [_](./NYT_NRpc_TDispatcherConfig.md) %}

{% include [_](./NYT_NRpc_TDispatcherDynamicConfig.md) %}

{% include [_](./NYT_NRpc_TDynamicChannelPoolConfig.md) %}

{% include [_](./NYT_NRpc_THistogramExponentialBounds.md) %}

{% include [_](./NYT_NRpc_TRetryingChannelConfig.md) %}

{% include [_](./NYT_NRpc_TServerConfig.md) %}

{% include [_](./NYT_NRpc_TServiceDiscoveryEndpointsConfig.md) %}

{% include [_](./NYT_NRpc_TTimeHistogramConfig.md) %}

{% include [_](./NYT_NTCMalloc_TDynamicHeapSizeLimitConfig.md) %}

{% include [_](./NYT_NTCMalloc_TDynamicTCMallocConfig.md) %}

{% include [_](./NYT_NTCMalloc_THeapSizeLimitConfig.md) %}

{% include [_](./NYT_NTCMalloc_TTCMallocConfig.md) %}

{% include [_](./NYT_NTracing_TJaegerTracerConfig.md) %}

{% include [_](./NYT_NTracing_TJaegerTracerDynamicConfig.md) %}

{% include [_](./NYT_NYTree_TSize.md) %}

{% include [_](./NYT_NYson_EEnumYsonStorageType.md) %}

{% include [_](./NYT_NYson_EUtf8Check.md) %}

{% include [_](./NYT_NYson_TProtobufInteropConfig.md) %}

{% include [_](./NYT_NYson_TProtobufInteropDynamicConfig.md) %}

{% include [_](./NYT_TSingletonsDynamicConfig.md) %}

{% include [_](./TDuration.md) %}

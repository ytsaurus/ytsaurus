#include <yt/yt/flow/library/cpp/common/computation.h>
#include <yt/yt/flow/library/cpp/common/computation_controller.h>
#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/resource.h>
#include <yt/yt/flow/library/cpp/common/sink.h>
#include <yt/yt/flow/library/cpp/common/sink_controller.h>
#include <yt/yt/flow/library/cpp/common/source.h>
#include <yt/yt/flow/library/cpp/common/source_controller.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/traverse.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/collection_helpers.h>

#include <yt/yt/client/table_client/unversioned_row.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

struct TNullComputationController;

struct TNullComputation
    : public IComputation
{
    using TComputationController = TNullComputationController;

    TNullComputation(
        TComputationContextPtr /*context*/,
        TDynamicComputationContextPtr /*dynamicContext*/)
    { }

    void Run(const IComputationRunContextPtr& /*context*/) override
    { }

    void Reconfigure(TDynamicComputationContextPtr /*dynamicContext*/) override
    { }

    void SubscribeOnReconfigure(
        NYT::TCallback<void()> /*callback*/,
        EWatchReconfigure /*watchReconfigureMask*/) override
    { }

    void UpdateWatermarkState(TWatermarkStatePtr /*watermarkState*/) override
    { }

    void SetInputTraverse(THashMap<TStreamId, TStreamTraverseDataPtr> /*inputStreams*/) override
    { }

    TComputationOrchidStatePtr GetOrchidState() override
    {
        return New<TComputationOrchidState>();
    }

    TComputationStatusPtr GetStatus() override
    {
        auto status = New<TComputationStatus>();
        status->PartitionStatus = GetEphemeralNodeFactory()->CreateMap();
        return status;
    }

    TParametersPtr GetParametersBase() const override
    {
        return nullptr;
    }

    TDynamicParametersPtr GetDynamicParametersBase() const override
    {
        return nullptr;
    }

    TDynamicPartitionSpecPtr GetDynamicPartitionSpecBase() const override
    {
        return nullptr;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TNullComputationController
    : public IComputationController
{
    TNullComputationController(
        TComputationControllerContextPtr /*context*/,
        TDynamicComputationControllerContextPtr /*dynamicContext*/)
    { }

    bool IsFullCoverage(
        const std::vector<TPartitionId>& /*computationPartitions*/,
        const TFlowViewPtr& /*flowView*/) override
    {
        return {};
    }

    void DoPartitioning(
        const std::vector<TPartitionId>& /*computationPartitions*/,
        const TFlowViewPtr& /*flowView*/) override
    { }

    double ComputePartitionWeight(const TPartitionId& /*partitionId*/, const TFlowViewPtr& /*flowView*/) override
    {
        return 1.0;
    }

    TProcessPartitionTraverseDataResultPtr ProcessPartitionTraverseData(
        const THashMap<TPartitionId, TNodeTraverseDataPtr>& /*traverseData*/,
        const TFlowViewPtr& /*flowView*/) override
    {
        return New<TProcessPartitionTraverseDataResult>();
    }

    void Init(IInitContextPtr /*initContext*/) override
    { }

    void Sync() override
    { }

    void Commit() override
    { }

    void UpdateWatermarkState(TWatermarkStatePtr /*watermarkState*/) override
    { }

    TParametersPtr GetParametersBase() const override
    {
        return nullptr;
    }

    TDynamicParametersPtr GetDynamicParametersBase() const override
    {
        return nullptr;
    }
};

YT_FLOW_DEFINE_COMPUTATION(TNullComputation);

////////////////////////////////////////////////////////////////////////////////

struct TNullSourceController
    : public ISourceController
{
    TNullSourceController(TSourceControllerContextPtr /*context*/, TDynamicSourceControllerContextPtr /*dynamicContext*/)
    { }

    std::optional<THashMap<TKey, NYTree::IMapNodePtr>> ListKeys() override
    {
        return {};
    }

    std::string GetGroup(const TKey& /*key*/) override
    {
        return "default";
    }

    void Init(IInitContextPtr /*initContext*/) override
    { }

    void Sync() override
    { }

    void Commit() override
    { }

    void ProcessPartitionStatuses(const THashMap<TKey, TExtendedSourcePartitionStatusPtr>& /*statuses*/) override
    { }

    std::optional<TStreamTraverseDataPtr> GetFutureKeysStreamTraverseData() override
    {
        return {};
    }

    TParametersPtr GetParametersBase() const override
    {
        return nullptr;
    }

    TDynamicParametersPtr GetDynamicParametersBase() const override
    {
        return nullptr;
    }
};

struct TNullSource
    : public ISource
{
    using TSourceController = TNullSourceController;

    TNullSource(TSourceContextPtr /*context*/, TDynamicSourceContextPtr /*dynamicContext*/)
    { }

    void Init(IInitContextPtr /*initContext*/) override
    { }

    void Terminate() override
    { }

    void Sync() override
    { }

    void Commit() override
    { }

    TFuture<std::vector<TMessageBatch>> GetNextBatch(const TMessageBatcherSettingsPtr& /*batcherSettings*/) override
    {
        return {};
    }

    void MarkPublished(const TSourceMessageBatchCookie& /*cookie*/) override
    { }

    void MarkPersisted(const TSourceMessageBatchCookie& /*cookie*/) override
    { }

    TInflightStreamTraverseDataPtr BuildInflight() override
    {
        return New<TInflightStreamTraverseData>();
    }

    std::optional<TSystemTimestamp> GetPersistedEventWatermark() override
    {
        return {};
    }

    std::optional<TSystemTimestamp> GetReadEventWatermark() override
    {
        return {};
    }

    NYTree::IMapNodePtr GetPartitionStatus() override
    {
        return GetEphemeralNodeFactory()->CreateMap();
    }

    TParametersPtr GetParametersBase() const override
    {
        return nullptr;
    }

    TDynamicParametersPtr GetDynamicParametersBase() const override
    {
        return nullptr;
    }

    TDynamicPartitionSpecPtr GetDynamicPartitionSpecBase() const override
    {
        return nullptr;
    }
};

YT_FLOW_DEFINE_SOURCE(TNullSource);

////////////////////////////////////////////////////////////////////////////////

struct TNullSinkController
    : public ISinkController
{
    TNullSinkController(TSinkControllerContextPtr /*context*/, TDynamicSinkControllerContextPtr /*dynamicContext*/)
    { }

    void Init(IInitContextPtr /*initContext*/) override
    { }

    void Sync() override
    { }

    void Commit() override
    { }

    void UpdateWatermarkState(TWatermarkStatePtr /*watermarkState*/) override
    { }

    std::optional<i64> GetReceiverChannelCount() override
    {
        return {};
    }

    TParametersPtr GetParametersBase() const override
    {
        return nullptr;
    }

    TDynamicParametersPtr GetDynamicParametersBase() const override
    {
        return nullptr;
    }
};

struct TNullSink
    : public ISink
{
    using TSinkController = TNullSinkController;

    TNullSink(TSinkContextPtr /*context*/, TDynamicSinkContextPtr /*dynamicContext*/)
    { }

    void Init(IInitContextPtr /*initContext*/) override
    { }

    void Sync(NApi::IDynamicTableTransactionPtr /*transaction*/) override
    { }

    void Commit() override
    { }

    void Distribute(const TOutputMessageConstPtr& message, TOnDistributedCallback onDistributed) override
    {
        Y_UNUSED(message);
        onDistributed();
    }

    TParametersPtr GetParametersBase() const override
    {
        return nullptr;
    }

    TDynamicParametersPtr GetDynamicParametersBase() const override
    {
        return nullptr;
    }
};

YT_FLOW_DEFINE_SINK(TNullSink);

struct TNullResource
    : public IResource
{
    TNullResource(TResourceContextPtr /*context*/, TDynamicResourceContextPtr /*dynamicContext*/)
    { }

    TFuture<void> Load(const THashMap<TResourceId, IResourcePtr>& /*dependencies*/) override
    {
        return OKFuture;
    }

    void Reconfigure(const TDynamicResourceContextPtr& /*dynamicContext*/) override
    { }

    size_t GetDependenciesCount() const
    {
        return 0;
    }

    TYsonStructPtr GetParametersBase() const final
    {
        return nullptr;
    }

    TYsonStructPtr GetDynamicParametersBase() const final
    {
        return nullptr;
    }
};

YT_FLOW_DEFINE_RESOURCE(TNullResource);

////////////////////////////////////////////////////////////////////////////////
// Fixtures for YT-path ownership validation.

struct TWritingManagerParameters
    : public IExternalStateManager::TParameters
{
    NYPath::TRichYPath Path;

    REGISTER_YSON_STRUCT(TWritingManagerParameters);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("path", &TThis::Path)
            .AddOption(EYTPathOwnership::ExclusiveWrite);
    }
};

struct TSharedWriterManagerParameters
    : public IExternalStateManager::TParameters
{
    NYPath::TRichYPath Path;

    REGISTER_YSON_STRUCT(TSharedWriterManagerParameters);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("path", &TThis::Path)
            .AddOption(EYTPathOwnership::SharedWrite);
    }
};

struct TOpaqueManagerParameters
    : public IExternalStateManager::TParameters
{
    NYPath::TRichYPath Table;

    REGISTER_YSON_STRUCT(TOpaqueManagerParameters);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("table", &TThis::Table);
    }
};

struct TNestedWriterSpec
    : public NYTree::TYsonStruct
{
    NYPath::TRichYPath Path;

    REGISTER_YSON_STRUCT(TNestedWriterSpec);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("path", &TThis::Path)
            .AddOption(EYTPathOwnership::ExclusiveWrite)
            .Default();
    }
};

DEFINE_REFCOUNTED_TYPE(TNestedWriterSpec)

struct TEmbeddedWriterManagerParameters
    : public IExternalStateManager::TParameters
{
    TIntrusivePtr<TNestedWriterSpec> State;

    REGISTER_YSON_STRUCT(TEmbeddedWriterManagerParameters);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("state", &TThis::State)
            .DefaultNew();
    }
};

struct TEmptyManagerDynamicParameters
    : public IExternalStateManager::TDynamicParameters
{
    REGISTER_YSON_STRUCT(TEmptyManagerDynamicParameters);

    static void Register(TRegistrar /*registrar*/)
    { }
};

template <class TStaticParameters>
struct TFixtureExternalStateManager
    : public IExternalStateManager
{
    YT_FLOW_EXTEND_PARAMETERS(TStaticParameters);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TEmptyManagerDynamicParameters);

    TFixtureExternalStateManager(
        TExternalStateManagerContextPtr /*context*/,
        TDynamicExternalStateManagerContextPtr /*dynamicContext*/)
    { }

    IStateHolderPtr GetState(const TKey& /*key*/) override
    {
        return nullptr;
    }

    TFuture<void> PreloadKeyStates(const THashSet<TKey>& /*keys*/) override
    {
        return OKFuture;
    }

    NTableClient::TTableSchemaPtr GetKeySchema() const override
    {
        return nullptr;
    }

    void Sync(IRetryableTransactionPtr /*transaction*/) override
    { }

    void ValidateStateClass(const std::type_info& /*expectedStateType*/) const override
    { }

    IExternalStateManager::TParametersPtr GetParametersBase() const override
    {
        return nullptr;
    }

    IExternalStateManager::TDynamicParametersPtr GetDynamicParametersBase() const override
    {
        return nullptr;
    }
};

struct TWritingExternalStateManager
    : public TFixtureExternalStateManager<TWritingManagerParameters>
{
    using TFixtureExternalStateManager::TFixtureExternalStateManager;
};

struct TSharedWriterExternalStateManager
    : public TFixtureExternalStateManager<TSharedWriterManagerParameters>
{
    using TFixtureExternalStateManager::TFixtureExternalStateManager;
};

struct TOpaqueExternalStateManager
    : public TFixtureExternalStateManager<TOpaqueManagerParameters>
{
    using TFixtureExternalStateManager::TFixtureExternalStateManager;
};

struct TEmbeddedWriterExternalStateManager
    : public TFixtureExternalStateManager<TEmbeddedWriterManagerParameters>
{
    using TFixtureExternalStateManager::TFixtureExternalStateManager;
};

YT_FLOW_DEFINE_EXTERNAL_STATE_MANAGER(TWritingExternalStateManager);
YT_FLOW_DEFINE_EXTERNAL_STATE_MANAGER(TSharedWriterExternalStateManager);
YT_FLOW_DEFINE_EXTERNAL_STATE_MANAGER(TOpaqueExternalStateManager);
YT_FLOW_DEFINE_EXTERNAL_STATE_MANAGER(TEmbeddedWriterExternalStateManager);

////////////////////////////////////////////////////////////////////////////////

struct TReadingJoinerParameters
    : public IExternalStateJoiner::TParameters
{
    NYPath::TRichYPath Path;

    REGISTER_YSON_STRUCT(TReadingJoinerParameters);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("path", &TThis::Path)
            .AddOption(EYTPathOwnership::ReadOnly);
    }
};

struct TEmptyJoinerDynamicParameters
    : public IExternalStateJoiner::TDynamicParameters
{
    REGISTER_YSON_STRUCT(TEmptyJoinerDynamicParameters);

    static void Register(TRegistrar /*registrar*/)
    { }
};

struct TReadingExternalStateJoiner
    : public IExternalStateJoiner
{
    YT_FLOW_EXTEND_PARAMETERS(TReadingJoinerParameters);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TEmptyJoinerDynamicParameters);

    TReadingExternalStateJoiner(
        TExternalStateJoinerContextPtr /*context*/,
        TDynamicExternalStateJoinerContextPtr /*dynamicContext*/)
    { }

    IStateHolderPtr GetState(const TKey& /*key*/) override
    {
        return nullptr;
    }

    TFuture<void> PreloadKeyStates(const THashSet<TKey>& /*keys*/) override
    {
        return OKFuture;
    }

    NTableClient::TTableSchemaPtr GetKeySchema() const override
    {
        return nullptr;
    }

    const IPayloadConverterCachePtr& GetConverterCache() const override
    {
        static const IPayloadConverterCachePtr Null;
        return Null;
    }

    const std::optional<THashSet<TStreamId>>& GetKeyProviderStreams() const override
    {
        static const std::optional<THashSet<TStreamId>> Null;
        return Null;
    }

    bool HasKeySchemaOverride() const override
    {
        return false;
    }

    void Reset() override
    { }

    void ValidateStateClass(const std::type_info& /*expectedStateType*/) const override
    { }

    IExternalStateJoiner::TParametersPtr GetParametersBase() const override
    {
        return nullptr;
    }

    IExternalStateJoiner::TDynamicParametersPtr GetDynamicParametersBase() const override
    {
        return nullptr;
    }
};

YT_FLOW_DEFINE_EXTERNAL_STATE_JOINER(TReadingExternalStateJoiner);

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TSpecTest, DuplicateOutputStreamIds)
{
    TStringBuf specYson(R""""(
        {
            computations = {
                c1 = {
                    computation_class_name = "NYT::NFlow::TNullComputation";
                    output_stream_ids = [ s ];
                    group_by_schema = [];
                    source_streams = {
                        injected_stream = {
                            source_class_name = "NYT::NFlow::TNullSource";
                        };
                    };
                };
                c2 = {
                    computation_class_name = "NYT::NFlow::TNullComputation";
                    output_stream_ids = [ s ];
                    group_by_schema = [];
                    source_streams = {
                        injected_stream = {
                            source_class_name = "NYT::NFlow::TNullSource";
                        };
                    };
                }
            };
            streams = {
                s = {
                    schema = [];
                };
            };
        }
    )"""");
    auto spec = ConvertTo<TPipelineSpecPtr>(TYsonStringBuf(specYson));
    EXPECT_THROW_WITH_SUBSTRING(
        { ValidatePipelineSpec(spec); },
        "Stream \"s\" is registered both in computation \"c2\" and \"c1\"");
}

TEST(TSpecTest, InputStreamReuse)
{
    TStringBuf specYson(R""""(
        {
            computations = {
                c1 = {
                    computation_class_name = "NYT::NFlow::TNullComputation";
                    output_stream_ids = [ s ];
                    group_by_schema = [];
                    source_streams = {
                        injected_stream = {
                            source_class_name = "NYT::NFlow::TNullSource";
                        };
                    };
                };
                c2 = {
                    computation_class_name = "NYT::NFlow::TNullComputation";
                    input_stream_ids = [ s ];
                    group_by_schema = [];
                };
                c3 = {
                    computation_class_name = "NYT::NFlow::TNullComputation";
                    input_stream_ids = [ s ];
                    group_by_schema = [];
                }
            };
            streams = {
                s = {
                    schema = [];
                };
            };
        }
    )"""");
    auto spec = ConvertTo<TPipelineSpecPtr>(TYsonStringBuf(specYson));
    ValidatePipelineSpec(spec);
}

TEST(TSpecTest, UnknownInputStreamId)
{
    TStringBuf specYson(R""""(
        {
            computations = {
                c = {
                    computation_class_name = "NYT::NFlow::TNullComputation";
                    input_stream_ids = [ s ];
                    group_by_schema = [];
                }
            };
            streams = {
                s = {
                    schema = [];
                };
            };
        }
    )"""");
    auto spec = ConvertTo<TPipelineSpecPtr>(TYsonStringBuf(specYson));
    EXPECT_THROW_WITH_SUBSTRING(
        { ValidatePipelineSpec(spec); },
        "Stream \"s\" does not have producer");
}

TEST(TSpecTest, SinkInputStreamNotInOwnOutputs)
{
    TStringBuf specYson(R""""(
        {
            computations = {
                producer = {
                    computation_class_name = "NYT::NFlow::TNullComputation";
                    output_stream_ids = [ s ];
                    group_by_schema = [];
                    source_streams = {
                        injected_stream = {
                            source_class_name = "NYT::NFlow::TNullSource";
                        };
                    };
                };
                consumer = {
                    computation_class_name = "NYT::NFlow::TNullComputation";
                    input_stream_ids = [ s ];
                    group_by_schema = [];
                    sinks = {
                        my_sink = {
                            sink_class_name = "NYT::NFlow::TNullSink";
                            input_stream_ids = [ s ];
                        };
                    };
                };
            };
            streams = {
                s = {
                    schema = [];
                };
            };
        }
    )"""");
    auto spec = ConvertTo<TPipelineSpecPtr>(TYsonStringBuf(specYson));
    EXPECT_THROW_WITH_SUBSTRING(
        { ValidatePipelineSpec(spec); },
        "Input stream \"s\" of sink \"my_sink\" in computation \"consumer\" is not an output stream of this computation");
}

TEST(TSpecTest, SinkInputStreamMatchesOwnOutput)
{
    TStringBuf specYson(R""""(
        {
            computations = {
                c = {
                    computation_class_name = "NYT::NFlow::TNullComputation";
                    output_stream_ids = [ s ];
                    group_by_schema = [];
                    source_streams = {
                        injected_stream = {
                            source_class_name = "NYT::NFlow::TNullSource";
                        };
                    };
                    sinks = {
                        my_sink = {
                            sink_class_name = "NYT::NFlow::TNullSink";
                            input_stream_ids = [ s ];
                        };
                    };
                }
            };
            streams = {
                s = {
                    schema = [];
                };
            };
        }
    )"""");
    auto spec = ConvertTo<TPipelineSpecPtr>(TYsonStringBuf(specYson));
    ValidatePipelineSpec(spec);
}

TEST(TSpecTest, UnknownRequiredResourceId)
{
    TStringBuf specYson(R""""(
        {
            computations = {
                c = {
                    computation_class_name = "NYT::NFlow::TNullComputation";
                    output_stream_ids = [ s ];
                    required_resource_ids = { r = {}; };
                    group_by_schema = [];
                    source_streams = {
                        injected_stream = {
                            source_class_name = "NYT::NFlow::TNullSource";
                        };
                    };
                }
            };
            streams = {
                s = {
                    schema = [];
                }
            }
        }
    )"""");
    auto spec = ConvertTo<TPipelineSpecPtr>(TYsonStringBuf(specYson));
    EXPECT_THROW_WITH_SUBSTRING(
        { ValidatePipelineSpec(spec); },
        "Unknown resource \"r\" required in computation \"c\"");
}

TEST(TSpecTest, UnusedOutputStream)
{
    TStringBuf specYson(R""""(
        {
            computations = {
                c = {
                    computation_class_name = "NYT::NFlow::TNullComputation";
                    output_stream_ids = [ s ];
                    group_by_schema = [];
                    source_streams = {
                        injected_stream = {
                            source_class_name = "NYT::NFlow::TNullSource";
                        };
                    };
                }
            };
            streams = {
                s = {
                    schema = [];
                };
            };
        }
    )"""");
    auto spec = ConvertTo<TPipelineSpecPtr>(TYsonStringBuf(specYson));
    ValidatePipelineSpec(spec);
}

TEST(TSpecTest, CycleFound)
{
    TStringBuf specYson(R""""(
        {
            computations = {
                injector = {
                    computation_class_name = "NYT::NFlow::TNullComputation";
                    output_stream_ids = [ event ];
                    group_by_schema = [];
                    source_streams = {
                        injected_stream = {
                            source_class_name = "NYT::NFlow::TNullSource";
                        };
                    };
                };
                state = {
                    computation_class_name = "NYT::NFlow::TNullComputation";
                    group_by_schema = [];
                    input_stream_ids = [ event; response; ];
                    output_stream_ids = [ request ];
                };
                processor = {
                    computation_class_name = "NYT::NFlow::TNullComputation";
                    group_by_schema = [];
                    input_stream_ids = [ request ];
                    output_stream_ids = [ response ];
                }
            };
            streams = {
                event = {
                    schema = [];
                };
                request = {
                    schema = [];
                };
                response = {
                    schema = [];
                };
            }
        }
    )"""");
    auto spec = ConvertTo<TPipelineSpecPtr>(TYsonStringBuf(specYson));
    auto graph = BuildStreamGraph(spec);
    THashMap<TStreamId, std::vector<TStreamId>> expected = {
        {"event", {"request"}},
        {"request", {"response"}},
        {"response", {"request"}},
    };
    EXPECT_THROW_WITH_SUBSTRING(
        { ValidatePipelineSpec(spec); },
        "Stream graph should be acyclic");
}

TEST(TSpecTest, CycleResolved)
{
    TStringBuf specYson(R""""(
        {
            computations = {
                injector = {
                    computation_class_name = "NYT::NFlow::TNullComputation";
                    output_stream_ids = [ event ];
                    group_by_schema = [];
                    source_streams = {
                        injected_stream = {
                            source_class_name = "NYT::NFlow::TNullSource";
                        };
                    };
                };
                state = {
                    computation_class_name = "NYT::NFlow::TNullComputation";
                    group_by_schema = [];
                    input_stream_ids = [ event; response; ];
                    output_stream_ids = [ request ];
                    streams_dependency = {
                        request = [event];
                    };

                };
                processor = {
                    computation_class_name = "NYT::NFlow::TNullComputation";
                    group_by_schema = [];
                    input_stream_ids = [ request ];
                    output_stream_ids = [ response ];
                }
            };
            streams = {
                event = {
                    schema = [];
                };
                request = {
                    schema = [];
                };
                response = {
                    schema = [];
                };
            }
        }
    )"""");
    auto spec = ConvertTo<TPipelineSpecPtr>(TYsonStringBuf(specYson));
    auto graph = BuildStreamGraph(spec);
    THashMap<TStreamId, std::vector<TStreamId>> expected = {
        {"injector/injected_stream", {"event"}},
        {"event", {"request"}},
        {"request", {"response"}},
        {"response", {}},
    };
    ASSERT_EQ(graph, expected);
    ValidatePipelineSpec(spec);
}

TEST(TSpecTest, StreamDependencyUnknownInput)
{
    TStringBuf specYson(R""""(
        {
            computations = {
                injector = {
                    computation_class_name = "NYT::NFlow::TNullComputation";
                    output_stream_ids = [ event ];
                    group_by_schema = [];
                    source_streams = {
                        injected_stream = {
                            source_class_name = "NYT::NFlow::TNullSource";
                        };
                    };
                };
                state = {
                    computation_class_name = "NYT::NFlow::TNullComputation";
                    group_by_schema = [];
                    input_stream_ids = [ event; response; ];
                    output_stream_ids = [ request ];
                    streams_dependency = {
                        request = [event; hit;];
                    };

                };
                processor = {
                    computation_class_name = "NYT::NFlow::TNullComputation";
                    group_by_schema = [];
                    input_stream_ids = [ request ];
                    output_stream_ids = [ response ];
                }
            };
            streams = {
                event = {
                    schema = [];
                };
                request = {
                    schema = [];
                };
                response = {
                    schema = [];
                };
            }
        }
    )"""");
    auto spec = ConvertTo<TPipelineSpecPtr>(TYsonStringBuf(specYson));
    EXPECT_THROW_WITH_SUBSTRING(
        { ValidatePipelineSpec(spec); },
        "Unknown stream \"hit\" in computation \"state\" in \"streams_dependency\"");
}

TEST(TSpecTest, StreamDependencyUnknownOutput)
{
    TStringBuf specYson(R""""(
        {
            computations = {
                injector = {
                    computation_class_name = "NYT::NFlow::TNullComputation";
                    output_stream_ids = [ event ];
                    group_by_schema = [];
                    source_streams = {
                        injected_stream = {
                            source_class_name = "NYT::NFlow::TNullSource";
                        };
                    };
                };
                state = {
                    computation_class_name = "NYT::NFlow::TNullComputation";
                    group_by_schema = [];
                    input_stream_ids = [ event; response; ];
                    output_stream_ids = [ request ];
                    streams_dependency = {
                        request = [event];
                        timer = [response];
                    };

                };
                processor = {
                    computation_class_name = "NYT::NFlow::TNullComputation";
                    group_by_schema = [];
                    input_stream_ids = [ request ];
                    output_stream_ids = [ response ];
                }
            };
            streams = {
                event = {
                    schema = [];
                };
                request = {
                    schema = [];
                };
                response = {
                    schema = [];
                };
            }
        }
    )"""");
    auto spec = ConvertTo<TPipelineSpecPtr>(TYsonStringBuf(specYson));
    EXPECT_THROW_WITH_SUBSTRING(
        { ValidatePipelineSpec(spec); },
        "Unknown stream \"timer\" in computation \"state\" in \"streams_dependency\"");
}

TEST(TSpecTest, ComputeAllowedInputStreams)
{
    static const auto sort = [] (const THashSet<TStreamId>& streams) {
        std::vector<TStreamId> result(streams.begin(), streams.end());
        std::sort(result.begin(), result.end());
        return result;
    };

    TStringBuf specYson(R""""(
        {
            computation_class_name = "NYT::NFlow::TNullComputation";
            group_by_schema = [];
            input_stream_ids = [ event; response_1; response_2 ];
            output_stream_ids = [ request_1; request_2 ];
            timer_streams = {
                timer_1 = {};
                timer_2 = {};
            };
            streams_dependency = {
                request_1 = [event];
                request_2 = [response_1; response_2; timer_1; timer_2];
                timer_1 = [event; response_1];
                timer_2 = [response_2];
            };
        }
    )"""");
    auto spec = ConvertTo<TComputationSpecPtr>(TYsonStringBuf(specYson));
    EXPECT_THAT(sort(ComputeAllowedInputStreams({}, spec)),
        ::testing::ElementsAre());
    EXPECT_THAT(sort(ComputeAllowedInputStreams({"request_1"}, spec)),
        ::testing::ElementsAre(TStreamId("event")));
    EXPECT_THAT(sort(ComputeAllowedInputStreams({"request_2"}, spec)),
        ::testing::ElementsAre(TStreamId("response_1"), TStreamId("response_2"), TStreamId("timer_1"), TStreamId("timer_2")));
    EXPECT_THAT(sort(ComputeAllowedInputStreams({"request_1", "request_2"}, spec)),
        ::testing::ElementsAre(TStreamId("event"), TStreamId("response_1"), TStreamId("response_2"), TStreamId("timer_1"), TStreamId("timer_2")));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TSpecValidateParseabilityTest, Simple)
{
    auto pipelineSpecConfigString = TYsonString(TStringBuf(R""""(
        {
            computations = {
                injector = {
                    computation_class_name = "NYT::NFlow::TNullComputation";
                    output_stream_ids = [ event ];
                    group_by_schema = [];
                    source_streams = {
                        injected_stream = {
                            source_class_name = "NYT::NFlow::TNullSource";
                            parameters = {};
                        };
                    };
                    parameters = {};
                };
            };
            streams = {
                event = {
                    schema = [];
                };
            }
        }
    )""""));

    auto dynamicPipelineSpecConfigString = TYsonString(TStringBuf(R""""(
        {
            computations = {
                injector = {
                    source_streams = {
                        injected_stream = {
                            parameters = {};
                        };
                    };
                    parameters = {};
                };
            };
        }
    )""""));

    auto validate = [&] (auto mutator) {
        auto spec = ConvertTo<IMapNodePtr>(pipelineSpecConfigString);
        mutator(spec);
        auto errors = TRegistry::Get()->ValidatePipelineSpecParseability(spec);
        for (const auto& error : errors) {
            THROW_ERROR_EXCEPTION(error);
        }
    };

    EXPECT_NO_THROW(
        validate([] (auto& /*spec*/) {
            // Do nothing.
        }));

    EXPECT_THROW_WITH_SUBSTRING(
        validate([] (auto& spec) {
            SetNodeByYPath(spec, "/unrecognized_field_1", ConvertTo<INodePtr>(42));
        }),
        "unrecognized_field_1");

    EXPECT_THROW_WITH_SUBSTRING(
        validate([] (auto& spec) {
            SetNodeByYPath(spec, "/computations/injector/parameters/unrecognized_field_2", ConvertTo<INodePtr>(42));
        }),
        "unrecognized_field_2");

    EXPECT_THROW_WITH_SUBSTRING(
        validate([] (auto& spec) {
            SetNodeByYPath(spec, "/computations/injector/source_streams/injected_stream/parameters/unrecognized_field_3", ConvertTo<INodePtr>(42));
        }),
        "unrecognized_field_3");

    auto addSink = [] (auto& spec, bool malformedParameters) {
        SetNodeByYPath(
            spec,
            "/computations/injector/sinks/my_sink",
            ConvertTo<INodePtr>(TYsonString(TStringBuf(R""""(
                {
                    "sink_class_name" = "NYT::NFlow::TNullSink";
                    "input_stream_ids" = ["event";];
                    "parameters" = {};
                })""""))),
            /*force*/ true);
        if (malformedParameters) {
            SetNodeByYPath(
                spec,
                "/computations/injector/sinks/my_sink/parameters/unrecognized_field_4",
                ConvertTo<INodePtr>(42));
        }
    };

    EXPECT_NO_THROW(
        validate([&] (auto& spec) {
            addSink(spec, false);
        }));
    EXPECT_THROW_WITH_SUBSTRING(
        validate([&] (auto& spec) {
            addSink(spec, true);
        }),
        "unrecognized_field_4");

    auto dynamicValidate = [&] (auto mutator) {
        auto spec = ConvertTo<IMapNodePtr>(pipelineSpecConfigString);
        auto dynamicSpec = ConvertTo<IMapNodePtr>(dynamicPipelineSpecConfigString);
        mutator(spec, dynamicSpec);
        auto errors = TRegistry::Get()->ValidateDynamicPipelineSpecParseability(ConvertTo<TPipelineSpecPtr>(spec), dynamicSpec);
        for (const auto& error : errors) {
            THROW_ERROR_EXCEPTION(error);
        }
    };

    EXPECT_NO_THROW(
        dynamicValidate([] (auto& /*spec*/, auto& /*dynamicSpec*/) {
            // Do nothing.
        }));

    EXPECT_THROW_WITH_SUBSTRING(
        dynamicValidate([] (auto& /*spec*/, auto& dynamicSpec) {
            SetNodeByYPath(dynamicSpec, "/unrecognized_field_1", ConvertTo<INodePtr>(42));
        }),
        "unrecognized_field_1");

    EXPECT_THROW_WITH_SUBSTRING(
        dynamicValidate([] (auto& /*spec*/, auto& dynamicSpec) {
            SetNodeByYPath(dynamicSpec, "/computations/injector/parameters/unrecognized_field_2", ConvertTo<INodePtr>(42));
        }),
        "unrecognized_field_2");

    EXPECT_THROW_WITH_SUBSTRING(
        dynamicValidate([] (auto& /*spec*/, auto& dynamicSpec) {
            SetNodeByYPath(dynamicSpec, "/computations/injector/source_streams/injected_stream/parameters/unrecognized_field_3", ConvertTo<INodePtr>(42));
        }),
        "unrecognized_field_3");

    auto addSinkWithDynamicSpec = [] (auto& spec, auto& dynamicSpec, bool malformedParameters) {
        SetNodeByYPath(
            spec,
            "/computations/injector/sinks/my_sink",
            ConvertTo<INodePtr>(TYsonString(TStringBuf(R""""(
                {
                    "sink_class_name" = "NYT::NFlow::TNullSink";
                    "input_stream_ids" = ["event";];
                })""""))),
            /*force*/ true);
        SetNodeByYPath(
            dynamicSpec,
            "/computations/injector/sinks/my_sink",
            ConvertTo<INodePtr>(TYsonString(TStringBuf(R""""(
                {
                    "parameters" = {};
                })""""))),
            /*force*/ true);
        if (malformedParameters) {
            SetNodeByYPath(
                dynamicSpec,
                "/computations/injector/sinks/my_sink/parameters/unrecognized_field_4",
                ConvertTo<INodePtr>(42));
        }
    };

    EXPECT_NO_THROW(
        dynamicValidate([&] (auto& spec, auto& dynamicSpec) {
            addSinkWithDynamicSpec(spec, dynamicSpec, false);
        }));
    EXPECT_THROW_WITH_SUBSTRING(
        dynamicValidate([&] (auto& spec, auto& dynamicSpec) {
            addSinkWithDynamicSpec(spec, dynamicSpec, true);
        }),
        "unrecognized_field_4");
}

TEST(TSpecValidateParseabilityTest, DynamicSpecWithUnknownComputation)
{
    auto pipelineSpecConfigString = TYsonString(TStringBuf(R""""(
        {
            computations = {
                c1 = {
                    computation_class_name = "NYT::NFlow::TNullComputation";
                    group_by_schema = [];
                };
            };
        }
    )""""));

    auto dynamicPipelineSpecConfigString = TYsonString(TStringBuf(R""""(
        {
            computations = {
                unknown = {};
            };
        }
    )""""));

    auto spec = ConvertTo<TPipelineSpecPtr>(pipelineSpecConfigString);
    auto dynamicSpec = ConvertTo<IMapNodePtr>(dynamicPipelineSpecConfigString);
    auto errors = TRegistry::Get()->ValidateDynamicPipelineSpecParseability(spec, dynamicSpec);

    EXPECT_FALSE(errors.empty());
    EXPECT_THAT(ToString(errors[0]), ::testing::HasSubstr("unknown"));
    EXPECT_THAT(ToString(errors[0]), ::testing::HasSubstr("does not exist in static spec"));
}

TEST(TSpecValidateParseabilityTest, DynamicSpecWithUnknownSourceStream)
{
    auto pipelineSpecConfigString = TYsonString(TStringBuf(R""""(
        {
            computations = {
                c1 = {
                    computation_class_name = "NYT::NFlow::TNullComputation";
                    group_by_schema = [];
                    source_streams = {
                        s1 = {
                            source_class_name = "NYT::NFlow::TNullSource";
                        };
                    };
                };
            };
        }
    )""""));

    auto dynamicPipelineSpecConfigString = TYsonString(TStringBuf(R""""(
        {
            computations = {
                c1 = {
                    source_streams = {
                        unknown = {};
                    };
                };
            };
        }
    )""""));

    auto spec = ConvertTo<TPipelineSpecPtr>(pipelineSpecConfigString);
    auto dynamicSpec = ConvertTo<IMapNodePtr>(dynamicPipelineSpecConfigString);
    auto errors = TRegistry::Get()->ValidateDynamicPipelineSpecParseability(spec, dynamicSpec);

    EXPECT_FALSE(errors.empty());
    EXPECT_THAT(ToString(errors[0]), ::testing::HasSubstr("unknown"));
    EXPECT_THAT(ToString(errors[0]), ::testing::HasSubstr("c1"));
    EXPECT_THAT(ToString(errors[0]), ::testing::HasSubstr("does not exist in static spec"));
}

TEST(TSpecValidateParseabilityTest, DynamicSpecWithUnknownSink)
{
    auto pipelineSpecConfigString = TYsonString(TStringBuf(R""""(
        {
            computations = {
                c1 = {
                    computation_class_name = "NYT::NFlow::TNullComputation";
                    group_by_schema = [];
                    sinks = {
                        sink1 = {
                            sink_class_name = "NYT::NFlow::TNullSink";
                        };
                    };
                };
            };
        }
    )""""));

    auto dynamicPipelineSpecConfigString = TYsonString(TStringBuf(R""""(
        {
            computations = {
                c1 = {
                    sinks = {
                        unknown = {};
                    };
                };
            };
        }
    )""""));

    auto spec = ConvertTo<TPipelineSpecPtr>(pipelineSpecConfigString);
    auto dynamicSpec = ConvertTo<IMapNodePtr>(dynamicPipelineSpecConfigString);
    auto errors = TRegistry::Get()->ValidateDynamicPipelineSpecParseability(spec, dynamicSpec);

    EXPECT_FALSE(errors.empty());
    EXPECT_THAT(ToString(errors[0]), ::testing::HasSubstr("unknown"));
    EXPECT_THAT(ToString(errors[0]), ::testing::HasSubstr("c1"));
    EXPECT_THAT(ToString(errors[0]), ::testing::HasSubstr("does not exist in static spec"));
}

TEST(TSpecValidateParseabilityTest, DynamicSpecWithValidKeys)
{
    auto pipelineSpecConfigString = TYsonString(TStringBuf(R""""(
        {
            computations = {
                c1 = {
                    computation_class_name = "NYT::NFlow::TNullComputation";
                    group_by_schema = [];
                    source_streams = {
                        s1 = {
                            source_class_name = "NYT::NFlow::TNullSource";
                        };
                    };
                    sinks = {
                        sink1 = {
                            sink_class_name = "NYT::NFlow::TNullSink";
                        };
                    };
                };
            };
        }
    )""""));

    auto dynamicPipelineSpecConfigString = TYsonString(TStringBuf(R""""(
        {
            computations = {
                c1 = {
                    source_streams = {
                        s1 = {};
                    };
                    sinks = {
                        sink1 = {};
                    };
                };
            };
        }
    )""""));

    auto spec = ConvertTo<TPipelineSpecPtr>(pipelineSpecConfigString);
    auto dynamicSpec = ConvertTo<IMapNodePtr>(dynamicPipelineSpecConfigString);
    auto errors = TRegistry::Get()->ValidateDynamicPipelineSpecParseability(spec, dynamicSpec);

    EXPECT_TRUE(errors.empty());
}

TEST(TSpecValidateParseabilityTest, DynamicSpecWithUnknownResource)
{
    auto pipelineSpecConfigString = TYsonString(TStringBuf(R""""(
        {
            resources = {
                r1 = {
                    resource_class_name = "NYT::NFlow::TNullResource";
                };
            };
        }
    )""""));

    auto dynamicPipelineSpecConfigString = TYsonString(TStringBuf(R""""(
        {
            resources = {
                unknown = {};
            };
        }
    )""""));

    auto spec = ConvertTo<TPipelineSpecPtr>(pipelineSpecConfigString);
    auto dynamicSpec = ConvertTo<IMapNodePtr>(dynamicPipelineSpecConfigString);
    auto errors = TRegistry::Get()->ValidateDynamicPipelineSpecParseability(spec, dynamicSpec);

    EXPECT_FALSE(errors.empty());
    EXPECT_THAT(ToString(errors[0]), ::testing::HasSubstr("unknown"));
    EXPECT_THAT(ToString(errors[0]), ::testing::HasSubstr("does not exist in static spec"));
}

TEST(TSpecValidateParseabilityTest, DynamicSpecWithResourceDynamicParameters)
{
    auto pipelineSpecConfigString = TYsonString(TStringBuf(R""""(
        {
            resources = {
                r1 = {
                    resource_class_name = "NYT::NFlow::TNullResource";
                };
            };
        }
    )""""));

    auto dynamicPipelineSpecConfigString = TYsonString(TStringBuf(R""""(
        {
            resources = {
                r1 = {
                    parameters = {};
                };
            };
        }
    )""""));

    auto spec = ConvertTo<TPipelineSpecPtr>(pipelineSpecConfigString);
    auto dynamicSpec = ConvertTo<IMapNodePtr>(dynamicPipelineSpecConfigString);
    auto errors = TRegistry::Get()->ValidateDynamicPipelineSpecParseability(spec, dynamicSpec);

    EXPECT_TRUE(errors.empty());
}

TEST(TSpecValidateParseabilityTest, DynamicSpecWithResourceDynamicParametersUnrecognized)
{
    auto pipelineSpecConfigString = TYsonString(TStringBuf(R""""(
        {
            resources = {
                r1 = {
                    resource_class_name = "NYT::NFlow::TNullResource";
                };
            };
        }
    )""""));

    auto dynamicPipelineSpecConfigString = TYsonString(TStringBuf(R""""(
        {
            resources = {
                r1 = {
                    parameters = {
                        unrecognized_field = 42;
                    };
                };
            };
        }
    )""""));

    auto spec = ConvertTo<TPipelineSpecPtr>(pipelineSpecConfigString);
    auto dynamicSpec = ConvertTo<IMapNodePtr>(dynamicPipelineSpecConfigString);
    auto errors = TRegistry::Get()->ValidateDynamicPipelineSpecParseability(spec, dynamicSpec);

    EXPECT_FALSE(errors.empty());
    EXPECT_THAT(ToString(errors[0]), ::testing::HasSubstr("unrecognized_field"));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TSpecTest, ReadDelayCycle)
{
    // Initial graph: injector/injected_stream -> event -> request -> response.
    TStringBuf specYson(R""""(
        {
            computations = {
                injector = {
                    computation_class_name = "NYT::NFlow::TNullComputation";
                    output_stream_ids = [ event ];
                    group_by_schema = [];
                    source_streams = {
                        injected_stream = {
                            source_class_name = "NYT::NFlow::TNullSource";
                        };
                    };
                    watermark_strategy = {
                        watermark_alignment = {};
                    };
                };
                state = {
                    computation_class_name = "NYT::NFlow::TNullComputation";
                    group_by_schema = [];
                    input_stream_ids = [ event; response; ];
                    output_stream_ids = [ request ];
                    streams_dependency = {
                        request = [event];
                    };
                };
                processor = {
                    computation_class_name = "NYT::NFlow::TNullComputation";
                    group_by_schema = [];
                    input_stream_ids = [ request ];
                    output_stream_ids = [ response ];
                }
            };
            streams = {
                event = { schema = []; };
                request = { schema = []; };
                response = { schema = []; };
            }
        }
    )"""");
    auto spec = ConvertTo<TPipelineSpecPtr>(TYsonStringBuf(specYson));
    ValidatePipelineSpec(spec);

    // Adding edge: response -> injector/injected_stream.
    // And getting cycle: response -> injector/injected_stream -> event -> request -> response.
    spec->Computations["injector"]->WatermarkStrategy->WatermarkAlignment->ReadDelays = {{TStreamId("response"), TDuration::Seconds(30)}};
    auto graph = BuildStreamGraph(spec, /*addReadDelayEdges*/ true);
    ASSERT_TRUE(graph.contains("response"));
    ASSERT_THAT(graph["response"], ::testing::Contains(TStreamId("injector/injected_stream")));
    EXPECT_THROW_WITH_SUBSTRING(
        { ValidatePipelineSpec(spec); },
        "Stream graph should be acyclic");
}

TEST(TSpecTest, ReadDelayWithoutSourceStreams)
{
    TStringBuf specYson(R""""(
        {
            computations = {
                reader = {
                    computation_class_name = "NYT::NFlow::TNullComputation";
                    group_by_schema = [];
                    output_stream_ids = [ event ];
                    source_streams = {
                        s = { source_class_name = "NYT::NFlow::TNullSource"; };
                    };
                };
                processor = {
                    computation_class_name = "NYT::NFlow::TNullComputation";
                    group_by_schema = [];
                    input_stream_ids = [ event ];
                    watermark_strategy = {
                        watermark_alignment = {
                            read_delays = {
                                event = "30s";
                            };
                        };
                    };
                };
            };
            streams = {
                event = { schema = []; };
            }
        }
    )"""");
    auto spec = ConvertTo<TPipelineSpecPtr>(TYsonStringBuf(specYson));
    EXPECT_THROW_WITH_SUBSTRING(
        { ValidatePipelineSpec(spec); },
        "has non-empty \"read_delays\" but no source streams");
}

////////////////////////////////////////////////////////////////////////////////

TEST(TSpecTest, PreservesUnrecognizedFieldsOnRoundTrip)
{
    TStringBuf specYson(R""""(
        {
            unknown_field = 42;
            computations = {
                c = {
                    computation_class_name = "NYT::NFlow::TNullComputation";
                    unknown_computation_field = "value";
                };
            };
        }
    )"""");
    auto roundTripped = ConvertTo<IMapNodePtr>(ConvertTo<TPipelineSpecPtr>(TYsonStringBuf(specYson)));

    EXPECT_EQ(GetNodeByYPath(roundTripped, "/unknown_field")->GetValue<i64>(), 42);
    EXPECT_EQ(GetNodeByYPath(roundTripped, "/computations/c/unknown_computation_field")->GetValue<std::string>(), "value");
}

TEST(TDynamicSpecTest, PreservesUnrecognizedFieldsOnRoundTrip)
{
    TStringBuf dynamicSpecYson(R""""(
        {
            unknown_field = 42;
            computations = {
                c = {
                    unknown_computation_field = "value";
                };
            };
        }
    )"""");
    auto roundTripped = ConvertTo<IMapNodePtr>(ConvertTo<TDynamicPipelineSpecPtr>(TYsonStringBuf(dynamicSpecYson)));

    EXPECT_EQ(GetNodeByYPath(roundTripped, "/unknown_field")->GetValue<i64>(), 42);
    EXPECT_EQ(GetNodeByYPath(roundTripped, "/computations/c/unknown_computation_field")->GetValue<std::string>(), "value");
}

////////////////////////////////////////////////////////////////////////////////

TEST(TDynamicSpecTest, ThrottlerIdsResolved)
{
    TStringBuf dynamicSpecYson(R""""(
        {
            throttlers = {
                rows = { limit = 100.0; };
                bytes = { limit = 10000.0; };
            };
            computations = {
                c = {
                    input_rows_throttler_id = "rows";
                    input_bytes_throttler_id = "bytes";
                };
            };
        }
    )"""");
    auto dynamicSpec = ConvertTo<TDynamicPipelineSpecPtr>(TYsonStringBuf(dynamicSpecYson));
    ValidateDynamicPipelineSpec(dynamicSpec);
}

TEST(TDynamicSpecTest, UnknownInputRowsThrottlerId)
{
    TStringBuf dynamicSpecYson(R""""(
        {
            throttlers = {
                known = { limit = 100.0; };
            };
            computations = {
                c = {
                    input_rows_throttler_id = "missing";
                };
            };
        }
    )"""");
    auto dynamicSpec = ConvertTo<TDynamicPipelineSpecPtr>(TYsonStringBuf(dynamicSpecYson));
    EXPECT_THROW_WITH_SUBSTRING(
        { ValidateDynamicPipelineSpec(dynamicSpec); },
        "Throttler \"missing\" referenced in input_rows_throttler_id is not declared in dynamic_spec/throttlers");
}

TEST(TDynamicSpecTest, UnknownInputBytesThrottlerId)
{
    TStringBuf dynamicSpecYson(R""""(
        {
            throttlers = {
                known = { limit = 100.0; };
            };
            computations = {
                c = {
                    input_bytes_throttler_id = "missing";
                };
            };
        }
    )"""");
    auto dynamicSpec = ConvertTo<TDynamicPipelineSpecPtr>(TYsonStringBuf(dynamicSpecYson));
    EXPECT_THROW_WITH_SUBSTRING(
        { ValidateDynamicPipelineSpec(dynamicSpec); },
        "Throttler \"missing\" referenced in input_bytes_throttler_id is not declared in dynamic_spec/throttlers");
}

////////////////////////////////////////////////////////////////////////////////

TEST(TResolveUseCompactInputMessagesTest, DefaultUintKeyEnablesCompact)
{
    TStringBuf specYson(R""""(
        {
            computation_class_name = "NYT::NFlow::TNullComputation";
            group_by_schema = [];
        }
    )"""");
    auto spec = ConvertTo<TComputationSpecPtr>(TYsonStringBuf(specYson));
    EXPECT_TRUE(ResolveUseCompactInputMessages(spec));
}

TEST(TResolveUseCompactInputMessagesTest, NonUintKeyDisablesCompact)
{
    TStringBuf specYson(R""""(
        {
            computation_class_name = "NYT::NFlow::TNullComputation";
            group_by_schema = [];
            experimental_enable_non_uint_key = %true;
        }
    )"""");
    auto spec = ConvertTo<TComputationSpecPtr>(TYsonStringBuf(specYson));
    EXPECT_FALSE(ResolveUseCompactInputMessages(spec));
}

TEST(TResolveUseCompactInputMessagesTest, ExplicitFalseWins)
{
    TStringBuf specYson(R""""(
        {
            computation_class_name = "NYT::NFlow::TNullComputation";
            group_by_schema = [];
            use_compact_input_messages = %false;
        }
    )"""");
    auto spec = ConvertTo<TComputationSpecPtr>(TYsonStringBuf(specYson));
    EXPECT_FALSE(ResolveUseCompactInputMessages(spec));
}

TEST(TResolveUseCompactInputMessagesTest, ExplicitTrueWinsOverNonUintKey)
{
    TStringBuf specYson(R""""(
        {
            computation_class_name = "NYT::NFlow::TNullComputation";
            group_by_schema = [];
            use_compact_input_messages = %true;
            experimental_enable_non_uint_key = %true;
        }
    )"""");
    auto spec = ConvertTo<TComputationSpecPtr>(TYsonStringBuf(specYson));
    EXPECT_TRUE(ResolveUseCompactInputMessages(spec));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TSpecTest, CompactInputMessagesEnabledWithNonUintKeyRejected)
{
    TStringBuf specYson(R""""(
        {
            computations = {
                producer = {
                    computation_class_name = "NYT::NFlow::TNullComputation";
                    output_stream_ids = [ s ];
                    group_by_schema = [];
                    source_streams = {
                        injected_stream = {
                            source_class_name = "NYT::NFlow::TNullSource";
                        };
                    };
                };
                consumer = {
                    computation_class_name = "NYT::NFlow::TNullComputation";
                    input_stream_ids = [ s ];
                    group_by_schema = [];
                    use_compact_input_messages = %true;
                    experimental_enable_non_uint_key = %true;
                }
            };
            streams = {
                s = {
                    schema = [];
                };
            };
        }
    )"""");
    auto spec = ConvertTo<TPipelineSpecPtr>(TYsonStringBuf(specYson));
    EXPECT_THROW_WITH_SUBSTRING(
        { ValidatePipelineSpec(spec); },
        "\"use_compact_input_messages\" cannot be enabled while \"experimental_enable_non_uint_key\" is set");
}

// The options are incompatible only when both are enabled: an explicitly disabled
// use_compact_input_messages coexists with experimental_enable_non_uint_key.
TEST(TSpecTest, CompactInputMessagesDisabledWithNonUintKeyAccepted)
{
    TStringBuf specYson(R""""(
        {
            computations = {
                producer = {
                    computation_class_name = "NYT::NFlow::TNullComputation";
                    output_stream_ids = [ s ];
                    group_by_schema = [];
                    source_streams = {
                        injected_stream = {
                            source_class_name = "NYT::NFlow::TNullSource";
                        };
                    };
                };
                consumer = {
                    computation_class_name = "NYT::NFlow::TNullComputation";
                    input_stream_ids = [ s ];
                    group_by_schema = [];
                    use_compact_input_messages = %false;
                    experimental_enable_non_uint_key = %true;
                }
            };
            streams = {
                s = {
                    schema = [];
                };
            };
        }
    )"""");
    auto spec = ConvertTo<TPipelineSpecPtr>(TYsonStringBuf(specYson));
    ValidatePipelineSpec(spec);
}

TEST(TSpecTest, CompactInputMessagesWithUintKeyAccepted)
{
    TStringBuf specYson(R""""(
        {
            computations = {
                producer = {
                    computation_class_name = "NYT::NFlow::TNullComputation";
                    output_stream_ids = [ s ];
                    group_by_schema = [];
                    source_streams = {
                        injected_stream = {
                            source_class_name = "NYT::NFlow::TNullSource";
                        };
                    };
                };
                consumer = {
                    computation_class_name = "NYT::NFlow::TNullComputation";
                    input_stream_ids = [ s ];
                    group_by_schema = [];
                    use_compact_input_messages = %true;
                }
            };
            streams = {
                s = {
                    schema = [];
                };
            };
        }
    )"""");
    auto spec = ConvertTo<TPipelineSpecPtr>(TYsonStringBuf(specYson));
    ValidatePipelineSpec(spec);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TSpecTest, KeyVisitorStreamsRequireUintFirstColumn)
{
    TStringBuf specYson(R""""(
        {
            computations = {
                c = {
                    computation_class_name = "NYT::NFlow::TNullComputation";
                    output_stream_ids = [];
                    group_by_schema = [{name = key; type = string; sort_order = ascending}];
                    source_streams = {};
                    key_visitor_streams = {visit = {}};
                };
            };
        }
    )"""");
    auto spec = ConvertTo<TPipelineSpecPtr>(TYsonStringBuf(specYson));
    EXPECT_THROW_WITH_SUBSTRING(
        { ValidatePipelineSpec(spec); },
        "Uint64 as the first column");
}

TEST(TSpecTest, KeyVisitorStreamsAreNotInAutoDependencyGraph)
{
    TStringBuf specYson(R""""(
        {
            computations = {
                c = {
                    computation_class_name = "NYT::NFlow::TNullComputation";
                    output_stream_ids = [ visits ];
                    group_by_schema = [
                        {name = hash; expression = "farm_hash(key)"; type = uint64; required = %true; sort_order = ascending};
                        {name = key; type = string; sort_order = ascending};
                    ];
                    source_streams = {};
                    key_visitor_streams = {visit_iter = {}};
                };
            };
        }
    )"""");
    auto spec = ConvertTo<TPipelineSpecPtr>(TYsonStringBuf(specYson));
    const auto& deps = spec->Computations.at(TComputationId("c"))->StreamsDependency;
    EXPECT_FALSE(deps.contains(TStreamId("visit_iter")))
        << "key_visitor stream must not be auto-registered as a key";
    EXPECT_FALSE(deps.at(TStreamId("visits")).contains(TStreamId("visit_iter")))
        << "output stream must not auto-depend on the key_visitor stream";
}

TEST(TSpecTest, KeyVisitorStreamCanBeAddedToOutputDepsExplicitly)
{
    TStringBuf specYson(R""""(
        {
            computations = {
                c = {
                    computation_class_name = "NYT::NFlow::TNullComputation";
                    output_stream_ids = [ visits ];
                    group_by_schema = [
                        {name = hash; expression = "farm_hash(key)"; type = uint64; required = %true; sort_order = ascending};
                        {name = key; type = string; sort_order = ascending};
                    ];
                    source_streams = {};
                    key_visitor_streams = {visit_iter = {}};
                    streams_dependency = {
                        visits = [ visit_iter ];
                    };
                };
            };
        }
    )"""");
    auto spec = ConvertTo<TPipelineSpecPtr>(TYsonStringBuf(specYson));
    const auto& depsForVisits = spec->Computations.at(TComputationId("c"))->StreamsDependency.at(TStreamId("visits"));
    EXPECT_TRUE(depsForVisits.contains(TStreamId("visit_iter")))
        << "explicit user-provided dependency on visit-stream must be preserved";
}

TEST(TSpecTest, StateJoinerKeyTypeMismatchNoOverride)
{
    TStringBuf specYson(R""""(
        {
            computations = {
                source = {
                    computation_class_name = "NYT::NFlow::TNullComputation";
                    group_by_schema = [ { name = Key; type = uint64; required = %true; } ];
                };
                joiner = {
                    computation_class_name = "NYT::NFlow::TNullComputation";
                    group_by_schema = [ { name = Key; type = string; required = %true; } ];
                    state_joiners = {
                        "/from_source" = {
                            computation_id = source;
                            state_name = "/state";
                        };
                    };
                }
            };
        }
    )"""");
    auto spec = ConvertTo<TPipelineSpecPtr>(TYsonStringBuf(specYson));
    EXPECT_THROW_WITH_SUBSTRING(
        { ValidatePipelineSpec(spec); },
        "key column 0 has type");
}

TEST(TSpecTest, StateJoinerKeyTypeMismatchWithOverride)
{
    TStringBuf specYson(R""""(
        {
            computations = {
                source = {
                    computation_class_name = "NYT::NFlow::TNullComputation";
                    group_by_schema = [ { name = Key; type = uint64; required = %true; } ];
                };
                joiner = {
                    computation_class_name = "NYT::NFlow::TNullComputation";
                    group_by_schema = [ { name = Key; type = uint64; required = %true; } ];
                    state_joiners = {
                        "/from_source" = {
                            computation_id = source;
                            state_name = "/state";
                            join_on = {
                                key_schema_override = [ { name = Key; type = string; required = %true; } ];
                            };
                        };
                    };
                }
            };
        }
    )"""");
    auto spec = ConvertTo<TPipelineSpecPtr>(TYsonStringBuf(specYson));
    EXPECT_THROW_WITH_SUBSTRING(
        { ValidatePipelineSpec(spec); },
        "key column 0 has type");
}

TEST(TSpecTest, StateJoinerKeyTypesMatchNamesDiffer)
{
    TStringBuf specYson(R""""(
        {
            computations = {
                source = {
                    computation_class_name = "NYT::NFlow::TNullComputation";
                    group_by_schema = [ { name = SourceKey; type = uint64; required = %true; } ];
                };
                joiner = {
                    computation_class_name = "NYT::NFlow::TNullComputation";
                    group_by_schema = [ { name = JoinerKey; type = uint64; required = %true; } ];
                    state_joiners = {
                        "/from_source" = {
                            computation_id = source;
                            state_name = "/state";
                        };
                    };
                }
            };
        }
    )"""");
    auto spec = ConvertTo<TPipelineSpecPtr>(TYsonStringBuf(specYson));
    ValidatePipelineSpec(spec);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TSpecYTPathOwnershipTest, TwoExclusiveWritersSamePathConflict)
{
    TStringBuf specYson(R""""(
        {
            computations = {
                c = {
                    computation_class_name = "NYT::NFlow::TNullComputation";
                    group_by_schema = [];
                    external_state_managers = {
                        "/m1" = {
                            external_state_manager_class_name = "NYT::NFlow::TWritingExternalStateManager";
                            parameters = { path = "<cluster=primary>//tmp/state"; };
                        };
                        "/m2" = {
                            external_state_manager_class_name = "NYT::NFlow::TWritingExternalStateManager";
                            parameters = { path = "<cluster=primary>//tmp/state"; };
                        };
                    };
                };
            };
        }
    )"""");
    auto spec = ConvertTo<TPipelineSpecPtr>(TYsonStringBuf(specYson));
    EXPECT_THROW_WITH_SUBSTRING(
        { ValidatePipelineSpec(spec); },
        "claimed for writing");
}

TEST(TSpecYTPathOwnershipTest, ExclusiveWriteDifferentPathsOk)
{
    TStringBuf specYson(R""""(
        {
            computations = {
                c = {
                    computation_class_name = "NYT::NFlow::TNullComputation";
                    group_by_schema = [];
                    external_state_managers = {
                        "/m1" = {
                            external_state_manager_class_name = "NYT::NFlow::TWritingExternalStateManager";
                            parameters = { path = "<cluster=primary>//tmp/state1"; };
                        };
                        "/m2" = {
                            external_state_manager_class_name = "NYT::NFlow::TWritingExternalStateManager";
                            parameters = { path = "<cluster=primary>//tmp/state2"; };
                        };
                    };
                };
            };
        }
    )"""");
    auto spec = ConvertTo<TPipelineSpecPtr>(TYsonStringBuf(specYson));
    ValidatePipelineSpec(spec);
}

TEST(TSpecYTPathOwnershipTest, ExclusiveWriteDifferentClustersOk)
{
    TStringBuf specYson(R""""(
        {
            computations = {
                c = {
                    computation_class_name = "NYT::NFlow::TNullComputation";
                    group_by_schema = [];
                    external_state_managers = {
                        "/m1" = {
                            external_state_manager_class_name = "NYT::NFlow::TWritingExternalStateManager";
                            parameters = { path = "<cluster=primary>//tmp/state"; };
                        };
                        "/m2" = {
                            external_state_manager_class_name = "NYT::NFlow::TWritingExternalStateManager";
                            parameters = { path = "<cluster=secondary>//tmp/state"; };
                        };
                    };
                };
            };
        }
    )"""");
    auto spec = ConvertTo<TPipelineSpecPtr>(TYsonStringBuf(specYson));
    ValidatePipelineSpec(spec);
}

TEST(TSpecYTPathOwnershipTest, ReadOnlyPlusExclusiveWriteOk)
{
    TStringBuf specYson(R""""(
        {
            computations = {
                c = {
                    computation_class_name = "NYT::NFlow::TNullComputation";
                    group_by_schema = [];
                    external_state_managers = {
                        "/m" = {
                            external_state_manager_class_name = "NYT::NFlow::TWritingExternalStateManager";
                            parameters = { path = "<cluster=primary>//tmp/state"; };
                        };
                    };
                    external_state_joiners = {
                        "/j" = {
                            external_state_joiner_class_name = "NYT::NFlow::TReadingExternalStateJoiner";
                            parameters = { path = "<cluster=primary>//tmp/state"; };
                        };
                    };
                };
            };
        }
    )"""");
    auto spec = ConvertTo<TPipelineSpecPtr>(TYsonStringBuf(specYson));
    ValidatePipelineSpec(spec);
}

TEST(TSpecYTPathOwnershipTest, ExclusiveWritePlusSharedWriteConflict)
{
    TStringBuf specYson(R""""(
        {
            computations = {
                c = {
                    computation_class_name = "NYT::NFlow::TNullComputation";
                    group_by_schema = [];
                    external_state_managers = {
                        "/m1" = {
                            external_state_manager_class_name = "NYT::NFlow::TWritingExternalStateManager";
                            parameters = { path = "<cluster=primary>//tmp/state"; };
                        };
                        "/m2" = {
                            external_state_manager_class_name = "NYT::NFlow::TSharedWriterExternalStateManager";
                            parameters = { path = "<cluster=primary>//tmp/state"; };
                        };
                    };
                };
            };
        }
    )"""");
    auto spec = ConvertTo<TPipelineSpecPtr>(TYsonStringBuf(specYson));
    EXPECT_THROW_WITH_SUBSTRING(
        { ValidatePipelineSpec(spec); },
        "claimed for writing");
}

TEST(TSpecYTPathOwnershipTest, SharedWriteTwiceOk)
{
    TStringBuf specYson(R""""(
        {
            computations = {
                c = {
                    computation_class_name = "NYT::NFlow::TNullComputation";
                    group_by_schema = [];
                    external_state_managers = {
                        "/m1" = {
                            external_state_manager_class_name = "NYT::NFlow::TSharedWriterExternalStateManager";
                            parameters = { path = "<cluster=primary>//tmp/state"; };
                        };
                        "/m2" = {
                            external_state_manager_class_name = "NYT::NFlow::TSharedWriterExternalStateManager";
                            parameters = { path = "<cluster=primary>//tmp/state"; };
                        };
                    };
                };
            };
        }
    )"""");
    auto spec = ConvertTo<TPipelineSpecPtr>(TYsonStringBuf(specYson));
    ValidatePipelineSpec(spec);
}

TEST(TSpecYTPathOwnershipTest, OpaqueManagerNotCheckedOk)
{
    TStringBuf specYson(R""""(
        {
            computations = {
                c = {
                    computation_class_name = "NYT::NFlow::TNullComputation";
                    group_by_schema = [];
                    external_state_managers = {
                        "/m1" = {
                            external_state_manager_class_name = "NYT::NFlow::TOpaqueExternalStateManager";
                            parameters = { table = "<cluster=primary>//tmp/state"; };
                        };
                        "/m2" = {
                            external_state_manager_class_name = "NYT::NFlow::TOpaqueExternalStateManager";
                            parameters = { table = "<cluster=primary>//tmp/state"; };
                        };
                    };
                };
            };
        }
    )"""");
    auto spec = ConvertTo<TPipelineSpecPtr>(TYsonStringBuf(specYson));
    ValidatePipelineSpec(spec);
}

TEST(TSpecYTPathOwnershipTest, PathNormalizationConflict)
{
    TStringBuf specYson(R""""(
        {
            computations = {
                c = {
                    computation_class_name = "NYT::NFlow::TNullComputation";
                    group_by_schema = [];
                    external_state_managers = {
                        "/m1" = {
                            external_state_manager_class_name = "NYT::NFlow::TWritingExternalStateManager";
                            parameters = { path = "<cluster=primary>//tmp/state"; };
                        };
                        "/m2" = {
                            external_state_manager_class_name = "NYT::NFlow::TWritingExternalStateManager";
                            parameters = { path = "<cluster=primary;ranges=[]>//tmp/state"; };
                        };
                    };
                };
            };
        }
    )"""");
    auto spec = ConvertTo<TPipelineSpecPtr>(TYsonStringBuf(specYson));
    EXPECT_THROW_WITH_SUBSTRING(
        { ValidatePipelineSpec(spec); },
        "claimed for writing");
}

TEST(TSpecYTPathOwnershipTest, UnsetEmbeddedExclusiveWritePathsOk)
{
    TStringBuf specYson(R""""(
        {
            computations = {
                c1 = {
                    computation_class_name = "NYT::NFlow::TNullComputation";
                    group_by_schema = [];
                    external_state_managers = {
                        "/m" = {
                            external_state_manager_class_name = "NYT::NFlow::TEmbeddedWriterExternalStateManager";
                            parameters = {};
                        };
                    };
                };
                c2 = {
                    computation_class_name = "NYT::NFlow::TNullComputation";
                    group_by_schema = [];
                    external_state_managers = {
                        "/m" = {
                            external_state_manager_class_name = "NYT::NFlow::TEmbeddedWriterExternalStateManager";
                            parameters = {};
                        };
                    };
                };
            };
        }
    )"""");
    auto spec = ConvertTo<TPipelineSpecPtr>(TYsonStringBuf(specYson));
    ValidatePipelineSpec(spec);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow

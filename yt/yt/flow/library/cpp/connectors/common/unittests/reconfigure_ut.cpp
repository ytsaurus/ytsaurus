#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/connectors/common/sink_base.h>
#include <yt/yt/flow/library/cpp/connectors/common/sink_controller_base.h>
#include <yt/yt/flow/library/cpp/connectors/common/source_base.h>
#include <yt/yt/flow/library/cpp/connectors/common/source_controller_base.h>

#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NFlow {
namespace {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

struct TRetentionSourceDynamicParameters
    : public ISource::TDynamicParameters
{
    int Level = 1;

    REGISTER_YSON_STRUCT(TRetentionSourceDynamicParameters);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("level", &TThis::Level)
            .Default(1)
            .GreaterThan(0);
    }
};

DEFINE_REFCOUNTED_TYPE(TRetentionSourceDynamicParameters);

struct TRetentionPartitionSpec
    : public ISource::TDynamicPartitionSpec
{
    int Limit = 1;

    REGISTER_YSON_STRUCT(TRetentionPartitionSpec);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("limit", &TThis::Limit)
            .Default(1)
            .GreaterThan(0);
    }
};

DEFINE_REFCOUNTED_TYPE(TRetentionPartitionSpec);

struct TRetentionSinkDynamicParameters
    : public ISink::TDynamicParameters
{
    int Level = 1;

    REGISTER_YSON_STRUCT(TRetentionSinkDynamicParameters);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("level", &TThis::Level)
            .Default(1)
            .GreaterThan(0);
    }
};

DEFINE_REFCOUNTED_TYPE(TRetentionSinkDynamicParameters);

////////////////////////////////////////////////////////////////////////////////

class TRetentionSourceController
    : public TSourceControllerBase
{
public:
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TRetentionSourceDynamicParameters);

    using TSourceControllerBase::TSourceControllerBase;

    std::optional<THashMap<TKey, IMapNodePtr>> ListKeys() override
    {
        return {};
    }
};

class TRetentionSource
    : public TSourceBase
{
public:
    using TSourceController = TRetentionSourceController;

    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TRetentionSourceDynamicParameters);
    YT_FLOW_EXTEND_DYNAMIC_PARTITION_SPEC(TRetentionPartitionSpec);

    using TSourceBase::TSourceBase;

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
        YT_UNIMPLEMENTED();
    }

    void MarkPublished(const TSourceMessageBatchCookie& /*cookie*/) override
    { }

    void MarkPersisted(const TSourceMessageBatchCookie& /*cookie*/) override
    { }

    TInflightStreamTraverseDataPtr BuildInflight() override
    {
        YT_UNIMPLEMENTED();
    }

    std::optional<TSystemTimestamp> GetPersistedEventWatermark() override
    {
        return std::nullopt;
    }

    std::optional<TSystemTimestamp> GetReadEventWatermark() override
    {
        return std::nullopt;
    }
};

YT_FLOW_DEFINE_SOURCE(TRetentionSource);

class TRetentionSinkController
    : public TSinkControllerBase
{
public:
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TRetentionSinkDynamicParameters);

    using TSinkControllerBase::TSinkControllerBase;

    std::optional<i64> GetReceiverChannelCount() override
    {
        return 1;
    }
};

class TRetentionSink
    : public TSinkBase
{
public:
    using TSinkController = TRetentionSinkController;

    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TRetentionSinkDynamicParameters);

    using TSinkBase::TSinkBase;

    void Init(IInitContextPtr /*initContext*/) override
    { }

    void Distribute(const TOutputMessageConstPtr& /*message*/, TOnDistributedCallback /*onDistributed*/) override
    {
        YT_UNIMPLEMENTED();
    }

    void Sync(NApi::IDynamicTableTransactionPtr /*transaction*/) override
    { }

    void Commit() override
    { }
};

YT_FLOW_DEFINE_SINK(TRetentionSink);

////////////////////////////////////////////////////////////////////////////////

IMapNodePtr MakeLevelParameters(int level)
{
    return BuildYsonNodeFluently()
        .BeginMap()
        .Item("level")
        .Value(level)
        .EndMap()
        ->AsMap();
}

IMapNodePtr MakeLimitPartitionSpec(int limit)
{
    return BuildYsonNodeFluently()
        .BeginMap()
        .Item("limit")
        .Value(limit)
        .EndMap()
        ->AsMap();
}

TDynamicSourceSpecPtr MakeDynamicSourceSpec(int level)
{
    auto spec = New<TDynamicSourceSpec>();
    spec->Parameters = MakeLevelParameters(level);
    return spec;
}

TDynamicSinkSpecPtr MakeDynamicSinkSpec(int level)
{
    auto spec = New<TDynamicSinkSpec>();
    spec->Parameters = MakeLevelParameters(level);
    return spec;
}

TSourceSpecPtr MakeSourceSpec()
{
    auto spec = New<TSourceSpec>();
    spec->SourceClassName = TypeName<TRetentionSource>();
    return spec;
}

TSinkSpecPtr MakeSinkSpec()
{
    auto spec = New<TSinkSpec>();
    spec->SinkClassName = TypeName<TRetentionSink>();
    return spec;
}

TDynamicSourceContextPtr MakeDynamicSourceContext(int level, int limit)
{
    auto context = New<TDynamicSourceContext>();
    context->DynamicSourceSpec = MakeDynamicSourceSpec(level);
    context->DynamicPartitionSpec = MakeLimitPartitionSpec(limit);
    return context;
}

TDynamicSourceControllerContextPtr MakeDynamicSourceControllerContext(int level)
{
    auto context = New<TDynamicSourceControllerContext>();
    context->DynamicSourceSpec = MakeDynamicSourceSpec(level);
    return context;
}

TDynamicSinkContextPtr MakeDynamicSinkContext(int level)
{
    auto context = New<TDynamicSinkContext>();
    context->DynamicSinkSpec = MakeDynamicSinkSpec(level);
    return context;
}

TDynamicSinkControllerContextPtr MakeDynamicSinkControllerContext(int level)
{
    auto context = New<TDynamicSinkControllerContext>();
    context->DynamicSinkSpec = MakeDynamicSinkSpec(level);
    return context;
}

////////////////////////////////////////////////////////////////////////////////

TEST(TCommonBaseReconfigureTest, SourceRetainsStateWhenPartitionSpecIsRejected)
{
    auto context = New<TSourceContext>();
    context->SourceSpec = MakeSourceSpec();
    auto source = New<TRetentionSource>(context, MakeDynamicSourceContext(1, 1));

    auto acceptedContext = MakeDynamicSourceContext(2, 3);
    source->Reconfigure(acceptedContext);

    EXPECT_THROW(source->Reconfigure(MakeDynamicSourceContext(4, 0)), std::exception);

    EXPECT_EQ(source->GetDynamicContext(), acceptedContext);
    EXPECT_EQ(source->GetDynamicParameters()->Level, 2);
    EXPECT_EQ(source->GetDynamicPartitionSpec()->Limit, 3);

    auto nextContext = MakeDynamicSourceContext(5, 6);
    source->Reconfigure(nextContext);
    EXPECT_EQ(source->GetDynamicContext(), nextContext);
    EXPECT_EQ(source->GetDynamicParameters()->Level, 5);
    EXPECT_EQ(source->GetDynamicPartitionSpec()->Limit, 6);
}

TEST(TCommonBaseReconfigureTest, SourceControllerRetainsStateWhenParametersAreRejected)
{
    auto context = New<TSourceControllerContext>();
    context->SourceSpec = MakeSourceSpec();
    auto controller = New<TRetentionSourceController>(context, MakeDynamicSourceControllerContext(1));

    auto acceptedContext = MakeDynamicSourceControllerContext(2);
    controller->Reconfigure(acceptedContext);

    EXPECT_THROW(controller->Reconfigure(MakeDynamicSourceControllerContext(0)), std::exception);

    EXPECT_EQ(controller->GetDynamicContext(), acceptedContext);
    EXPECT_EQ(controller->GetDynamicParameters()->Level, 2);

    auto nextContext = MakeDynamicSourceControllerContext(5);
    controller->Reconfigure(nextContext);
    EXPECT_EQ(controller->GetDynamicContext(), nextContext);
    EXPECT_EQ(controller->GetDynamicParameters()->Level, 5);
}

TEST(TCommonBaseReconfigureTest, SinkRetainsStateWhenParametersAreRejected)
{
    auto context = New<TSinkContext>();
    context->SinkSpec = MakeSinkSpec();
    auto sink = New<TRetentionSink>(context, MakeDynamicSinkContext(1));

    auto acceptedContext = MakeDynamicSinkContext(2);
    sink->Reconfigure(acceptedContext);

    EXPECT_THROW(sink->Reconfigure(MakeDynamicSinkContext(0)), std::exception);

    EXPECT_EQ(sink->GetDynamicContext(), acceptedContext);
    EXPECT_EQ(sink->GetDynamicParameters()->Level, 2);

    auto nextContext = MakeDynamicSinkContext(5);
    sink->Reconfigure(nextContext);
    EXPECT_EQ(sink->GetDynamicContext(), nextContext);
    EXPECT_EQ(sink->GetDynamicParameters()->Level, 5);
}

TEST(TCommonBaseReconfigureTest, SinkControllerRetainsStateWhenParametersAreRejected)
{
    auto context = New<TSinkControllerContext>();
    context->SinkSpec = MakeSinkSpec();
    auto controller = New<TRetentionSinkController>(context, MakeDynamicSinkControllerContext(1));

    auto acceptedContext = MakeDynamicSinkControllerContext(2);
    controller->Reconfigure(acceptedContext);

    EXPECT_THROW(controller->Reconfigure(MakeDynamicSinkControllerContext(0)), std::exception);

    EXPECT_EQ(controller->GetDynamicContext(), acceptedContext);
    EXPECT_EQ(controller->GetDynamicParameters()->Level, 2);

    auto nextContext = MakeDynamicSinkControllerContext(5);
    controller->Reconfigure(nextContext);
    EXPECT_EQ(controller->GetDynamicContext(), nextContext);
    EXPECT_EQ(controller->GetDynamicParameters()->Level, 5);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow

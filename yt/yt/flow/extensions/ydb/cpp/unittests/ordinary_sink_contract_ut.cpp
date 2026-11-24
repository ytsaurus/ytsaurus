#include "ordinary_sink_contract_ut.h"

#include <yt/yt/flow/extensions/ydb/cpp/attributes.h>
#include <yt/yt/flow/extensions/ydb/cpp/sink.h>
#include <yt/yt/flow/extensions/ydb/cpp/state.h>
#include <yt/yt/flow/extensions/ydb/cpp/writer.h>
#include <yt/yt/flow/extensions/ydb/cpp/ydb_topic_driver.h>

namespace NYT::NFlow::NOrdinarySinkContract {
namespace {

struct TPublicShell
{
    using TPlainSink = TYdbSink;
    using TFramingSink = TYdbFramingSink;
    using TAtLeastOnceSink = TYdbAtLeastOnceSink;
    using TAtLeastOnceFramingSink = TYdbAtLeastOnceFramingSink;
    using THeartbeatState = TYdbHeartbeatState;

    static inline const std::string ExpectedTopicPath = "topic";

    static TSinkSpecShape MakeSinkShape(const std::string& sinkClassName, const std::string& endpoint)
    {
        return {
            .SinkClassName = sinkClassName,
            .ResourceClassName = TypeName<TYdbTopicDriver>(),
            .ConnectionParameters = {
                {"endpoint", NYTree::ConvertToNode(endpoint)},
                {"database", NYTree::ConvertToNode("/Root")},
                {"topic", NYTree::ConvertToNode(ExpectedTopicPath)},
            },
        };
    }

    static bool IsReservedAttributeName(const std::string& name)
    {
        return IsReservedYdbTopicAttributeName(name);
    }

    static void ValidateAttributes(const TYdbTopicAttributes& attributes)
    {
        ValidateYdbTopicAttributes(attributes);
    }

    static NYson::TYsonString PackAttributes(const TYdbTopicAttributes& attributes)
    {
        return PackYdbTopicAttributes(attributes);
    }

    static TYdbTopicAttributes UnpackAttributes(const NYson::TYsonString& value)
    {
        return UnpackYdbTopicAttributes(value);
    }

    static IYdbWriterPtr CreateWriter(
        IResourcePtr resource,
        std::shared_ptr<NYdb::NTopic::TTopicClient> topicClient,
        NYdb::NTopic::TWriteSessionSettings settings,
        IInvokerPtr invoker,
        NLogging::TLogger logger,
        IStatusProfilerPtr statusProfiler)
    {
        return CreateYdbWriter(
            std::move(resource),
            std::move(topicClient),
            std::move(settings),
            std::move(invoker),
            std::move(logger),
            std::move(statusProfiler));
    }
};

INSTANTIATE_TYPED_TEST_SUITE_P(Public, TOrdinarySinkContractTest, TPublicShell);
INSTANTIATE_TYPED_TEST_SUITE_P(Public, TSinkHeartbeatContractTest, TPublicShell);

} // namespace
} // namespace NYT::NFlow::NOrdinarySinkContract

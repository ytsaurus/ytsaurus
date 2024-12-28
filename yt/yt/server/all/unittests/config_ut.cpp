#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/controller_agent/config.h>
#include <yt/yt/server/http_proxy/config.h>
#include <yt/yt/server/lib/scheduler/config.h>
#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/rpc_proxy/config.h>

#include <yt/yt/ytlib/scheduler/config.h>

#include <yt/yt/core/ytree/convert.h>

#include <library/cpp/testing/common/env.h>

#include <library/cpp/resource/resource.h>

#include <util/folder/path.h>

namespace NYT {
namespace {

using namespace NYT::NYTree;
using namespace NYT::NYson;

////////////////////////////////////////////////////////////////////////////////

TYsonString ReadConfigStr(TStringBuf fileName)
{
    return TYsonString(NResource::Find(TString("/configs/") + fileName));
}

template <typename T>
TIntrusivePtr<T> ConvertToViaPullParser(TYsonStringBuf yson)
{
    TMemoryInput input(yson.AsStringBuf());
    TYsonPullParser parser(&input, EYsonType::Node);
    TYsonPullParserCursor cursor(&parser);
    auto res = New<T>();
    res->Load(&cursor);
    return res;
}

template <typename T>
TIntrusivePtr<T> ConvertToViaNode(TYsonStringBuf yson)
{
    auto node = ConvertTo<INodePtr>(yson);
    auto res = New<T>();
    res->Load(node);
    return res;
}

template <typename T>
void TestConfigParsing(TStringBuf fileName)
{
    auto configStr = ReadConfigStr(fileName);
    auto configNode = ConvertToViaNode<T>(configStr);
    auto configPullParser = ConvertToViaPullParser<T>(configStr);

    ASSERT_TRUE(AreNodesEqual(
        ConvertTo<INodePtr>(configPullParser),
        ConvertTo<INodePtr>(configNode)));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TConfigParsingTest, ServerConfig)
{
    {
        SCOPED_TRACE("TCellMasterProgramConfig");
        TestConfigParsing<NCellMaster::TCellMasterProgramConfig>("master.yson");
    }
    {
        SCOPED_TRACE("TSchedulerProgramConfig");
        TestConfigParsing<NScheduler::TSchedulerProgramConfig>("scheduler.yson");
    }
    {
        SCOPED_TRACE("TControllerAgentConfig");
        TestConfigParsing<NControllerAgent::TControllerAgentConfig>("controller-agent.yson");
    }
    {
        SCOPED_TRACE("THttpProxyProgramConfig");
        TestConfigParsing<NHttpProxy::TProxyProgramConfig>("http-proxy.yson");
    }
    {
        SCOPED_TRACE("TRpcProxyConfig");
        TestConfigParsing<NRpcProxy::TProxyProgramConfig>("rpc-proxy.yson");
    }
    {
        SCOPED_TRACE("TClusterNodeProgramConfig");
        TestConfigParsing<NClusterNode::TClusterNodeProgramConfig>("node.yson");
    }
}

TEST(TConfigParsingTest, OperationSpec)
{
    {
        SCOPED_TRACE("TMapOperationSpec");
        TestConfigParsing<NScheduler::TMapOperationSpec>("map_spec.yson");
    }
    {
        SCOPED_TRACE("TReduceOperationSpec");
        TestConfigParsing<NScheduler::TReduceOperationSpec>("reduce_spec.yson");
    }
    {
        SCOPED_TRACE("TMapReduceOperationSpec");
        TestConfigParsing<NScheduler::TMapReduceOperationSpec>("map_reduce_spec.yson");
    }
    {
        SCOPED_TRACE("TVanillaOperationSpec");
        TestConfigParsing<NScheduler::TVanillaOperationSpec>("vanilla_spec.yson");
    }
    {
        SCOPED_TRACE("TSortOperationSpec");
        TestConfigParsing<NScheduler::TSortOperationSpec>("sort_spec.yson");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT

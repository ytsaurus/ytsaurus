#include "client_mock.h"
#include "transaction_mock.h"

#include <yt/client/misc/discovery.h>

#include <yt/core/concurrency/action_queue.h>

#include <yt/core/test_framework/framework.h>

#include <yt/core/ytree/fluent.h>

namespace NYT {
namespace {

using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::InvokeWithoutArgs;
using ::testing::StrictMock;
using ::testing::NiceMock;
using ::testing::Return;
using ::testing::ResultOf;
using ::testing::_;

using namespace NApi;
using namespace NYTree;
using namespace NYson;
using namespace NConcurrency;
using namespace NLogging;

using TStrictMockClient = StrictMock<TMockClient>;
DEFINE_REFCOUNTED_TYPE(TStrictMockClient)

using TStrictMockTransaction = StrictMock<TMockTransaction>;
DEFINE_REFCOUNTED_TYPE(TStrictMockTransaction)

////////////////////////////////////////////////////////////////////////////////

std::vector<TString> GetNames(const THashMap<TString, TAttributeMap>& listResult)
{
    auto result = GetKeys(listResult);
    std::sort(result.begin(), result.end());
    return result;
}

TEST(TDiscoveryTest, Simple)
{
    auto MockClient = New<TStrictMockClient>();

    NYPath::TYPath path = "/test/1234";
    std::vector<TString> keys = {};
    NApi::TListNodeOptions options;
    options.Attributes = keys;
    options.Attributes->push_back("lock_count");

    NYson::TYsonString listRet("[<locks=[{child_key=tmp}]>dead_node;<locks=[{child_key=lock}]>alive_node;]");

    EXPECT_CALL(*MockClient, ListNode(path, _))
        .WillRepeatedly(Return(MakeFuture(listRet)));

    TDiscoveryConfigPtr config = New<TDiscoveryConfig>(path);
    config->UpdatePeriod = TDuration::Seconds(1);
    auto discovery = New<TDiscovery>(config, MockClient, GetCurrentInvoker(), keys, TLogger("Test"));
    WaitFor(discovery->StartPolling())
        .ThrowOnError();

    std::vector<TString> expected = {"alive_node"};
    EXPECT_THAT(discovery->List(), ResultOf(GetNames, expected));

    WaitFor(discovery->StopPolling())
        .ThrowOnError();
}

TYsonString GetLockYson(bool created, bool locked)
{
    return BuildYsonStringFluently()
            .BeginList()
                .DoIf(created, [&] (TFluentList fluent) {
                    fluent.Item()
                        .BeginAttributes()
                            .Item("locks")
                                .BeginList()
                                    .DoIf(locked, [&] (TFluentList fluent) {
                                        fluent.Item().BeginMap()
                                            .Item("child_key").Value("lock")
                                        .EndMap();
                                    })
                                .EndList()
                        .EndAttributes()
                        .Value("test_node");
                })
            .EndList();
}

TEST(TDiscoveryTest, Enter)
{
    auto MockClient = New<TStrictMockClient>();
    auto MockTransaction = New<TStrictMockTransaction>();

    NYPath::TYPath path = "/test/1234";
    std::vector<TString> keys = {};

    bool locked = false;
    bool created = false;

    EXPECT_CALL(*MockClient, ListNode(path, _))
        .WillRepeatedly(InvokeWithoutArgs([&] {
                return MakeFuture(GetLockYson(created, locked));
            }));

    EXPECT_CALL(*MockClient, StartTransaction(_, _))
        .WillOnce(Return(MakeFuture(MockTransaction).As<ITransactionPtr>()));

    EXPECT_CALL(*MockClient, CreateNode(path + "/test_node", _, _))
        .WillOnce(InvokeWithoutArgs([&] {
                created = true;
                return MakeFuture(NCypressClient::TNodeId());
            }
        ));

    EXPECT_CALL(*MockTransaction, LockNode(path + "/test_node", _, _))
        .WillOnce(InvokeWithoutArgs([&] {
                locked = true;
                return MakeFuture(TLockNodeResult());
            }));

    EXPECT_CALL(*MockTransaction, SubscribeAborted(_))
        .Times(1);

    EXPECT_CALL(*MockClient, SetNode(path + "/test_node/@expiration_time", _, _))
        .WillOnce(InvokeWithoutArgs([&] {
              return VoidFuture;
        }));

    TDiscoveryConfigPtr config = New<TDiscoveryConfig>(path);
    config->UpdatePeriod = TDuration::MilliSeconds(50);
    auto discovery = New<TDiscovery>(config, MockClient, GetCurrentInvoker(), keys, TLogger("Test"));
    WaitFor(discovery->StartPolling())
        .ThrowOnError();

    EXPECT_THAT(discovery->List(), ResultOf(GetNames, std::vector<TString>()));

    WaitFor(discovery->Enter("test_node", TAttributeMap()))
        .ThrowOnError();

    TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(100));
    std::vector<TString> expected = {"test_node"};
    EXPECT_THAT(discovery->List(), ResultOf(GetNames, expected));

    WaitFor(discovery->StopPolling())
        .ThrowOnError();
}

THashMap<TString, TString> TransformAttributes(TCreateNodeOptions options)
{
    THashMap<TString, TString> result;
    if (options.Attributes) {
        for (const auto& [name, value] : options.Attributes->ToMap()->GetChildren()) {
            result[name] = value->AsString()->GetValue();
        }
    }
    return result;
}

TEST(TDiscoveryTest, Leave) {
    auto MockClient = New<TStrictMockClient>();
    auto MockTransaction = New<TStrictMockTransaction>();

    NYPath::TYPath path = "/test/1234";
    std::vector<TString> keys = {};

    TAttributeMap attrs;
    attrs["host"] = BuildYsonNodeFluently().Value("something.ru");
    THashMap<TString, TString> comparableAttrs;
    comparableAttrs["host"] = "something.ru";

    bool locked = false;
    bool created = false;

    EXPECT_CALL(*MockClient, ListNode(path, _))
        .WillRepeatedly(InvokeWithoutArgs([&] {
                return MakeFuture(GetLockYson(created, locked));
            }));

    EXPECT_CALL(*MockClient, StartTransaction(_, _))
        .WillOnce(Return(MakeFuture((ITransactionPtr)MockTransaction)));

    EXPECT_CALL(*MockClient, CreateNode(path + "/test_node", _, ResultOf(TransformAttributes, comparableAttrs)))
        .WillOnce(InvokeWithoutArgs([&] {
                created = true;
                return MakeFuture(NCypressClient::TNodeId());
            }
        ));

    EXPECT_CALL(*MockTransaction, LockNode(path + "/test_node", _, _))
        .WillOnce(InvokeWithoutArgs([&] {
                locked = true;
                return MakeFuture(TLockNodeResult());
            }));

    EXPECT_CALL(*MockClient, SetNode(path + "/test_node/@expiration_time", _, _))
        .WillOnce(InvokeWithoutArgs([&] {
              return VoidFuture;
        }));

    EXPECT_CALL(*MockTransaction, Abort(_))
        .WillOnce(InvokeWithoutArgs([&] {
                locked = false;
                return VoidFuture;
            }));

    EXPECT_CALL(*MockTransaction, SubscribeAborted(_))
        .Times(1);

    EXPECT_CALL(*MockTransaction, UnsubscribeAborted(_))
        .Times(1);

    TDiscoveryConfigPtr config = New<TDiscoveryConfig>(path);
    config->UpdatePeriod = TDuration::MilliSeconds(50);
    auto discovery = New<TDiscovery>(config, MockClient, GetCurrentInvoker(), keys, TLogger("Test"));
    WaitFor(discovery->StartPolling())
        .ThrowOnError();

    EXPECT_THAT(discovery->List(), ResultOf(GetNames, std::vector<TString>()));

    WaitFor(discovery->Enter("test_node", attrs))
        .ThrowOnError();
    WaitFor(discovery->Leave())
        .ThrowOnError();

    TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(100));

    EXPECT_THAT(discovery->List(), ResultOf(GetNames, std::vector<TString>()));
    EXPECT_THAT(created, true);
    EXPECT_THAT(locked, false);

    WaitFor(discovery->StopPolling())
        .ThrowOnError();
}

TEST(TDiscoveryTest, Ban)
{
    auto MockClient = New<TStrictMockClient>();
    auto MockTransaction = New<TStrictMockTransaction>();

    NYPath::TYPath path = "/test/1234";
    std::vector<TString> keys = {};

    EXPECT_CALL(*MockClient, ListNode(path, _))
        .WillRepeatedly(Return(MakeFuture(BuildYsonStringFluently()
            .BeginList()
                .Item().Value(TYsonString("<locks=[{}]>dead_node"))
                .Item().Value(TYsonString("<locks=[{child_key=lock}]>alive_node1"))
                .Item().Value(TYsonString("<locks=[{};{child_key=lock}]>alive_node2"))
            .EndList())));

    std::vector<TString> expected = {"alive_node1", "alive_node2"};

    TDiscoveryConfigPtr config = New<TDiscoveryConfig>(path);
    config->UpdatePeriod = TDuration::MilliSeconds(50);
    config->BanTimeout = TDuration::MilliSeconds(50);
    auto discovery = New<TDiscovery>(config, MockClient, GetCurrentInvoker(), keys, TLogger("Test"));
    WaitFor(discovery->StartPolling())
        .ThrowOnError();;

    EXPECT_THAT(discovery->List(), ResultOf(GetNames, expected));

    discovery->Ban("alive_node2");
    expected.pop_back();

    EXPECT_THAT(discovery->List(), ResultOf(GetNames, expected));

    TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(100));
    expected.push_back("alive_node2");
    EXPECT_THAT(discovery->List(), ResultOf(GetNames, expected));

    WaitFor(discovery->StopPolling())
        .ThrowOnError();
}

THashMap<TString, std::vector<TString>> GetAttributesKeys(THashMap<TString, TAttributeMap> listResult)
{
    THashMap<TString, std::vector<TString>> result;
    for (const auto& [name, attributes] : listResult) {
        result[name] = GetKeys(attributes);
        std::sort(result[name].begin(), result[name].end());
    }
    return result;
}

TEST(TDiscoveryTest, Attributes)
{
    auto MockClient = New<TStrictMockClient>();
    auto MockTransaction = New<TStrictMockTransaction>();

    NYPath::TYPath path = "/test/1234";
    std::vector<TString> keys = {"a1", "a2"};

    EXPECT_CALL(*MockClient, ListNode(path, _))
        .WillRepeatedly(Return(MakeFuture(BuildYsonStringFluently()
            .BeginList()
                .Item()
                    .BeginAttributes()
                        .Item("locks").Value(TYsonString("[{child_key=tmp}]"))
                    .EndAttributes()
                    .Value("dead_node")
                .Item()
                    .BeginAttributes()
                        .Item("locks").Value(TYsonString("[{child_key=lock}]"))
                        .Item("a1").Value(1)
                        .Item("a2").Value(2)
                    .EndAttributes()
                    .Value("alive_node1")
                .Item()
                    .BeginAttributes()
                        .Item("locks").Value(TYsonString("[{child_key=lock}]"))
                        .Item("a1").Value(1)
                        .Item("a2").Value(2)
                    .EndAttributes()
                    .Value("alive_node2")
            .EndList())));

    TDiscoveryConfigPtr config = New<TDiscoveryConfig>(path);
    config->UpdatePeriod = TDuration::MilliSeconds(50);
    auto discovery = New<TDiscovery>(config, MockClient, GetCurrentInvoker(), keys, TLogger("Test"));
    WaitFor(discovery->StartPolling())
        .ThrowOnError();

    THashMap<TString, std::vector<TString>> expected;
    expected["alive_node1"] = expected["alive_node2"] = {"a1", "a2", "locks"};

    EXPECT_THAT(discovery->List(), ResultOf(GetAttributesKeys, expected));

    WaitFor(discovery->StopPolling())
        .ThrowOnError();
}

TEST(TDiscoveryTest, CreationRace)
{
    auto MockClient = New<TStrictMockClient>();
    auto MockTransaction = New<TStrictMockTransaction>();

    NYPath::TYPath path = "/test/1234";
    std::vector<TString> keys = {};

    bool locked = false;
    bool created = false;

    auto allowLockResponse = NewPromise<void>();
    auto lockWait = allowLockResponse.ToFuture();

    EXPECT_CALL(*MockClient, ListNode(path, _))
        .WillRepeatedly(InvokeWithoutArgs([&] {
                return MakeFuture(GetLockYson(created, locked));
            }));

    EXPECT_CALL(*MockClient, StartTransaction(_, _))
        .WillOnce(Return(MakeFuture(MockTransaction).As<ITransactionPtr>()));

    EXPECT_CALL(*MockClient, CreateNode(path + "/test_node", _, _))
        .WillOnce(InvokeWithoutArgs([&] {
                created = true;
                return MakeFuture(NCypressClient::TNodeId());
            }
        ));

    EXPECT_CALL(*MockTransaction, LockNode(path + "/test_node", _, _))
        .WillOnce(InvokeWithoutArgs([&] {
                WaitFor(lockWait)
                    .ThrowOnError();
                locked = true;
                return MakeFuture(TLockNodeResult());
            }));

    EXPECT_CALL(*MockClient, SetNode(path + "/test_node/@expiration_time", _, _))
        .WillOnce(InvokeWithoutArgs([&] {
              return VoidFuture;
        }));


    EXPECT_CALL(*MockTransaction, SubscribeAborted(_))
        .Times(1);

    TDiscoveryConfigPtr config = New<TDiscoveryConfig>(path);
    config->Directory = path;
    config->UpdatePeriod = TDuration::MilliSeconds(50);

    TActionQueuePtr ActionQueue(New<TActionQueue>("TDiscovery"));

    auto discovery = New<TDiscovery>(config, MockClient, ActionQueue->GetInvoker(), keys, TLogger("Test"));
    WaitFor(discovery->StartPolling())
        .ThrowOnError();

    EXPECT_THAT(discovery->List(), ResultOf(GetNames, std::vector<TString>()));

    auto enterFuture = discovery->Enter("test_node", TAttributeMap());

    TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(50));

    std::vector<TString> expected = {"test_node"};
    EXPECT_THAT(discovery->List(), ResultOf(GetNames, expected));

    allowLockResponse.Set();

    WaitFor(discovery->StopPolling())
        .ThrowOnError();

    WaitFor(enterFuture)
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT

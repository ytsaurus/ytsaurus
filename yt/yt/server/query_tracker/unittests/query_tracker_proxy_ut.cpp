#include <yt/yt/server/query_tracker/query_tracker_proxy.h>
#include <yt/yt/server/query_tracker/config.h>

#include <yt/yt/client/transaction_client/noop_timestamp_provider.h>

#include <yt/yt/client/unittests/mock/client.h>
#include <yt/yt/client/unittests/mock/transaction.h>

#include <yt/yt/ytlib/query_tracker_client/records/query.record.h>

namespace NYT::NQueryTracker {
namespace {

using namespace NApi;
using namespace NQueryTracker;
using namespace NQueryTrackerClient::NRecords;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NYson;
using namespace NYPath;
using namespace NYTree;

using ::testing::StrictMock;
using ::testing::Return;
using ::testing::_;

using TStrictMockClient = StrictMock<TMockClient>;
DEFINE_REFCOUNTED_TYPE(TStrictMockClient)

using TStrictMockTransaction = StrictMock<TMockTransaction>;
DEFINE_REFCOUNTED_TYPE(TStrictMockTransaction)

const TString StateRoot = "//sys/query_tracker";
const TQueryTrackerProxyConfigPtr Config = New<TQueryTrackerProxyConfig>();

const TYsonString EmptyMap = TYsonString(TString("{}"));
const TYsonString EmptyList = TYsonString(TString("[]"));

////////////////////////////////////////////////////////////////////////////////

template<typename TResult, typename TRecordDescriptor, typename TRecord>
TFuture<TResult> MakeQueryResult(
    const std::vector<TRecord>& records = {},
    bool isPartialRecord = false)
{
    auto rowBuffer = New<TRowBuffer>();

    std::vector<TUnversionedRow> rows;
    rows.reserve(records.size());
    for (const auto& record : records) {
        rows.push_back(record.ToUnversionedRow(
            rowBuffer,
            TRecordDescriptor::Get()->GetIdMapping()));
    };

    auto schema = TRecordDescriptor::Get()->GetSchema();
    if (isPartialRecord) {
        schema = schema->ToKeys();
    }

    auto rowset = CreateRowset(schema, MakeSharedRange(rows, std::move(rowBuffer)));

    return MakeFuture(TResult{
        .Rowset = rowset,
    });
}

TUnversionedLookupRowsResult MakeEmptyLookupActiveRowsResult(int rows)
{
    return {
        .Rowset = CreateRowset(TActiveQueryDescriptor::Get()->GetSchema(), MakeSharedRange(
            std::vector<TUnversionedRow>(rows, TUnversionedRow{}),
            New<TRowBuffer>())),
    };
}

template <typename TRecord>
TRecord CreateSimpleQuery(const TQueryId& queryId, ui64 startTime = 0, const std::vector<TString>& acos = {})
{
    return {
        .Key = {.QueryId = queryId},
        .Engine = EQueryEngine::Mock,
        .Query = "",
        .Files = EmptyList,
        .Settings = EmptyMap,
        .User = "user",
        .AccessControlObjects = ConvertToYsonString(acos),
        .StartTime = TInstant::FromValue(startTime),
        .State = EQueryState::Draft,
        .Progress = EmptyMap,
        .Annotations = EmptyMap,
        .Secrets = EmptyList,
    };
}

class TQueryTrackerProxyTest
    : public ::testing::Test
{
public:
    void SetUp() override
    {
        MockClient = New<TStrictMockClient>();
        MockTransaction = New<TStrictMockTransaction>();

        Proxy = CreateQueryTrackerProxy(MockClient, StateRoot, Config);

        EXPECT_CALL(*MockClient, StartTransaction(_, _))
            .WillOnce(Return(MakeFuture((ITransactionPtr)MockTransaction)));
    }

    void ExpectAcosExist(const std::vector<TString>& acos)
    {
        for (const auto& aco : acos) {
            EXPECT_CALL(*MockClient, NodeExists(TYPath(Format("//sys/access_control_object_namespaces/queries/%v", aco)), _))
                .WillOnce(Return(MakeFuture(true)));
        }
    }

public:
    TIntrusivePtr<TStrictMockClient> MockClient;
    TIntrusivePtr<TStrictMockTransaction> MockTransaction;
    TQueryTrackerProxyPtr Proxy;
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TQueryTrackerProxyTest, StartDraftQuery)
{
    EXPECT_CALL(*MockTransaction, ModifyRows(TYPath("//sys/query_tracker/finished_queries"), _, _, _));
    EXPECT_CALL(*MockTransaction, ModifyRows(TYPath("//sys/query_tracker/finished_queries_by_user_and_start_time"), _, _, _));
    EXPECT_CALL(*MockTransaction, ModifyRows(TYPath("//sys/query_tracker/finished_queries_by_aco_and_start_time"), _, _, _));
    EXPECT_CALL(*MockTransaction, ModifyRows(TYPath("//sys/query_tracker/finished_queries_by_start_time"), _, _, _));
    EXPECT_CALL(*MockTransaction, Commit(_)).WillOnce(Return(MakeFuture(TTransactionCommitResult{})));

    ExpectAcosExist({"aco1", "aco2"});

    auto queryId = TQueryId::Create();
    Proxy->StartQuery(
        queryId,
        EQueryEngine::Mock,
        "query",
        TStartQueryOptions {
            .Draft = true,
            .AccessControlObjects = std::vector<TString>{"aco1", "aco2"},
        },
        "user");
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TQueryTrackerProxyTest, AlterAnnotationAndAcoInFinishedQuery)
{
    auto queryId = TQueryId::Create();
    std::vector<TFinishedQuery> records{
        CreateSimpleQuery<TFinishedQuery>(queryId, 0, {"aco1", "aco2"}),
    };

    EXPECT_CALL(*MockClient, LookupRows(TYPath("//sys/query_tracker/active_queries"), _, _, _))
        .WillRepeatedly(Return(MakeFuture(MakeEmptyLookupActiveRowsResult(1))));
    EXPECT_CALL(*MockClient, LookupRows(TYPath("//sys/query_tracker/finished_queries"), _, _, _))
        .WillRepeatedly(Return(MakeQueryResult<TUnversionedLookupRowsResult, TFinishedQueryDescriptor>(records)));

    EXPECT_CALL(*MockTransaction, ModifyRows(TYPath("//sys/query_tracker/finished_queries"), _, _, _));
    EXPECT_CALL(*MockTransaction, ModifyRows(TYPath("//sys/query_tracker/finished_queries_by_start_time"), _, _, _));
    EXPECT_CALL(*MockTransaction, ModifyRows(TYPath("//sys/query_tracker/finished_queries_by_user_and_start_time"), _, _, _));
    // One DeleteRows and ModifyRows call is expected each. But in mockTransaction, DeleteRows is implemented through ModifyRows
    EXPECT_CALL(*MockTransaction, ModifyRows(TYPath("//sys/query_tracker/finished_queries_by_aco_and_start_time"), _, _, _)).Times(2);
    EXPECT_CALL(*MockTransaction, Commit(_)).WillOnce(Return(MakeFuture(TTransactionCommitResult{})));

    ExpectAcosExist({"aco2", "aco3"});

    Proxy->AlterQuery(
        queryId,
        TAlterQueryOptions {
            .Annotations = ConvertToNode(ConvertToYsonString(std::map<TString, TString>{{"qwe", "asd"}}))->AsMap(),
            .AccessControlObjects = std::vector<TString>{"aco2", "aco3"},
        },
        "user");
}

TEST_F(TQueryTrackerProxyTest,  AlterAnnotationInFinishedQuery)
{
    auto queryId = TQueryId::Create();
    std::vector<TFinishedQuery> records{
        CreateSimpleQuery<TFinishedQuery>(queryId, 0, {"aco"}),
    };

    EXPECT_CALL(*MockClient, LookupRows(TYPath("//sys/query_tracker/active_queries"), _, _, _))
        .WillRepeatedly(Return(MakeFuture(MakeEmptyLookupActiveRowsResult(1))));
    EXPECT_CALL(*MockClient, LookupRows(TYPath("//sys/query_tracker/finished_queries"), _, _, _))
        .WillRepeatedly(Return(MakeQueryResult<TUnversionedLookupRowsResult, TFinishedQueryDescriptor>(records)));

    EXPECT_CALL(*MockTransaction, ModifyRows(TYPath("//sys/query_tracker/finished_queries"), _, _, _));
    EXPECT_CALL(*MockTransaction, ModifyRows(TYPath("//sys/query_tracker/finished_queries_by_start_time"), _, _, _));
    EXPECT_CALL(*MockTransaction, ModifyRows(TYPath("//sys/query_tracker/finished_queries_by_user_and_start_time"), _, _, _));
    EXPECT_CALL(*MockTransaction, ModifyRows(TYPath("//sys/query_tracker/finished_queries_by_aco_and_start_time"), _, _, _));
    EXPECT_CALL(*MockTransaction, Commit(_)).WillOnce(Return(MakeFuture(TTransactionCommitResult{})));

    Proxy->AlterQuery(
        queryId,
        TAlterQueryOptions {
            .Annotations = ConvertToNode(ConvertToYsonString(std::map<TString, TString>{{"qwe", "asd"}}))->AsMap(),
        },
        "user");
}

TEST_F(TQueryTrackerProxyTest, AlterAcoInFinishedQuery)
{
    auto queryId = TQueryId::Create();
    std::vector<TFinishedQuery> records{
        CreateSimpleQuery<TFinishedQuery>(queryId, 0, {"aco1", "aco2"}),
    };

    EXPECT_CALL(*MockClient, LookupRows(TYPath("//sys/query_tracker/active_queries"), _, _, _))
        .WillRepeatedly(Return(MakeFuture(MakeEmptyLookupActiveRowsResult(1))));
    EXPECT_CALL(*MockClient, LookupRows(TYPath("//sys/query_tracker/finished_queries"), _, _, _))
        .WillRepeatedly(Return(MakeQueryResult<TUnversionedLookupRowsResult, TFinishedQueryDescriptor>(records)));

    EXPECT_CALL(*MockTransaction, ModifyRows(TYPath("//sys/query_tracker/finished_queries"), _, _, _));
    EXPECT_CALL(*MockTransaction, ModifyRows(TYPath("//sys/query_tracker/finished_queries_by_start_time"), _, _, _));
    // One DeleteRows and ModifyRows call is expected each. But in mockTransaction, DeleteRows is implemented through ModifyRows
    EXPECT_CALL(*MockTransaction, ModifyRows(TYPath("//sys/query_tracker/finished_queries_by_aco_and_start_time"), _, _, _)).Times(2);
    EXPECT_CALL(*MockTransaction, Commit(_)).WillOnce(Return(MakeFuture(TTransactionCommitResult{})));

    ExpectAcosExist({"aco2", "aco3"});

    Proxy->AlterQuery(
        queryId,
        TAlterQueryOptions {
            .AccessControlObjects = std::vector<TString>{"aco2", "aco3"},
        },
        "user");
}

////////////////////////////////////////////////////////////////////////////////

class TListQueryTrackerProxyTest
    : public ::testing::Test
{
public:
    void SetUp() override
    {
        MockClient = New<TStrictMockClient>();
        MockClient->SetTimestampProvider(CreateNoopTimestampProvider());
        Proxy = CreateQueryTrackerProxy(MockClient, StateRoot, Config);
    }

    void ExpectListQueriesGetAcoCalls(bool everyoneAcoExists, bool isSuperuser = false)
    {
        if (!isSuperuser) {
            auto existAcos = EmptyMap;
            if (everyoneAcoExists) {
                existAcos = BuildYsonStringFluently()
                    .BeginMap()
                        .Item("aco")
                        .BeginAttributes()
                            .Item("principal_acl")
                                .BeginList()
                                    .Item().BeginMap()
                                        .Item("action").Value("allow")
                                        .Item("subjects").BeginList().Item().Value("everyone").EndList()
                                        .Item("permissions").BeginList().Item().Value("use").EndList()
                                    .EndMap()
                                .EndList()
                        .EndAttributes()
                        .BeginMap()
                        .EndMap()
                    .EndMap();
            }

            EXPECT_CALL(*MockClient, GetNode(TYPath("//sys/access_control_object_namespaces/queries"), _))
                .WillOnce(Return(MakeFuture(existAcos)));
        }

        std::vector<TString> memberOfClosure({"users", "everyone"});
        if (isSuperuser) {
            memberOfClosure.push_back(NSecurityClient::SuperusersGroupName);
        }
        EXPECT_CALL(*MockClient, GetNode(TYPath("//sys/users/user/@member_of_closure"), _))
            .WillOnce(Return(MakeFuture(ConvertToYsonString(memberOfClosure))));
    }

public:
    TIntrusivePtr<TStrictMockClient> MockClient;
    TQueryTrackerProxyPtr Proxy;
};

TEST_F(TListQueryTrackerProxyTest, ListQueries)
{
    ExpectListQueriesGetAcoCalls(/*everyoneAcoExists*/ true);

    TQueryId queryId[6];
    i64 startTime[6];
    for (int i = 0; i < 6; i++) {
        queryId[i] = TQueryId::Create();
        startTime[i] = i;
    }
    std::vector<TFinishedQueryByUserAndStartTimePartial> finishedQueryByUserAndStartTimeRecords = {
        { .Key = { .MinusStartTime = -startTime[4], .QueryId = queryId[4] } },
        { .Key = { .MinusStartTime = -startTime[1], .QueryId = queryId[1] } },
    };
    std::vector<TFinishedQueryByAcoAndStartTimePartial> finishedQueryByAcoAndStartTimeRecords = {
        { .Key = { .MinusStartTime = -startTime[5], .QueryId = queryId[5] } },
        { .Key = { .MinusStartTime = -startTime[2], .QueryId = queryId[2] } },
    };
    std::vector<TFinishedQuery> finishedQueryRecords{
        CreateSimpleQuery<TFinishedQuery>(queryId[2], startTime[2]),
        CreateSimpleQuery<TFinishedQuery>(queryId[4], startTime[4]),
        CreateSimpleQuery<TFinishedQuery>(queryId[5], startTime[5]),
    };
    std::vector<TActiveQuery> activeQueryRecords{
        CreateSimpleQuery<TActiveQuery>(queryId[0], startTime[0]),
        CreateSimpleQuery<TActiveQuery>(queryId[3], startTime[3]),
    };

    EXPECT_CALL(*MockClient, SelectRows("([minus_start_time]), ([query_id]) FROM [//sys/query_tracker/finished_queries_by_user_and_start_time] WHERE (user = {User}) ORDER BY (minus_start_time) ASC LIMIT 4", _))
        .WillOnce(Return(MakeQueryResult<TSelectRowsResult, TFinishedQueryByUserAndStartTimeDescriptor>(finishedQueryByUserAndStartTimeRecords, /*isPartialRecord*/ true)));
    EXPECT_CALL(*MockClient, SelectRows("([minus_start_time]), ([query_id]) FROM [//sys/query_tracker/finished_queries_by_aco_and_start_time] WHERE (access_control_object IN {acosForUser}) GROUP BY (minus_start_time), (query_id) ORDER BY (minus_start_time) ASC LIMIT 4", _))
        .WillOnce(Return(MakeQueryResult<TSelectRowsResult, TFinishedQueryByAcoAndStartTimeDescriptor>(finishedQueryByAcoAndStartTimeRecords, /*isPartialRecord*/ true)));
    EXPECT_CALL(*MockClient, SelectRows(std::string(Format("* FROM [//sys/query_tracker/finished_queries] WHERE ([query_id] in (\"%v\", \"%v\", \"%v\"))", queryId[5], queryId[4], queryId[2])), _))
        .WillOnce(Return(MakeQueryResult<TSelectRowsResult, TFinishedQueryDescriptor>(finishedQueryRecords)));
    EXPECT_CALL(*MockClient, SelectRows("* FROM [//sys/query_tracker/active_queries] WHERE (user = {User} OR list_contains(access_control_objects, \"aco\"))", _))
        .WillOnce(Return(MakeQueryResult<TSelectRowsResult, TActiveQueryDescriptor, TActiveQuery>(activeQueryRecords)));

    auto result = Proxy->ListQueries(
        TListQueriesOptions{ .Limit = 3 },
        "user");

    ASSERT_EQ(result.Queries.size(), 3ul);
    ASSERT_EQ(result.Queries[0].Id, queryId[5]);
    ASSERT_EQ(result.Queries[1].Id, queryId[4]);
    ASSERT_EQ(result.Queries[2].Id, queryId[3]);
    ASSERT_TRUE(result.Incomplete);
}

TEST_F(TListQueryTrackerProxyTest, ListQueriesWithoutAco)
{
    ExpectListQueriesGetAcoCalls(/*everyoneAcoExists*/ false);

    TQueryId queryId[4];
    i64 startTime[4];
    for (int i = 0; i < 4; i++) {
        queryId[i] = TQueryId::Create();
        startTime[i] = i;
    }
    std::vector<TFinishedQueryByUserAndStartTimePartial> finishedQueryByUserAndStartTimeRecords = {
        { .Key = { .MinusStartTime = -startTime[3], .QueryId = queryId[3] } },
        { .Key = { .MinusStartTime = -startTime[1], .QueryId = queryId[1] } },
    };
    std::vector<TFinishedQuery> finishedQueryRecords{
        CreateSimpleQuery<TFinishedQuery>(queryId[1], startTime[1]),
        CreateSimpleQuery<TFinishedQuery>(queryId[3], startTime[3]),
    };
    std::vector<TActiveQuery> activeQueryRecords{
        CreateSimpleQuery<TActiveQuery>(queryId[0], startTime[0]),
        CreateSimpleQuery<TActiveQuery>(queryId[2], startTime[2]),
    };

    EXPECT_CALL(*MockClient, SelectRows("([minus_start_time]), ([query_id]) FROM [//sys/query_tracker/finished_queries_by_user_and_start_time] WHERE (user = {User}) ORDER BY (minus_start_time) ASC LIMIT 4", _))
        .WillOnce(Return(MakeQueryResult<TSelectRowsResult, TFinishedQueryByUserAndStartTimeDescriptor>(finishedQueryByUserAndStartTimeRecords, /*isPartialRecord*/ true)));
    EXPECT_CALL(*MockClient, SelectRows(std::string(Format("* FROM [//sys/query_tracker/finished_queries] WHERE ([query_id] in (\"%v\", \"%v\"))", queryId[3], queryId[1])), _))
        .WillOnce(Return(MakeQueryResult<TSelectRowsResult, TFinishedQueryDescriptor>(finishedQueryRecords)));
    EXPECT_CALL(*MockClient, SelectRows("* FROM [//sys/query_tracker/active_queries] WHERE (user = {User})", _))
        .WillOnce(Return(MakeQueryResult<TSelectRowsResult, TActiveQueryDescriptor, TActiveQuery>(activeQueryRecords)));

    auto result = Proxy->ListQueries(
        TListQueriesOptions{ .Limit = 3 },
        "user");

    ASSERT_EQ(result.Queries.size(), 3ul);
    ASSERT_EQ(result.Queries[0].Id, queryId[3]);
    ASSERT_EQ(result.Queries[1].Id, queryId[2]);
    ASSERT_EQ(result.Queries[2].Id, queryId[1]);
    ASSERT_TRUE(result.Incomplete);
}

TEST_F(TListQueryTrackerProxyTest, ListQueriesOnOnlyActive)
{
    ExpectListQueriesGetAcoCalls(/*everyoneAcoExists*/ false);

    std::vector<TFinishedQueryByUserAndStartTimePartial> finishedQueryByUserAndStartTimeRecords{};
    TQueryId queryId = TQueryId::Create();
    std::vector<TActiveQuery> activeQueryRecords{
        CreateSimpleQuery<TActiveQuery>(queryId),
    };

    EXPECT_CALL(*MockClient, SelectRows("([minus_start_time]), ([query_id]) FROM [//sys/query_tracker/finished_queries_by_user_and_start_time] WHERE (user = {User}) ORDER BY (minus_start_time) ASC LIMIT 101", _))
        .WillOnce(Return(MakeQueryResult<TSelectRowsResult, TFinishedQueryByUserAndStartTimeDescriptor>(finishedQueryByUserAndStartTimeRecords, /*isPartialRecord*/ true)));
    EXPECT_CALL(*MockClient, SelectRows("* FROM [//sys/query_tracker/active_queries] WHERE (user = {User})", _))
        .WillOnce(Return(MakeQueryResult<TSelectRowsResult, TActiveQueryDescriptor, TActiveQuery>(activeQueryRecords)));

    auto result = Proxy->ListQueries(TListQueriesOptions{}, "user");
    ASSERT_EQ(result.Queries.size(), 1ul);
    ASSERT_EQ(result.Queries[0].Id, queryId);
    ASSERT_FALSE(result.Incomplete);
}

TEST_F(TListQueryTrackerProxyTest, ListQueriesForSuperuser)
{
    ExpectListQueriesGetAcoCalls(/*everyoneAcoExists*/ false, /*isSuperuser*/ true);

    auto startTime = 1;
    auto queryId = TQueryId::Create();
    std::vector<TFinishedQueryByStartTimePartial> finishedQueryByStartTimeRecords = {
        { .Key = { .MinusStartTime = -startTime, .QueryId = queryId } },
    };
    std::vector<TFinishedQuery> finishedQueryRecords{
        CreateSimpleQuery<TFinishedQuery>(queryId, startTime),
    };
    std::vector<TActiveQuery> activeQueryRecords;

    EXPECT_CALL(*MockClient, SelectRows("([minus_start_time]), ([query_id]) FROM [//sys/query_tracker/finished_queries_by_start_time] ORDER BY (minus_start_time) ASC LIMIT 101", _))
        .WillOnce(Return(MakeQueryResult<TSelectRowsResult, TFinishedQueryByStartTimeDescriptor>(finishedQueryByStartTimeRecords, /*isPartialRecord*/ true)));
    EXPECT_CALL(*MockClient, SelectRows(std::string(Format("* FROM [//sys/query_tracker/finished_queries] WHERE ([query_id] in (\"%v\"))", queryId)), _))
        .WillOnce(Return(MakeQueryResult<TSelectRowsResult, TFinishedQueryDescriptor>(finishedQueryRecords)));
    EXPECT_CALL(*MockClient, SelectRows("* FROM [//sys/query_tracker/active_queries]", _))
        .WillOnce(Return(MakeQueryResult<TSelectRowsResult, TActiveQueryDescriptor, TActiveQuery>(activeQueryRecords)));

    auto result = Proxy->ListQueries(
        TListQueriesOptions{},
        "user");

    ASSERT_EQ(result.Queries.size(), 1ul);
    ASSERT_FALSE(result.Incomplete);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NQueryTracker

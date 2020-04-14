//%NUM_MASTERS=1
//%NUM_NODES=3
//%NUM_SCHEDULERS=0
//%DRIVER_BACKENDS=['native']
//%ENABLE_RPC_PROXY=True
//%DELTA_MASTER_CONFIG={"object_service":{"timeout_backoff_lead_time":100}}

#include <yt/tests/cpp/api_test_base.h>
#include <yt/tests/cpp/modify_rows_test.h>

#include <yt/client/api/rowset.h>
#include <yt/client/api/transaction.h>

#include <yt/ytlib/api/native/config.h>
#include <yt/ytlib/api/native/client.h>
#include <yt/ytlib/api/native/connection.h>

#include <yt/ytlib/cypress_client/cypress_ypath_proxy.h>

#include <yt/ytlib/object_client/public.h>
#include <yt/ytlib/object_client/object_service_proxy.h>

#include <yt/ytlib/table_client/config.h>

#include <yt/client/table_client/helpers.h>
#include <yt/client/table_client/row_buffer.h>
#include <yt/client/table_client/schema.h>
#include <yt/client/table_client/unversioned_row.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/logging/config.h>
#include <yt/core/logging/log_manager.h>

#include <yt/core/test_framework/framework.h>

#include <yt/core/yson/string.h>

#include <util/datetime/base.h>

#include <util/random/random.h>

#include <cstdlib>
#include <functional>
#include <tuple>

////////////////////////////////////////////////////////////////////////////////

namespace NYT {
namespace NCppTests {
namespace {

using namespace NApi;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NRpc;
using namespace NSecurityClient;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TBatchRequestTest
    : public TApiTestBase
{
protected:
    using TReqExecuteBatchPtr = TObjectServiceProxy::TReqExecuteBatchPtr;
    using TSubrequestType = TString;

    enum EAllowedSubrequestCategory
    {
        AllowAll,
        AllowMutationsOnly,
        AllowNonMutationsOnly
    };

    void TestBatchRequest(
        int subrequestCount,
        EAllowedSubrequestCategory subrequestCategory = AllowAll)
    {
        auto subrequestTypes = ChooseRandomSubrequestTypes(subrequestCount, subrequestCategory);
        TestBatchRequest(subrequestTypes);
    }

    void TestBatchRequest(
        const std::vector<TSubrequestType>& subrequestTypes)
    {
        auto channel = static_cast<NApi::NNative::IClient*>(Client_.Get())->
            GetMasterChannelOrThrow(EMasterChannelKind::Follower);
        TObjectServiceProxy proxy(channel);

        auto batchReq = proxy.ExecuteBatch();
        batchReq->SetTimeout(TDuration::MilliSeconds(200));
        MaybeSetMutationId(batchReq, subrequestTypes);

        FillWithSubrequests(batchReq, subrequestTypes);

        WaitFor(batchReq->Invoke())
            .ThrowOnError();
    }

    void FillWithSubrequests(
        const TReqExecuteBatchPtr& batchReq,
        const std::vector<TSubrequestType>& subrequestTypes)
    {
        for (const auto& subrequestType : subrequestTypes) {
            auto addSubrequest = GetSubrequestAdder(subrequestType);
            (this->*addSubrequest)(batchReq);
        }
    }

    void AddCreateTableSubrequest(const TReqExecuteBatchPtr& batchReq)
    {
        RecentTableGuid_ = TGuid::Create();

        auto req = TCypressYPathProxy::Create(GetRecentTablePath());
        req->set_type(static_cast<int>(EObjectType::Table));
        req->set_recursive(false);
        auto attributes = CreateEphemeralAttributes();
        attributes->Set("replication_factor", 1);
        ToProto(req->mutable_node_attributes(), *attributes);
        GenerateMutationId(req);
        batchReq->AddRequest(req, GenerateRequestKey("create"));
    }

    void AddReadSubrequest(const TReqExecuteBatchPtr& batchReq)
    {
        auto req = TCypressYPathProxy::Get(GetRecentTablePath() + "/@");
        batchReq->AddRequest(req, GenerateRequestKey("get"));
    }

    void AddWriteSubrequest(const TReqExecuteBatchPtr& batchReq)
    {
        auto req = TCypressYPathProxy::Set(GetRecentTablePath() + "/@replication_factor");
        req->set_value(TYsonString("3").GetData());
        GenerateMutationId(req);
        batchReq->AddRequest(req, GenerateRequestKey("set"));
    }

private:
    using TSubrequestAdder = void (TBatchRequestTest::*)(const TReqExecuteBatchPtr&);

    void MaybeSetMutationId(const TReqExecuteBatchPtr& batchReq, const std::vector<TSubrequestType>& subrequestTypes)
    {
        auto isMutating = false;
        for (const auto& subrequestType : subrequestTypes) {
            if (subrequestType != "get") {
                isMutating = true;
                break;
            }
        }

        if (isMutating) {
            GenerateMutationId(batchReq);
        }
    }

    std::vector<TSubrequestType> ChooseRandomSubrequestTypes(
        int subrequestCount,
        EAllowedSubrequestCategory subrequestCategory)
    {
        YT_VERIFY(subrequestCount >= 0);

        std::vector<TSubrequestType> result;

        if (subrequestCount == 0) {
            return result;
        }

        result.reserve(subrequestCount);

        // Start with creating at least one table so that there's
        // something to work with.
        bool forceCreate = subrequestCategory != AllowNonMutationsOnly;
        for (auto i = 0; i < subrequestCount; ++i) {
            auto adderId = forceCreate
                ? "create" :
                ChooseRandomSubrequestType(subrequestCategory);
            result.push_back(adderId);
        }

        return result;
    }

    TSubrequestType ChooseRandomSubrequestType(
        EAllowedSubrequestCategory subrequestCategory)
    {
        int shift;
        int spread;
        switch (subrequestCategory)
        {
            case AllowAll:
                shift = 0;
                spread = 3;
                break;
            case AllowMutationsOnly:
                shift = 1;
                spread = 2;
                break;
            case AllowNonMutationsOnly:
                shift = 0;
                spread = 1;
                break;
            default:
                YT_ABORT();
        }

        switch (shift + RandomNumber<ui32>(spread)) {
            case 0:
                return "read";
            case 1:
                return "create";
            case 2:
                return "write";
            default:
                YT_ABORT();
        }
    }

    TSubrequestAdder GetSubrequestAdder(const TSubrequestType& subrequestType)
    {
        if (subrequestType == "create") {
            return &TBatchRequestTest::AddCreateTableSubrequest;
        }

        if (subrequestType == "read") {
            return &TBatchRequestTest::AddReadSubrequest;
        }

        if (subrequestType == "write") {
            return &TBatchRequestTest::AddWriteSubrequest;
        }

        YT_ABORT();
    }

    TString GetRecentTablePath() const
    {
        YT_VERIFY(!RecentTableGuid_.IsEmpty());
        return Format("//tmp/%v", RecentTableGuid_);
    }

    TString GenerateRequestKey(const TString& prefix)
    {
        return Format("%v %v %v", prefix, RecentTableGuid_, SubrequestCounter_++);
    }

    TGuid RecentTableGuid_;
    int SubrequestCounter_ = 0;
};

TEST_F(TBatchRequestTest, TestEmptyBatchRequest)
{
    TestBatchRequest(0);
}

TEST_F(TBatchRequestTest, TestBatchRequestNoMutations)
{
    // Create a table to read via a separate batch request.
    TestBatchRequest({"create"});

    TestBatchRequest(99, AllowNonMutationsOnly);
    TestBatchRequest(100, AllowNonMutationsOnly);
    TestBatchRequest(101, AllowNonMutationsOnly);
}

TEST_F(TBatchRequestTest, TestBatchRequestOnlyMutations)
{
    TestBatchRequest(99, AllowMutationsOnly);
    TestBatchRequest(100, AllowMutationsOnly);
    TestBatchRequest(101, AllowMutationsOnly);
}

TEST_F(TBatchRequestTest, TestBatchRequestWith1Subrequest)
{
    TestBatchRequest(1);
}

TEST_F(TBatchRequestTest, TestBatchRequestWith50Subrequests)
{
    TestBatchRequest(50);
}

TEST_F(TBatchRequestTest, TestBatchRequestWith99Subrequests)
{
    TestBatchRequest(99);
}

TEST_F(TBatchRequestTest, TestBatchRequestWith100Subrequests)
{
    TestBatchRequest(100);
}

TEST_F(TBatchRequestTest, TestBatchRequestWith101Subrequests)
{
    TestBatchRequest(101);
}

TEST_F(TBatchRequestTest, TestBatchRequestWith150Subrequests)
{
    TestBatchRequest(150);
}

TEST_F(TBatchRequestTest, TestBatchRequestWith199Subrequests)
{
    TestBatchRequest(199);
}

TEST_F(TBatchRequestTest, TestBatchRequestWith200Subrequests)
{
    TestBatchRequest(200);
}

TEST_F(TBatchRequestTest, TestBatchRequestWith201Subrequests)
{
    TestBatchRequest(201);
}

TEST_F(TBatchRequestTest, TestBatchRequestWith1151Subrequests)
{
    TestBatchRequest(1151);
}

////////////////////////////////////////////////////////////////////////////////

class TBatchWithRetriesTest
    : public TApiTestBase
{
protected:
    TString GenerateTablePath()
    {
        return Format("//tmp/%v", TGuid::Create());
    }

    static TYPathRequestPtr GetRequest(const TString& tablePath)
    {
        return TCypressYPathProxy::Get(tablePath + "/@");
    }

    static TYPathRequestPtr CreateRequest(const TString& tablePath)
    {
        auto request = TCypressYPathProxy::Create(tablePath);
        request->set_type(static_cast<int>(EObjectType::Table));
        request->set_recursive(false);
        auto attributes = CreateEphemeralAttributes();
        attributes->Set("replication_factor", 1);
        ToProto(request->mutable_node_attributes(), *attributes);
        GenerateMutationId(request);

        return request;
    }

    int InvokeAndGetRetryCount(TYPathRequestPtr request, TErrorCode errorCode, int maxRetryCount)
    {
        auto config = New<TReqExecuteBatchWithRetriesConfig>();
        config->RetryCount = maxRetryCount;
        config->StartBackoff = TDuration::MilliSeconds(100);
        config->BackoffMultiplier = 1;

        int retryCount = 0;
        auto needRetry = [&retryCount, errorCode] (int currentRetry, const TError& error) {
            if (error.FindMatching(errorCode)) {
                retryCount = currentRetry + 1;
                return true;
            }
            return false;
        };

        auto channel = dynamic_cast<NApi::NNative::IClient*>(Client_.Get())->
            GetMasterChannelOrThrow(EMasterChannelKind::Follower);
        TObjectServiceProxy proxy(channel);

        auto batchRequest = proxy.ExecuteBatchWithRetries(config, BIND(needRetry));
        batchRequest->AddRequest(std::move(request));

        auto response = WaitFor(batchRequest->Invoke());
        response.ThrowOnError();

        return retryCount;
    }
};

TEST_F(TBatchWithRetriesTest, TestRetryCount)
{
    auto tablePath = GenerateTablePath();
    auto request = GetRequest(tablePath);
    ASSERT_EQ(InvokeAndGetRetryCount(request, NYTree::EErrorCode::ResolveError, 5), 5);
}

TEST_F(TBatchWithRetriesTest, TestCorrectRequest)
{
    auto tablePath = GenerateTablePath();
    auto badRequest = GetRequest(tablePath);
    ASSERT_EQ(InvokeAndGetRetryCount(badRequest, NYTree::EErrorCode::ResolveError, 5), 5);

    auto createRequest = CreateRequest(tablePath);
    ASSERT_EQ(InvokeAndGetRetryCount(createRequest, NYTree::EErrorCode::AlreadyExists, 5), 0);

    auto createRequest2 = CreateRequest(tablePath);
    ASSERT_EQ(InvokeAndGetRetryCount(createRequest2, NYTree::EErrorCode::AlreadyExists, 5), 5);

    auto goodRequest = GetRequest(tablePath);
    ASSERT_EQ(InvokeAndGetRetryCount(goodRequest, NYTree::EErrorCode::ResolveError, 5), 0);
}

////////////////////////////////////////////////////////////////////////////////

class TVersionedWriteTest
    : public TDynamicTablesTestBase
{
public:
    static void SetUpTestCase()
    {
        TDynamicTablesTestBase::SetUpTestCase();
        CreateTable(
            "//tmp/write_test", // tablePath
            "[" // schema
            "{name=k0;type=int64;sort_order=ascending};"
            "{name=k1;type=int64;sort_order=ascending};"
            "{name=k2;type=int64;sort_order=ascending};"
            "{name=v3;type=int64};"
            "{name=v4;type=int64};"
            "{name=v5;type=int64}]"
        );

        ReplicatorClient_ = CreateClient(ReplicatorUserName);
    }

    static void TearDownTestCase()
    {
        ReplicatorClient_.Reset();
    }

protected:
    TRowBufferPtr Buffer_ = New<TRowBuffer>();

    TVersionedRow BuildVersionedRow(
        const TString& keyYson,
        const TString& valueYson)
    {
        return YsonToVersionedRow(Buffer_, keyYson, valueYson);
    }

    static IClientPtr ReplicatorClient_;
};

IClientPtr TVersionedWriteTest::ReplicatorClient_;

////////////////////////////////////////////////////////////////////////////////

TEST_F(TVersionedWriteTest, TestWriteRemapping)
{
    WriteVersionedRow(
        {"k0", "k1", "k2", "v5", "v3", "v4"},
        "<id=0> 30; <id=1> 30; <id=2> 30;",
        "<id=3;ts=1> 15; <id=4;ts=1> 13; <id=5;ts=1> 14;",
        ReplicatorClient_);
    WriteVersionedRow(
        {"k0", "k1", "k2", "v4", "v5", "v3"},
        "<id=0> 30; <id=1> 30; <id=2> 30;",
        "<id=3;ts=2> 24; <id=4;ts=2> 25; <id=5;ts=2> 23;",
        ReplicatorClient_);

    auto preparedKey = PrepareUnversionedRow(
        {"k0", "k1", "k2", "v3", "v4", "v5"},
        "<id=0> 30; <id=1> 30; <id=2> 30");

    auto res = WaitFor(Client_->VersionedLookupRows(
        Table_,
        std::get<1>(preparedKey),
        std::get<0>(preparedKey)))
        .ValueOrThrow();

    ASSERT_EQ(1, res->GetRows().Size());

    auto actual = ToString(res->GetRows()[0]);
    auto expected = ToString(BuildVersionedRow(
        "<id=0> 30; <id=1> 30; <id=2> 30",
        "<id=3;ts=2> 23; <id=3;ts=1> 13; <id=4;ts=2> 24; <id=4;ts=1> 14; <id=5;ts=2> 25; <id=5;ts=1> 15;"));
    EXPECT_EQ(expected, actual);

    EXPECT_THROW(
        WriteVersionedRow(
            {"k0", "k2", "k1", "v3"},
            "<id=0> 30; <id=1> 30; <id=2> 30;",
            "<id=3;ts=3> 100;",
            ReplicatorClient_),
        TErrorException);

    EXPECT_THROW(
        WriteVersionedRow(
            {"k0", "k1", "v3", "k2"},
            "<id=0> 30; <id=1> 30; <id=3> 30;",
            "<id=2;ts=3> 100;",
            ReplicatorClient_),
        TErrorException);
}

TEST_F(TVersionedWriteTest, TestWriteTypeChecking)
{
    EXPECT_THROW(
        WriteVersionedRow(
            {"k0", "k1", "k2", "v3"},
            "<id=0> 30; <id=1> 30; <id=2> 30;",
            "<id=2;ts=3> %true;",
            ReplicatorClient_),
        TErrorException);
}

TEST_F(TVersionedWriteTest, TestInsertDuplicateKeyColumns)
{
    auto preparedKey = PrepareUnversionedRow(
        {"k0", "k1", "k2"},
        "<id=0> 20; <id=1> 21; <id=2> 22");

    EXPECT_THROW(
        WriteUnversionedRow(
            {"k0", "k1", "k2", "v3", "v4", "v5"},
            "<id=0> 20; <id=1> 21; <id=2> 22; <id=3> 13; <id=4> 14; <id=5> 15; <id=5> 25",
            ReplicatorClient_),
        TErrorException);
}

////////////////////////////////////////////////////////////////////////////////

class TVersionedWriteTestWithRequired
    : public TVersionedWriteTest
{
public:
    static void SetUpTestCase()
    {
        TDynamicTablesTestBase::SetUpTestCase();
        CreateTable(
            "//tmp/write_test_required", // tablePath
            "[" // schema
            "{name=k0;type=int64;sort_order=ascending};"
            "{name=k1;type=int64;sort_order=ascending};"
            "{name=k2;type=int64;sort_order=ascending};"
            "{name=v3;type=int64;required=%true};"
            "{name=v4;type=int64};"
            "{name=v5;type=int64}]"
        );

        ReplicatorClient_ = CreateClient(ReplicatorUserName);
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TVersionedWriteTestWithRequired, TestNoRequiredColumns)
{
    EXPECT_THROW(
        WriteVersionedRow(
            {"k0", "k1", "k2", "v4"},
            "<id=0> 30; <id=1> 30; <id=2> 30;",
            "<id=3;ts=1> 10",
            ReplicatorClient_),
        TErrorException);

    EXPECT_THROW(
        WriteVersionedRow(
            {"k0", "k1", "k2", "v3", "v4"},
            "<id=0> 30; <id=1> 30; <id=2> 30;",
            "<id=3;ts=2> 10; <id=4;ts=2> 10; <id=4;ts=1> 15",
            ReplicatorClient_),
        TErrorException);

    EXPECT_THROW(
        WriteVersionedRow(
            {"k0", "k1", "k2", "v3", "v4"},
            "<id=0> 30; <id=1> 30; <id=2> 30;",
            "<id=4;ts=2> 10; <id=4;ts=1> 15",
            ReplicatorClient_),
        TErrorException);

    WriteVersionedRow(
        {"k0", "k1", "k2", "v3", "v4"},
        "<id=0> 40; <id=1> 40; <id=2> 40;",
        "<id=3;ts=2> 10; <id=3;ts=1> 20; <id=4;ts=1> 15",
        ReplicatorClient_);
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TModifyRowsTest, TestNoSeqNumbers)
{
    WriteSimpleRow(0, 10);
    WriteSimpleRow(1, 11);
    WriteSimpleRow(0, 12);
    WriteSimpleRow(1, 13);
    WriteSimpleRow(0, 14);
    WriteSimpleRow(1, 15);
    SyncCommit();

    CheckTableContents({{0, 14}, {1, 15}});
}

TEST_F(TModifyRowsTest, TestOrderedSeqNumbers)
{
    WriteSimpleRow(0, 10, 0);
    WriteSimpleRow(1, 11, 1);
    WriteSimpleRow(0, 12, 2);
    WriteSimpleRow(1, 13, 3);
    WriteSimpleRow(0, 14, 4);
    WriteSimpleRow(1, 15, 5);
    SyncCommit();

    CheckTableContents({{0, 14}, {1, 15}});
}

TEST_F(TModifyRowsTest, TestShuffledSeqNumbers)
{
    WriteSimpleRow(0, 14, 4);
    WriteSimpleRow(1, 13, 3);
    WriteSimpleRow(0, 10, 0);
    WriteSimpleRow(1, 15, 5);
    WriteSimpleRow(0, 12, 2);
    WriteSimpleRow(1, 11, 1);
    SyncCommit();

    CheckTableContents({{0, 14}, {1, 15}});
}

TEST_F(TModifyRowsTest, TestRepeatingSeqNumbers)
{
    WriteSimpleRow(0, 10, 0);
    WriteSimpleRow(0, 11, 1);
    EXPECT_THROW(WriteSimpleRow(0, 12, 1), TErrorException);
    EXPECT_THROW(SyncCommit(), TErrorException);

    CheckTableContents({});
}

TEST_F(TModifyRowsTest, TestMissingSeqNumbers)
{
    WriteSimpleRow(1, 11, 0);
    WriteSimpleRow(0, 12, 1);
    WriteSimpleRow(1, 13, 2);
    WriteSimpleRow(0, 14, 4);
    WriteSimpleRow(1, 15, 5);
    EXPECT_THROW(SyncCommit(), TErrorException);

    CheckTableContents({});
}

TEST_F(TModifyRowsTest, TestNegativeSeqNumbers)
{
    EXPECT_THROW(WriteSimpleRow(0, 10, -1), TErrorException);
    EXPECT_THROW(SyncCommit(), TErrorException);

    CheckTableContents({});
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NCppTests
} // namespace NYT

#include <yt/yt/tests/cpp/test_base/api_test_base.h>
#include <yt/yt/tests/cpp/test_base/private.h>

#include <yt/yt/tests/cpp/modify_rows_test.h>

#include <yt/yt/server/lib/signature/components.h>
#include <yt/yt/server/lib/signature/config.h>
#include <yt/yt/server/lib/signature/cypress_key_store.h>
#include <yt/yt/server/lib/signature/key_info.h>
#include <yt/yt/server/lib/signature/signature_generator.h>
#include <yt/yt/server/lib/signature/signature_validator.h>

#include <yt/yt/client/api/rowset.h>
#include <yt/yt/client/api/transaction.h>
#include <yt/yt/client/api/table_writer.h>

#include <yt/yt/client/signature/signature.h>

#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/chunk_client/helpers.h>
#include <yt/yt/ytlib/chunk_client/input_chunk.h>

#include <yt/yt/ytlib/cypress_client/cypress_ypath_proxy.h>
#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/object_client/config.h>
#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/ytlib/table_client/table_ypath_proxy.h>

#include <yt/yt/ytlib/transaction_client/config.h>

#include <yt/yt/client/api/internal_client.h>
#include <yt/yt/client/api/security_client.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/client/transaction_client/helpers.h>
#include <yt/yt/client/transaction_client/timestamp_provider.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/scheduler.h>
#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/logging/config.h>
#include <yt/yt/core/logging/log_manager.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ypath/helpers.h>

#include <yt/yt/core/yson/string.h>
#include <yt/yt/core/yson/protobuf_helpers.h>

#include <util/datetime/base.h>

#include <util/random/random.h>

////////////////////////////////////////////////////////////////////////////////

namespace NYT::NCppTests {
namespace {

using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NRpc;
using namespace NSecurityClient;
using namespace NSignature;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTransactionClient;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;

using ::testing::HasSubstr;
using ::testing::SizeIs;

////////////////////////////////////////////////////////////////////////////////

const auto Logger = CppTestsLogger;

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
        auto client = DynamicPointerCast<NApi::NNative::IClient>(Client_);
        auto proxy = CreateObjectServiceReadProxy(client, EMasterChannelKind::Follower);
        auto batchReq = proxy.ExecuteBatch();
        batchReq->SetTimeout(TDuration::Seconds(1));
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
        req->set_type(ToProto(EObjectType::Table));
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
        req->set_value(ToProto(TYsonString(TStringBuf("3"))));
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

TEST_F(TBatchRequestTest, TestInvalidObjectIdError)
{
    auto client = DynamicPointerCast<NApi::NNative::IClient>(Client_);
    auto proxy = CreateObjectServiceReadProxy(client, EMasterChannelKind::Follower);
    auto batchRequest = proxy.ExecuteBatch();
    auto unexistingObject = MakeId(EObjectType::MapNode, TCellTag(10), 0, 0);
    auto invalidCellTag = MakeId(EObjectType::MapNode, TCellTag(9), 0, 0);

    batchRequest->AddRequest(TYPathProxy::Get(FromObjectId(unexistingObject)));
    batchRequest->AddRequest(TYPathProxy::Get(FromObjectId(invalidCellTag)));

    auto batchResponse = WaitFor(batchRequest->Invoke())
        .ValueOrThrow();

    auto unexistingObjectResponse = batchResponse->GetResponse<TYPathProxy::TRspGet>(0);
    EXPECT_FALSE(unexistingObjectResponse.IsOK());
    EXPECT_FALSE(unexistingObjectResponse.InnerErrors().empty());
    EXPECT_EQ(unexistingObjectResponse.InnerErrors().front().GetMessage(), Format("No such object %v", unexistingObject));

    auto invalidCellTagResponse = batchResponse->GetResponse<TYPathProxy::TRspGet>(1);
    EXPECT_FALSE(invalidCellTagResponse.IsOK());
    EXPECT_EQ(invalidCellTagResponse.GetMessage(), Format("Unknown cell tag %v", TCellTag(9)));
}

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
        request->set_type(ToProto(EObjectType::Table));
        request->set_recursive(false);
        auto attributes = CreateEphemeralAttributes();
        attributes->Set("replication_factor", 1);
        ToProto(request->mutable_node_attributes(), *attributes);
        GenerateMutationId(request);

        return request;
    }

    int InvokeAndGetRetryCount(TYPathRequestPtr request, TErrorCode errorCode, int maxRetryCount)
    {
        auto config = New<TReqExecuteBatchRetriesConfig>();
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

        auto client = DynamicPointerCast<NApi::NNative::IClient>(Client_);
        auto proxy = CreateObjectServiceReadProxy(client, EMasterChannelKind::Follower);
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

class TParallelBatchWithRetriesTest
    : public TBatchWithRetriesTest
{
protected:
    int InvokeAndGetRetryCount(
        TYPathRequestPtr request,
        int requestCount,
        TErrorCode errorCode,
        int maxRetryCount,
        int subbatchSize,
        int maxParallelSubbatchCount)
    {
        auto config = New<TReqExecuteBatchRetriesConfig>();
        config->RetryCount = maxRetryCount;
        config->StartBackoff = TDuration::MilliSeconds(100);
        config->BackoffMultiplier = 1;

        std::atomic<int> retryCount = 0;
        auto needRetry = [&retryCount, errorCode] (int /* currentRetry */, const TError& error) {
            if (error.FindMatching(errorCode)) {
                ++retryCount;
                return true;
            }
            return false;
        };

        auto client = DynamicPointerCast<NApi::NNative::IClient>(Client_);
        auto proxy = CreateObjectServiceReadProxy(client, EMasterChannelKind::Follower);
        auto batchRequest = proxy.ExecuteBatchWithRetriesInParallel(config, BIND(needRetry), subbatchSize, maxParallelSubbatchCount);

        for (int i = 0; i < requestCount; ++i) {
            if (i > 0) {
                GenerateMutationId(request);
            }
            batchRequest->AddRequest(request);
        }

        auto response = WaitFor(batchRequest->Invoke());
        response.ThrowOnError();

        return retryCount;
    }
};

TEST_F(TParallelBatchWithRetriesTest, TestRetryCount)
{
    auto tablePath = GenerateTablePath();
    auto request = GetRequest(tablePath);
    ASSERT_EQ(
        15,
        InvokeAndGetRetryCount(
            request,
            /* requestCount */ 3,
            NYTree::EErrorCode::ResolveError,
            /* maxRetryCount */ 5,
            /* subbatchSize */ 10,
            /* maxParallelSubbatchCount */ 1));
}

TEST_F(TParallelBatchWithRetriesTest, TestCorrectRequest)
{
    auto tablePath = GenerateTablePath();
    auto badRequest = GetRequest(tablePath);
    ASSERT_EQ(
        15,
        InvokeAndGetRetryCount(
            badRequest,
            /* requestCount */ 3,
            NYTree::EErrorCode::ResolveError,
            /* maxRetryCount */ 5,
            /* subbatchSize */ 10,
            /* maxParallelSubbatchCount */ 1));

    ASSERT_EQ(
        15,
        InvokeAndGetRetryCount(
            badRequest,
            /* requestCount */ 3,
            NYTree::EErrorCode::ResolveError,
            /* maxRetryCount */ 5,
            /* subbatchSize */ 1,
            /* maxParallelSubbatchCount */ 3));

    ASSERT_EQ(
        15,
        InvokeAndGetRetryCount(
            badRequest,
            /* requestCount */ 3,
            NYTree::EErrorCode::ResolveError,
            /* maxRetryCount */ 5,
            /* subbatchSize */ 2,
            /* maxParallelSubbatchCount */ 2));

    auto createRequest = CreateRequest(tablePath);
    ASSERT_EQ(
        10,
        InvokeAndGetRetryCount(
            createRequest,
            /* requestCount */ 3,
            NYTree::EErrorCode::AlreadyExists,
            /* maxRetryCount */ 5,
            /* subbatchSize */ 2,
            /* maxParallelSubbatchCount */ 2));

    auto createRequest2 = CreateRequest(tablePath);
    ASSERT_EQ(
        15,
        InvokeAndGetRetryCount(
            createRequest2,
            /* requestCount */ 3,
            NYTree::EErrorCode::AlreadyExists,
            /* maxRetryCount */ 5,
            /* subbatchSize */ 2,
            /* maxParallelSubbatchCount */ 2));

    auto goodRequest = GetRequest(tablePath);
    ASSERT_EQ(
        0,
        InvokeAndGetRetryCount(
            goodRequest,
            /* requestCount */ 3,
            NYTree::EErrorCode::ResolveError,
            /* maxRetryCount */ 5,
            /* subbatchSize */ 2,
            /* maxParallelSubbatchCount */ 2));
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
            "{name=v5;type=int64}]");

        ReplicatorClient_ = CreateClient(ReplicatorUserName);
    }

    static void TearDownTestCase()
    {
        ReplicatorClient_.Reset();
        TDynamicTablesTestBase::TearDownTestCase();
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

    auto rowset = WaitFor(Client_->VersionedLookupRows(
        Table_,
        std::get<1>(preparedKey),
        std::get<0>(preparedKey)))
        .ValueOrThrow()
        .Rowset;

    ASSERT_EQ(1u, rowset->GetRows().Size());

    auto actual = ToString(rowset->GetRows()[0]);
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
            "{name=v5;type=int64}]");

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

    ValidateTableContent({{0, 14}, {1, 15}});
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

    ValidateTableContent({{0, 14}, {1, 15}});
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

    ValidateTableContent({{0, 14}, {1, 15}});
}

TEST_F(TModifyRowsTest, TestRepeatingSeqNumbers)
{
    WriteSimpleRow(0, 10, 0);
    WriteSimpleRow(0, 11, 1);
    EXPECT_THROW(WriteSimpleRow(0, 12, 1), TErrorException);
    EXPECT_THROW(SyncCommit(), TErrorException);

    ValidateTableContent({});
}

TEST_F(TModifyRowsTest, TestMissingSeqNumbers)
{
    WriteSimpleRow(1, 11, 0);
    WriteSimpleRow(0, 12, 1);
    WriteSimpleRow(1, 13, 2);
    WriteSimpleRow(0, 14, 4);
    WriteSimpleRow(1, 15, 5);
    EXPECT_THROW(SyncCommit(), TErrorException);

    ValidateTableContent({});
}

TEST_F(TModifyRowsTest, TestNegativeSeqNumbers)
{
    EXPECT_THROW(WriteSimpleRow(0, 10, -1), TErrorException);
    EXPECT_THROW(SyncCommit(), TErrorException);

    ValidateTableContent({});
}

////////////////////////////////////////////////////////////////////////////////

class TAlterTableTest
    : public TApiTestBase
{
protected:
    void AlterTable(TYPath path, const NTableClient::NProto::TTableSchemaExt& schema)
    {
        auto req = NTableClient::TTableYPathProxy::Alter(path);
        SetTransactionId(req, TGuid{});
        SetMutationId(req, GenerateMutationId(), false);

        req->mutable_schema()->CopyFrom(schema);

        auto client = DynamicPointerCast<NApi::NNative::IClient>(Client_);
        auto proxy = CreateObjectServiceWriteProxy(client);
        WaitFor(proxy.Execute(req))
            .ThrowOnError();
    }
};

TEST_F(TAlterTableTest, TestUnknownType)
{
    auto createRes = Client_->CreateNode("//tmp/t1", EObjectType::Table);
    WaitFor(createRes)
        .ThrowOnError();

    {
        // Type not set.
        NTableClient::NProto::TTableSchemaExt schema;
        auto* column = schema.add_columns();
        column->set_name("foo");
        column->set_stable_name("foo");

        EXPECT_THROW_WITH_SUBSTRING(
            AlterTable("//tmp/t1", schema),
            "required fileds are not set");
    }

    {
        // Type is bad.
        NTableClient::NProto::TTableSchemaExt schema;
        auto* column = schema.add_columns();
        column->set_name("foo");
        column->set_stable_name("foo");
        column->set_type(ToProto(EValueType::Min));

        EXPECT_THROW_WITH_SUBSTRING(
            AlterTable("//tmp/t1", schema),
            "has no corresponding logical type");
    }

    {
        // Type is unknown.
        NTableClient::NProto::TTableSchemaExt schema;
        auto* column = schema.add_columns();
        column->set_name("foo");
        column->set_stable_name("foo");
        column->set_type(-1);

        EXPECT_THROW_WITH_SUBSTRING(
            AlterTable("//tmp/t1", schema),
            "Error casting");
    }

    {
        // Simple type is unknown.
        NTableClient::NProto::TTableSchemaExt schema;
        auto* column = schema.add_columns();
        column->set_name("foo");
        column->set_stable_name("foo");
        column->set_type(ToProto(EValueType::Any));
        column->set_simple_logical_type(-1);

        EXPECT_THROW_WITH_SUBSTRING(
            AlterTable("//tmp/t1", schema),
            "Error casting");
    }

    {
        // Mismatch of type and simple logical type.
        NTableClient::NProto::TTableSchemaExt schema;
        auto* column = schema.add_columns();
        column->set_name("foo");
        column->set_stable_name("foo");
        column->set_type(ToProto(EValueType::Any));
        column->set_simple_logical_type(ToProto(ESimpleLogicalValueType::Int64));

        EXPECT_NO_THROW(AlterTable("//tmp/t1", schema));
    }

    {
        // Unknown simple type in type_v3
        NTableClient::NProto::TTableSchemaExt schema;
        auto* column = schema.add_columns();
        column->set_name("foo");
        column->set_stable_name("foo");
        column->set_type(ToProto(EValueType::Int64));
        column->mutable_logical_type()->set_simple(-1);

        EXPECT_THROW_WITH_SUBSTRING(
            AlterTable("//tmp/t1", schema),
            "Error casting");
    }

    {
        // Unset type in type_v3
        NTableClient::NProto::TTableSchemaExt schema;
        auto* column = schema.add_columns();
        column->set_name("foo");
        column->set_stable_name("foo");
        column->set_type(ToProto(EValueType::Int64));
        column->mutable_logical_type();

        EXPECT_THROW_WITH_SUBSTRING(
            AlterTable("//tmp/t1", schema),
            "Cannot parse unknown logical type from proto");
    }

    {
        // Unknown type in type_v3
        NTableClient::NProto::TTableSchemaExt schema;
        auto* column = schema.add_columns();
        column->set_name("foo");
        column->set_stable_name("foo");
        column->set_type(ToProto(EValueType::Int64));
        column->mutable_logical_type();
        auto unknownFields = column->GetReflection()->MutableUnknownFields(column);
        unknownFields->AddVarint(100500, 0);

        EXPECT_THROW_WITH_SUBSTRING(
            AlterTable("//tmp/t1", schema),
            "Cannot parse unknown logical type from proto");
    }
}

////////////////////////////////////////////////////////////////////////////////

class TConcatenateTest
    : public TApiTestBase
{
protected:
    void CreateTableWithData(const TString& path, TCreateNodeOptions options, int valueCount) const
    {
        WaitFor(Client_->CreateNode(path, EObjectType::Table, options))
            .ThrowOnError();
        auto writer = WaitFor(Client_->CreateTableWriter(path))
            .ValueOrThrow();

        auto columnId = writer->GetNameTable()->GetIdOrRegisterName("key");
        std::vector<TUnversionedRow> rows;
        auto rowBuffer = New<TRowBuffer>();

        for (auto i = 0; i < valueCount; ++i) {
            TUnversionedRowBuilder rowBuilder;
            // Same value to allow sequential concatinations for sorted table.
            rowBuilder.AddValue(MakeUnversionedInt64Value(10, columnId));
            rows.push_back(rowBuffer->CaptureRow(rowBuilder.GetRow()));
        }

        YT_VERIFY(writer->Write(rows));
        WaitFor(writer->Close())
            .ThrowOnError();
    }
};

TEST_F(TConcatenateTest, TestParallelConcatenateToSortedWithAppend)
{
    auto threadPool = CreateThreadPool(2, "ParallelConcatenate");
    TCreateNodeOptions options;
    options.Attributes = CreateEphemeralAttributes();
    options.Attributes->Set("schema", New<TTableSchema>(std::vector<TColumnSchema>{{"key", EValueType::Int64, ESortOrder::Ascending}}));

    CreateTableWithData("//tmp/table1", options, 5);
    CreateTableWithData("//tmp/table2", options, 5);
    CreateTableWithData("//tmp/table3", options, 5);
    CreateTableWithData("//tmp/concat", options, 0);

    auto barrierPromise = NewPromise<void>();
    std::vector<TFuture<void>> futures;
    for (int i = 0; i < 3; ++i) {
        futures.push_back(BIND([
            client = Client_,
            barrierFuture = barrierPromise.ToFuture()
        ] {
            WaitForFast(barrierFuture)
                .ThrowOnError();
            WaitFor(client->ConcatenateNodes(
                {"//tmp/table1", "//tmp/table2", "//tmp/table3"},
                "<append=true>//tmp/concat"))
                .ThrowOnError();
        })
            .AsyncVia(threadPool->GetInvoker())
            .Run());
    }

    barrierPromise.Set();
    auto results = WaitFor(AllSet(std::move(futures))).ValueOrThrow();

    int failedCount = 0;
    for (auto& result : results) {
        if (!result.IsOK()) {
            ++failedCount;
            YT_LOG_INFO("Concatenate failed with error (ErrorMessage: %v)", result);
            EXPECT_TRUE(result.FindMatching(NCypressClient::EErrorCode::ConcurrentTransactionLockConflict));
        }
    }

    EXPECT_GT(failedCount, 0);

    futures.clear();
    for (auto path : {"//tmp/table1", "//tmp/table2", "//tmp/table3", "//tmp/concat"}) {
        futures.push_back(Client_->RemoveNode(path));
    }
    WaitFor(AllSet(std::move(futures))).ThrowOnError();
}

TEST_F(TConcatenateTest, TestParallelConcatenateWithAppend)
{
    TCreateNodeOptions options;
    options.Attributes = CreateEphemeralAttributes();
    options.Attributes->Set("schema", New<TTableSchema>(std::vector<TColumnSchema>{{"key", EValueType::Int64}}));

    CreateTableWithData("//tmp/table1", options, 5);
    CreateTableWithData("//tmp/table2", options, 5);
    CreateTableWithData("//tmp/table3", options, 5);
    CreateTableWithData("//tmp/concat", options, 0);

    std::vector<TFuture<void>> futures;
    futures.push_back(Client_->ConcatenateNodes({"//tmp/table1", "//tmp/table2", "//tmp/table3"}, "<append=true>//tmp/concat"));
    futures.push_back(Client_->ConcatenateNodes({"//tmp/table1", "//tmp/table2", "//tmp/table3"}, "<append=true>//tmp/concat"));

    auto results = WaitFor(AllSet(std::move(futures))).ValueOrThrow();

    int failedCount = 0;
    for (auto& result : results) {
        if (!result.IsOK()) {
            ++failedCount;
        }
    }

    EXPECT_EQ(failedCount, 0);

    futures.clear();
    for (auto path : {"//tmp/table1", "//tmp/table2", "//tmp/table3", "//tmp/concat"}) {
        futures.push_back(Client_->RemoveNode(path));
    }
    WaitFor(AllSet(std::move(futures))).ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

class TGetOrderedTabletSafeTrimRowCountTest
    : public TDynamicTablesTestBase
{
public:
    static TTimestamp GenerateTimestamp()
    {
        return WaitFor(Client_->GetTimestampProvider()->GenerateTimestamps()).ValueOrThrow();
    }
};

TEST_F(TGetOrderedTabletSafeTrimRowCountTest, Basic)
{
    CreateTable(
        "//tmp/test_find_ordered_tablet_store", // tablePath
        "[" // schema
        "{name=v1;type=string}]");

    auto writeRow = [] (int count = 1) {
        for (int i = 0; i < count; ++i) {
            WriteUnversionedRow({"v1"}, "<id=0> GroundControlToMajorTom;");
        }
        return GenerateTimestamp();
    };

    auto flush = [] {
        SyncFlushTable(Table_);
        return GenerateTimestamp();
    };

    auto sleep = [] (i64 ms) {
        TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(ms));
        return GenerateTimestamp();
    };

    auto t0 = GenerateTimestamp();

    sleep(1200);

    auto t1 = writeRow(2);
    auto t2 = writeRow(3);

    auto t3 = sleep(1200);

    auto t4 = flush();

    sleep(1200);

    auto t5 = writeRow(2);

    sleep(1200);

    auto t6 = writeRow();

    auto t7 = sleep(1200);

    auto internalClient = DynamicPointerCast<IInternalClient>(Client_);
    YT_VERIFY(internalClient);

    auto orderedStores = WaitFor(internalClient->GetOrderedTabletSafeTrimRowCount({
        {Table_, 0, t0},
        {Table_, 0, t1},
        {Table_, 0, t2},
        {Table_, 0, t3},
        {Table_, 0, t4},
        {Table_, 0, t5},
        {Table_, 0, t6},
        {Table_, 0, t7},
    }))
        .ValueOrThrow();

    ASSERT_EQ(orderedStores[0].ValueOrThrow(), 0);
    ASSERT_EQ(orderedStores[1].ValueOrThrow(), 0);
    ASSERT_EQ(orderedStores[2].ValueOrThrow(), 5);
    ASSERT_EQ(orderedStores[3].ValueOrThrow(), 5);
    ASSERT_EQ(orderedStores[4].ValueOrThrow(), 5);
    ASSERT_EQ(orderedStores[5].ValueOrThrow(), 5);
    ASSERT_EQ(orderedStores[6].ValueOrThrow(), 8);
    ASSERT_EQ(orderedStores[7].ValueOrThrow(), 8);

    auto t8 = flush();

    auto t9 = sleep(1200);

    orderedStores = WaitFor(internalClient->GetOrderedTabletSafeTrimRowCount({
        {Table_, 0, t0},
        {Table_, 0, t1},
        {Table_, 0, t2},
        {Table_, 0, t3},
        {Table_, 0, t4},
        {Table_, 0, t5},
        {Table_, 0, t6},
        {Table_, 0, t7},
        {Table_, 0, t8},
        {Table_, 0, t9},
    }))
        .ValueOrThrow();

    ASSERT_EQ(orderedStores[0].ValueOrThrow(), 0);
    ASSERT_EQ(orderedStores[1].ValueOrThrow(), 0);
    ASSERT_EQ(orderedStores[2].ValueOrThrow(), 5);
    ASSERT_EQ(orderedStores[3].ValueOrThrow(), 5);
    ASSERT_EQ(orderedStores[4].ValueOrThrow(), 5);
    ASSERT_EQ(orderedStores[5].ValueOrThrow(), 5);
    ASSERT_EQ(orderedStores[6].ValueOrThrow(), 8);
    ASSERT_EQ(orderedStores[7].ValueOrThrow(), 8);
    ASSERT_EQ(orderedStores[8].ValueOrThrow(), 8);
    ASSERT_EQ(orderedStores[9].ValueOrThrow(), 8);

    auto tabletId = ConvertTo<TString>(WaitFor(Client_->GetNode(Table_ + "/@tablets/0/tablet_id")).ValueOrThrow());

    WaitFor(Client_->TrimTable(Table_, 0, 8))
        .ThrowOnError();

    SyncFreezeTable(Table_);
    SyncUnfreezeTable(Table_);

    WaitUntil([&] {
        auto trimmedRowCount = ConvertTo<i64>(WaitFor(Client_->GetNode(Format("#%v/orchid/trimmed_row_count", tabletId))).ValueOrThrow());
        return trimmedRowCount = 8;
    }, "Trimmed row count is outdated");

    WaitUntil([&] {
        auto allRowsResult = WaitFor(Client_->SelectRows(Format("* from [%v]", Table_)))
            .ValueOrThrow();
        return allRowsResult.Rowset->GetRows().empty();
    }, "Table is not empty");

    WaitUntil([&] {
        auto stores = ConvertTo<std::vector<TStoreId>>(WaitFor(Client_->ListNode(Format("#%v/orchid/stores", tabletId))).ValueOrThrow());
        return std::ssize(stores) == 1;
    }, "Stores beside a single dynamic store exist");

    SyncFreezeTable(Table_);

    WaitUntil([&] {
        auto stores = ConvertTo<std::vector<TStoreId>>(WaitFor(Client_->ListNode(Format("#%v/orchid/stores", tabletId))).ValueOrThrow());
        return stores.empty();
    }, "Stores exist");

    orderedStores = WaitFor(internalClient->GetOrderedTabletSafeTrimRowCount({
        {Table_, 0, t1},
        {Table_, 0, t5},
        {Table_, 0, t9},
        {"//tmp/nonexistent/path", 0, t5},
    }))
        .ValueOrThrow();

    ASSERT_EQ(orderedStores[0].ValueOrThrow(), 8);
    ASSERT_EQ(orderedStores[1].ValueOrThrow(), 8);
    ASSERT_EQ(orderedStores[2].ValueOrThrow(), 8);
    ASSERT_FALSE(orderedStores[3].IsOK());

    SyncUnfreezeTable(Table_);
}

////////////////////////////////////////////////////////////////////////////////

class TFetchHunkChunkSpecsTest
    : public TDynamicTablesTestBase
{
public:
    std::vector<TChunkId> GetHunkChunkIds(TTabletId tabletId)
    {
        auto hunkChunkIdsResponse = WaitFor(Client_->GetNode(FromObjectId(tabletId) + "/orchid/hunk_chunks"))
            .ValueOrThrow();
        auto hunkChunkIdsAsStrings = ConvertTo<IMapNodePtr>(hunkChunkIdsResponse)->GetKeys();
        return ConvertTo<std::vector<TChunkId>>(hunkChunkIdsAsStrings);
    }

    void UpdateHunkChunkIds(TTabletId tabletId, std::vector<TChunkId>* hunkChunkIds)
    {
        auto newHunkChunkIds = GetHunkChunkIds(tabletId);
        auto hunkChunkIdsSet = THashSet<TChunkId>(hunkChunkIds->begin(), hunkChunkIds->end());
        for (auto hunkChunkId : newHunkChunkIds) {
            if (!hunkChunkIdsSet.contains(hunkChunkId)) {
                hunkChunkIds->push_back(hunkChunkId);
            }
        }
    }

    auto FetchHunkChunkSpecs(
        TTableId tableId,
        std::vector<NChunkClient::TReadRange>&& ranges,
        std::function<void(TChunkOwnerYPathProxy::TReqFetchPtr&)> prepareRequest)
    {
        auto req = TChunkOwnerYPathProxy::Fetch(FromObjectId(tableId));
        req->set_chunk_list_content_type(ToProto(EChunkListContentType::Hunk));
        req->set_supported_chunk_features(ToUnderlying(GetSupportedChunkFeatures()));
        ToProto(req->mutable_ranges(), std::move(ranges));
        prepareRequest(req);
        auto client = DynamicPointerCast<NApi::NNative::IClient>(Client_);
        auto proxy = CreateObjectServiceReadProxy(client, EMasterChannelKind::Follower);
        auto fetchResponse = WaitFor(proxy.Execute(req)).ValueOrThrow();
        return fetchResponse->chunks();
    }
};

TEST_F(TFetchHunkChunkSpecsTest, Simple)
{
    CreateTable(
        "//tmp/test_fetch_hunk_chunk_specs", // tablePath
        "[" // schema
        "{name=k1;type=int64;sort_order=ascending};"
        "{name=v1;type=string;max_inline_hunk_size=1}]");

    WaitFor(Client_->SetNode(Table_ + "/@mount_config/enable_compaction_and_partitioning",  ConvertToYsonString(false)))
        .ThrowOnError();

    SyncUnmountTable(Table_);
    SyncMountTable(Table_);

    auto tableIdResponse = WaitFor(Client_->GetNode(Table_ + "/@id"))
        .ValueOrThrow();
    auto tableId = ConvertTo<TTableId>(tableIdResponse);

    auto tabletIdResponse = WaitFor(Client_->GetNode(Table_ + "/@tablets/0/tablet_id"))
        .ValueOrThrow();
    auto tabletId = ConvertTo<TTabletId>(tabletIdResponse);

    constexpr int HunkCount = 3;
    std::vector<TChunkId> hunkChunkIds;
    for (int index = 0; index < HunkCount; ++index) {
        WriteUnversionedRow(
            {"k1", "v1"},
            Format("<id=0> %v; <id=1;ts=%v> PP%v", index, index + 1, index) + "HunkValueHunkValueHunkValue;");

        SyncFlushTable(Table_);

        UpdateHunkChunkIds(tabletId, &hunkChunkIds);
    }

    EXPECT_EQ(std::ssize(hunkChunkIds), HunkCount);

    auto tabletCountResponse = WaitFor(Client_->GetNode(Table_ + "/@tablet_count"))
        .ValueOrThrow();
    auto tabletCount = ConvertTo<int>(tabletCountResponse);
    ASSERT_EQ(tabletCount, 1);

    {
        auto hunkChunkSpecs = FetchHunkChunkSpecs(
            tableId,
            std::vector<NChunkClient::TReadRange>{TReadRange()},
            [] (TChunkOwnerYPathProxy::TReqFetchPtr& /*req*/) {});

        EXPECT_EQ(std::ssize(hunkChunkSpecs), HunkCount);
        auto hunkChunkIdsSet = THashSet<TChunkId>(hunkChunkIds.begin(), hunkChunkIds.end());
        for (const auto& chunkSpec : hunkChunkSpecs) {
            auto hunkChunkId = FromProto<TChunkId>(chunkSpec.chunk_id());
            EXPECT_TRUE(hunkChunkIdsSet.contains(hunkChunkId));
            auto inputChunk = New<TInputChunk>(chunkSpec);
            EXPECT_EQ(inputChunk->GetChunkFormat(), EChunkFormat::HunkDefault);
            EXPECT_EQ(FromProto<EChunkType>(chunkSpec.chunk_meta().type()), EChunkType::Hunk);
        }
    }

    constexpr int RequestedHunkCount = 2;
    for (int shift = 0; shift <= 1; ++shift) {
        YT_VERIFY(shift + RequestedHunkCount <= HunkCount);
        auto range = TReadRange();
        range.LowerLimit().SetChunkIndex(shift);
        range.UpperLimit().SetChunkIndex(shift + RequestedHunkCount);
        auto hunkChunkSpecs = FetchHunkChunkSpecs(
            tableId,
            std::vector<NChunkClient::TReadRange>{range},
            [](TChunkOwnerYPathProxy::TReqFetchPtr& /*req*/) {});

        EXPECT_EQ(std::ssize(hunkChunkSpecs), RequestedHunkCount);
        auto hunkChunkIdsSet = THashSet<TChunkId>(
            hunkChunkIds.begin() + shift,
            hunkChunkIds.begin() + shift + RequestedHunkCount);
        for (const auto& chunkSpec : hunkChunkSpecs) {
            auto hunkChunkId = FromProto<TChunkId>(chunkSpec.chunk_id());
            EXPECT_TRUE(hunkChunkIdsSet.contains(hunkChunkId));
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

class TPingTransactionsTest
    : public TApiTestBase
{
public:
    static void ReconfigurePingBatcher(std::optional<TDuration> batchPeriod = {}, std::optional<int> batchSize = {})
    {
        auto nativeConnection = dynamic_cast<NNative::IConnection*>(Connection_.Get());
        auto config = CloneYsonStruct(nativeConnection->GetConfig());
        if (batchPeriod) {
            config->TransactionManager->PingBatcher->BatchPeriod = *batchPeriod;
        }
        if (batchSize) {
            config->TransactionManager->PingBatcher->BatchSize = *batchSize;
        }
        nativeConnection->Reconfigure(config);
    }

    static void ReconfigureDefaultTimeout(TDuration timeout)
    {
        auto nativeConnection = dynamic_cast<NNative::IConnection*>(Connection_.Get());
        auto config = CloneYsonStruct(nativeConnection->GetConfig());
        config->TransactionManager->DefaultTransactionTimeout = timeout;
        nativeConnection->Reconfigure(config);
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TPingTransactionsTest, Basic)
{
    auto startOptions = TTransactionStartOptions{
        .Ping = false,
    };
    auto tx1 = WaitFor(Client_->StartTransaction(ETransactionType::Master, startOptions))
        .ValueOrThrow();

    auto cellId = CellTagFromId(tx1->GetId());
    auto unknownTransactionId = MakeId(EObjectType::Transaction, cellId, RandomNumber<ui32>(), RandomNumber<ui32>());

    TTransactionAttachOptions attachOptions = {};
    attachOptions.Ping = false;
    auto unknownTx = Client_->AttachTransaction(unknownTransactionId, attachOptions);

    auto pingResult1 = WaitFor(tx1->Ping());
    auto unknownPingResult = WaitFor(unknownTx->Ping());
    auto pingResult2 = WaitFor(tx1->Ping());
    EXPECT_FALSE(unknownPingResult.IsOK());
    EXPECT_TRUE(pingResult1.IsOK());
    EXPECT_TRUE(pingResult2.IsOK());
}

TEST_F(TPingTransactionsTest, StartWithTimeout)
{
    ReconfigureDefaultTimeout(TDuration::Seconds(5));

    auto startAttributes = CreateEphemeralAttributes();
    startAttributes->Set("timeout", TDuration::Seconds(30));

    auto startOptions = TTransactionStartOptions{
        .Ping = false,
        .Attributes = std::move(startAttributes),
    };
    auto tx = WaitFor(Client_->StartTransaction(ETransactionType::Master, startOptions))
        .ValueOrThrow();

    Sleep(TDuration::Seconds(6));

    EXPECT_NO_THROW(WaitFor(tx->Commit())
        .ValueOrThrow());

    ReconfigureDefaultTimeout(TDuration::Seconds(30));
}

TEST_F(TPingTransactionsTest, MaxBatchSize)
{
    ReconfigurePingBatcher(TDuration::Days(1));

    auto startOptions = TTransactionStartOptions{
        .Timeout = TDuration::Days(365),
        .Ping = false,
        .StartCypressTransaction = false, // TODO(gryzlov-ad): add tests for Cypress transactions.
    };
    auto tx1 = WaitFor(Client_->StartTransaction(ETransactionType::Master, startOptions))
        .ValueOrThrow();
    auto tx2 = WaitFor(Client_->StartTransaction(ETransactionType::Master, startOptions))
        .ValueOrThrow();
    auto tx3 = WaitFor(Client_->StartTransaction(ETransactionType::Master, startOptions))
        .ValueOrThrow();

    auto future1 = tx1->Ping();
    auto future2 = tx2->Ping();
    auto future3 = tx3->Ping();

    WaitFor(AllSet(std::vector{future1, future2}))
        .ThrowOnError();

    // Batch size is set to 2 in yt/yt/tests/cpp/test_native_api/config.yson, so last ping should go into another batch.
    ASSERT_FALSE(future3.IsSet());

    auto future4 = tx1->Ping();
    WaitFor(future3)
        .ThrowOnError();

    ReconfigurePingBatcher(TDuration::Seconds(1));
}

TEST_F(TPingTransactionsTest, Reconfigure)
{
    auto startOptions = TTransactionStartOptions{
        .Timeout = TDuration::Days(365),
        .Ping = false,
        .StartCypressTransaction = false, // TODO(gryzlov-ad): add tests for Cypress transactions.
    };
    auto tx = WaitFor(Client_->StartTransaction(ETransactionType::Master, startOptions))
        .ValueOrThrow();

    NProfiling::TWallTimer timer;
    auto future = tx->Ping();
    WaitFor(future)
        .ThrowOnError();
    auto batchPeriod = timer.GetElapsedTime();

    ReconfigurePingBatcher(2 * batchPeriod);

    timer.Start();
    future = tx->Ping();
    WaitFor(future)
        .ThrowOnError();

    EXPECT_GE(timer.GetElapsedTime(), 2 * batchPeriod);
}

////////////////////////////////////////////////////////////////////////////////

class TCypressKeyWriterTest
    : public TApiTestBase
{
public:
    NNative::IConnectionPtr NativeConnection = DynamicPointerCast<NNative::IConnection>(Connection_);
    NNative::IClientPtr NativeClient = NativeConnection->CreateNativeClient(
        NNative::TClientOptions::FromUser(NSecurityClient::SignatureKeysmithUserName));
    TOwnerId OwnerId = TOwnerId("test");
    TInstant NowTime = Now();
    TInstant ExpiresAt = NowTime + TDuration::MilliSeconds(1);
    TKeyPairMetadata MetaValid = TKeyPairMetadataImpl<TKeyPairVersion{0, 1}>{
        .OwnerId = OwnerId,
        .KeyId = TKeyId(TGuid(1, 2, 3, 4)),
        .CreatedAt = NowTime,
        .ValidAfter = NowTime - TDuration::Hours(10),
        .ExpiresAt = ExpiresAt,
    };
    TKeyPairMetadata MetaExpired = TKeyPairMetadataImpl<TKeyPairVersion{0, 1}>{
        .OwnerId = OwnerId,
        .KeyId = TKeyId(TGuid(5, 6, 7, 8)),
        .CreatedAt = NowTime,
        .ValidAfter = NowTime - TDuration::Hours(10),
        .ExpiresAt = NowTime - TDuration::Hours(1),
    };
    TCypressKeyWriterConfigPtr Config = New<TCypressKeyWriterConfig>();
    TCypressKeyWriterPtr Writer;

    TCypressKeyWriterTest()
    {
        Config->Path = Format("//sys/public_keys/%v", TGuid::Create());
        WaitFor(NativeClient->CreateNode(Config->Path, EObjectType::MapNode))
            .ThrowOnError();
        Writer = New<TCypressKeyWriter>(Config, OwnerId, NativeClient);
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TCypressKeyWriterTest, GetOwner)
{
    EXPECT_EQ(Writer->GetOwner(), OwnerId);
}

TEST_F(TCypressKeyWriterTest, RegisterKey)
{
    auto keyInfo = New<TKeyInfo>(TPublicKey{}, MetaValid);

    {
        EXPECT_TRUE(
            WaitFor(Writer->RegisterKey(keyInfo))
                .IsOK());

        EXPECT_TRUE(
            WaitFor(Client_->GetNode(YPathJoin(Config->Path, "test")))
                .IsOK());
        auto keyNode = YPathJoin(Config->Path, "test", "4-3-2-1");
        EXPECT_EQ(
            ConvertTo<TKeyInfo>(WaitFor(Client_->GetNode(keyNode))
                .ValueOrThrow()),
            *keyInfo);
        EXPECT_EQ(
            ConvertTo<TInstant>(
                WaitFor(Client_->GetNode(keyNode + "/@expiration_time"))
                    .ValueOrThrow()),
            ExpiresAt + Config->KeyDeletionDelay);
        EXPECT_EQ(
            WaitFor(Client_->GetNode(keyNode + "/@type"))
                .ValueOrThrow(),
            ConvertToYsonString(EObjectType::Document));

        // Second registration should fail.
        EXPECT_THROW_WITH_SUBSTRING(
            WaitFor(Writer->RegisterKey(keyInfo))
                .ThrowOnError(),
            "already exists");
    }
}

TEST_F(TCypressKeyWriterTest, RegisterKeyFailed)
{
    Config->Path = "not/a/path";

    auto keyInfo = New<TKeyInfo>(TPublicKey{}, MetaValid);

    // Registration should fail gracefully.
    EXPECT_THROW_WITH_SUBSTRING(
        WaitFor(Writer->RegisterKey(keyInfo))
            .ThrowOnError(),
        "Unexpected \"literal\" token");
}

TEST_F(TCypressKeyWriterTest, RegisterExpiredKey)
{
    auto expiredKeyInfo = New<TKeyInfo>(TPublicKey{}, MetaExpired);

    EXPECT_TRUE(
        WaitFor(Writer->RegisterKey(expiredKeyInfo))
            .IsOK());
}

TEST_F(TCypressKeyWriterTest, RegisterMultipleKeys)
{
    std::vector<TKeyInfoPtr> keys;
    std::vector<TFuture<void>> registrationFutures;
    for (int i = 0; i < 10; ++i) {
        auto meta = TKeyPairMetadataImpl<TKeyPairVersion{0, 1}>{
            .OwnerId = OwnerId,
            .KeyId = TKeyId(TGuid(i, 0, 0, 0)),
            .CreatedAt = NowTime,
            .ValidAfter = NowTime - TDuration::Hours(10),
            .ExpiresAt = ExpiresAt,
        };
        keys.push_back(New<TKeyInfo>(TPublicKey{}, meta));
        registrationFutures.push_back(Writer->RegisterKey(keys.back()));
    }

    EXPECT_TRUE(WaitFor(AllSucceeded(registrationFutures))
        .IsOK());

    auto keyNodes = ConvertTo<std::vector<TString>>(
        WaitFor(Client_->ListNode(YPathJoin(Config->Path, "test")))
            .ValueOrThrow());
    EXPECT_THAT(keyNodes, SizeIs(keys.size()));
}

TEST_F(TCypressKeyWriterTest, BatchOperationPartialFailure)
{
    // Create a document node at the key path to simulate a conflict.
    auto keyPath = YPathJoin(Config->Path, "test", "4-3-2-1");
    TCreateNodeOptions options;
    options.IgnoreExisting = true;
    WaitFor(Client_->CreateNode(YPathJoin(Config->Path, "test"), EObjectType::MapNode, options))
        .ThrowOnError();
    WaitFor(Client_->CreateNode(keyPath, EObjectType::Document))
        .ThrowOnError();

    auto keyInfo = New<TKeyInfo>(TPublicKey{}, MetaValid);
    EXPECT_THROW_WITH_SUBSTRING(
        WaitFor(Writer->RegisterKey(keyInfo))
            .ThrowOnError(),
        "already exists");
}

TEST_F(TCypressKeyWriterTest, ReconfigureWhileRegistering)
{
    std::vector<TFuture<void>> futures;
    std::vector<TKeyInfoPtr> keyInfos;

    for (int i = 0; i < 10; ++i) {
        auto meta = TKeyPairMetadataImpl<TKeyPairVersion{0, 1}>{
            .OwnerId = OwnerId,
            .KeyId = TKeyId(TGuid(i, 0, 0, 0)),
            .CreatedAt = NowTime,
            .ValidAfter = NowTime - TDuration::Hours(10),
            .ExpiresAt = ExpiresAt,
        };
        keyInfos.push_back(New<TKeyInfo>(TPublicKey(), meta));
    }

    auto newConfig = New<TCypressKeyWriterConfig>();
    newConfig->Path = "//tmp/reconfigure_while_registering";
    WaitFor(Client_->CreateNode(newConfig->Path, EObjectType::MapNode))
        .ThrowOnError();

    for (const auto& keyInfo : keyInfos) {
        futures.push_back(Writer->RegisterKey(keyInfo));
    }

    Writer->Reconfigure(newConfig);

    for (const auto& keyInfo : keyInfos) {
        futures.push_back(Writer->RegisterKey(keyInfo));
    }

    WaitFor(AllSucceeded(futures))
        .ThrowOnError();

    for (const auto& keyInfo : keyInfos) {
        auto keyId = GetKeyId(keyInfo->Meta());
        auto oldPath = Format("%v/%v/%v", Config->Path, OwnerId, keyId);
        auto newPath = Format("%v/%v/%v", newConfig->Path, OwnerId, keyId);

        auto oldExists = WaitFor(NativeClient->NodeExists(oldPath))
            .ValueOrThrow();
        auto newExists = WaitFor(NativeClient->NodeExists(newPath))
            .ValueOrThrow();

        EXPECT_TRUE(oldExists && newExists);
    }
}

TEST_F(TCypressKeyWriterTest, Reconfigure)
{
    auto keyInfoValid = New<TKeyInfo>(TPublicKey(), MetaValid);
    WaitFor(Writer->RegisterKey(keyInfoValid))
        .ThrowOnError();

    auto newConfig = New<TCypressKeyWriterConfig>();
    newConfig->Path = Format("//tmp/reconfigured_keys");

    WaitFor(Client_->CreateNode(newConfig->Path, EObjectType::MapNode))
        .ThrowOnError();

    Writer->Reconfigure(newConfig);
    EXPECT_EQ(Writer->GetOwner(), OwnerId);

    TInstant NowTime = Now();
    TKeyPairMetadata metaWithNewOwner = TKeyPairMetadataImpl<TKeyPairVersion{0, 1}>{
        .OwnerId = Writer->GetOwner(),
        .KeyId = TKeyId(TGuid(5, 6, 7, 8)),
        .CreatedAt = NowTime,
        .ValidAfter = NowTime - TDuration::Hours(10),
        .ExpiresAt = NowTime - TDuration::Hours(1),
    };
    auto keyInfoExpired = New<TKeyInfo>(TPublicKey(), metaWithNewOwner);
    WaitFor(Writer->RegisterKey(keyInfoExpired))
        .ThrowOnError();

    EXPECT_EQ(
        ConvertTo<TKeyInfo>(WaitFor(Client_->GetNode(YPathJoin(newConfig->Path, OwnerId.Underlying(), "8-7-6-5")))
            .ValueOrThrow()),
        *keyInfoExpired);
}

TEST_F(TCypressKeyWriterTest, CleanupSoonExpiringKeysWhenLimitExceeded)
{
    Config->MaxKeyCount = 3;
    Writer->Reconfigure(Config);

    for (int i = 0; i < 5; ++i) {
        auto meta = TKeyPairMetadataImpl<TKeyPairVersion{0, 1}>{
            .OwnerId = OwnerId,
            .KeyId = TKeyId(TGuid(i, 0, 0, 0)),
            .CreatedAt = NowTime - TDuration::Seconds(10),
            .ValidAfter = NowTime,
            .ExpiresAt = NowTime + TDuration::Hours(1) + TDuration::Seconds(i),
        };
        WaitFor(Writer->RegisterKey(New<TKeyInfo>(TPublicKey{}, meta)))
            .ThrowOnError();
    }

    // Verify keys with nearest expiration time were deleted, others remain.
    auto ownerPath = YPathJoin(Config->Path, "test");
    EXPECT_FALSE(WaitFor(Client_->NodeExists(YPathJoin(ownerPath, "0-0-0-0")))
        .ValueOrThrow());
    EXPECT_FALSE(WaitFor(Client_->NodeExists(YPathJoin(ownerPath, "0-0-0-1")))
        .ValueOrThrow());
    EXPECT_TRUE(WaitFor(Client_->NodeExists(YPathJoin(ownerPath, "0-0-0-2")))
        .ValueOrThrow());
    EXPECT_TRUE(WaitFor(Client_->NodeExists(YPathJoin(ownerPath, "0-0-0-3")))
        .ValueOrThrow());
    EXPECT_TRUE(WaitFor(Client_->NodeExists(YPathJoin(ownerPath, "0-0-0-4")))
        .ValueOrThrow());
}

TEST_F(TCypressKeyWriterTest, NoCleanupWhenMaxKeyCountNotSet)
{
    Config->MaxKeyCount = std::nullopt;
    Writer->Reconfigure(Config);

    std::vector<TFuture<void>> registerFutures;
    int keyCount = 20;
    for (int i = 0; i < keyCount; ++i) {
        auto meta = TKeyPairMetadataImpl<TKeyPairVersion{0, 1}>{
            .OwnerId = OwnerId,
            .KeyId = TKeyId(TGuid(i, 0, 0, 0)),
            .CreatedAt = NowTime,
            .ValidAfter = NowTime,
            .ExpiresAt = ExpiresAt,
        };
        registerFutures.push_back(Writer->RegisterKey(New<TKeyInfo>(TPublicKey{}, std::move(meta))));
    }
    WaitFor(AllSucceeded(registerFutures))
        .ThrowOnError();

    // All keys should exist
    auto ownerPath = YPathJoin(Config->Path, "test");
    for (int i = 0; i < keyCount; ++i) {
        EXPECT_TRUE(WaitFor(Client_->NodeExists(Format("%v/%v", ownerPath, TGuid(i, 0, 0, 0))))
            .ValueOrThrow());
    }
}

////////////////////////////////////////////////////////////////////////////////

class TSignatureComponentsTest
    : public TApiTestBase
{
public:
    NNative::IConnectionPtr NativeConnection = DynamicPointerCast<NApi::NNative::IConnection>(Connection_);
    TCypressKeyReaderConfigPtr CypressKeyReaderConfig = New<TCypressKeyReaderConfig>();
    TCypressKeyWriterConfigPtr CypressKeyWriterConfig = New<TCypressKeyWriterConfig>();
    TKeyRotatorConfigPtr KeyRotatorConfig = New<TKeyRotatorConfig>();
    TSignatureGeneratorConfigPtr GeneratorConfig = New<TSignatureGeneratorConfig>();
    TSignatureGenerationConfigPtr GenerationConfig = New<TSignatureGenerationConfig>();
    TSignatureValidationConfigPtr ValidationConfig = New<TSignatureValidationConfig>();
    TSignatureComponentsConfigPtr Config = New<TSignatureComponentsConfig>();
    TActionQueuePtr ActionQueue = New<TActionQueue>();
    IInvokerPtr RotateInvoker = ActionQueue->GetInvoker();
    TSignatureComponentsPtr Components;
    const TOwnerId OwnerId = TOwnerId("test");

    TSignatureComponentsTest()
    {
        GenerationConfig->CypressKeyWriter = CypressKeyWriterConfig;
        GenerationConfig->KeyRotator = KeyRotatorConfig;
        GenerationConfig->Generator = GeneratorConfig;
        ValidationConfig->CypressKeyReader = CypressKeyReaderConfig;
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TSignatureComponentsTest, EmptyInit)
{
    Components = New<TSignatureComponents>(Config, OwnerId, NativeConnection, RotateInvoker);
    auto startRotationFuture = Components->StartRotation();
    auto stopRotationFuture = Components->StopRotation();
    EXPECT_TRUE(startRotationFuture.IsSet() && startRotationFuture.Get().IsOK());
    EXPECT_TRUE(stopRotationFuture.IsSet() && stopRotationFuture.Get().IsOK());

    auto generator = Components->GetSignatureGenerator();
    auto validator = Components->GetSignatureValidator();

    EXPECT_THROW_WITH_SUBSTRING(YT_UNUSED_FUTURE(generator->Sign("test")), "unsupported");
    auto signature = New<TSignature>();
    EXPECT_THROW_WITH_SUBSTRING(YT_UNUSED_FUTURE(validator->Validate(signature)), "unsupported");
}

TEST_F(TSignatureComponentsTest, Generation)
{
    Config->Generation = GenerationConfig;
    Components = New<TSignatureComponents>(Config, OwnerId, NativeConnection, RotateInvoker);

    WaitFor(Components->StartRotation())
        .ThrowOnError();

    WaitFor(Components->StopRotation())
        .ThrowOnError();

    auto generator = Components->GetSignatureGenerator();
    EXPECT_TRUE(generator);
    auto signature = generator->Sign("test");
    EXPECT_EQ(signature->Payload(), "test");
}

TEST_F(TSignatureComponentsTest, Validation)
{
    Config->Validation = ValidationConfig;
    Components = New<TSignatureComponents>(Config, OwnerId, NativeConnection, RotateInvoker);

    auto validator = Components->GetSignatureValidator();
    auto signature = New<TSignature>();
    auto validationResult = WaitFor(validator->Validate(signature))
        .ValueOrThrow();
    EXPECT_FALSE(validationResult);
}

TEST_F(TSignatureComponentsTest, DontCrashOnCypressFailure)
{
    Config->Generation = GenerationConfig;
    Config->Generation->KeyRotator->KeyRotationOptions.Period = TDuration::MilliSeconds(200);
    Config->Validation = ValidationConfig;
    Components = New<TSignatureComponents>(Config, OwnerId, NativeConnection, RotateInvoker);

    WaitFor(Components->StartRotation())
        .ThrowOnError();

    WaitFor(Components->StopRotation())
        .ThrowOnError();

    // Imitate read-only mode with an invalid path.
    Config->Generation->CypressKeyWriter->Path = Config->Validation->CypressKeyReader->Path = "not/a/path";

    YT_UNUSED_FUTURE(Components->StartRotation());

    WaitFor(Components->RotateOutOfBand())
        .ThrowOnError();

    auto generator = Components->GetSignatureGenerator();
    auto signature = generator->Sign("test");
    EXPECT_TRUE(signature);

    auto validator = Components->GetSignatureValidator();
    EXPECT_THROW_WITH_SUBSTRING(
        WaitFor(validator->Validate(signature))
            .ThrowOnError(),
        "Unexpected \"literal\" token");
}

TEST_F(TSignatureComponentsTest, ReconfigureEnableGeneration)
{
    // Start with empty config.
    Components = New<TSignatureComponents>(Config, OwnerId, NativeConnection, RotateInvoker);
    auto generator = Components->GetSignatureGenerator();

    EXPECT_THROW_WITH_SUBSTRING(YT_UNUSED_FUTURE(generator->Sign("test")), "unsupported");

    Config->Generation = GenerationConfig;
    WaitFor(Components->Reconfigure(Config))
        .ThrowOnError();

    // Generation side should work now.
    EXPECT_EQ(generator->Sign("test")->Payload(), "test");

    WaitFor(Components->StartRotation())
        .ThrowOnError();
    WaitFor(Components->StopRotation())
        .ThrowOnError();
}

TEST_F(TSignatureComponentsTest, ReconfigureDisableGeneration)
{
    Config->Generation = GenerationConfig;
    Components = New<TSignatureComponents>(Config, OwnerId, NativeConnection, RotateInvoker);
    auto generator = Components->GetSignatureGenerator();

    WaitFor(Components->StartRotation())
        .ThrowOnError();

    EXPECT_EQ(generator->Sign("test")->Payload(), "test");

    Config->Generation = nullptr;
    // It should be disabled immediately, so we don't wait for the rotation stop.
    YT_UNUSED_FUTURE(Components->Reconfigure(Config));
    EXPECT_THROW_WITH_SUBSTRING(YT_UNUSED_FUTURE(generator->Sign("test")), "unsupported");

    // Should be a no-op.
    auto startFuture = Components->StartRotation();
    EXPECT_TRUE(startFuture.IsSet() && startFuture.Get().IsOK());
}

TEST_F(TSignatureComponentsTest, ReconfigureEnableValidation)
{
    // Start with an empty config.
    Components = New<TSignatureComponents>(Config, OwnerId, NativeConnection, RotateInvoker);
    auto validator = Components->GetSignatureValidator();

    auto signature = New<TSignature>();
    EXPECT_THROW_WITH_SUBSTRING(YT_UNUSED_FUTURE(validator->Validate(signature)), "unsupported");

    Config->Validation = ValidationConfig;
    YT_UNUSED_FUTURE(Components->Reconfigure(Config));

    auto validationResult = WaitFor(validator->Validate(signature))
        .ValueOrThrow();
    // The validator shouldn't be a dummy one, but shouldn't throw too.
    EXPECT_FALSE(validationResult);
}

TEST_F(TSignatureComponentsTest, ReconfigureDisableValidation)
{
    Config->Validation = ValidationConfig;
    Components = New<TSignatureComponents>(Config, OwnerId, NativeConnection, RotateInvoker);
    auto validator = Components->GetSignatureValidator();

    auto signature = New<TSignature>();
    auto validationResult = WaitFor(validator->Validate(signature))
        .ValueOrThrow();
    EXPECT_FALSE(validationResult);

    Config->Validation = nullptr;
    YT_UNUSED_FUTURE(Components->Reconfigure(Config));

    // Should be an always-throwing now.
    EXPECT_THROW_WITH_SUBSTRING(YT_UNUSED_FUTURE(validator->Validate(signature)), "unsupported");
}

TEST_F(TSignatureComponentsTest, ReconfigureEnableBoth)
{
    // Start with empty config.
    Components = New<TSignatureComponents>(Config, OwnerId, NativeConnection, RotateInvoker);
    auto generator = Components->GetSignatureGenerator();
    auto validator = Components->GetSignatureValidator();

    Config->Generation = GenerationConfig;
    Config->Validation = ValidationConfig;
    WaitFor(Components->Reconfigure(Config))
        .ThrowOnError();

    auto signature = generator->Sign("test");
    EXPECT_EQ(signature->Payload(), "test");

    auto validationResult = WaitFor(validator->Validate(signature))
        .ValueOrThrow();
    EXPECT_TRUE(validationResult);
}

TEST_F(TSignatureComponentsTest, ReconfigureMultipleTimes)
{
    Components = New<TSignatureComponents>(Config, OwnerId, NativeConnection, RotateInvoker);
    auto generator = Components->GetSignatureGenerator();
    auto validator = Components->GetSignatureValidator();

    // First reconfigure: enable generation.
    Config->Generation = GenerationConfig;
    WaitFor(Components->Reconfigure(Config))
        .ThrowOnError();
    auto signature = generator->Sign("test");
    EXPECT_TRUE(signature);

    // Second reconfigure: enable validation too.
    Config->Validation = ValidationConfig;
    YT_UNUSED_FUTURE(Components->Reconfigure(Config));

    auto testSignature = New<TSignature>();
    EXPECT_NO_THROW(WaitFor(validator->Validate(testSignature)).ValueOrThrow());

    // Third reconfigure: disable generation.
    Config->Generation = nullptr;
    YT_UNUSED_FUTURE(Components->Reconfigure(Config));

    EXPECT_THROW_WITH_SUBSTRING(YT_UNUSED_FUTURE(generator->Sign("test")), "unsupported");

    // Validation should still work.
    EXPECT_NO_THROW(WaitFor(validator->Validate(testSignature)).ValueOrThrow());

    // Fourth reconfigure: disable validation too.
    Config->Validation = nullptr;
    YT_UNUSED_FUTURE(Components->Reconfigure(Config));
    EXPECT_THROW_WITH_SUBSTRING(YT_UNUSED_FUTURE(validator->Validate(testSignature)), "unsupported");
}

TEST_F(TSignatureComponentsTest, ReconfigureWhileRotating)
{
    // Start with fast rotation.
    GenerationConfig->KeyRotator->KeyRotationOptions.Period= TDuration::MilliSeconds(10);
    Config->Generation = GenerationConfig;
    Components = New<TSignatureComponents>(Config, OwnerId, NativeConnection, RotateInvoker);
    auto generator = Components->GetSignatureGenerator();

    WaitFor(Components->StartRotation())
        .ThrowOnError();

    // Let it rotate a few times.
    for (int i = 0; i < 100; ++i) {
        YT_UNUSED_FUTURE(Components->RotateOutOfBand());
        EXPECT_EQ(generator->Sign("payload")->Payload(), "payload");
    }

    // Reconfigure with slower rotation.
    Config->Generation->KeyRotator->KeyRotationOptions.Period = TDuration::Hours(10);
    YT_UNUSED_FUTURE(Components->Reconfigure(Config));

    // Should be still working perfectly fine.
    for (int i = 0; i < 100; ++i) {
        YT_UNUSED_FUTURE(Components->RotateOutOfBand());
        EXPECT_EQ(generator->Sign("payload")->Payload(), "payload");
    }

    // Disable generation at all.
    Config->Generation = nullptr;
    YT_UNUSED_FUTURE(Components->Reconfigure(Config));

    EXPECT_THROW_WITH_SUBSTRING(Y_UNUSED(generator->Sign("payload")), "unsupported");

    // Start rotation should be a no-op now.
    auto startFuture = Components->StartRotation();
    EXPECT_TRUE(startFuture.IsSet() && startFuture.Get().IsOK());
}

////////////////////////////////////////////////////////////////////////////////

class TCheckPermissionByAclTest
    : public TApiTestBase
{
public:
    TFuture<TNodeId> CreateUser(const std::string& name)
    {
        TCreateObjectOptions options;
        auto attributes = CreateEphemeralAttributes();
        attributes->Set("name", name);
        options.Attributes = std::move(attributes);
        return Client_->CreateObject(EObjectType::User, options);
    }
};

TEST_F(TCheckPermissionByAclTest, MissingSubjects)
{
    WaitFor(CreateUser("test-user"))
        .ThrowOnError();

    std::string aclString = "[{subjects=[\"buba\"];permissions=[\"read\"];action=\"allow\"}]";
    auto aclNode = ConvertToNode(TYsonString(aclString));

    TCheckPermissionByAclOptions options = {
        .IgnoreMissingSubjects = true,
    };
    auto checkPermissionResult = WaitFor(
        Client_->CheckPermissionByAcl(
            "test-user",
            EPermission::Create,
            aclNode,
            options))
        .ValueOrThrow();

    EXPECT_EQ(1, std::ssize(checkPermissionResult.MissingSubjects));
    EXPECT_EQ("buba", checkPermissionResult.MissingSubjects.back());
    EXPECT_EQ(0, std::ssize(checkPermissionResult.PendingRemovalSubjects));
}

TEST_F(TCheckPermissionByAclTest, PendingRemovalSubjects)
{
    std::vector<TFuture<TNodeId>> userCreationFutures = {
        CreateUser("uba"),
        CreateUser("buba"),
    };

    WaitFor(AllSucceeded(std::move(userCreationFutures)))
        .ThrowOnError();
    WaitFor(Client_->SetNode("//sys/users/buba/@pending_removal", ConvertToYsonString(true)))
        .ThrowOnError();

    std::string aclString = "[{subjects=[\"buba\"];permissions=[\"read\"];action=\"allow\"}]";
    auto aclNode = ConvertToNode(TYsonString(aclString));

    TCheckPermissionByAclOptions options = {
        .IgnorePendingRemovalSubjects = true,
    };
    auto checkPermissionResult = WaitFor(
        Client_->CheckPermissionByAcl(
            "uba",
            EPermission::Create,
            aclNode,
            options))
        .ValueOrThrow();

    EXPECT_EQ(0, std::ssize(checkPermissionResult.MissingSubjects));
    EXPECT_EQ(1, std::ssize(checkPermissionResult.PendingRemovalSubjects));
    EXPECT_EQ("buba", checkPermissionResult.PendingRemovalSubjects.back());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NCppTests

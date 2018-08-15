//%NUM_MASTERS=1
//%NUM_NODES=3
//%NUM_SCHEDULERS=0
//%DELTA_MASTER_CONFIG={ "object_service": { "timeout_backoff_lead_time": 100 } }

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
#include <yt/client/table_client/name_table.h>
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

void PrintTo(const TString& str, ::std::ostream* os)
{
    *os << str;
}

////////////////////////////////////////////////////////////////////////////////

namespace NYT {
namespace {

using namespace NApi;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NSecurityClient;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

class TApiTestBase
    : public ::testing::Test
{
protected:
    static NNative::IConnectionPtr Connection_;
    static NNative::IClientPtr Client_;

    static void SetUpTestCase()
    {
        const auto* configPath = std::getenv("YT_CONSOLE_DRIVER_CONFIG_PATH");
        TIFStream configStream(configPath);
        auto config = ConvertToNode(&configStream)->AsMap();

        if (auto logging = config->FindChild("logging")) {
            NLogging::TLogManager::Get()->Configure(ConvertTo<NLogging::TLogConfigPtr>(logging));
        }

        Connection_ = NApi::NNative::CreateConnection(ConvertTo<NNative::TConnectionConfigPtr>(config->GetChild("driver")));

        CreateClient(RootUserName);
    }

    static void TearDownTestCase()
    {
        Client_.Reset();
        Connection_.Reset();
    }

    static void CreateClient(const TString& userName)
    {
        TClientOptions clientOptions;
        clientOptions.User = userName;
        Client_ = Connection_->CreateNativeClient(clientOptions);
    }
};

NNative::IConnectionPtr TApiTestBase::Connection_;
NNative::IClientPtr TApiTestBase::Client_;

////////////////////////////////////////////////////////////////////////////////

TEST_F(TApiTestBase, TestClusterConnection)
{
    auto resOrError = Client_->GetNode(TYPath("/"));
    EXPECT_TRUE(resOrError.Get().IsOK());
}

TEST_F(TApiTestBase, TestCreateInvalidNode)
{
    auto resOrError = Client_->CreateNode(TYPath("//tmp/a"), EObjectType::SortedDynamicTabletStore);
    EXPECT_FALSE(resOrError.Get().IsOK());
}

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
        auto channel = Client_->GetMasterChannelOrThrow(EMasterChannelKind::Follower);
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
        YCHECK(subrequestCount >= 0);

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
                Y_UNREACHABLE();
        }

        switch (shift + RandomNumber<ui32>(spread)) {
            case 0:
                return "read";
            case 1:
                return "create";
            case 2:
                return "write";
            default:
                Y_UNREACHABLE();
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

        Y_UNREACHABLE();
    }

    TString GetRecentTablePath() const
    {
        YCHECK(!RecentTableGuid_.IsEmpty());
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

class TDynamicTablesTestBase
    : public TApiTestBase
{
public:
    static void TearDownTestCase()
    {
        SyncUnmountTable(Table_);

        WaitFor(Client_->RemoveNode(TYPath("//tmp/*")))
            .ThrowOnError();

        RemoveSystemObjects("//sys/tablet_cells");
        RemoveSystemObjects("//sys/tablet_cell_bundles", [] (const TString& name) {
            return name != "default";
        });

        WaitFor(Client_->SetNode(TYPath("//sys/accounts/tmp/@resource_limits/tablet_count"), ConvertToYsonString(0)))
            .ThrowOnError();

        TApiTestBase::TearDownTestCase();
    }

protected:
    static TYPath Table_;

    static void SetUpTestCase()
    {
        TApiTestBase::SetUpTestCase();

        auto cellId = WaitFor(Client_->CreateObject(EObjectType::TabletCell))
            .ValueOrThrow();
        WaitUntil(TYPath("#") + ToString(cellId) + "/@health", "good");

        WaitFor(Client_->SetNode(TYPath("//sys/accounts/tmp/@resource_limits/tablet_count"), ConvertToYsonString(1000)))
            .ThrowOnError();
    }

    static void CreateTableAndClient(
        const TString& tablePath,
        const TString& schema,
        const TString& userName = RootUserName)
    {
        // Client for root is already created in TApiTestBase::SetUpTestCase
        if (userName != RootUserName) {
            CreateClient(userName);
        }

        Table_ = tablePath;
        ASSERT_TRUE(tablePath.StartsWith("//tmp"));

        auto attributes = TYsonString("{dynamic=%true;schema=" + schema + "}");
        TCreateNodeOptions options;
        options.Attributes = ConvertToAttributes(attributes);

        WaitFor(Client_->CreateNode(Table_, EObjectType::Table, options))
            .ThrowOnError();

        SyncMountTable(Table_);
    }

    static void SyncMountTable(const TYPath& path)
    {
        WaitFor(Client_->MountTable(path))
            .ThrowOnError();
        WaitUntil(path + "/@tablet_state", "mounted");
    }

    static void SyncUnmountTable(const TYPath& path)
    {
        WaitFor(Client_->UnmountTable(path))
            .ThrowOnError();
        WaitUntil(path + "/@tablet_state", "unmounted");
    }

    static void WaitUntil(const TYPath& path, const TString& expected)
    {
        auto start = Now();
        bool reached = false;
        for (int attempt = 0; attempt < 2*30; ++attempt) {
            auto state = WaitFor(Client_->GetNode(path))
                .ValueOrThrow();
            auto value = ConvertTo<IStringNodePtr>(state)->GetValue();
            if (value == expected) {
                reached = true;
                break;
            }
            Sleep(TDuration::MilliSeconds(500));
        }

        if (!reached) {
            THROW_ERROR_EXCEPTION("%Qv is not %Qv after %v seconds",
                path,
                expected,
                (Now() - start).Seconds());
        }
    }

    static std::tuple<TSharedRange<TUnversionedRow>, TNameTablePtr> PrepareUnversionedRow(
        const std::vector<TString>& names,
        const TString& rowString)
    {
        auto nameTable = New<TNameTable>();
        for (const auto& name : names) {
            nameTable->GetIdOrRegisterName(name);
        }

        auto rowBuffer = New<TRowBuffer>();
        auto owningRow = YsonToSchemalessRow(rowString);
        std::vector<TUnversionedRow> rows{rowBuffer->Capture(owningRow.Get())};
        return std::make_tuple(MakeSharedRange(rows, std::move(rowBuffer)), std::move(nameTable));
    }

    static void WriteUnversionedRow(
        std::vector<TString> names,
        const TString& rowString)
    {
        auto preparedRow = PrepareUnversionedRow(names, rowString);
        WriteRows(
            std::get<1>(preparedRow),
            std::get<0>(preparedRow));
    }

    static void WriteRows(
        TNameTablePtr nameTable,
        TSharedRange<TUnversionedRow> rows)
    {
        auto transaction = WaitFor(Client_->StartTransaction(NTransactionClient::ETransactionType::Tablet))
            .ValueOrThrow();

        transaction->WriteRows(
            Table_,
            nameTable,
            rows);

        auto commitResult = WaitFor(transaction->Commit())
            .ValueOrThrow();

        const auto& timestamps = commitResult.CommitTimestamps.Timestamps;
        ASSERT_EQ(1, timestamps.size());
    }

    static std::tuple<TSharedRange<TVersionedRow>, TNameTablePtr> PrepareVersionedRow(
        const std::vector<TString>& names,
        const TString& keyYson,
        const TString& valueYson)
    {
        auto nameTable = New<TNameTable>();
        for (const auto& name : names) {
            nameTable->GetIdOrRegisterName(name);
        }

        auto rowBuffer = New<TRowBuffer>();
        auto row = YsonToVersionedRow(rowBuffer, keyYson, valueYson);
        std::vector<TVersionedRow> rows{row};
        return std::make_tuple(MakeSharedRange(rows, std::move(rowBuffer)), std::move(nameTable));
    }

    static void WriteVersionedRow(
        std::vector<TString> names,
        const TString& keyYson,
        const TString& valueYson)
    {
        auto preparedRow = PrepareVersionedRow(names, keyYson, valueYson);
        WriteRows(
            std::get<1>(preparedRow),
            std::get<0>(preparedRow));
    }

    static void WriteRows(
        TNameTablePtr nameTable,
        TSharedRange<TVersionedRow> rows)
    {
        auto transaction = WaitFor(Client_->StartTransaction(NTransactionClient::ETransactionType::Tablet))
            .ValueOrThrow();

        transaction->WriteRows(
            Table_,
            nameTable,
            rows);

        auto commitResult = WaitFor(transaction->Commit())
            .ValueOrThrow();
    }

private:
    static void RemoveSystemObjects(
        const TYPath& path,
        std::function<bool(const TString&)> filter = [] (const TString&) { return true; })
    {
        auto items = WaitFor(Client_->ListNode(path))
            .ValueOrThrow();
        auto itemsList = ConvertTo<IListNodePtr>(items);

        std::vector<TFuture<void>> asyncWait;
        for (const auto& item : itemsList->GetChildren()) {
            const auto& name = item->AsString()->GetValue();
            if (filter(name)) {
                asyncWait.push_back(Client_->RemoveNode(path + "/" + name));
            }
        }

        WaitFor(Combine(asyncWait))
            .ThrowOnError();
    }
};

TYPath TDynamicTablesTestBase::Table_;

////////////////////////////////////////////////////////////////////////////////

using TLookupFilterTestParam = std::tuple<
    std::vector<TString>,
    TString,
    SmallVector<int, TypicalColumnCount>,
    TString,
    TString,
    TString>;

class TLookupFilterTest
    : public TDynamicTablesTestBase
    , public ::testing::WithParamInterface<TLookupFilterTestParam>
{
public:
    static void SetUpTestCase()
    {
        TDynamicTablesTestBase::SetUpTestCase();

        CreateTableAndClient(
            "//tmp/lookup_test", // tablePath
            "[" // schema
            "{name=k0;type=int64;sort_order=ascending};"
            "{name=k1;type=int64;sort_order=ascending};"
            "{name=k2;type=int64;sort_order=ascending};"
            "{name=v3;type=int64};"
            "{name=v4;type=int64};"
            "{name=v5;type=int64}]");

        InitializeRows();
    }

protected:
    static THashMap<int, TTimestamp> CommitTimestamps_;
    TRowBufferPtr Buffer_ = New<TRowBuffer>();

    static void InitializeRows()
    {
        WriteUnversionedRow(
            {"k0", "k1", "k2", "v3", "v4", "v5"},
            "<id=0> 10; <id=1> 11; <id=2> 12; <id=3> 13; <id=4> 14; <id=5> 15",
            0);
    }

    static void WriteUnversionedRow(
        std::vector<TString> names,
        const TString& rowString,
        int timestampTag)
    {
        auto preparedRow = PrepareUnversionedRow(names, rowString);
        WriteRows(
            std::get<1>(preparedRow),
            std::get<0>(preparedRow),
            timestampTag);
    }

    static void WriteRows(
        TNameTablePtr nameTable,
        TSharedRange<TUnversionedRow> rows,
        int timestampTag)
    {
        auto transaction = WaitFor(Client_->StartTransaction(NTransactionClient::ETransactionType::Tablet))
            .ValueOrThrow();

        transaction->WriteRows(
            Table_,
            nameTable,
            rows);

        auto commitResult = WaitFor(transaction->Commit())
            .ValueOrThrow();

        const auto& timestamps = commitResult.CommitTimestamps.Timestamps;
        ASSERT_EQ(1, timestamps.size());
        CommitTimestamps_[timestampTag] = timestamps[0].second;
    }

    static void DeleteRow(
        std::vector<TString> names,
        const TString& rowString,
        int timestampTag)
    {
        auto preparedKey = PrepareUnversionedRow(names, rowString);
        DeleteRows(
            std::get<1>(preparedKey),
            std::get<0>(preparedKey),
            timestampTag);
    }

    static void DeleteRows(
        TNameTablePtr nameTable,
        TSharedRange<TUnversionedRow> rows,
        int timestampTag)
    {
        auto transaction = WaitFor(Client_->StartTransaction(NTransactionClient::ETransactionType::Tablet))
            .ValueOrThrow();

        transaction->DeleteRows(Table_, nameTable, rows);

        auto commitResult = WaitFor(transaction->Commit())
            .ValueOrThrow();

        const auto& timestamps = commitResult.CommitTimestamps.Timestamps;
        ASSERT_EQ(1, timestamps.size());
        CommitTimestamps_[timestampTag] = timestamps[0].second;
    }

    TVersionedRow BuildVersionedRow(
        const TString& keyYson,
        const TString& valueYson,
        const std::vector<TTimestamp>& extraWriteTimestamps = {},
        const std::vector<TTimestamp>& deleteTimestamps = {})
    {
        auto immutableRow = YsonToVersionedRow(
            Buffer_,
            keyYson,
            valueYson,
            deleteTimestamps,
            extraWriteTimestamps);
        auto row = TMutableVersionedRow(const_cast<TVersionedRowHeader*>(immutableRow.GetHeader()));

        for (auto* value = row.BeginValues(); value < row.EndValues(); ++value) {
            value->Timestamp = CommitTimestamps_.at(value->Timestamp);
        }
        for (auto* timestamp = row.BeginWriteTimestamps(); timestamp < row.EndWriteTimestamps(); ++timestamp) {
            *timestamp = CommitTimestamps_.at(*timestamp);
        }
        for (auto* timestamp = row.BeginDeleteTimestamps(); timestamp < row.EndDeleteTimestamps(); ++timestamp) {
            *timestamp = CommitTimestamps_.at(*timestamp);
        }

        return row;
    }
};

THashMap<int, TTimestamp> TLookupFilterTest::CommitTimestamps_;

////////////////////////////////////////////////////////////////////////////////

static auto s = TString("<unique_keys=%true;strict=%true>");
static auto su = TString("<unique_keys=%false;strict=%true>");
static auto k0 = "{name=k0;type=int64;sort_order=ascending};";
static auto k1 = "{name=k1;type=int64;sort_order=ascending};";
static auto k2 = "{name=k2;type=int64;sort_order=ascending};";
static auto ku0 = "{name=k0;type=int64};";
static auto ku1 = "{name=k1;type=int64};";
static auto ku2 = "{name=k2;type=int64};";
static auto v3 = "{name=v3;type=int64};";
static auto v4 = "{name=v4;type=int64};";
static auto v5 = "{name=v5;type=int64};";


TEST_F(TLookupFilterTest, TestLookupAll)
{
    auto preparedKey = PrepareUnversionedRow(
        {"k0", "k1", "k2"},
        "<id=0> 10; <id=1> 11; <id=2> 12");

    auto res = WaitFor(Client_->LookupRows(
        Table_,
        std::get<1>(preparedKey),
        std::get<0>(preparedKey)))
        .ValueOrThrow();

    auto actual = ToString(res->GetRows()[0]);
    auto expected = ToString(YsonToSchemalessRow("<id=0> 10; <id=1> 11; <id=2> 12; <id=3> 13; <id=4> 14; <id=5> 15"));
    EXPECT_EQ(expected, actual);

    auto schema = ConvertTo<TTableSchema>(TYsonString(
        s + "[" + k0 + k1 + k2 + v3 + v4 + v5 + "]"));

    auto actualSchema = ConvertToYsonString(res->Schema(), EYsonFormat::Text).GetData();
    auto expectedSchema = ConvertToYsonString(schema, EYsonFormat::Text).GetData();
    EXPECT_EQ(expectedSchema, actualSchema);
}

TEST_F(TLookupFilterTest, TestVersionedLookupAll)
{
    auto preparedKey = PrepareUnversionedRow(
        {"k0", "k1", "k2"},
        "<id=0> 10; <id=1> 11; <id=2> 12");

    auto res = WaitFor(Client_->VersionedLookupRows(
        Table_,
        std::get<1>(preparedKey),
        std::get<0>(preparedKey)))
        .ValueOrThrow();

    auto actual = ToString(res->GetRows()[0]);
    auto expected = ToString(BuildVersionedRow(
        "<id=0> 10; <id=1> 11; <id=2> 12",
        "<id=3;ts=0> 13; <id=4;ts=0> 14; <id=5;ts=0> 15"));
    EXPECT_EQ(expected, actual);

    auto schema = ConvertTo<TTableSchema>(TYsonString(
        s + "[" + k0 + k1 + k2 + v3 + v4 + v5 + "]"));

    auto actualSchema = ConvertToYsonString(res->Schema(), EYsonFormat::Text).GetData();
    auto expectedSchema = ConvertToYsonString(schema, EYsonFormat::Text).GetData();
    EXPECT_EQ(expectedSchema, actualSchema);
}

TEST_P(TLookupFilterTest, TestLookupFilter)
{
    const auto& param = GetParam();
    const auto& namedColumns = std::get<0>(param);
    const auto& keyString = std::get<1>(param);
    const auto& columnFilter = std::get<2>(param);
    const auto& resultKeyString = std::get<3>(param);
    const auto& resultValueString = std::get<4>(param);
    const auto& schemaString = std::get<5>(param);
    auto rowString = resultKeyString + resultValueString;

    auto preparedKey = PrepareUnversionedRow(
        namedColumns,
        keyString);

    TLookupRowsOptions options;
    options.ColumnFilter.All = false;
    options.ColumnFilter.Indexes = columnFilter;

    auto res = WaitFor(Client_->LookupRows(
        Table_,
        std::get<1>(preparedKey),
        std::get<0>(preparedKey),
        options))
        .ValueOrThrow();

    ASSERT_EQ(1, res->GetRows().Size());

    auto actual = ToString(res->GetRows()[0]);
    auto expected = ToString(YsonToSchemalessRow(rowString));
    EXPECT_EQ(expected, actual)
        << "key: " << keyString << std::endl
        << "namedColumns: " << ::testing::PrintToString(namedColumns) << std::endl
        << "columnFilter: " << ::testing::PrintToString(columnFilter) << std::endl
        << "expectedRow: " << rowString << std::endl
        << "expectedSchema: " << schemaString << std::endl;

    auto schema = ConvertTo<TTableSchema>(TYsonString(schemaString));
    auto actualSchema = ConvertToYsonString(res->Schema(), EYsonFormat::Text).GetData();
    auto expectedSchema = ConvertToYsonString(schema, EYsonFormat::Text).GetData();
    EXPECT_EQ(expectedSchema, actualSchema)
        << "key: " << keyString << std::endl
        << "namedColumns: " << ::testing::PrintToString(namedColumns) << std::endl
        << "columnFilter: " << ::testing::PrintToString(columnFilter) << std::endl
        << "expectedRow: " << rowString << std::endl
        << "expectedSchema: " << schemaString << std::endl;
}

TEST_P(TLookupFilterTest, TestVersionedLookupFilter)
{
    const auto& param = GetParam();
    const auto& namedColumns = std::get<0>(param);
    const auto& keyString = std::get<1>(param);
    const auto& columnFilter = std::get<2>(param);
    const auto& resultKeyString = std::get<3>(param);
    const auto& resultValueString = std::get<4>(param);
    const auto& schemaString = std::get<5>(param);

    bool hasNonKeyColumns = false;
    for (const auto& column : namedColumns) {
        if (column.StartsWith("v")) {
            hasNonKeyColumns = true;
        }
    }

    auto preparedKey = PrepareUnversionedRow(
        namedColumns,
        keyString);

    TVersionedLookupRowsOptions options;
    options.ColumnFilter.All = false;
    options.ColumnFilter.Indexes = columnFilter;

    auto res = WaitFor(Client_->VersionedLookupRows(
        Table_,
        std::get<1>(preparedKey),
        std::get<0>(preparedKey),
        options))
        .ValueOrThrow();

    ASSERT_EQ(1, res->GetRows().Size());

    auto actual = ToString(res->GetRows()[0]);
    auto expected = ToString(BuildVersionedRow(
        resultKeyString,
        resultValueString,
        hasNonKeyColumns ? std::vector<TTimestamp>{} : std::vector<TTimestamp>{0}));
    EXPECT_EQ(expected, actual)
        << "key: " << keyString << std::endl
        << "namedColumns: " << ::testing::PrintToString(namedColumns) << std::endl
        << "columnFilter: " << ::testing::PrintToString(columnFilter) << std::endl
        << "expectedRowKeys: " << resultKeyString << std::endl
        << "expectedRowValues: " << resultValueString << std::endl
        << "expectedSchema: " << schemaString << std::endl;

    auto schema = ConvertTo<TTableSchema>(TYsonString(schemaString));
    auto actualSchema = ConvertToYsonString(res->Schema(), EYsonFormat::Text).GetData();
    auto expectedSchema = ConvertToYsonString(schema, EYsonFormat::Text).GetData();
    EXPECT_EQ(expectedSchema, actualSchema)
        << "key: " << keyString << std::endl
        << "namedColumns: " << ::testing::PrintToString(namedColumns) << std::endl
        << "columnFilter: " << ::testing::PrintToString(columnFilter) << std::endl
        << "expectedRowKeys: " << resultKeyString << std::endl
        << "expectedRowValues: " << resultValueString << std::endl
        << "expectedSchema: " << schemaString << std::endl;
}

INSTANTIATE_TEST_CASE_P(
    TLookupFilterTest,
    TLookupFilterTest,
    ::testing::Values(
        TLookupFilterTestParam(
            {"k0", "k1", "k2"},
            "<id=0> 10; <id=1> 11; <id=2> 12;",
            {0,1,2},
            "<id=0> 10; <id=1> 11; <id=2> 12;", "",
            s + "[" + k0 + k1 + k2 + "]"),
        TLookupFilterTestParam(
            {"k0", "k1", "k2"},
            "<id=0> 10; <id=1> 11; <id=2> 12;",
            {0,2,1},
            "<id=0> 10; <id=1> 12; <id=2> 11;", "",
            su + "[" + k0 + ku2 + ku1 + "]"),
        TLookupFilterTestParam(
            {"k1", "k0", "k2"},
            "<id=2> 12; <id=0> 11; <id=1> 10;",
            {1,0,2},
            "<id=0> 10; <id=1> 11; <id=2> 12;", "",
            s + "[" + k0 + k1 + k2 + "]"),
        TLookupFilterTestParam(
            {"k0", "k1", "k2", "v3", "v4", "v5"},
            "<id=0> 10; <id=1> 11; <id=2> 12;",
            {3,4,5},
            "", "<id=0;ts=0> 13; <id=1;ts=0> 14; <id=2;ts=0> 15;",
            su + "[" + v3 + v4 + v5 + "]"),
        TLookupFilterTestParam(
            {"k0", "k1", "k2", "v3", "v4", "v5"},
            "<id=0> 10; <id=1> 11; <id=2> 12;",
            {1,5,3},
            "<id=0> 11;", "<id=1;ts=0> 15; <id=2;ts=0> 13;",
            su + "[" + ku1 + v5 + v3 + "]"),
        TLookupFilterTestParam(
            {"k0", "k1", "k2", "v3", "v4", "v5"},
            "<id=0> 10; <id=1> 11; <id=2> 12;",
            {3,4,5},
            "", "<id=0;ts=0> 13; <id=1;ts=0> 14; <id=2;ts=0> 15;",
            su + "[" + v3 + v4 + v5 + "]"),
        TLookupFilterTestParam(
            {"k0", "k1", "k2", "v3", "v4", "v5"},
            "<id=0> 10; <id=1> 11; <id=2> 12;",
            {5,3,4},
            "", "<id=0;ts=0> 15; <id=1;ts=0> 13; <id=2;ts=0> 14;",
            su + "[" + v5 + v3 + v4 + "]"),
        TLookupFilterTestParam(
            {"k1", "k0", "k2", "v5", "v3", "v4"},
            "<id=2> 12; <id=0> 11; <id=1> 10;",
            {1,0,2,4,5,3},
            "<id=0> 10; <id=1> 11; <id=2> 12;", "<id=3;ts=0> 13; <id=4;ts=0> 14; <id=5;ts=0> 15;",
            s + "[" + k0 + k1 + k2 + v3 + v4 + v5 + "]"),
        TLookupFilterTestParam(
            {"k1", "k0", "k2", "v5", "v3", "v4"},
            "<id=2> 12; <id=0> 11; <id=1> 10;",
            {2,1,5,4},
            "<id=0> 12; <id=1> 10;", "<id=2;ts=0> 14; <id=3;ts=0> 13;",
            su + "[" + ku2 + ku0 + v4 + v3 + "]")
));

TEST_F(TLookupFilterTest, TestRetentionConfig)
{
    WriteUnversionedRow(
        {"k0", "k1", "k2", "v3", "v4", "v5"},
        "<id=0> 20; <id=1> 20; <id=2> 20; <id=3> 20;",
        1);
    WriteUnversionedRow(
        {"k0", "k1", "k2", "v3", "v4", "v5"},
        "<id=0> 20; <id=1> 20; <id=2> 20; <id=3> 21;",
        2);

    auto preparedKey = PrepareUnversionedRow(
        {"k0", "k1", "k2", "v4"},
        "<id=0> 20; <id=1> 20; <id=2> 20");

    auto res = WaitFor(Client_->VersionedLookupRows(
        Table_,
        std::get<1>(preparedKey),
        std::get<0>(preparedKey)))
        .ValueOrThrow();

    ASSERT_EQ(1, res->GetRows().Size());

    auto actual = ToString(res->GetRows()[0]);
    auto expected = ToString(BuildVersionedRow(
        "<id=0> 20; <id=1> 20; <id=2> 20",
        "<id=3;ts=2> 21; <id=3;ts=1> 20;"));
    EXPECT_EQ(expected, actual);

    TVersionedLookupRowsOptions options;
    options.RetentionConfig = New<TRetentionConfig>();
    options.RetentionConfig->MinDataTtl = TDuration::MilliSeconds(0);
    options.RetentionConfig->MaxDataTtl = TDuration::MilliSeconds(1800000);
    options.RetentionConfig->MinDataVersions = 1;
    options.RetentionConfig->MaxDataVersions = 1;
    options.Timestamp = CommitTimestamps_[2] + 1;

    res = WaitFor(Client_->VersionedLookupRows(
        Table_,
        std::get<1>(preparedKey),
        std::get<0>(preparedKey),
        options))
        .ValueOrThrow();

    ASSERT_EQ(1, res->GetRows().Size());

    actual = ToString(res->GetRows()[0]);
    expected = ToString(BuildVersionedRow(
        "<id=0> 20; <id=1> 20; <id=2> 20",
        "<id=3;ts=2> 21;"));
    EXPECT_EQ(expected, actual);

    options.ColumnFilter.All = false;
    options.ColumnFilter.Indexes = {0,1,2,3};

    res = WaitFor(Client_->VersionedLookupRows(
        Table_,
        std::get<1>(preparedKey),
        std::get<0>(preparedKey),
        options))
        .ValueOrThrow();

    ASSERT_EQ(1, res->GetRows().Size());

    actual = ToString(res->GetRows()[0]);
    expected = ToString(BuildVersionedRow(
        "<id=0> 20; <id=1> 20; <id=2> 20",
        "",
        {2}));
    EXPECT_EQ(expected, actual);

    options.ColumnFilter.Indexes = {3};

    preparedKey = PrepareUnversionedRow(
        {"k0", "k1", "k2", "v3"},
        "<id=0> 20; <id=1> 20; <id=2> 20");
    res = WaitFor(Client_->VersionedLookupRows(
        Table_,
        std::get<1>(preparedKey),
        std::get<0>(preparedKey),
        options))
        .ValueOrThrow();

    ASSERT_EQ(1, res->GetRows().Size());

    actual = ToString(res->GetRows()[0]);
    expected = ToString(BuildVersionedRow(
        "",
        "<id=0;ts=2> 21;"));
    EXPECT_EQ(expected, actual);
}

// YT-7668
// Checks that in cases like
//   insert(key=k, value1=x, value2=y)
//   delete(key=k)
//   insert(key=k, value1=x)
//   versioned_lookup(key=k, column_filter=[value1])
// the information about the presence of the second insertion is not lost,
// although no versioned values are returned.
TEST_F(TLookupFilterTest, TestFilteredOutTimestamps)
{
    auto preparedKey = PrepareUnversionedRow(
        {"k0", "k1", "k2", "v3", "v4", "v5"},
        "<id=0> 30; <id=1> 30; <id=2> 30");
    TVersionedLookupRowsOptions options;

    auto executeLookup = [&] {
        auto res = WaitFor(Client_->VersionedLookupRows(
            Table_,
            std::get<1>(preparedKey),
            std::get<0>(preparedKey),
            options)).ValueOrThrow();
        EXPECT_EQ(1, res->GetRows().Size());
        return ToString(res->GetRows()[0]);
    };

    WriteUnversionedRow(
        {"k0", "k1", "k2", "v3", "v4", "v5"},
        "<id=0> 30; <id=1> 30; <id=2> 30; <id=3> 1; <id=4> 1; <id=5> 1",
        1);

    DeleteRows(std::get<1>(preparedKey), std::get<0>(preparedKey), 2);

    WriteUnversionedRow(
        {"k0", "k1", "k2", "v3"},
        "<id=0> 30; <id=1> 30; <id=2> 30; <id=3> 3;",
        3);

    options.ColumnFilter.All = true;
    options.RetentionConfig = New<TRetentionConfig>();
    options.RetentionConfig->MinDataTtl = TDuration::MilliSeconds(0);
    options.RetentionConfig->MaxDataTtl = TDuration::MilliSeconds(1800000);
    options.RetentionConfig->MinDataVersions = 1;
    options.RetentionConfig->MaxDataVersions = 1;

    auto actual = executeLookup();
    auto expected = ToString(BuildVersionedRow(
        "<id=0> 30; <id=1> 30; <id=2> 30",
        "<id=3;ts=3> 3",
        {},
        {2}));
    EXPECT_EQ(expected, actual);

    options.ColumnFilter.All = false;
    options.ColumnFilter.Indexes = {0, 1, 2, 4};

    actual = executeLookup();
    expected = ToString(BuildVersionedRow(
        "<id=0> 30; <id=1> 30; <id=2> 30",
        "",
        {3},
        {2}));
    EXPECT_EQ(expected, actual);

    WriteUnversionedRow(
        {"k0", "k1", "k2", "v4"},
        "<id=0> 30; <id=1> 30; <id=2> 30; <id=3> 4",
        4);

    actual = executeLookup();
    expected = ToString(BuildVersionedRow(
        "<id=0> 30; <id=1> 30; <id=2> 30",
        "<id=3;ts=4> 4",
        {3},
        {2}
    ));
    EXPECT_EQ(expected, actual);

    DeleteRows(std::get<1>(preparedKey), std::get<0>(preparedKey), 5);

    WriteUnversionedRow(
        {"k0", "k1", "k2", "v3"},
        "<id=0> 30; <id=1> 30; <id=2> 30; <id=3> 6;",
        6);

    options.ColumnFilter.Indexes = {0, 1, 2, 4, 5};
    options.RetentionConfig->MinDataVersions = 2;
    options.RetentionConfig->MaxDataVersions = 2;

    actual = executeLookup();
    expected = ToString(BuildVersionedRow(
        "<id=0> 30; <id=1> 30; <id=2> 30;",
        "<id=3;ts=4> 4",
        {6},
        {2, 5}
    ));
    EXPECT_EQ(expected, actual);

    options.RetentionConfig->MinDataVersions = 1;
    options.RetentionConfig->MaxDataVersions = 1;

    actual = executeLookup();
    expected = ToString(BuildVersionedRow(
        "<id=0> 30; <id=1> 30; <id=2> 30;",
        "",
        {6},
        {2, 5}
    ));
    EXPECT_EQ(expected, actual);
}

////////////////////////////////////////////////////////////////////////////////

class TVersionedWriteTest
    : public TDynamicTablesTestBase
{
public:
    static void SetUpTestCase()
    {
        TDynamicTablesTestBase::SetUpTestCase();
        CreateTableAndClient(
            "//tmp/write_test", // tablePath
            "[" // schema
            "{name=k0;type=int64;sort_order=ascending};"
            "{name=k1;type=int64;sort_order=ascending};"
            "{name=k2;type=int64;sort_order=ascending};"
            "{name=v3;type=int64};"
            "{name=v4;type=int64};"
            "{name=v5;type=int64}]",
            ReplicatorUserName // userName
        );
    }

protected:
    TRowBufferPtr Buffer_ = New<TRowBuffer>();

    TVersionedRow BuildVersionedRow(
        const TString& keyYson,
        const TString& valueYson)
    {
        return YsonToVersionedRow(Buffer_, keyYson, valueYson);
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TVersionedWriteTest, TestWriteRemapping)
{
    WriteVersionedRow(
        {"k0", "k1", "k2", "v5", "v3", "v4"},
        "<id=0> 30; <id=1> 30; <id=2> 30;",
        "<id=3;ts=1> 15; <id=4;ts=1> 13; <id=5;ts=1> 14;");
    WriteVersionedRow(
        {"k0", "k1", "k2", "v4", "v5", "v3"},
        "<id=0> 30; <id=1> 30; <id=2> 30;",
        "<id=3;ts=2> 24; <id=4;ts=2> 25; <id=5;ts=2> 23;");

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
            "<id=3;ts=3> 100;"),
        TErrorException);

    EXPECT_THROW(
        WriteVersionedRow(
            {"k0", "k1", "v3", "k2"},
            "<id=0> 30; <id=1> 30; <id=3> 30;",
            "<id=2;ts=3> 100;"),
        TErrorException);
}

TEST_F(TVersionedWriteTest, TestWriteTypeChecking)
{
    EXPECT_THROW(
        WriteVersionedRow(
            {"k0", "k1", "k2", "v3"},
            "<id=0> 30; <id=1> 30; <id=2> 30;",
            "<id=2;ts=3> %true;"),
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
            "<id=0> 20; <id=1> 21; <id=2> 22; <id=3> 13; <id=4> 14; <id=5> 15; <id=5> 25"),
        TErrorException);
}

TEST_F(TLookupFilterTest, TestLookupDuplicateKeyColumns)
{
    auto preparedKey = PrepareUnversionedRow(
        {"k0", "k1", "k2"},
        "<id=0> 20; <id=1> 21; <id=2> 22; <id=2> 22");

    EXPECT_THROW(WaitFor(Client_->LookupRows(
        Table_,
        std::get<1>(preparedKey),
        std::get<0>(preparedKey)))
        .ValueOrThrow(), TErrorException);
}

////////////////////////////////////////////////////////////////////////////////

class TVersionedWriteTestWithRequired
    : public TVersionedWriteTest
{
public:
    static void SetUpTestCase()
    {
        TDynamicTablesTestBase::SetUpTestCase();
        CreateTableAndClient(
            "//tmp/write_test_required", // tablePath
            "[" // schema
            "{name=k0;type=int64;sort_order=ascending};"
            "{name=k1;type=int64;sort_order=ascending};"
            "{name=k2;type=int64;sort_order=ascending};"
            "{name=v3;type=int64;required=%true};"
            "{name=v4;type=int64};"
            "{name=v5;type=int64}]",
            ReplicatorUserName // userName
        );
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TVersionedWriteTestWithRequired, TestNoRequiredColumns)
{
    EXPECT_THROW(
        WriteVersionedRow(
            {"k0", "k1", "k2", "v4"},
            "<id=0> 30; <id=1> 30; <id=2> 30;",
            "<id=3;ts=1> 10"),
        TErrorException);

    EXPECT_THROW(
        WriteVersionedRow(
            {"k0", "k1", "k2", "v3", "v4"},
            "<id=0> 30; <id=1> 30; <id=2> 30;",
            "<id=3;ts=2> 10; <id=4;ts=2> 10; <id=4;ts=1> 15"),
        TErrorException);

    EXPECT_THROW(
        WriteVersionedRow(
            {"k0", "k1", "k2", "v3", "v4"},
            "<id=0> 30; <id=1> 30; <id=2> 30;",
            "<id=4;ts=2> 10; <id=4;ts=1> 15"),
        TErrorException);

    WriteVersionedRow(
        {"k0", "k1", "k2", "v3", "v4"},
        "<id=0> 40; <id=1> 40; <id=2> 40;",
        "<id=3;ts=2> 10; <id=3;ts=1> 20; <id=4;ts=1> 15");
}

////////////////////////////////////////////////////////////////////////////////

class TOrderedDynamicTablesTest
    : public TDynamicTablesTestBase
{
public:
    static void SetUpTestCase()
    {
        TDynamicTablesTestBase::SetUpTestCase();

        CreateTableAndClient(
            "//tmp/write_ordered_test", // tablePath
            "[" // schema
            "{name=v1;type=int64};"
            "{name=v2;type=int64};"
            "{name=v3;type=int64}]");
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TOrderedDynamicTablesTest, TestOrderedTableWrite)
{
    WriteUnversionedRow(
        {"v3", "v1", "v2"},
        "<id=0> 15; <id=1> 13; <id=2> 14;");
    WriteUnversionedRow(
        {"v2", "v3", "v1"},
        "<id=0> 24; <id=1> 25; <id=2> 23;");

    WriteUnversionedRow(
        {"v3", "v1", "v2", "$tablet_index"},
        "<id=0> 15; <id=1> 13; <id=2> 14; <id=3> #;");
    WriteUnversionedRow(
        {"v2", "v3", "v1", "$tablet_index"},
        "<id=0> 24; <id=1> 25; <id=2> 23; <id=3> 0;");

    auto res = WaitFor(Client_->SelectRows(Format("* from [%s]", Table_))).ValueOrThrow();
    auto rows = res.Rowset->GetRows();

    ASSERT_EQ(4, rows.Size());

    auto actual = ToString(rows[0]);
    auto expected = ToString(YsonToSchemalessRow(
        "<id=0> 0; <id=1> 0; <id=2> 13; <id=3> 14; <id=4> 15;"));
    EXPECT_EQ(expected, actual);

    actual = ToString(rows[1]);
    expected = ToString(YsonToSchemalessRow(
        "<id=0> 0; <id=1> 1; <id=2> 23; <id=3> 24; <id=4> 25;"));
    EXPECT_EQ(expected, actual);

    actual = ToString(rows[2]);
    expected = ToString(YsonToSchemalessRow(
        "<id=0> 0; <id=1> 2; <id=2> 13; <id=3> 14; <id=4> 15;"));
    EXPECT_EQ(expected, actual);

    actual = ToString(rows[3]);
    expected = ToString(YsonToSchemalessRow(
        "<id=0> 0; <id=1> 3; <id=2> 23; <id=3> 24; <id=4> 25;"));
    EXPECT_EQ(expected, actual);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT

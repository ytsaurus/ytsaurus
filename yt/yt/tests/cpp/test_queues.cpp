#include <yt/yt/tests/cpp/test_base/api_test_base.h>
#include <yt/yt/tests/cpp/test_base/private.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/comparator.h>

#include <yt/yt/client/security_client/acl.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/client/queue_client/consumer_client.h>
#include <yt/yt/client/queue_client/partition_reader.h>
#include <yt/yt/client/queue_client/producer_client.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/concurrency/thread_pool.h>

#include <library/cpp/yt/string/format.h>

////////////////////////////////////////////////////////////////////////////////

namespace NYT::NYPath {

void PrintTo(TRichYPath value, ::std::ostream* os)
{
    *os << ToString(value);
}

} // namespace NYT::NYPath

namespace NYT::NApi {

void PrintTo(TListQueueConsumerRegistrationsResult value, ::std::ostream* os)
{
    *os << Format("QueuePath: %v, ConsumerPath: %v, Vital: %v", value.QueuePath, value.ConsumerPath, value.Vital);
}

} // namespace NYT::NApi

////////////////////////////////////////////////////////////////////////////////

namespace NYT::NCppTests {
namespace {

using namespace NApi;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NQueueClient;
using namespace NSecurityClient;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NYPath;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TString MakeValueRow(const std::vector<TString>& values)
{
    TString result;
    for (int i = 0; i < std::ssize(values); ++i) {
        result += Format("%v<id=%v> %v;", (i == 0 ? "" : " "), i, values[i]);
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

class TQueueTestBase
    : public TDynamicTablesTestBase
{
public:
    static constexpr auto RegistrationTablePath = "//sys/queue_agents/consumer_registrations";

    static void SetUpTestCase()
    {
        TDynamicTablesTestBase::SetUpTestCase();

        // TODO(achulkov2): Separate useful teardown methods from TDynamicTablesTestBase and stop inheriting from it altogether.
        CreateTable(
            "//tmp/fake", // tablePath
            "[" // schema
            "{name=key;type=uint64;sort_order=ascending};"
            "{name=value;type=uint64}]");

        CreateTableOnce(RegistrationTablePath, GetQueueAgentRegistrationTableSchema());
    }

    class TDynamicTable final
    {
    public:
        explicit TDynamicTable(
            TRichYPath path,
            TTableSchemaPtr schema,
            const IAttributeDictionaryPtr& extraAttributes = CreateEphemeralAttributes())
            : RichPath_(std::move(path))
            , Path_(RichPath_.GetPath())
            , Schema_(std::move(schema))
        {
            TCreateNodeOptions options;
            options.Attributes = extraAttributes;
            options.Attributes->Set("dynamic", true);
            options.Attributes->Set("schema", Schema_);

            WaitFor(Client_->CreateNode(Path_, EObjectType::Table, options))
                .ThrowOnError();

            SyncMountTable(Path_);
        }

        ~TDynamicTable()
        {
            SyncUnmountTable(Path_);

            WaitFor(Client_->RemoveNode(Path_))
                .ThrowOnError();
        }

        const TTableSchemaPtr& GetSchema() const
        {
            return Schema_;
        }

        const TYPath& GetPath() const
        {
            return Path_;
        }

        const TRichYPath& GetRichPath() const
        {
            return RichPath_;
        }

        const TRichYPath GetRichPathWithCluster() const
        {
            auto copy = RichPath_;
            copy.SetCluster(ClusterName_);
            return copy;
        }

    private:
        TRichYPath RichPath_;
        TYPath Path_;
        TTableSchemaPtr Schema_;
    };

    static void WriteSharedRange(const TYPath& path, const TNameTablePtr& nameTable, const TSharedRange<TUnversionedRow>& range)
    {
        auto transaction = WaitFor(Client_->StartTransaction(ETransactionType::Tablet))
            .ValueOrThrow();
        transaction->WriteRows(path, nameTable, range);

        WaitFor(transaction->Commit())
            .ThrowOnError();
    }

    static void WriteSingleRow(const TYPath& path, const TNameTablePtr& nameTable, TUnversionedRow row)
    {
        auto transaction = WaitFor(Client_->StartTransaction(ETransactionType::Tablet))
            .ValueOrThrow();

        TUnversionedRowsBuilder rowsBuilder;
        rowsBuilder.AddRow(row);
        transaction->WriteRows(path, nameTable, rowsBuilder.Build());

        WaitFor(transaction->Commit())
            .ThrowOnError();
    }

    static void WriteSingleRow(const TYPath& path, const TNameTablePtr& nameTable, const std::vector<TString>& values)
    {
        auto owningRow = YsonToSchemalessRow(MakeValueRow(values));
        WriteSingleRow(path, nameTable, owningRow);
    }

    static void WaitForRowCount(const TYPath& path, i64 rowCount)
    {
        WaitForPredicate([rowCount, path] {
            auto allRowsResult = WaitFor(Client_->SelectRows(Format("* from [%v]", path)))
                .ValueOrThrow();

            return std::ssize(allRowsResult.Rowset->GetRows()) == rowCount;
        },
        Format("%v rows were expected", rowCount));
    }

    auto CreateQueueAndConsumer(const TString& testName, std::optional<bool> useNativeTabletNodeApi = {}, int queueTabletCount = 1) const
    {
        auto queueAttributes = CreateEphemeralAttributes();
        queueAttributes->Set("tablet_count", queueTabletCount);
        TRichYPath queuePath = Format("//tmp/queue_%v_%v", testName, useNativeTabletNodeApi);
        auto queue = New<TDynamicTable>(
            queuePath,
            New<TTableSchema>(std::vector<TColumnSchema>{
                TColumnSchema("a", EValueType::Uint64),
                TColumnSchema("b", EValueType::String)}),
            queueAttributes);
        TRichYPath consumerPath = Format("//tmp/consumer_%v_%v", testName, useNativeTabletNodeApi);
        auto consumer = New<TDynamicTable>(
            consumerPath,
            New<TTableSchema>(std::vector<TColumnSchema>{
                TColumnSchema("ShardId", EValueType::Uint64, ESortOrder::Ascending),
                TColumnSchema("Offset", EValueType::Uint64),
            }, /*strict*/ true, /*uniqueKeys*/ true));
        WaitFor(Client_->SetNode(consumer->GetPath() + "/@target_queue", ConvertToYsonString("primary:" + queue->GetPath())))
            .ThrowOnError();
        WaitFor(Client_->SetNode(queue->GetPath() + "/@inherit_acl", ConvertToYsonString(false)))
            .ThrowOnError();
        WaitFor(Client_->SetNode(consumer->GetPath() + "/@inherit_acl", ConvertToYsonString(false)))
            .ThrowOnError();

        auto queueNameTable = TNameTable::FromSchema(*queue->GetSchema());

        return std::tuple{queue, consumer, queueNameTable};
    }

    void CreateQueueProducer(const TRichYPath& path)
    {
        WaitFor(Client_->CreateNode(path.GetPath(), EObjectType::QueueProducer, TCreateNodeOptions{}))
            .ThrowOnError();

        WaitUntilEqual(path.GetPath() + "/@tablet_state", "mounted");
    }

    // NB: Only creates user once per test YT instance.
    IClientPtr CreateUser(const TString& name) const
    {
        if (!WaitFor(Client_->NodeExists("//sys/users/" + name)).ValueOrThrow()) {
            TCreateObjectOptions options;
            auto attributes = CreateEphemeralAttributes();
            attributes->Set("name", name);
            options.Attributes = std::move(attributes);
            WaitFor(Client_->CreateObject(NObjectClient::EObjectType::User, options))
                .ThrowOnError();
        }

        return CreateClient(name);
    }

    void AssertPermission(const std::string& user, const TYPath& path, EPermission permission, ESecurityAction action) const
    {
        auto permissionResponse = WaitFor(Client_->CheckPermission(user, path, permission))
            .ValueOrThrow();
        ASSERT_EQ(permissionResponse.Action, action);
    }

    void AssertPermissionAllowed(const std::string& user, const TYPath& path, EPermission permission) const
    {
        AssertPermission(user, path, permission, ESecurityAction::Allow);
    }

    void AssertPermissionDenied(const std::string& user, const TYPath& path, EPermission permission) const
    {
        AssertPermission(user, path, permission, ESecurityAction::Deny);
    }

    static const TTableSchemaPtr& GetQueueAgentRegistrationTableSchema()
    {
        static const TTableSchemaPtr RegistrationTableSchema = New<TTableSchema>(std::vector<TColumnSchema>{
            TColumnSchema("queue_cluster", EValueType::String, ESortOrder::Ascending),
            TColumnSchema("queue_path", EValueType::String, ESortOrder::Ascending),
            TColumnSchema("consumer_cluster", EValueType::String, ESortOrder::Ascending),
            TColumnSchema("consumer_path", EValueType::String, ESortOrder::Ascending),
            TColumnSchema("vital", EValueType::Boolean),
            TColumnSchema("partitions", EValueType::Any),
        });

        return RegistrationTableSchema;
    }

    static void CreateTableOnce(const TYPath& path, const TTableSchemaPtr& schema)
    {
        if (!WaitFor(Client_->NodeExists(path)).ValueOrThrow()) {
            TCreateNodeOptions options;
            options.Attributes = CreateEphemeralAttributes();
            options.Attributes->Set("dynamic", true);
            options.Attributes->Set("schema", schema);
            options.Recursive = true;
            WaitFor(Client_->CreateNode(path, EObjectType::Table, options))
                .ThrowOnError();
            SyncMountTable(path);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

using TQueueApiPermissionsTest = TQueueTestBase;

////////////////////////////////////////////////////////////////////////////////

TEST_W(TQueueApiPermissionsTest, PullQueue)
{
    auto testUser = "u1";
    auto userClient = CreateUser(testUser);

    for (auto useNativeTabletNodeApi : std::vector<bool>{false, true}) {
        auto [queue, _, queueNameTable] =
            CreateQueueAndConsumer("PullQueue", useNativeTabletNodeApi);

        WriteSingleRow(queue->GetPath(), queueNameTable, {"42u", "a"});
        WriteSingleRow(queue->GetPath(), queueNameTable, {"43u", "b"});
        WriteSingleRow(queue->GetPath(), queueNameTable, {"44u", "c"});

        SyncFlushTable(queue->GetPath());

        WriteSingleRow(queue->GetPath(), queueNameTable, {"45u", "d"});
        WriteSingleRow(queue->GetPath(), queueNameTable, {"46u", "e"});

        AssertPermissionDenied(testUser, queue->GetPath(), EPermission::Read);

        auto rowsetOrError = WaitFor(userClient->PullQueue(queue->GetPath(), 0, 0, {}));
        EXPECT_FALSE(rowsetOrError.IsOK());
        EXPECT_TRUE(rowsetOrError.FindMatching(NSecurityClient::EErrorCode::AuthorizationError));
        EXPECT_TRUE(ToString(rowsetOrError).Contains("No Read permission for //tmp/queue"));

        WaitFor(Client_->SetNode(
            queue->GetPath() + "/@acl/end",
            ConvertToYsonString(TSerializableAccessControlEntry(
                ESecurityAction::Allow,
                {testUser},
                EPermission::Read))))
            .ThrowOnError();

        AssertPermissionAllowed(testUser, queue->GetPath(), EPermission::Read);

        auto rowset = WaitFor(userClient->PullQueue(queue->GetRichPath(), 0, 0, {}))
            .ValueOrThrow();
        EXPECT_FALSE(rowset->GetRows().empty());

        WaitFor(Client_->SetNode(
            queue->GetPath() + "/@acl/end",
            ConvertToYsonString(TSerializableAccessControlEntry(
                ESecurityAction::Deny,
                {testUser},
                EPermission::Read))))
            .ThrowOnError();

        AssertPermissionDenied(testUser, queue->GetPath(), EPermission::Read);

        rowsetOrError = WaitFor(userClient->PullQueue(queue->GetPath(), 0, 0, {}));
        EXPECT_FALSE(rowsetOrError.IsOK());
        EXPECT_TRUE(rowsetOrError.FindMatching(NSecurityClient::EErrorCode::AuthorizationError));
    }
}

TEST_W(TQueueApiPermissionsTest, PullQueueConsumer)
{
    auto testUser = "u1";
    auto userClient = CreateUser(testUser);

    auto [queue, consumer, queueNameTable] =
        CreateQueueAndConsumer("PullQueueConsumer");

    WriteSingleRow(queue->GetPath(), queueNameTable, {"42u", "a"});
    WriteSingleRow(queue->GetPath(), queueNameTable, {"43u", "b"});
    WriteSingleRow(queue->GetPath(), queueNameTable, {"44u", "c"});

    SyncFlushTable(queue->GetPath());

    WriteSingleRow(queue->GetPath(), queueNameTable, {"45u", "d"});
    WriteSingleRow(queue->GetPath(), queueNameTable, {"46u", "e"});

    AssertPermissionDenied(testUser, queue->GetPath(), EPermission::Read);
    AssertPermissionDenied(testUser, consumer->GetPath(), EPermission::Read);

    auto rowsetOrError = WaitFor(userClient->PullQueueConsumer(consumer->GetPath(), queue->GetPath(), 0, 0, {}));
    EXPECT_FALSE(rowsetOrError.IsOK());
    EXPECT_TRUE(rowsetOrError.FindMatching(NSecurityClient::EErrorCode::AuthorizationError));
    EXPECT_TRUE(ToString(rowsetOrError).Contains("No Read permission for //tmp/consumer"));

    WaitFor(Client_->SetNode(
        consumer->GetPath() + "/@acl/end",
        ConvertToYsonString(TSerializableAccessControlEntry(
            ESecurityAction::Allow,
            {testUser},
            EPermission::Read))))
        .ThrowOnError();

    rowsetOrError = WaitFor(userClient->PullQueueConsumer(consumer->GetPath(), queue->GetPath(), 0, 0, {}));
    EXPECT_FALSE(rowsetOrError.IsOK());
    EXPECT_TRUE(rowsetOrError.FindMatching(NSecurityClient::EErrorCode::AuthorizationError));

    WaitFor(Client_->RegisterQueueConsumer(queue->GetPath(), consumer->GetRichPath(), /*vital*/ false))
        .ThrowOnError();

    auto rowset = WaitFor(userClient->PullQueueConsumer(consumer->GetRichPath(), queue->GetPath(), 0, 0, {}))
        .ValueOrThrow();
    EXPECT_FALSE(rowset->GetRows().empty());

    WaitFor(Client_->UnregisterQueueConsumer(queue->GetRichPath(), consumer->GetPath()))
        .ThrowOnError();

    rowsetOrError = WaitFor(userClient->PullQueueConsumer(consumer->GetPath(), queue->GetPath(), 0, 0, {}));
    EXPECT_FALSE(rowsetOrError.IsOK());
    EXPECT_TRUE(rowsetOrError.FindMatching(NSecurityClient::EErrorCode::AuthorizationError));
}

////////////////////////////////////////////////////////////////////////////////

using TListRegistrationsTest = TQueueTestBase;

////////////////////////////////////////////////////////////////////////////////

TEST_W(TListRegistrationsTest, ListQueueConsumerRegistrations)
{
    auto [firstQueue, firstConsumer, firstQueueNameTable] =
        CreateQueueAndConsumer("first");
    auto [secondQueue, secondConsumer, secondQueueNameTable] =
        CreateQueueAndConsumer("second");

    WaitFor(Client_->RegisterQueueConsumer(firstQueue->GetPath(), firstConsumer->GetRichPath(), /*vital*/ false))
        .ThrowOnError();
    WaitFor(Client_->RegisterQueueConsumer(firstQueue->GetPath(), secondConsumer->GetRichPath(), /*vital*/ true))
        .ThrowOnError();
    WaitFor(Client_->RegisterQueueConsumer(secondQueue->GetPath(), secondConsumer->GetRichPath(), /*vital*/ false, TRegisterQueueConsumerOptions{.Partitions = std::vector{1, 5, 4, 3}}))
        .ThrowOnError();

    auto registrations = WaitFor(Client_->ListQueueConsumerRegistrations(firstQueue->GetRichPath(), /*consumerPath*/ {}))
        .ValueOrThrow();
    EXPECT_THAT(registrations, testing::UnorderedElementsAre(
        testing::FieldsAre(firstQueue->GetRichPathWithCluster(), firstConsumer->GetRichPathWithCluster(), false, std::nullopt),
        testing::FieldsAre(firstQueue->GetRichPathWithCluster(), secondConsumer->GetRichPathWithCluster(), true, std::nullopt)));

    registrations = WaitFor(Client_->ListQueueConsumerRegistrations(/*queuePath*/ {}, /*consumerPath*/ {}))
        .ValueOrThrow();
    EXPECT_THAT(registrations, testing::UnorderedElementsAre(
        testing::FieldsAre(firstQueue->GetRichPathWithCluster(), firstConsumer->GetRichPathWithCluster(), false, std::nullopt),
        testing::FieldsAre(firstQueue->GetRichPathWithCluster(), secondConsumer->GetRichPathWithCluster(), true, std::nullopt),
        testing::FieldsAre(secondQueue->GetRichPathWithCluster(), secondConsumer->GetRichPathWithCluster(), false, std::vector{1, 5, 4, 3})));

    registrations = WaitFor(Client_->ListQueueConsumerRegistrations(/*queuePath*/ {}, secondConsumer->GetRichPath()))
        .ValueOrThrow();
    EXPECT_THAT(registrations, testing::UnorderedElementsAre(
        testing::FieldsAre(firstQueue->GetRichPathWithCluster(), secondConsumer->GetRichPathWithCluster(), true, std::nullopt),
        testing::FieldsAre(secondQueue->GetRichPathWithCluster(), secondConsumer->GetRichPathWithCluster(), false, std::vector{1, 5, 4, 3})));

    registrations = WaitFor(Client_->ListQueueConsumerRegistrations(firstQueue->GetRichPath(), secondConsumer->GetRichPath()))
        .ValueOrThrow();
    EXPECT_THAT(registrations, testing::UnorderedElementsAre(
        testing::FieldsAre(firstQueue->GetRichPathWithCluster(), secondConsumer->GetRichPathWithCluster(), true, std::nullopt)));

    WaitFor(Client_->UnregisterQueueConsumer(firstQueue->GetRichPath(), secondConsumer->GetPath()))
        .ThrowOnError();
    WaitFor(Client_->UnregisterQueueConsumer(secondQueue->GetRichPath(), secondConsumer->GetPath()))
        .ThrowOnError();
    WaitFor(Client_->UnregisterQueueConsumer(firstQueue->GetRichPath(), firstConsumer->GetPath()))
        .ThrowOnError();
    WaitFor(Client_->RegisterQueueConsumer(secondQueue->GetRichPath(), firstConsumer->GetPath(), /*vital*/ true))
        .ThrowOnError();
    WaitFor(Client_->RegisterQueueConsumer(firstQueue->GetRichPath(), secondConsumer->GetPath(), /*vital*/ false))
        .ThrowOnError();

    registrations = WaitFor(Client_->ListQueueConsumerRegistrations(firstQueue->GetRichPath(), /*consumerPath*/ {}))
        .ValueOrThrow();
    EXPECT_THAT(registrations, testing::UnorderedElementsAre(
        testing::FieldsAre(firstQueue->GetRichPathWithCluster(), secondConsumer->GetRichPathWithCluster(), false, std::nullopt)));

    registrations = WaitFor(Client_->ListQueueConsumerRegistrations(secondQueue->GetRichPath(), /*consumerPath*/ {}))
        .ValueOrThrow();
    EXPECT_THAT(registrations, testing::UnorderedElementsAre(
        testing::FieldsAre(secondQueue->GetRichPathWithCluster(), firstConsumer->GetRichPathWithCluster(), true, std::nullopt)));

    registrations = WaitFor(Client_->ListQueueConsumerRegistrations(/*queuePath*/ {}, firstConsumer->GetRichPath()))
        .ValueOrThrow();
    EXPECT_THAT(registrations, testing::UnorderedElementsAre(
        testing::FieldsAre(secondQueue->GetRichPathWithCluster(), firstConsumer->GetRichPathWithCluster(), true, std::nullopt)));

    registrations = WaitFor(Client_->ListQueueConsumerRegistrations(/*queuePath*/ {}, secondConsumer->GetRichPath()))
        .ValueOrThrow();
    EXPECT_THAT(registrations, testing::UnorderedElementsAre(
        testing::FieldsAre(firstQueue->GetRichPathWithCluster(), secondConsumer->GetRichPathWithCluster(), false, std::nullopt)));

    WaitFor(Client_->UnregisterQueueConsumer(firstQueue->GetRichPath(), secondConsumer->GetPath()))
        .ThrowOnError();

    registrations = WaitFor(Client_->ListQueueConsumerRegistrations(firstQueue->GetRichPath(), /*consumerPath*/ {}))
        .ValueOrThrow();
    EXPECT_THAT(registrations, testing::UnorderedElementsAre());

    registrations = WaitFor(Client_->ListQueueConsumerRegistrations(secondQueue->GetRichPath(), /*consumerPath*/ {}))
        .ValueOrThrow();
    EXPECT_THAT(registrations, testing::UnorderedElementsAre(
        testing::FieldsAre(secondQueue->GetRichPathWithCluster(), firstConsumer->GetRichPathWithCluster(), true, std::nullopt)));

    registrations = WaitFor(Client_->ListQueueConsumerRegistrations(/*queuePath*/ {}, secondConsumer->GetRichPath()))
        .ValueOrThrow();
    EXPECT_THAT(registrations, testing::UnorderedElementsAre());

    WaitFor(Client_->UnregisterQueueConsumer(secondQueue->GetRichPath(), firstConsumer->GetPath()))
        .ThrowOnError();

    registrations = WaitFor(Client_->ListQueueConsumerRegistrations(firstQueue->GetRichPath(), /*consumerPath*/ {}))
        .ValueOrThrow();
    EXPECT_THAT(registrations, testing::UnorderedElementsAre());

    registrations = WaitFor(Client_->ListQueueConsumerRegistrations(secondQueue->GetRichPath(), /*consumerPath*/ {}))
        .ValueOrThrow();
    EXPECT_THAT(registrations, testing::UnorderedElementsAre());

    registrations = WaitFor(Client_->ListQueueConsumerRegistrations(/*queuePath*/ {}, firstConsumer->GetRichPath()))
        .ValueOrThrow();
    EXPECT_THAT(registrations, testing::UnorderedElementsAre());

    registrations = WaitFor(Client_->ListQueueConsumerRegistrations(/*queuePath*/ {}, secondConsumer->GetRichPath()))
        .ValueOrThrow();
    EXPECT_THAT(registrations, testing::UnorderedElementsAre());
}

////////////////////////////////////////////////////////////////////////////////

class TConsumerApiTest
    : public TQueueTestBase
{
public:
    static void TearDownTestCase();

    static void SetUpTestCase();

protected:
    void CreateConsumer(const TRichYPath& path)
    {
        TCreateNodeOptions options;
        options.Force = true;
        options.Attributes = CreateEphemeralAttributes();
        options.Attributes->Set("dynamic", true);
        options.Attributes->Set("schema", GetConsumerSchema());

        WaitFor(Client_->CreateNode(path.GetPath(), EObjectType::Table, options))
            .ThrowOnError();

        YT_UNUSED_FUTURE(Client_->MountTable(path.GetPath()));
        WaitUntilEqual(path.GetPath() + "/@tablet_state", "mounted");
    }

    void CreateQueue(const TRichYPath& path)
    {
        TCreateNodeOptions options;
        options.Force = true;
        options.Attributes = CreateEphemeralAttributes();
        options.Attributes->Set("dynamic", true);
        options.Attributes->Set("schema", New<TTableSchema>(std::vector<TColumnSchema>{
            TColumnSchema("data", EValueType::String).SetRequired(true),
            TColumnSchema("$timestamp", EValueType::Uint64).SetRequired(true),
            TColumnSchema("$cumulative_data_weight", EValueType::Int64).SetRequired(true),
        }, /*strict*/ true, /*uniqueKeys*/ false));

        WaitFor(Client_->CreateNode(path.GetPath(), EObjectType::Table, options))
            .ThrowOnError();

        YT_UNUSED_FUTURE(Client_->MountTable(path.GetPath()));
        WaitUntilEqual(path.GetPath() + "/@tablet_state", "mounted");
    }
};

void TConsumerApiTest::TearDownTestCase()
{
    WaitFor(Client_->SetNode(TYPath("//sys/accounts/tmp/@resource_limits/tablet_count"), ConvertToYsonString(0)))
        .ThrowOnError();

    TQueueTestBase::TearDownTestCase();
}

void TConsumerApiTest::SetUpTestCase()
{
    TQueueTestBase::SetUpTestCase();

    auto cellId = WaitFor(Client_->CreateObject(EObjectType::TabletCell))
        .ValueOrThrow();
    WaitUntilEqual(TYPath("#") + ToString(cellId) + "/@health", "good");

    WaitFor(Client_->SetNode(TYPath("//sys/accounts/tmp/@resource_limits/tablet_count"), ConvertToYsonString(1000)))
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TConsumerApiTest, TestAdvanceQueueConsumerViaProxy)
{
    TRichYPath consumerPath;
    consumerPath.SetPath("//tmp/test_consumer");

    TRichYPath queuePath;
    queuePath.SetPath("//tmp/test_queue");

    CreateConsumer(consumerPath);
    CreateQueue(queuePath);

    TRichYPath queueLinkPath;
    queueLinkPath.SetPath("//tmp/test_queue_link");

    WaitFor(Client_->LinkNode(queuePath.GetPath(), queueLinkPath.GetPath()))
        .ValueOrThrow();

    auto consumerClient = NQueueClient::CreateSubConsumerClient(Client_, Client_, consumerPath.GetPath(), queuePath);

    auto partitions = WaitFor(consumerClient->CollectPartitions(1))
        .ValueOrThrow();
    ASSERT_EQ(partitions.size(), 1u);
    EXPECT_EQ(partitions[0].PartitionIndex, 0);
    EXPECT_EQ(partitions[0].NextRowIndex, 0);

    WaitFor(Client_->RegisterQueueConsumer(queuePath, consumerPath, true))
        .ThrowOnError();

    auto transaction = WaitFor(Client_->StartTransaction(NTransactionClient::ETransactionType::Tablet))
        .ValueOrThrow();

    WaitFor(transaction->AdvanceQueueConsumer(consumerPath, queueLinkPath, 0, {}, 1, {}))
        .ThrowOnError();
    WaitFor(transaction->Commit())
        .ThrowOnError();

    partitions = WaitFor(consumerClient->CollectPartitions(1))
        .ValueOrThrow();
    ASSERT_EQ(partitions.size(), 1u);
    EXPECT_EQ(partitions[0].PartitionIndex, 0);
    EXPECT_EQ(partitions[0].NextRowIndex, 1);

    transaction = WaitFor(Client_->StartTransaction(NTransactionClient::ETransactionType::Tablet))
        .ValueOrThrow();

    WaitFor(transaction->AdvanceQueueConsumer(consumerPath, queueLinkPath, 0, {}, 10, {}))
        .ThrowOnError();

    WaitFor(transaction->Commit())
        .ThrowOnError();

    partitions = WaitFor(consumerClient->CollectPartitions(1))
        .ValueOrThrow();
    ASSERT_EQ(partitions.size(), 1u);
    EXPECT_EQ(partitions[0].PartitionIndex, 0);
    EXPECT_EQ(partitions[0].NextRowIndex, 10);
}

////////////////////////////////////////////////////////////////////////////////

using TProducerApiTest = TQueueTestBase;

////////////////////////////////////////////////////////////////////////////////

TEST_F(TProducerApiTest, TestApi)
{
    TRichYPath producerPath;
    producerPath.SetPath("//tmp/test_producer");

    TRichYPath queuePath;
    queuePath.SetPath("//tmp/test_queue");

    CreateQueueProducer(producerPath);

    auto queueAttributes = CreateEphemeralAttributes();
    queueAttributes->Set("tablet_count", 3);
    auto queue = New<TDynamicTable>(
            queuePath,
            New<TTableSchema>(std::vector<TColumnSchema>{
                TColumnSchema("a", EValueType::Uint64),
                TColumnSchema("b", EValueType::String)}),
            queueAttributes);

    NQueueClient::TQueueProducerSessionId sessionId{"session_1"};
    NQueueClient::TQueueProducerEpoch epoch{0};

    WaitFor(Client_->CreateQueueProducerSession(producerPath, queuePath, sessionId))
        .ValueOrThrow();

    auto transaction = WaitFor(Client_->StartTransaction(ETransactionType::Tablet))
        .ValueOrThrow();

    TUnversionedRowsBuilder rowsBuilder;
    int rowCount = 10;
    for (int rowIndex = 0; rowIndex < rowCount; ++rowIndex) {
        TUnversionedRowBuilder rowBuilder;

        rowBuilder.AddValue(MakeUnversionedUint64Value(rowIndex, 0));

        TString value = ToString(rowIndex * rowIndex);
        rowBuilder.AddValue(MakeUnversionedStringValue(value, 1));

        rowsBuilder.AddRow(rowBuilder.GetRow());
    }
    auto rows = rowsBuilder.Build();

    auto nameTable = TNameTable::FromSchema(*queue->GetSchema());

    auto result = WaitFor(transaction->PushQueueProducer(
        producerPath,
        queuePath,
        sessionId,
        epoch,
        nameTable,
        rows,
        TPushQueueProducerOptions{
            .SequenceNumber = TQueueProducerSequenceNumber{0},
        }))
        .ValueOrThrow();

    ASSERT_EQ(result.LastSequenceNumber, TQueueProducerSequenceNumber(9));

    WaitFor(transaction->Commit())
        .ThrowOnError();

    auto allRowsResult = WaitFor(Client_->SelectRows(Format("* from [%v]", queuePath)))
        .ValueOrThrow();

    ASSERT_EQ(std::ssize(allRowsResult.Rowset->GetRows()), 10);

    auto sessionRowsResult = WaitFor(Client_->SelectRows(Format("queue_cluster, queue_path, session_id, sequence_number, epoch from [%v]", producerPath)))
        .ValueOrThrow();
    auto sessionRows = sessionRowsResult.Rowset->GetRows();

    auto actualSessionRow = ToString(sessionRows[0]);
    auto expectedSessionRow = ToString(YsonToSchemalessRow(
        "<id=0> \"primary\"; <id=1> \"//tmp/test_queue\"; <id=2> session_1; <id=3> 9; <id=4> 0;"));

    ASSERT_EQ(actualSessionRow, expectedSessionRow);
}

TEST_F(TProducerApiTest, TestProducerClient)
{
    TRichYPath producerPath;
    producerPath.SetPath("//tmp/producer");

    TRichYPath queuePath;
    queuePath.SetPath("//tmp/queue");

    CreateQueueProducer(producerPath);

    auto queueAttributes = CreateEphemeralAttributes();
    queueAttributes->Set("tablet_count", 3);
    auto queue = New<TDynamicTable>(
            queuePath,
            New<TTableSchema>(std::vector<TColumnSchema>{
                TColumnSchema("a", EValueType::Uint64),
                TColumnSchema("b", EValueType::String)}),
            queueAttributes);

    auto nameTable = TNameTable::FromSchema(*queue->GetSchema());

    NQueueClient::TQueueProducerSessionId sessionId{ "session_1" };

    auto producerClient = CreateProducerClient(
        Client_,
        producerPath);

    std::vector<NQueueClient::TQueueProducerSequenceNumber> acks;
    auto ackCallback = BIND([&acks] (NQueueClient::TQueueProducerSequenceNumber sequenceNumber) -> void {
        acks.push_back(sequenceNumber);
    });

    auto pushBatch = [&] (const IProducerSessionPtr& producerSession, std::optional<i64> startSequenceNumber = std::nullopt) {
        TUnversionedRowsBuilder rowsBuilder;
        int rowCount = 10;
        for (int rowIndex = 0; rowIndex < rowCount; ++rowIndex) {
            TUnversionedRowBuilder rowBuilder;

            rowBuilder.AddValue(MakeUnversionedUint64Value(rowIndex, 0));

            TString value = ToString(rowIndex * rowIndex);
            rowBuilder.AddValue(MakeUnversionedStringValue(value, 1));

            if (startSequenceNumber) {
                rowBuilder.AddValue(MakeUnversionedInt64Value(*startSequenceNumber + rowIndex, 2));
            }

            rowsBuilder.AddRow(rowBuilder.GetRow());
        }
        auto rows = rowsBuilder.Build();
        if (!producerSession->Write(rows)) {
            WaitFor(producerSession->GetReadyEvent())
                .ThrowOnError();
        }
    };

    auto checkQueue = [&] (int64_t expectedCount) {
        WaitForRowCount(queuePath.GetPath(), expectedCount);
    };

    auto checkProducer = [&] (int64_t expectedLastSequenceNumber, int64_t expectedEpoch) {
        WaitForPredicate([&] {
            auto sessionRowsResult = WaitFor(Client_->SelectRows(Format("queue_cluster, queue_path, session_id, sequence_number, epoch from [%v]", producerPath)))
                .ValueOrThrow();
            auto sessionRows = sessionRowsResult.Rowset->GetRows();

            auto actualSessionRow = ToString(sessionRows[0]);
            auto expectedSessionRow = ToString(YsonToSchemalessRow(
                Format(
                    "<id=0> \"primary\"; <id=1> \"//tmp/queue\"; <id=2> session_1; <id=3> %v; <id=4> %v;",
                    expectedLastSequenceNumber, expectedEpoch)));

            return actualSessionRow == expectedSessionRow;
        }, Format("Producer session with %v last sequence number and %v epoch was expected", expectedLastSequenceNumber, expectedEpoch));

        EXPECT_EQ(acks.back(), NQueueClient::TQueueProducerSequenceNumber{expectedLastSequenceNumber});
    };

    {
        auto producerSession = WaitFor(producerClient->CreateSession(
            queuePath,
            nameTable,
            sessionId,
            TProducerSessionOptions{
                .AutoSequenceNumber = true,
                .BatchOptions = TProducerSessionBatchOptions{
                    .RowCount = 100,
                },
                .AckCallback = ackCallback,
            }))
            .ValueOrThrow();

        for (int batchId = 0; batchId < 5; ++batchId) {
            pushBatch(producerSession);
            if (batchId % 2 == 1) {
                WaitFor(producerSession->Flush())
                    .ThrowOnError();

                auto expectedCount = 10 * (batchId + 1);
                checkQueue(expectedCount);
                checkProducer(expectedCount - 1, 0);
            }
        }

        WaitFor(producerSession->Flush())
            .ThrowOnError();

        checkQueue(50);
        checkProducer(49, 0);

        WaitFor(producerSession->Close())
            .ThrowOnError();

        checkQueue(50);
        checkProducer(49, 0);

        acks.clear();
    }

    {
        auto producerSession = WaitFor(producerClient->CreateSession(
            queuePath,
            nameTable,
            sessionId,
            TProducerSessionOptions{
                .AutoSequenceNumber = true,
                .BatchOptions = TProducerSessionBatchOptions{
                    .RowCount = 100,
                },
                .AckCallback = ackCallback,
            }))
            .ValueOrThrow();

        for (int batchId = 0; batchId < 2; ++batchId) {
            pushBatch(producerSession);
        }
        WaitFor(producerSession->Close())
            .ThrowOnError();

        checkQueue(70);
        checkProducer(69, 1);

        acks.clear();
    }

    {
        // Rows will be flushed after each batch.
        auto producerSession = WaitFor(producerClient->CreateSession(
            queuePath,
            nameTable,
            sessionId,
            TProducerSessionOptions{
                .AutoSequenceNumber = true,
                .BatchOptions = TProducerSessionBatchOptions{
                    .ByteSize = 15,
                },
                .AckCallback = ackCallback,
            }))
            .ValueOrThrow();

        for (int batchId = 0; batchId < 3; ++batchId) {
            pushBatch(producerSession);
            auto size = 70 + (batchId + 1) * 10;
            checkQueue(size);
            checkProducer(size - 1, 2);
        }
        WaitFor(producerSession->Close())
            .ThrowOnError();

        checkQueue(100);
        checkProducer(99, 2);

        acks.clear();
    }

    auto nameTableWithSequenceNumber = TNameTable::FromSchema(
        TTableSchema(std::vector<TColumnSchema>{
                TColumnSchema("a", EValueType::Uint64),
                TColumnSchema("b", EValueType::String),
                TColumnSchema("$sequence_number", EValueType::Int64),
        }));

    auto createSessionWithoutAutoSequenceNumber = [&] {
        return WaitFor(producerClient->CreateSession(
            queuePath,
            nameTable,
            sessionId,
            TProducerSessionOptions{
                .BatchOptions = TProducerSessionBatchOptions{
                    .RowCount = 100,
                },
                .AckCallback = ackCallback,
            }))
            .ValueOrThrow();
    };

    {
        auto producerSession = createSessionWithoutAutoSequenceNumber();

        // 5 rows will be ignored as a dublicate and 15 rows will be written.
        for (int batchId = 0; batchId < 2; ++batchId) {
            pushBatch(producerSession, 95 + batchId * 10);
        }

        WaitFor(producerSession->Close())
            .ThrowOnError();

        checkQueue(115);
        checkProducer(114, 3);

        acks.clear();
    }

    {
        auto producerSession = createSessionWithoutAutoSequenceNumber();

        // All rows will be ignored.
        pushBatch(producerSession, 105);

        WaitFor(producerSession->Close())
            .ThrowOnError();

        checkQueue(115);
        checkProducer(114, 4);

        acks.clear();
    }

    {
        auto producerSession = createSessionWithoutAutoSequenceNumber();

        // 1 row will be ignored and 9 rows will written.
        pushBatch(producerSession, 114);

        WaitFor(producerSession->Close())
            .ThrowOnError();

        checkQueue(124);
        checkProducer(123, 5);

        acks.clear();
    }

    // Flush in background.
    auto threadPool = CreateThreadPool(1, "ThreadPool");
    {
        auto backgroundFlushPeriod = TDuration::Seconds(5);
        auto producerSession = WaitFor(producerClient->CreateSession(
            queuePath,
            nameTable,
            sessionId,
            TProducerSessionOptions{
                .AutoSequenceNumber = true,
                .BackgroundFlushPeriod = backgroundFlushPeriod,
                .AckCallback = ackCallback,
            },
            threadPool->GetInvoker()
        )).ValueOrThrow();

        pushBatch(producerSession);

        // Wait for the first flush.
        checkQueue(134);
        checkProducer(133, 6);

        pushBatch(producerSession);
        // Old values, flush was not called yet.
        checkQueue(134);
        checkProducer(133, 6);

        Sleep(backgroundFlushPeriod);

        checkQueue(144);
        checkProducer(143, 6);

        pushBatch(producerSession);

        WaitFor(producerSession->Flush())
            .ThrowOnError();

        checkQueue(154);
        checkProducer(153, 6);

        WaitFor(producerSession->Close())
            .ThrowOnError();

        acks.clear();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NCppTests

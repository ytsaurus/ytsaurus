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

#include <yt/yt/client/queue_client/partition_reader.h>
#include <yt/yt/client/queue_client/consumer_client.h>

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

    static void UnmountAndMount(const TYPath& path)
    {
        SyncUnmountTable(path);
        SyncMountTable(path);
    }

    static void WaitForRowCount(const TYPath& path, i64 rowCount)
    {
        WaitForPredicate([rowCount, path] {
            auto allRowsResult = WaitFor(Client_->SelectRows(Format("* from [%v]", path)))
                .ValueOrThrow();

            return std::ssize(allRowsResult.Rowset->GetRows()) == rowCount;
        });
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

    void AssertPermission(const TString& user, const TYPath& path, EPermission permission, ESecurityAction action) const
    {
        auto permissionResponse = WaitFor(Client_->CheckPermission(user, path, permission))
            .ValueOrThrow();
        ASSERT_EQ(permissionResponse.Action, action);
    }

    void AssertPermissionAllowed(const TString& user, const TYPath& path, EPermission permission) const
    {
        AssertPermission(user, path, permission, ESecurityAction::Allow);
    }

    void AssertPermissionDenied(const TString& user, const TYPath& path, EPermission permission) const
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

        UnmountAndMount(queue->GetPath());

        WriteSingleRow(queue->GetPath(), queueNameTable, {"45u", "d"});
        WriteSingleRow(queue->GetPath(), queueNameTable, {"46u", "e"});

        AssertPermissionDenied(testUser, queue->GetPath(), EPermission::Read);

        auto rowsetOrError = WaitFor(userClient->PullQueue(queue->GetPath(), 0, 0, {}));
        EXPECT_FALSE(rowsetOrError.IsOK());
        EXPECT_TRUE(rowsetOrError.FindMatching(NSecurityClient::EErrorCode::AuthorizationError));

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

TEST_W(TQueueApiPermissionsTest, PullConsumer)
{
    auto testUser = "u1";
    auto userClient = CreateUser(testUser);

    auto [queue, consumer, queueNameTable] =
        CreateQueueAndConsumer("PullConsumer");

    WriteSingleRow(queue->GetPath(), queueNameTable, {"42u", "a"});
    WriteSingleRow(queue->GetPath(), queueNameTable, {"43u", "b"});
    WriteSingleRow(queue->GetPath(), queueNameTable, {"44u", "c"});

    UnmountAndMount(queue->GetPath());

    WriteSingleRow(queue->GetPath(), queueNameTable, {"45u", "d"});
    WriteSingleRow(queue->GetPath(), queueNameTable, {"46u", "e"});

    AssertPermissionDenied(testUser, queue->GetPath(), EPermission::Read);
    AssertPermissionDenied(testUser, consumer->GetPath(), EPermission::Read);

    auto rowsetOrError = WaitFor(userClient->PullConsumer(consumer->GetPath(), queue->GetPath(), 0, 0, {}));
    EXPECT_FALSE(rowsetOrError.IsOK());
    EXPECT_TRUE(rowsetOrError.FindMatching(NSecurityClient::EErrorCode::AuthorizationError));

    WaitFor(Client_->SetNode(
        consumer->GetPath() + "/@acl/end",
        ConvertToYsonString(TSerializableAccessControlEntry(
            ESecurityAction::Allow,
            {testUser},
            EPermission::Read))))
        .ThrowOnError();

    rowsetOrError = WaitFor(userClient->PullConsumer(consumer->GetPath(), queue->GetPath(), 0, 0, {}));
    EXPECT_FALSE(rowsetOrError.IsOK());
    EXPECT_TRUE(rowsetOrError.FindMatching(NSecurityClient::EErrorCode::AuthorizationError));

    WaitFor(Client_->RegisterQueueConsumer(queue->GetPath(), consumer->GetRichPath(), /*vital*/ false))
        .ThrowOnError();

    auto rowset = WaitFor(userClient->PullConsumer(consumer->GetRichPath(), queue->GetPath(), 0, 0, {}))
        .ValueOrThrow();
    EXPECT_FALSE(rowset->GetRows().empty());

    WaitFor(Client_->UnregisterQueueConsumer(queue->GetRichPath(), consumer->GetPath()))
        .ThrowOnError();

    rowsetOrError = WaitFor(userClient->PullConsumer(consumer->GetPath(), queue->GetPath(), 0, 0, {}));
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
    WaitFor(Client_->RegisterQueueConsumer(secondQueue->GetPath(), secondConsumer->GetRichPath(), /*vital*/ false,TRegisterQueueConsumerOptions{.Partitions = std::vector{1, 5, 4, 3}}))
        .ThrowOnError();

    auto registrations = WaitFor(Client_->ListQueueConsumerRegistrations(firstQueue->GetRichPath(), /*consumerPath*/ {}))
        .ValueOrThrow();
    EXPECT_THAT(registrations, testing::UnorderedElementsAre(
        testing::FieldsAre(firstQueue->GetRichPathWithCluster(), firstConsumer->GetRichPathWithCluster(), false, std::nullopt),
        testing::FieldsAre(firstQueue->GetRichPathWithCluster(), secondConsumer->GetRichPathWithCluster(), true, std::nullopt)
    ));

    registrations = WaitFor(Client_->ListQueueConsumerRegistrations(/*queuePath*/ {}, /*consumerPath*/ {}))
        .ValueOrThrow();
    EXPECT_THAT(registrations, testing::UnorderedElementsAre(
        testing::FieldsAre(firstQueue->GetRichPathWithCluster(), firstConsumer->GetRichPathWithCluster(), false, std::nullopt),
        testing::FieldsAre(firstQueue->GetRichPathWithCluster(), secondConsumer->GetRichPathWithCluster(), true, std::nullopt),
        testing::FieldsAre(secondQueue->GetRichPathWithCluster(), secondConsumer->GetRichPathWithCluster(), false, std::vector{1, 5, 4, 3})
    ));

    registrations = WaitFor(Client_->ListQueueConsumerRegistrations(/*queuePath*/ {}, secondConsumer->GetRichPath()))
        .ValueOrThrow();
    EXPECT_THAT(registrations, testing::UnorderedElementsAre(
        testing::FieldsAre(firstQueue->GetRichPathWithCluster(), secondConsumer->GetRichPathWithCluster(), true, std::nullopt),
        testing::FieldsAre(secondQueue->GetRichPathWithCluster(), secondConsumer->GetRichPathWithCluster(), false, std::vector{1, 5, 4, 3})
    ));

    registrations = WaitFor(Client_->ListQueueConsumerRegistrations(firstQueue->GetRichPath(), secondConsumer->GetRichPath()))
        .ValueOrThrow();
    EXPECT_THAT(registrations, testing::UnorderedElementsAre(
        testing::FieldsAre(firstQueue->GetRichPathWithCluster(), secondConsumer->GetRichPathWithCluster(), true, std::nullopt)
    ));

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
        testing::FieldsAre(firstQueue->GetRichPathWithCluster(), secondConsumer->GetRichPathWithCluster(), false, std::nullopt)
    ));

    registrations = WaitFor(Client_->ListQueueConsumerRegistrations(secondQueue->GetRichPath(), /*consumerPath*/ {}))
        .ValueOrThrow();
    EXPECT_THAT(registrations, testing::UnorderedElementsAre(
        testing::FieldsAre(secondQueue->GetRichPathWithCluster(), firstConsumer->GetRichPathWithCluster(), true, std::nullopt)
    ));

    registrations = WaitFor(Client_->ListQueueConsumerRegistrations(/*queuePath*/ {}, firstConsumer->GetRichPath()))
        .ValueOrThrow();
    EXPECT_THAT(registrations, testing::UnorderedElementsAre(
        testing::FieldsAre(secondQueue->GetRichPathWithCluster(), firstConsumer->GetRichPathWithCluster(), true, std::nullopt)
    ));

    registrations = WaitFor(Client_->ListQueueConsumerRegistrations(/*queuePath*/ {}, secondConsumer->GetRichPath()))
        .ValueOrThrow();
    EXPECT_THAT(registrations, testing::UnorderedElementsAre(
        testing::FieldsAre(firstQueue->GetRichPathWithCluster(), secondConsumer->GetRichPathWithCluster(), false, std::nullopt)
    ));

    WaitFor(Client_->UnregisterQueueConsumer(firstQueue->GetRichPath(), secondConsumer->GetPath()))
        .ThrowOnError();

    registrations = WaitFor(Client_->ListQueueConsumerRegistrations(firstQueue->GetRichPath(), /*consumerPath*/ {}))
        .ValueOrThrow();
    EXPECT_THAT(registrations, testing::UnorderedElementsAre());

    registrations = WaitFor(Client_->ListQueueConsumerRegistrations(secondQueue->GetRichPath(), /*consumerPath*/ {}))
        .ValueOrThrow();
    EXPECT_THAT(registrations, testing::UnorderedElementsAre(
        testing::FieldsAre(secondQueue->GetRichPathWithCluster(), firstConsumer->GetRichPathWithCluster(), true, std::nullopt)
    ));

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

using TPartitionReaderTest = TQueueTestBase;

////////////////////////////////////////////////////////////////////////////////

TEST_W(TPartitionReaderTest, Simple)
{
    for (auto useNativeTabletNodeApi : std::vector<bool>{false, true}) {
        auto [queue, consumer, queueNameTable] =
            CreateQueueAndConsumer("Simple", useNativeTabletNodeApi);

        WriteSingleRow(queue->GetPath(), queueNameTable, {"42u", "hello"});
        UnmountAndMount(queue->GetPath());

        auto partitionReaderConfig = New<TPartitionReaderConfig>();
        partitionReaderConfig->UseNativeTabletNodeApi = useNativeTabletNodeApi;
        auto partitionReader = CreatePartitionReader(partitionReaderConfig, Client_, consumer->GetPath(), /*partitionIndex*/ 0);

        WaitFor(partitionReader->Open())
            .ThrowOnError();

        auto queueRowset = WaitFor(partitionReader->Read())
            .ValueOrThrow();
        auto aColumnIndex = queueRowset->GetNameTable()->GetIdOrThrow("a");

        ASSERT_EQ(queueRowset->GetRows().size(), 1u);
        EXPECT_EQ(queueRowset->GetStartOffset(), 0u);
        EXPECT_EQ(queueRowset->GetFinishOffset(), 1u);
        EXPECT_EQ(queueRowset->GetRows()[0][aColumnIndex].Data.Uint64, 42u);

        auto transaction = WaitFor(Client_->StartTransaction(ETransactionType::Tablet))
            .ValueOrThrow();
        queueRowset->Commit(transaction);
        WaitFor(transaction->Commit())
            .ThrowOnError();

        auto consumerClient = CreateBigRTConsumerClient(consumer->GetPath(), *consumer->GetSchema());
        auto partitions = WaitFor(consumerClient->CollectPartitions(Client_, 1))
            .ValueOrThrow();

        ASSERT_EQ(partitions.size(), 1u);
        EXPECT_EQ(partitions[0].NextRowIndex, 1);
    }
}

TEST_W(TPartitionReaderTest, HintBiggerThanMaxDataWeight)
{
    for (auto useNativeTabletNodeApi : std::vector<bool>{false, true}) {
        auto [queue, consumer, queueNameTable] =
            CreateQueueAndConsumer("HintBiggerThanMaxDataWeight", useNativeTabletNodeApi);

        WriteSingleRow(queue->GetPath(), queueNameTable, {"42u", "longlonglonglonglonglonglonglonglonglonglonglong"});
        WriteSingleRow(queue->GetPath(), queueNameTable, {"43u", "longlonglonglonglonglonglonglonglonglonglonglong"});
        WriteSingleRow(queue->GetPath(), queueNameTable, {"44u", "longlonglonglonglonglonglonglonglonglonglonglong"});
        WriteSingleRow(queue->GetPath(), queueNameTable, {"45u", "longlonglonglonglonglonglonglonglonglonglonglong"});
        UnmountAndMount(queue->GetPath());

        WriteSingleRow(queue->GetPath(), queueNameTable, {"46u", "longlonglonglonglonglonglonglonglonglonglonglong"});
        WriteSingleRow(queue->GetPath(), queueNameTable, {"47u", "longlonglonglonglonglonglonglonglonglonglonglong"});
        WriteSingleRow(queue->GetPath(), queueNameTable, {"48u", "longlonglonglonglonglonglonglonglonglonglonglong"});
        WriteSingleRow(queue->GetPath(), queueNameTable, {"49u", "longlonglonglonglonglonglonglonglonglonglonglong"});
        UnmountAndMount(queue->GetPath());

        WriteSingleRow(queue->GetPath(), queueNameTable, {"50u", "longlonglonglonglonglonglonglonglonglonglonglong"});
        WriteSingleRow(queue->GetPath(), queueNameTable, {"51u", "longlonglonglonglonglonglonglonglonglonglonglong"});
        WriteSingleRow(queue->GetPath(), queueNameTable, {"52u", "longlonglonglonglonglonglonglonglonglonglonglong"});
        WriteSingleRow(queue->GetPath(), queueNameTable, {"53u", "longlonglonglonglonglonglonglonglonglonglonglong"});
        // No flush, just so some rows might be in the dynamic store.

        auto partitionReaderConfig = New<TPartitionReaderConfig>();
        partitionReaderConfig->UseNativeTabletNodeApi = useNativeTabletNodeApi;
        // We expect to fetch only one row.
        partitionReaderConfig->MaxDataWeight = 10;
        auto partitionReader = CreatePartitionReader(partitionReaderConfig, Client_, consumer->GetPath(), /*partitionIndex*/ 0);

        WaitFor(partitionReader->Open())
            .ThrowOnError();

        auto queueRowset = WaitFor(partitionReader->Read())
            .ValueOrThrow();
        auto aColumnIndex = queueRowset->GetNameTable()->GetIdOrThrow("a");

        ASSERT_EQ(queueRowset->GetRows().size(), 1u);
        EXPECT_EQ(queueRowset->GetStartOffset(), 0u);
        EXPECT_EQ(queueRowset->GetFinishOffset(), 1u);
        EXPECT_EQ(queueRowset->GetRows()[0][aColumnIndex].Data.Uint64, 42u);

        auto transaction = WaitFor(Client_->StartTransaction(ETransactionType::Tablet))
            .ValueOrThrow();
        queueRowset->Commit(transaction);
        WaitFor(transaction->Commit())
            .ThrowOnError();

        auto consumerClient = CreateBigRTConsumerClient(consumer->GetPath(), *consumer->GetSchema());
        auto partitions = WaitFor(consumerClient->CollectPartitions(Client_, 1))
            .ValueOrThrow();

        ASSERT_EQ(partitions.size(), 1u);
        EXPECT_EQ(partitions[0].NextRowIndex, 1);

        partitionReaderConfig = New<TPartitionReaderConfig>();
        partitionReaderConfig->UseNativeTabletNodeApi = useNativeTabletNodeApi;
        // Now we should get more than one row.
        partitionReaderConfig->MaxDataWeight = 1_MB;
        partitionReader = CreatePartitionReader(partitionReaderConfig, Client_, consumer->GetPath(), /*partitionIndex*/ 0);

        WaitFor(partitionReader->Open())
            .ThrowOnError();

        queueRowset = WaitFor(partitionReader->Read())
            .ValueOrThrow();

        ASSERT_GT(queueRowset->GetRows().size(), 1u);
        EXPECT_EQ(queueRowset->GetStartOffset(), 1u);
        EXPECT_GT(queueRowset->GetFinishOffset(), 2u);
        EXPECT_EQ(queueRowset->GetRows()[0][aColumnIndex].Data.Uint64, 43u);

        transaction = WaitFor(Client_->StartTransaction(ETransactionType::Tablet))
            .ValueOrThrow();
        queueRowset->Commit(transaction);
        WaitFor(transaction->Commit())
            .ThrowOnError();

        partitions = WaitFor(consumerClient->CollectPartitions(Client_, 1))
            .ValueOrThrow();

        ASSERT_EQ(partitions.size(), 1u);
        EXPECT_GT(partitions[0].NextRowIndex, 1);
    }
}

TEST_W(TPartitionReaderTest, MultiplePartitions)
{
    for (auto useNativeTabletNodeApi : std::vector<bool>{false, true}) {
        auto [queue, consumer, _] =
            CreateQueueAndConsumer("MultiplePartitions", useNativeTabletNodeApi, /*queueTabletCount*/ 3);

        auto queueNameTable = TNameTable::FromSchema(*queue->GetSchema()->ToWrite());

        WriteSingleRow(queue->GetPath(), queueNameTable, {"0", "42u", "s"});
        WriteSingleRow(queue->GetPath(), queueNameTable, {"0", "43u", "h"});
        WriteSingleRow(queue->GetPath(), queueNameTable, {"0", "44u", "o"});
        WriteSingleRow(queue->GetPath(), queueNameTable, {"0", "45u", "r"});
        WriteSingleRow(queue->GetPath(), queueNameTable, {"0", "46u", "t"});

        TString veryLongString;
        for (int i = 0; i < 500; ++i) {
            veryLongString += "abacaba";
        }

        WriteSingleRow(queue->GetPath(), queueNameTable,{"1", "47u", veryLongString});
        WriteSingleRow(queue->GetPath(), queueNameTable,{"1", "48u", veryLongString});
        WriteSingleRow(queue->GetPath(), queueNameTable,{"1", "480u", veryLongString});
        WriteSingleRow(queue->GetPath(), queueNameTable,{"1", "481u", veryLongString});
        WriteSingleRow(queue->GetPath(), queueNameTable,{"1", "482u", veryLongString});

        WriteSingleRow(queue->GetPath(), queueNameTable, {"2", "49u", "hello"});

        WriteSingleRow(queue->GetPath(), queueNameTable, {"0", "50u", "s"});
        WriteSingleRow(queue->GetPath(), queueNameTable, {"0", "51u", "t"});
        WriteSingleRow(queue->GetPath(), queueNameTable, {"0", "52u", "r"});
        WriteSingleRow(queue->GetPath(), queueNameTable, {"0", "53u", "i"});
        WriteSingleRow(queue->GetPath(), queueNameTable, {"0", "54u", "n"});
        WriteSingleRow(queue->GetPath(), queueNameTable, {"0", "55u", "g"});

        WriteSingleRow(queue->GetPath(), queueNameTable, {"2", "56u", "darkness"});
        WriteSingleRow(queue->GetPath(), queueNameTable, {"2", "57u", "my"});
        WriteSingleRow(queue->GetPath(), queueNameTable, {"2", "58u", "old"});
        WriteSingleRow(queue->GetPath(), queueNameTable, {"2", "59u", "friend"});

        UnmountAndMount(queue->GetPath());

        auto partitionReaderConfig = New<TPartitionReaderConfig>();
        partitionReaderConfig->UseNativeTabletNodeApi = useNativeTabletNodeApi;
        // Average data weight per row for all tablets is quite big due to large rows.
        // However, it should be computed per-tablet, and it is ~10 for partition 0.
        // So setting MaxDataWeight = 150 for partition 0 should read significantly more than one row.
        partitionReaderConfig->MaxDataWeight = 150;
        auto partitionReader0 = CreatePartitionReader(partitionReaderConfig, Client_, consumer->GetPath(), /*partitionIndex*/ 0);

        WaitFor(partitionReader0->Open())
            .ThrowOnError();

        auto queueRowset0 = WaitFor(partitionReader0->Read())
            .ValueOrThrow();
        auto aColumnIndex = queueRowset0->GetNameTable()->GetIdOrThrow("a");

        ASSERT_GT(queueRowset0->GetRows().size(), 5u);
        EXPECT_EQ(queueRowset0->GetStartOffset(), 0u);
        EXPECT_GT(queueRowset0->GetFinishOffset(), 5u);
        EXPECT_EQ(queueRowset0->GetRows()[0][aColumnIndex].Data.Uint64, 42u);

        auto partitionReaderConfig1 = New<TPartitionReaderConfig>();
        // Setting MaxDataWeight = 150 for partition 1 should read only one row.
        partitionReaderConfig1->MaxDataWeight = 150;
        auto partitionReader1 = CreatePartitionReader(partitionReaderConfig1, Client_, consumer->GetPath(), /*partitionIndex*/ 1);
        WaitFor(partitionReader1->Open())
            .ThrowOnError();

        auto queueRowset1 = WaitFor(partitionReader1->Read())
            .ValueOrThrow();

        ASSERT_EQ(queueRowset1->GetRows().size(), 1u);
        EXPECT_EQ(queueRowset1->GetStartOffset(), 0u);
        EXPECT_EQ(queueRowset1->GetFinishOffset(), 1u);
        EXPECT_EQ(queueRowset1->GetRows()[0][aColumnIndex].Data.Uint64, 47u);

        auto partitionReaderConfig2 = New<TPartitionReaderConfig>();
        auto partitionReader2 = CreatePartitionReader(partitionReaderConfig2, Client_, consumer->GetPath(), /*partitionIndex*/ 2);
        WaitFor(partitionReader2->Open())
            .ThrowOnError();

        auto queueRowset2 = WaitFor(partitionReader2->Read())
            .ValueOrThrow();

        ASSERT_EQ(queueRowset2->GetRows().size(), 5u);
        EXPECT_EQ(queueRowset2->GetStartOffset(), 0u);
        EXPECT_EQ(queueRowset2->GetFinishOffset(), 5u);
        EXPECT_EQ(queueRowset2->GetRows()[0][aColumnIndex].Data.Uint64, 49u);

        auto transaction02 = WaitFor(Client_->StartTransaction(ETransactionType::Tablet))
            .ValueOrThrow();
        queueRowset0->Commit(transaction02);
        queueRowset2->Commit(transaction02);
        WaitFor(transaction02->Commit())
            .ThrowOnError();

        auto transaction1 = WaitFor(Client_->StartTransaction(ETransactionType::Tablet))
            .ValueOrThrow();
        queueRowset1->Commit(transaction1);
        WaitFor(transaction1->Commit())
            .ThrowOnError();

        auto consumerClient = CreateBigRTConsumerClient(consumer->GetPath(), *consumer->GetSchema());
        auto partitions = WaitFor(consumerClient->CollectPartitions(Client_, 3))
            .ValueOrThrow();

        ASSERT_EQ(partitions.size(), 3u);
        EXPECT_GT(partitions[0].NextRowIndex, 5);
        EXPECT_EQ(partitions[1].NextRowIndex, 1);
        EXPECT_EQ(partitions[2].NextRowIndex, 5);
    }
}

TEST_W(TPartitionReaderTest, BatchSizesAreReasonable)
{
    for (auto useNativeTabletNodeApi : std::vector<bool>{false, true}) {
        auto [queue, consumer, queueNameTable] =
            CreateQueueAndConsumer("BatchSizesAreReasonable", useNativeTabletNodeApi);

        std::unique_ptr<TUnversionedRowsBuilder> rowsBuilder = std::make_unique<TUnversionedRowsBuilder>();
        int rowCount = 10'000;
        for (int rowIndex = 0; rowIndex < rowCount; ++rowIndex) {
            TUnversionedRowBuilder rowBuilder;

            rowBuilder.AddValue(MakeUnversionedUint64Value(rowIndex, 0));

            TString value('a', 1_KB + RandomNumber(24u));
            rowBuilder.AddValue(MakeUnversionedStringValue(value, 1));

            rowsBuilder->AddRow(rowBuilder.GetRow());

            // The first condition forces some rows to be written and later flushed,
            // so that the resulting table's flushed_row_count is not zero and the
            // initial data weight per row hint can be computed.
            if (rowIndex == rowCount / 14 || RandomNumber(2000u) == 0 || rowIndex + 1 == rowCount) {
                WriteSharedRange(queue->GetPath(), queueNameTable, rowsBuilder->Build());
                rowsBuilder = std::make_unique<TUnversionedRowsBuilder>();
            }

            // For the first condition, see the comment above. Note that rowCount / 13 >= rowCount / 14.
            if (rowIndex == rowCount / 13 || RandomNumber(2500u) == 0) {
                UnmountAndMount(queue->GetPath());
            }
        }

        auto approximateBatchSize = static_cast<ui64>(rowCount) * 1_KB / 100;

        auto partitionReaderConfig = New<TPartitionReaderConfig>();
        partitionReaderConfig->UseNativeTabletNodeApi = useNativeTabletNodeApi;
        partitionReaderConfig->MaxRowCount = 5000;
        // We expect to fetch only one row.
        partitionReaderConfig->MaxDataWeight = approximateBatchSize;
        auto partitionReader = CreatePartitionReader(partitionReaderConfig, Client_, consumer->GetPath(), /*partitionIndex*/ 0);

        WaitFor(partitionReader->Open())
            .ThrowOnError();


        i64 rowsRead = 0;
        int smallBatches = 0;
        int bigBatches = 0;

        while (true) {
            auto queueRowset = WaitFor(partitionReader->Read())
                .ValueOrThrow();
            auto aColumnIndex = queueRowset->GetNameTable()->GetIdOrThrow("a");

            if (queueRowset->GetRows().empty()) {
                break;
            }

            EXPECT_EQ(queueRowset->GetRows()[0][aColumnIndex].Data.Uint64, static_cast<ui64>(rowsRead));
            ASSERT_EQ(queueRowset->GetStartOffset(), rowsRead);

            auto batchDataWeight = GetDataWeight(queueRowset->GetRows());
            if (batchDataWeight < approximateBatchSize / 2) {
                ++smallBatches;
            }
            if (batchDataWeight > 3 * approximateBatchSize / 2) {
                ++bigBatches;
            }

            auto transaction = WaitFor(Client_->StartTransaction(ETransactionType::Tablet))
                .ValueOrThrow();
            queueRowset->Commit(transaction);
            WaitFor(transaction->Commit())
                .ThrowOnError();

            rowsRead += std::ssize(queueRowset->GetRows());
        }

        // Account for the potentially small last batch.
        EXPECT_LE(smallBatches, 10);
        EXPECT_EQ(bigBatches, 0);
        EXPECT_EQ(rowsRead, rowCount);
    }
}

TEST_W(TPartitionReaderTest, ReaderCatchingUp)
{
    for (auto useNativeTabletNodeApi : std::vector<bool>{false, true}) {
        auto [queue, consumer, queueNameTable] =
            CreateQueueAndConsumer("ReaderCatchingUp", useNativeTabletNodeApi);

        WriteSingleRow(queue->GetPath(), queueNameTable, {"42u", "hello"});
        UnmountAndMount(queue->GetPath());

        auto partitionReaderConfig = New<TPartitionReaderConfig>();
        partitionReaderConfig->UseNativeTabletNodeApi = useNativeTabletNodeApi;
        auto partitionReader = CreatePartitionReader(partitionReaderConfig, Client_, consumer->GetPath(), /*partitionIndex*/ 0);

        WaitFor(partitionReader->Open())
            .ThrowOnError();

        WriteSingleRow(queue->GetPath(), queueNameTable, {"43u", "darkness"});
        WriteSingleRow(queue->GetPath(), queueNameTable, {"44u", "my"});

        WriteSingleRow(queue->GetPath(), queueNameTable, {"45u", "old"});
        WriteSingleRow(queue->GetPath(), queueNameTable, {"46u", "friend"});

        Client_->TrimTable(queue->GetPath(), 0, 2);
        WaitForRowCount(queue->GetPath(), 3);

        auto queueRowset = WaitFor(partitionReader->Read())
            .ValueOrThrow();
        auto aColumnIndex = queueRowset->GetNameTable()->GetIdOrThrow("a");

        ASSERT_EQ(queueRowset->GetRows().size(), 3u);
        EXPECT_EQ(queueRowset->GetStartOffset(), 2u);
        EXPECT_EQ(queueRowset->GetFinishOffset(), 5u);
        EXPECT_EQ(queueRowset->GetRows()[0][aColumnIndex].Data.Uint64, 44u);

        auto transaction = WaitFor(Client_->StartTransaction(ETransactionType::Tablet))
            .ValueOrThrow();
        queueRowset->Commit(transaction);
        WaitFor(transaction->Commit())
            .ThrowOnError();

        auto consumerClient = CreateBigRTConsumerClient(consumer->GetPath(), *consumer->GetSchema());
        auto partitions = WaitFor(consumerClient->CollectPartitions(Client_, 1))
            .ValueOrThrow();

        ASSERT_EQ(partitions.size(), 1u);
        EXPECT_EQ(partitions[0].NextRowIndex, 5);
    }
}

TEST_W(TPartitionReaderTest, EmptyQueue)
{
    for (auto useNativeTabletNodeApi : std::vector<bool>{false, true}) {
        auto [queue, consumer, queueNameTable] =
            CreateQueueAndConsumer("EmptyQueue", useNativeTabletNodeApi);

        auto partitionReaderConfig = New<TPartitionReaderConfig>();
        partitionReaderConfig->UseNativeTabletNodeApi = useNativeTabletNodeApi;
        auto partitionReader = CreatePartitionReader(partitionReaderConfig, Client_, consumer->GetPath(), /*partitionIndex*/ 0);

        WaitFor(partitionReader->Open())
            .ThrowOnError();

        auto queueRowset = WaitFor(partitionReader->Read())
            .ValueOrThrow();
        ASSERT_EQ(queueRowset->GetRows().size(), 0u);
        EXPECT_EQ(queueRowset->GetStartOffset(), 0);
        EXPECT_EQ(queueRowset->GetFinishOffset(), 0);

        auto transaction = WaitFor(Client_->StartTransaction(ETransactionType::Tablet))
            .ValueOrThrow();
        queueRowset->Commit(transaction);
        WaitFor(transaction->Commit())
            .ThrowOnError();

        auto consumerClient = CreateBigRTConsumerClient(consumer->GetPath(), *consumer->GetSchema());
        auto partitions = WaitFor(consumerClient->CollectPartitions(Client_, 1))
            .ValueOrThrow();

        ASSERT_EQ(partitions.size(), 1u);
        EXPECT_EQ(partitions[0].NextRowIndex, 0);

        WriteSingleRow(queue->GetPath(), queueNameTable, {"43u", "darkness"});
        WriteSingleRow(queue->GetPath(), queueNameTable, {"44u", "my"});
        UnmountAndMount(queue->GetPath());

        Client_->TrimTable(queue->GetPath(), 0, 2);
        WaitForRowCount(queue->GetPath(), 0);

        queueRowset = WaitFor(partitionReader->Read())
            .ValueOrThrow();
        ASSERT_EQ(queueRowset->GetRows().size(), 0u);
        EXPECT_EQ(queueRowset->GetStartOffset(), 0);
        EXPECT_EQ(queueRowset->GetFinishOffset(), 0);

        WriteSingleRow(queue->GetPath(), queueNameTable, {"45u", "darkness"});
        WriteSingleRow(queue->GetPath(), queueNameTable, {"46u", "my"});

        queueRowset = WaitFor(partitionReader->Read())
            .ValueOrThrow();
        ASSERT_EQ(queueRowset->GetRows().size(), 2u);
        EXPECT_EQ(queueRowset->GetStartOffset(), 2);
        EXPECT_EQ(queueRowset->GetFinishOffset(), 4);

        transaction = WaitFor(Client_->StartTransaction(ETransactionType::Tablet))
            .ValueOrThrow();
        queueRowset->Commit(transaction);
        WaitFor(transaction->Commit())
            .ThrowOnError();

        partitions = WaitFor(consumerClient->CollectPartitions(Client_, 1))
            .ValueOrThrow();

        ASSERT_EQ(partitions.size(), 1u);
        EXPECT_EQ(partitions[0].NextRowIndex, 4);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NCppTests

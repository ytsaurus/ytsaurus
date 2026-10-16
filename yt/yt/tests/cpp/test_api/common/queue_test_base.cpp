#include "queue_test_base.h"

#include <yt/yt/tests/cpp/test_base/private.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/attributes.h>
#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/ytlib/queue_client/records/consumer_registration.record.h>

#include <library/cpp/yt/string/format.h>

namespace NYT::NCppTests {

using namespace NApi;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NSecurityClient;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NYPath;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

std::string MakeValueRow(const std::vector<std::string>& values)
{
    std::string result;
    for (int i = 0; i < std::ssize(values); ++i) {
        result += Format("%v<id=%v> %v;", (i == 0 ? "" : " "), i, values[i]);
    }
    return result;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void TQueueTestBase::SetUpTestCase()
{
    TDynamicTablesTestBase::SetUpTestCase();

    // TODO(achulkov2): Separate useful teardown methods from TDynamicTablesTestBase and stop inheriting from it altogether.
    CreateTable(
        /*tablePath*/ "//tmp/fake",
        /*schema*/ TYsonString(R"([
            {name=key;type=uint64;sort_order=ascending};
            {name=value;type=uint64}
        ])"_sb));

    CreateTableOnce(RegistrationTablePath, NQueueClient::NRecords::TConsumerRegistrationDescriptor::Get()->GetSchema());
}

////////////////////////////////////////////////////////////////////////////////

TQueueTestBase::TDynamicTable::TDynamicTable(
    TRichYPath path,
    TTableSchemaPtr schema,
    const IAttributeDictionaryPtr& extraAttributes)
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

TQueueTestBase::TDynamicTable::~TDynamicTable()
{
    SyncUnmountTable(Path_);

    WaitFor(Client_->RemoveNode(Path_))
        .ThrowOnError();
}

const TTableSchemaPtr& TQueueTestBase::TDynamicTable::GetSchema() const
{
    return Schema_;
}

const TYPath& TQueueTestBase::TDynamicTable::GetPath() const
{
    return Path_;
}

const TRichYPath& TQueueTestBase::TDynamicTable::GetRichPath() const
{
    return RichPath_;
}

TRichYPath TQueueTestBase::TDynamicTable::GetRichPathWithCluster() const
{
    auto copy = RichPath_;
    copy.SetCluster(ClusterName_);
    return copy;
}

////////////////////////////////////////////////////////////////////////////////

void TQueueTestBase::WriteSharedRange(const TYPath& path, const TNameTablePtr& nameTable, const TSharedRange<TUnversionedRow>& range)
{
    auto transaction = WaitFor(Client_->StartTransaction(ETransactionType::Tablet))
        .ValueOrThrow();
    transaction->WriteRows(path, nameTable, range);

    WaitFor(transaction->Commit())
        .ThrowOnError();
}

void TQueueTestBase::WriteSingleRow(const TYPath& path, const TNameTablePtr& nameTable, TUnversionedRow row)
{
    auto transaction = WaitFor(Client_->StartTransaction(ETransactionType::Tablet))
        .ValueOrThrow();

    TUnversionedRowsBuilder rowsBuilder;
    rowsBuilder.AddRow(row);
    transaction->WriteRows(path, nameTable, rowsBuilder.Build());

    WaitFor(transaction->Commit())
        .ThrowOnError();
}

void TQueueTestBase::WriteSingleRow(const TYPath& path, const TNameTablePtr& nameTable, const std::vector<std::string>& values)
{
    auto owningRow = YsonToSchemalessRow(MakeValueRow(values));
    WriteSingleRow(path, nameTable, owningRow);
}

void TQueueTestBase::WaitForRowCount(const TYPath& path, i64 rowCount)
{
    WaitForPredicate([rowCount, path] {
        auto allRowsResult = WaitFor(Client_->SelectRows(Format("* from [%v]", path)))
            .ValueOrThrow();

        return std::ssize(allRowsResult.Rowset->GetRows()) == rowCount;
    },
    Format("%v rows were expected", rowCount));
}

std::tuple<TQueueTestBase::TDynamicTablePtr, TQueueTestBase::TDynamicTablePtr, TNameTablePtr>
TQueueTestBase::CreateQueueAndConsumer(const std::string& testName, std::optional<bool> useNativeTabletNodeApi, int queueTabletCount) const
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

void TQueueTestBase::CreateQueueProducer(const TRichYPath& path)
{
    WaitFor(Client_->CreateNode(path.GetPath(), EObjectType::QueueProducer, TCreateNodeOptions{}))
        .ThrowOnError();

    WaitUntilEqual(path.GetPath() + "/@tablet_state", "mounted");
}

IClientPtr TQueueTestBase::CreateUser(const std::string& name) const
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

void TQueueTestBase::AssertPermission(const std::string& user, const TYPath& path, EPermission permission, ESecurityAction action) const
{
    auto permissionResponse = WaitFor(Client_->CheckPermission(user, path, permission))
        .ValueOrThrow();
    ASSERT_EQ(permissionResponse.Action, action);
}

void TQueueTestBase::AssertPermissionAllowed(const std::string& user, const TYPath& path, EPermission permission) const
{
    AssertPermission(user, path, permission, ESecurityAction::Allow);
}

void TQueueTestBase::AssertPermissionDenied(const std::string& user, const TYPath& path, EPermission permission) const
{
    AssertPermission(user, path, permission, ESecurityAction::Deny);
}

void TQueueTestBase::CreateTableOnce(const TYPath& path, const TTableSchemaPtr& schema)
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCppTests

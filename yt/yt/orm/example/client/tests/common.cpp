#include "common.h"

#include <yt/yt/orm/example/client/native/autogen/config.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/name_table.h>

#include <yt/yt/client/api/rowset.h>

#include <yt/yt/core/concurrency/delayed_executor.h>

#include <yt/yt/core/rpc/grpc/channel.h>
#include <yt/yt/core/rpc/roaming_channel.h>
#include <yt/yt/core/rpc/balancing_channel.h>
#include <yt/yt/core/rpc/config.h>

#include <yt/yt/core/ytree/fluent.h>

#include <util/generic/guid.h>
#include <util/system/env.h>

namespace NYT::NOrm::NExample::NClient::NTests {

////////////////////////////////////////////////////////////////////////////////

namespace {

TYsonPayload BuildUserPayload(const TObjectId& userId)
{
    return BuildYsonPayload(BIND([&] (NYson::IYsonConsumer* consumer) {
        NYTree::BuildYsonFluently(consumer)
            .BeginMap()
                .Item("meta")
                    .BeginMap()
                        .Item("id").Value(userId)
                    .EndMap()
            .EndMap();
    }));
}

TYsonPayload BuildPublisherPayload(i64 publisherId)
{
    return BuildYsonPayload(BIND([&] (NYson::IYsonConsumer* consumer) {
        NYTree::BuildYsonFluently(consumer)
            .BeginMap()
                .Item("meta")
                    .BeginMap()
                        .Item("id").Value(publisherId)
                    .EndMap()
            .EndMap();
    }));
}

TYsonPayload BuildBookPayload(
    i64 bookId,
    i64 publisherId,
    bool released)
{
    return BuildYsonPayload(BIND([&] (NYson::IYsonConsumer* consumer) {
        NYTree::BuildYsonFluently(consumer)
            .BeginMap()
                .Item("meta")
                    .BeginMap()
                        .Item("id").Value(bookId)
                        .Item("id2").Value(TNativeClientTestSuite::BookId2Always_)
                        .Item("isbn").Value("ISBN")
                        .Item("publisher_id").Value(publisherId)
                    .EndMap()
                .Item("spec")
                    .BeginMap()
                        .Item("name").Value(ToString(bookId))
                        .Item("year").Value(2021)
                        .Item("font").Value("Font")
                        .Item("genres")
                            .BeginList()
                                .Item().Value("Genre 1")
                                .Item().Value("Genre 2")
                            .EndList()
                        .Item("keywords")
                            .BeginList()
                                .Item().Value("Key word 1")
                                .Item().Value("Key word 2")
                                .Item().Value("Key word 3")
                                .Item().Value("Key word 4")
                                .Item().Value("Key word 5")
                            .EndList()
                        .Item("design")
                            .BeginMap()
                                .Item("color").Value("Black")
                                .Item("cover")
                                    .BeginMap()
                                        .Item("hardcover").Value(true)
                                        .Item("size").Value(1024)
                                        .Item("dpi").Value(-300)
                                    .EndMap()
                            .EndMap()
                        .Item("page_count").Value(64)
                    .EndMap()
                .Item("status")
                    .BeginMap()
                        .Item("released").Value(released)
                    .EndMap()
            .EndMap();
    }));
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

const NLogging::TLogger Logger("NativeClientTest");

////////////////////////////////////////////////////////////////////////////////

void TNativeClientTestSuite::SetUp()
{
    Client_ = CreateClient();
}

const IClientPtr& TNativeClientTestSuite::GetClient() const
{
    return Client_;
}

int TNativeClientTestSuite::GetExpectedMasterCount() const
{
    auto rawMasterCount = GetEnv("EXAMPLE_MASTER_COUNT");
    return rawMasterCount.Empty() ? 1 : FromString<int>(rawMasterCount);
}

void TNativeClientTestSuite::WaitAllMastersAlive() const
{
    WaitForPredicate([&] {
        return std::ssize(WaitFor(Client_->GetMasters())
            .ValueOrThrow().MasterInfos) == GetExpectedMasterCount();
    });
}

////////////////////////////////////////////////////////////////////////////////

void TGenericOrmClientTest::SetUp()
{
    auto channelFactory = NYT::NRpc::NGrpc::GetGrpcChannelFactory();
    auto endpointDescription = TString("ObjectService");
    auto endpointAttributes = NYTree::ConvertToAttributes(NYTree::BuildYsonStringFluently()
        .BeginMap()
        .EndMap());
    auto config = New<NRpc::TBalancingChannelConfig>();;
    config->Addresses = {GetEnv("EXAMPLE_MASTER_GRPC_INSECURE_ADDR")};
    ChannelProvider_ = NRpc::CreateBalancingChannelProvider(
        config,
        std::move(channelFactory),
        endpointDescription,
        std::move(endpointAttributes));
}

NRpc::IRoamingChannelProviderPtr TGenericOrmClientTest::GetChannelProvider()
{
    return ChannelProvider_;
}

////////////////////////////////////////////////////////////////////////////////

IClientPtr TNativeClientTestSuite::CreateClient()
{
    auto address = GetEnv("EXAMPLE_MASTER_GRPC_INSECURE_ADDR");
    auto configNode = NYTree::BuildYsonNodeFluently()
        .BeginMap()
            .Item("secure").Value(false)
            .Item("use_legacy_connection").Value(false)
            .Item("addresses")
                .BeginList()
                    .Item().Value(address)
                .EndList()
        .EndMap();

    auto config = New<TConnectionConfig>();
    config->Load(configNode);

    auto client = NNative::CreateClient(std::move(config));

    return client;
}

////////////////////////////////////////////////////////////////////////////////

bool CheckObjectExistence(
    const IClientPtr& client,
    TObjectTypeValue objectType,
    const TString& objectId)
{
    auto payloadCount = WaitFor(client->GetObject(
        objectId,
        objectType,
        {"/meta/id"},
        TGetObjectOptions{
            .IgnoreNonexistent = true,
        }))
        .ValueOrThrow()
        .Result
        .ValuePayloads
        .size();
    YT_VERIFY(payloadCount == 0 || payloadCount == 1);
    return payloadCount == 1;
}

void CreateUser(
    const IClientPtr& client,
    TTransactionId transactionId,
    const TString& userId)
{
    WaitFor(client->CreateObject(
        TObjectTypeValues::User,
        BuildUserPayload(userId),
        TCreateObjectOptions{
            .TransactionId = transactionId,
        }))
        .ThrowOnError();
}

void CreatePublisher(
    const IClientPtr& client,
    i64 publisherId,
    TTransactionId transactionId)
{
    WaitFor(client->CreateObject(
        TObjectTypeValues::Publisher,
        BuildPublisherPayload(publisherId),
        TCreateObjectOptions{
            .TransactionId = transactionId,
            .Format = EPayloadFormat::Protobuf,
        }))
        .ThrowOnError();
}

TObjectIdentity CreateBook(
    const IClientPtr& client,
    i64 publisherId,
    i64 bookId,
    bool released,
    std::optional<TUpdateIfExisting> updateIfExisting)
{
    return WaitFor(client->CreateObject(
        TObjectTypeValues::Book,
        BuildBookPayload(bookId, publisherId, released),
        TCreateObjectOptions{
            .Format = EPayloadFormat::Protobuf,
        },
        std::move(updateIfExisting)))
        .ValueOrThrow()
        .Meta;
}

////////////////////////////////////////////////////////////////////////////////

bool TTestTable::TRow::operator == (const TRow& other) const
{
    return Key == other.Key && Value == other.Value;
}

////////////////////////////////////////////////////////////////////////////////

TTestTable TTestTable::Create(const NYT::NApi::IClientPtr& ytClient)
{
    auto tablePath = TString{"//tmp/state_"} + CreateGuidAsString().substr(0, 6);

    static const TString rawAttributes = "{dynamic=%true;schema=[{name=key;type=string;sort_order=ascending};{name=value;type=string}]}";
    auto attributes = NYson::TYsonString(rawAttributes);
    NApi::TCreateNodeOptions options;
    options.Attributes = NYTree::ConvertToAttributes(attributes);

    WaitFor(ytClient->CreateNode(tablePath, NObjectClient::EObjectType::Table, options))
        .ThrowOnError();
    WaitFor(ytClient->MountTable(tablePath))
        .ThrowOnError();

    auto isMounted = [&tablePath, &ytClient] {
        auto yson = WaitFor(ytClient->GetNode(tablePath + "/@tablet_state"))
            .ValueOrThrow();
        auto tabletState = ParseEnum<NTabletClient::ETabletState>(NYTree::ConvertTo<TString>(yson));
        return tabletState == NTabletClient::ETabletState::Mounted;
    };

    while (!isMounted()) {
        TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(500));
    }

    return TTestTable(tablePath);
}

void TTestTable::InsertRow(
    const NApi::ITransactionPtr& ytTransaction,
    const TString& key,
    const TString& value)
{
    auto schema = New<NTableClient::TNameTable>();
    schema->RegisterName("key");
    schema->RegisterName("value");

    NTableClient::TUnversionedRowsBuilder rowBuilder;
    rowBuilder.AddRow(key, value);

    ytTransaction->WriteRows(TablePath_, schema, rowBuilder.Build());
}

std::vector<TTestTable::TRow> TTestTable::Select(
    const NApi::IClientPtr& ytClient)
{
    TStringBuilder query;
    query.AppendFormat("key, value from [%v]", TablePath_);

    auto selectResult = WaitFor(ytClient->SelectRows(query.Flush()))
        .ValueOrThrow();
    std::vector<TTestTable::TRow> result;
    for (const auto& row : selectResult.Rowset->GetRows()) {
        TString key;
        TString value;
        NTableClient::FromUnversionedRow(
            row,
            &key,
            &value);
        result.push_back(TRow{std::move(key), std::move(value)});
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

TTestTable::TTestTable(TString tablePath)
    : TablePath_(std::move(tablePath))
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NExample::NClient::NTests

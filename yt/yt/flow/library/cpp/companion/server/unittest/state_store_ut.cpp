#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/companion/server/runtime_init_context.h>
#include <yt/yt/flow/library/cpp/companion/server/state_store.h>

#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/payload_converter.h>
#include <yt/yt/flow/library/cpp/common/schema.h>

#include <yt/yt/flow/library/cpp/process_function/testing/entity_builders.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow::NCompanionServer {
namespace {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TCompanionStateStorePtr MakeStore()
{
    return New<TCompanionStateStore>(
        THashSet<std::string>{"counter"},
        THashSet<std::string>{"profile"},
        THashSet<std::string>{"joined"},
        NTesting::DefaultTestKeySchema());
}

std::string YsonBytes(i64 value)
{
    return std::string(NYson::ConvertToYsonString(value).ToString());
}

//! Batch input whose only purpose is to bring the given keys into the batch key set.
TBatchInput MakeBatchInputWithKeys(const std::vector<ui64>& keyValues)
{
    TBatchInput input;
    auto schema = NTesting::DefaultTestKeySchema();
    for (auto keyValue : keyValues) {
        input.Messages.push_back(NTesting::MakeTestMessage(
            TStreamId("input"),
            MakeKey(keyValue),
            schema,
            [&] (TMessageBuilder& builder) {
                builder.Payload().Set(keyValue, "key");
            }));
    }
    return input;
}

//! Key schema of a joined state whose partitioning column is computed, as
//! |join_on.key_schema_override| declares it in a pipeline spec.
NTableClient::TTableSchemaPtr ComputedJoinedKeySchema()
{
    return ConvertTo<NTableClient::TTableSchemaPtr>(NYson::TYsonString(TStringBuf(R"""(
        [
            {name="hash"; type="uint64"; required=%true; expression="farm_hash(word)"; sort_order="ascending";};
            {name="word"; type="string"; sort_order="ascending";};
        ]
    )""")));
}

NTableClient::TTableSchemaPtr JoinedStateSchema()
{
    return ConvertTo<NTableClient::TTableSchemaPtr>(NYson::TYsonString(TStringBuf(R"""(
        [
            {name="count"; type="uint64";};
        ]
    )""")));
}

NTableClient::TTableSchemaPtr WordMessageSchema()
{
    return ConvertTo<NTableClient::TTableSchemaPtr>(NYson::TYsonString(TStringBuf(R"""(
        [
            {name="word"; type="string";};
        ]
    )""")));
}

TCompanionStateStorePtr MakeJoinedStore(
    const NTableClient::TTableSchemaPtr& keySchema,
    bool hasKeySchemaOverride)
{
    return New<TCompanionStateStore>(
        THashSet<std::string>{},
        THashSet<std::string>{},
        THashSet<std::string>{"joined"},
        New<NTableClient::TTableSchema>(),
        THashMap<std::string, TCompanionExternalStateJoinerConfig>{
            {"joined", {
                    .KeySchema = keySchema,
                    .ConverterCache = CreatePayloadConverterCache(/*evaluatorCache*/ nullptr),
                    .HasKeySchemaOverride = hasKeySchemaOverride,
                       }},
        });
}

TEST(TCompanionStateStoreTest, InternalStateLifecycle)
{
    auto store = MakeStore();
    auto provider = store->RegisterInternalState(
        "counter",
        &New<TYsonSerializableStateHolder<i64>>);
    TMutableStateKeyClient<i64> client(provider);

    auto key1 = MakeKey(ui64{1});
    auto key2 = MakeKey(ui64{2});
    auto key3 = MakeKey(ui64{3});

    auto input = MakeBatchInputWithKeys({1, 3});
    auto& holder = input.InternalStates["counter"];
    holder.StateName = "counter";
    holder.StateItems.push_back({.Key = key1, .Reset = false, .State = YsonBytes(5)});
    holder.StateItems.push_back({.Key = key2, .Reset = false, .State = YsonBytes(7)});
    store->LoadBatch(input);

    // Incoming state deserializes into the typed holder.
    EXPECT_EQ(*client.GetState(key1), 5);

    // Modify key1; leave key2 untouched; create key3.
    *client.GetState(key1) = 6;
    *client.GetState(key3) = 100;

    std::vector<NCompanion::TStateHolder<std::string>> internalStates;
    std::vector<NCompanion::TStateHolder<TPayload>> externalStates;
    store->CollectModified(&internalStates, &externalStates);

    ASSERT_EQ(std::ssize(internalStates), 1);
    THashMap<TKey, std::string> modified;
    for (const auto& item : internalStates[0].StateItems) {
        EXPECT_FALSE(item.Reset);
        modified[item.Key] = item.State;
    }
    EXPECT_EQ(
        modified,
        (THashMap<TKey, std::string>{{key1, YsonBytes(6)}, {key3, YsonBytes(100)}}));
}

TEST(TCompanionStateStoreTest, InternalStateResetAndUnchanged)
{
    auto store = MakeStore();
    auto provider = store->RegisterInternalState(
        "counter",
        &New<TYsonSerializableStateHolder<i64>>);
    TMutableStateKeyClient<i64> client(provider);

    auto key1 = MakeKey(ui64{1});
    auto key2 = MakeKey(ui64{2});

    auto input = MakeBatchInputWithKeys({1, 2});
    auto& holder = input.InternalStates["counter"];
    holder.StateName = "counter";
    holder.StateItems.push_back({.Key = key1, .Reset = false, .State = YsonBytes(5)});
    holder.StateItems.push_back({.Key = key2, .Reset = false, .State = YsonBytes(7)});
    store->LoadBatch(input);

    // Reading without writing does not mark the state modified.
    EXPECT_EQ(*client.GetState(key2), 7);
    // Clearing an existing state produces a reset item.
    client.GetState(key1).Clear();

    std::vector<NCompanion::TStateHolder<std::string>> internalStates;
    std::vector<NCompanion::TStateHolder<TPayload>> externalStates;
    store->CollectModified(&internalStates, &externalStates);

    ASSERT_EQ(std::ssize(internalStates), 1);
    ASSERT_EQ(std::ssize(internalStates[0].StateItems), 1);
    EXPECT_EQ(internalStates[0].StateItems[0].Key, key1);
    EXPECT_TRUE(internalStates[0].StateItems[0].Reset);
}

TEST(TCompanionStateStoreTest, UndeclaredStateThrows)
{
    auto store = MakeStore();
    EXPECT_THROW_WITH_SUBSTRING(
        store->RegisterInternalState("unknown", &New<TYsonSerializableStateHolder<i64>>),
        "not declared");
    EXPECT_THROW_WITH_SUBSTRING(
        store->GetExternalStateManager("unknown"),
        "not declared");
    EXPECT_THROW_WITH_SUBSTRING(
        store->GetExternalStateJoiner("unknown"),
        "not declared");
}

TEST(TCompanionStateStoreTest, ConflictingCanonicalNamesThrow)
{
    EXPECT_THROW_WITH_SUBSTRING(
        Y_UNUSED(New<TCompanionStateStore>(
            THashSet<std::string>{"counter", "/counter"},
            THashSet<std::string>{},
            THashSet<std::string>{},
            NTesting::DefaultTestKeySchema())),
        "canonicalize to the same name");
}

TEST(TCompanionStateStoreTest, WriteOutsideBatchKeysThrows)
{
    auto store = MakeStore();
    auto provider = store->RegisterInternalState(
        "counter",
        &New<TYsonSerializableStateHolder<i64>>);
    TMutableStateKeyClient<i64> client(provider);

    store->LoadBatch(MakeBatchInputWithKeys({1}));
    *client.GetState(MakeKey(ui64{99})) = 1;

    std::vector<NCompanion::TStateHolder<std::string>> internalStates;
    std::vector<NCompanion::TStateHolder<TPayload>> externalStates;
    EXPECT_THROW_WITH_SUBSTRING(
        store->CollectModified(&internalStates, &externalStates),
        "outside the current batch");
}

TEST(TCompanionStateStoreTest, ExternalStateLifecycle)
{
    auto store = MakeStore();
    auto manager = store->GetExternalStateManager("profile");
    TMutableStateKeyClient<TSimpleExternalState> client(manager);

    auto stateSchema = NTesting::DefaultTestKeySchema();
    auto key1 = MakeKey(ui64{1});
    auto key2 = MakeKey(ui64{2});

    TPayloadBuilder builder(stateSchema);
    builder.Set(ui64{5}, "key");
    auto payload = builder.Finish();

    auto input = MakeBatchInputWithKeys({1, 2});
    auto& holder = input.ExternalStates["profile"];
    holder.StateName = "profile";
    holder.Schema = stateSchema;
    holder.StateItems.push_back({.Key = key1, .Reset = false, .State = payload});
    store->LoadBatch(input);

    // Incoming payload is visible.
    EXPECT_EQ(client.GetState(key1)->template GetColumnValue<ui64>("key"), ui64{5});

    // Overwrite key1, create key2.
    {
        TPayloadBuilder update(stateSchema);
        update.Set(ui64{6}, "key");
        client.GetState(key1)->Payload = update.Finish();
    }
    {
        TPayloadBuilder update(stateSchema);
        update.Set(ui64{7}, "key");
        client.GetState(key2)->Payload = update.Finish();
    }

    std::vector<NCompanion::TStateHolder<std::string>> internalStates;
    std::vector<NCompanion::TStateHolder<TPayload>> externalStates;
    store->CollectModified(&internalStates, &externalStates);

    ASSERT_EQ(std::ssize(externalStates), 1);
    EXPECT_EQ(externalStates[0].StateName, "profile");
    ASSERT_TRUE(externalStates[0].Schema);
    THashMap<TKey, ui64> modified;
    for (const auto& item : externalStates[0].StateItems) {
        EXPECT_FALSE(item.Reset);
        modified[item.Key] = GetColumnValue<ui64>(item.State, 0);
    }
    EXPECT_EQ(modified, (THashMap<TKey, ui64>{{key1, 6}, {key2, 7}}));
}

TEST(TCompanionStateStoreTest, ExternalStateResetAndUnchanged)
{
    auto store = MakeStore();
    auto manager = store->GetExternalStateManager("profile");
    TMutableStateKeyClient<TSimpleExternalState> client(manager);

    auto stateSchema = NTesting::DefaultTestKeySchema();
    auto key1 = MakeKey(ui64{1});
    auto key2 = MakeKey(ui64{2});

    TPayloadBuilder builder(stateSchema);
    builder.Set(ui64{5}, "key");
    auto payload = builder.Finish();

    auto input = MakeBatchInputWithKeys({1, 2});
    auto& holder = input.ExternalStates["profile"];
    holder.StateName = "profile";
    holder.Schema = stateSchema;
    holder.StateItems.push_back({.Key = key1, .Reset = false, .State = payload});
    holder.StateItems.push_back({.Key = key2, .Reset = false, .State = payload});
    store->LoadBatch(input);

    // Reading without writing does not mark the state modified.
    EXPECT_EQ(client.GetState(key2)->template GetColumnValue<ui64>("key"), ui64{5});
    // Clearing an existing state produces a reset item, which is what makes
    // the worker delete the row.
    client.GetState(key1)->Clear();

    std::vector<NCompanion::TStateHolder<std::string>> internalStates;
    std::vector<NCompanion::TStateHolder<TPayload>> externalStates;
    store->CollectModified(&internalStates, &externalStates);

    ASSERT_EQ(std::ssize(externalStates), 1);
    ASSERT_EQ(std::ssize(externalStates[0].StateItems), 1);
    EXPECT_EQ(externalStates[0].StateItems[0].Key, key1);
    EXPECT_TRUE(externalStates[0].StateItems[0].Reset);
}

TEST(TCompanionStateStoreTest, JoinedStateKeySchemaOverride)
{
    auto keySchema = NTesting::DefaultTestKeySchema();
    auto store = MakeJoinedStore(keySchema, /*hasKeySchemaOverride*/ true);
    TJoinedStateKeyClient<TSimpleExternalState> client(store->GetExternalStateJoiner("joined"));

    auto joinedKey = MakeKey(ui64{1});
    TPayloadBuilder stateBuilder(keySchema);
    stateBuilder.Set(ui64{42}, "key");

    TBatchInput input;
    input.Messages.push_back(NTesting::MakeTestMessage(
        TStreamId("input"),
        MakeKey(ui64{999}),
        keySchema,
        [&] (TMessageBuilder& builder) {
            builder.Payload().Set(ui64{1}, "key");
        }));
    auto& holder = input.JoinedExternalStates["joined"];
    holder.StateName = "joined";
    holder.Schema = keySchema;
    holder.StateItems.push_back({.Key = joinedKey, .Reset = false, .State = stateBuilder.Finish()});
    store->LoadBatch(input);

    EXPECT_EQ(client.ResolveKey(input.Messages[0]), joinedKey);
    auto state = client.GetState(input.Messages[0]);
    ASSERT_TRUE(state.IsInitialized());
    EXPECT_EQ(state->template GetColumnValue<ui64>("key"), ui64{42});
}

TEST(TCompanionStateStoreTest, JoinedStateKeySchemaOverrideStripsExpressionColumns)
{
    auto keySchema = ComputedJoinedKeySchema();
    auto store = MakeJoinedStore(keySchema, /*hasKeySchemaOverride*/ true);
    TJoinedStateKeyClient<TSimpleExternalState> client(store->GetExternalStateJoiner("joined"));

    auto stateSchema = JoinedStateSchema();
    TPayloadBuilder stateBuilder(stateSchema);
    stateBuilder.Set(ui64{42}, "count");

    TBatchInput input;
    input.Messages.push_back(NTesting::MakeTestMessage(
        TStreamId("input"),
        MakeKey(ui64{999}),
        WordMessageSchema(),
        [&] (TMessageBuilder& builder) {
            builder.Payload().Set("hello", "word");
        }));
    auto& holder = input.JoinedExternalStates["joined"];
    holder.StateName = "joined";
    holder.Schema = stateSchema;
    // The worker sends keys laid out on the full override schema, hash included.
    holder.StateItems.push_back({
        .Key = MakeKey(ui64{12345}, TStringBuf("hello")),
        .Reset = false,
        .State = stateBuilder.Finish(),
    });
    store->LoadBatch(input);

    EXPECT_EQ(*StripExpressionColumns(keySchema), *client.GetKeySchema());
    EXPECT_EQ(MakeKey(TStringBuf("hello")), client.ResolveKey(input.Messages[0]));

    auto state = client.GetState(input.Messages[0]);
    ASSERT_TRUE(state.IsInitialized());
    EXPECT_EQ(state->template GetColumnValue<ui64>("count"), ui64{42});

    // The state stays addressable by the full key the worker sent, too.
    auto stateByWireKey = client.GetState(MakeKey(ui64{12345}, TStringBuf("hello")));
    ASSERT_TRUE(stateByWireKey.IsInitialized());
    EXPECT_EQ(stateByWireKey->template GetColumnValue<ui64>("count"), ui64{42});

    // A key the batch carried no joined state for is an uninitialized accessor.
    EXPECT_FALSE(client.GetState(MakeKey(TStringBuf("missing"))).IsInitialized());
}

TEST(TCompanionStateStoreTest, JoinedStateWireKeyOutsideOverrideSchemaIsKeptVerbatim)
{
    auto store = MakeJoinedStore(ComputedJoinedKeySchema(), /*hasKeySchemaOverride*/ true);
    TJoinedStateKeyClient<TSimpleExternalState> client(store->GetExternalStateJoiner("joined"));

    auto stateSchema = JoinedStateSchema();
    TPayloadBuilder stateBuilder(stateSchema);
    stateBuilder.Set(ui64{42}, "count");

    auto wireKey = MakeKey(ui64{12345});

    TBatchInput input;
    auto& holder = input.JoinedExternalStates["joined"];
    holder.StateName = "joined";
    holder.Schema = stateSchema;
    // A key narrower than the override schema is not laid out on it, so it must not be re-laid
    // out: doing so would read past its end.
    holder.StateItems.push_back({.Key = wireKey, .Reset = false, .State = stateBuilder.Finish()});
    store->LoadBatch(input);

    auto state = client.GetState(wireKey);
    ASSERT_TRUE(state.IsInitialized());
    EXPECT_EQ(state->template GetColumnValue<ui64>("count"), ui64{42});
}

TEST(TCompanionStateStoreTest, JoinedStateWithoutOverrideKeepsWireKeys)
{
    auto keySchema = ComputedJoinedKeySchema();
    auto store = MakeJoinedStore(keySchema, /*hasKeySchemaOverride*/ false);
    TJoinedStateKeyClient<TSimpleExternalState> client(store->GetExternalStateJoiner("joined"));

    auto stateSchema = JoinedStateSchema();
    TPayloadBuilder stateBuilder(stateSchema);
    stateBuilder.Set(ui64{42}, "count");

    auto wireKey = MakeKey(ui64{12345}, TStringBuf("hello"));

    TBatchInput input;
    input.Messages.push_back(NTesting::MakeTestMessage(
        TStreamId("input"),
        wireKey,
        WordMessageSchema(),
        [&] (TMessageBuilder& builder) {
            builder.Payload().Set("hello", "word");
        }));
    auto& holder = input.JoinedExternalStates["joined"];
    holder.StateName = "joined";
    holder.Schema = stateSchema;
    holder.StateItems.push_back({.Key = wireKey, .Reset = false, .State = stateBuilder.Finish()});
    store->LoadBatch(input);

    // Without an override the message key is used verbatim, expression columns and all.
    EXPECT_EQ(wireKey, client.ResolveKey(input.Messages[0]));

    auto state = client.GetState(input.Messages[0]);
    ASSERT_TRUE(state.IsInitialized());
    EXPECT_EQ(state->template GetColumnValue<ui64>("count"), ui64{42});
    EXPECT_FALSE(client.GetState(MakeKey(TStringBuf("hello"))).IsInitialized());
}

TEST(TCompanionStateStoreTest, JoinedStateReadOnly)
{
    auto store = MakeStore();
    auto joiner = store->GetExternalStateJoiner("joined");
    TJoinedStateKeyClient<TSimpleExternalState> client(joiner);

    auto stateSchema = NTesting::DefaultTestKeySchema();
    auto key1 = MakeKey(ui64{1});

    TPayloadBuilder builder(stateSchema);
    builder.Set(ui64{42}, "key");

    auto input = MakeBatchInputWithKeys({1});
    auto& holder = input.JoinedExternalStates["joined"];
    holder.StateName = "joined";
    holder.Schema = stateSchema;
    holder.StateItems.push_back({.Key = key1, .Reset = false, .State = builder.Finish()});
    store->LoadBatch(input);

    auto seeded = client.GetState(key1);
    ASSERT_TRUE(seeded.IsInitialized());
    EXPECT_EQ(seeded->template GetColumnValue<ui64>("key"), ui64{42});

    // A key the batch carried no joined state for is an uninitialized accessor.
    EXPECT_FALSE(client.GetState(MakeKey(ui64{2})).IsInitialized());
}

TEST(TCompanionRuntimeInitContextTest, PrefixAndParameters)
{
    auto store = MakeStore();
    auto parameters = ConvertTo<IMapNodePtr>(NYson::TYsonString(TStringBuf("{answer=42}")));
    auto initContext = New<TCompanionRuntimeInitContext>(store, parameters);

    EXPECT_EQ(initContext->GetParametersNode()->GetChildOrThrow("answer")->AsInt64()->GetValue(), 42);

    TMutableStateKeyClient<i64> client;
    initContext->InitClient(client, "counter");
    EXPECT_TRUE(client.IsInitialized());

    EXPECT_THROW_WITH_SUBSTRING(
        Y_UNUSED(initContext->GetStaticResource(TResourceId("some_resource"))),
        "not available in this companion process");
    EXPECT_THROW_WITH_SUBSTRING(
        Y_UNUSED(initContext->AsPartition()),
        "not available in a companion process");
    EXPECT_THROW_WITH_SUBSTRING(
        Y_UNUSED(initContext->GetPartitionId()),
        "not available in a companion process");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NCompanionServer

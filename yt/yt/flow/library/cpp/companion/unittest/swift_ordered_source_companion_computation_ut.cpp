#include <yt/yt/core/test_framework/framework.h>
#include <yt/yt/flow/library/cpp/companion/swift_ordered_source_companion_computation.h>

namespace NYT::NFlow::NCompanion {
namespace {

using namespace NTableClient;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TEST(TSwiftOrderedSourceCompanionComputationTest, CreateLocalStreamSpecStorage)
{
    auto sourceSchema = ConvertTo<TTableSchemaPtr>(TYsonString(TStringBuf(R""""(
    [
        {name="data"; type="string";};
    ]
    )"""")));

    auto eventSchema = ConvertTo<TTableSchemaPtr>(TYsonString(TStringBuf(R""""(
    [
        { name="data"; type="string";};
        { name="number"; type="int64";};
    ]
    )"""")));

    auto streamSpecStorageState = ConvertTo<TStreamSpecStorageStatePtr>(TYsonString(TStringBuf(R""""(
    {
        stream_specs={
            event_1={
                "999"={
                    schema=[
                        { name="data"; type="string";};
                        { name="number"; type="int64";};
                    ];
                };
            };
        };
    }
    )"""")));

    auto streamSpecs = New<TStreamSpecs>(streamSpecStorageState->StreamSpecs);
    THashMap<TStreamId, NTableClient::TTableSchemaPtr> sourceStreamsSchemas;
    sourceStreamsSchemas[TStreamId("queue_1")] = sourceSchema;
    THashSet<TStreamId> outputStreamIds;
    outputStreamIds.emplace(TStreamId("event_1"));

    auto localStreamSpecs = CreateLocalStreamSpecs(sourceStreamsSchemas, outputStreamIds, streamSpecs);
    auto queueStreamSpecId = localStreamSpecs->GetLastSpecId(TStreamId("queue_1"));
    auto sourceStreamSpecId = localStreamSpecs->GetStreamSpecId(sourceSchema);
    EXPECT_EQ(queueStreamSpecId.Underlying(), sourceStreamSpecId.Underlying());

    auto eventStreamSpecId = localStreamSpecs->GetLastSpecId(TStreamId("event_1"));
    auto eventStreamSpec = localStreamSpecs->GetSpec(eventStreamSpecId);
    // Compare YsonString representation for the sake of test simplicity.
    EXPECT_EQ(ConvertToYsonString(eventStreamSpec->Schema), ConvertToYsonString(eventSchema));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NCompanion

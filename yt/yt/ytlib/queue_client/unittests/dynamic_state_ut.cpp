#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/ytlib/queue_client/dynamic_state.h>

#include <yt/yt/client/api/rowset.h>

#include <yt/yt/client/table_client/comparator.h>

#include <yt/yt/client/queue_client/config.h>

#include <yt/yt/core/ytree/helpers.h>

#include <yt/yt/client/complex_types/uuid_text.h>

namespace NYT::NQueueClient {
namespace {

using namespace NYTree;
using namespace NYson;
using namespace NTableClient;
using namespace NObjectClient;
using namespace NComplexTypes;

////////////////////////////////////////////////////////////////////////////////

// TODO(achulkov2): Move this unittest along with TCrossClusterReference.
TEST(TCrossClusterReferenceTest, FromString)
{
    EXPECT_EQ(
        (TCrossClusterReference{.Cluster = "kek", .Path = "keker"}),
        TCrossClusterReference::FromString("kek:keker"));

    EXPECT_EQ(
        (TCrossClusterReference{.Cluster = "haha", .Path = "haha:haha:"}),
        TCrossClusterReference::FromString("haha:haha:haha:"));

    EXPECT_THROW(TCrossClusterReference::FromString("hahahaha"), TErrorException);
}

////////////////////////////////////////////////////////////////////////////////

template <class TRow>
void CheckConversions(
    const TCrossClusterReference& object,
    TRowRevision rowRevision,
    const IAttributeDictionaryPtr& cypressAttributes,
    const TRow& expectedRow)
{
    auto row0 = TRow::FromAttributeDictionary(object, rowRevision, cypressAttributes);
    auto rowset = TRow::InsertRowRange({row0});
    auto row1 = TRow::ParseRowRange(rowset->GetRows(), rowset->GetNameTable())[0];
    EXPECT_EQ(row0, expectedRow);
    EXPECT_EQ(row1, expectedRow);
}

TEST(TTableRowTest, QueueBoilerplateSanity)
{
    auto expectedAutoTrimConfig = TQueueAutoTrimConfig::Create();
    expectedAutoTrimConfig.Enable = true;

    TString guidBytes = "\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10";
    auto objectId = TObjectId(GuidFromBytes(guidBytes));

    TString ysonAttributes = "{attribute_revision=43u; type=table; sorted=%false; dynamic=%true; "
        "auto_trim_config={enable=%true}; queue_agent_stage=fun; "
        + Format("id=%Qv}", objectId);

    CheckConversions<TQueueTableRow>(
        {.Cluster = "mamma", .Path = "mia"},
        15,
        ConvertToAttributes(TYsonStringBuf(ysonAttributes)),
        TQueueTableRow{
            .Ref = {.Cluster = "mamma", .Path = "mia"},
            .RowRevision = 15,
            .Revision = 43,
            .ObjectType = NObjectClient::EObjectType::Table,
            .Dynamic = true,
            .Sorted = false,
            .AutoTrimConfig = expectedAutoTrimConfig,
            .QueueAgentStage = "fun",
            .ObjectId = objectId,
            .SynchronizationError = TError(),
        });
}

TEST(TTableRowTest, ConsumerBoilerplateSanity)
{
    CheckConversions<TConsumerTableRow>(
        {.Cluster = "mamma", .Path = "mia"},
        15,
        ConvertToAttributes(TYsonStringBuf(
            "{attribute_revision=43u; type=table; treat_as_queue_consumer=%true; "
            "schema=[{name=a; type=int64; sort_order=ascending}];"
            "queue_agent_stage=fun}")),
        {
            .Ref = {.Cluster = "mamma", .Path = "mia"},
            .RowRevision = 15,
            .Revision = 43,
            .ObjectType = NObjectClient::EObjectType::Table,
            .TreatAsQueueConsumer = true,
            .Schema = TTableSchema({TColumnSchema("a", EValueType::Int64, ESortOrder::Ascending)}),
            .QueueAgentStage = "fun",
            .SynchronizationError = TError(),
        });

    // Check with optional fields absent.
    CheckConversions<TConsumerTableRow>(
        {.Cluster ="mamma", .Path ="mia"},
        15,
        ConvertToAttributes(TYsonStringBuf(
            "{attribute_revision=43u; type=table; "
            "schema=[{name=a; type=int64; sort_order=ascending}];}")),
        {
            .Ref = {.Cluster = "mamma", .Path = "mia"},
            .RowRevision = 15,
            .Revision = 43,
            .ObjectType = NObjectClient::EObjectType::Table,
            .TreatAsQueueConsumer = false,
            .Schema = TTableSchema({TColumnSchema("a", EValueType::Int64, ESortOrder::Ascending)}),
            .SynchronizationError = TError(),
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NQueueClient

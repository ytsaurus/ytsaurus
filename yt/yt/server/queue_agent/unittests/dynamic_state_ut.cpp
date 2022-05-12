#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/queue_agent/dynamic_state.h>

#include <yt/yt/client/api/rowset.h>

#include <yt/yt/client/table_client/comparator.h>

#include <yt/yt/core/ytree/helpers.h>

namespace NYT::NQueueAgent {
namespace {

using namespace NYTree;
using namespace NYson;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

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
    auto row1 = TRow::ParseRowRange(rowset->GetRows(), rowset->GetNameTable(), rowset->GetSchema())[0];
    EXPECT_EQ(row0, expectedRow);
    EXPECT_EQ(row1, expectedRow);
}

TEST(TTableRowTest, QueueBoilerplateSanity)
{
    CheckConversions<TQueueTableRow>(
        {.Cluster = "mamma", .Path = "mia"},
        15,
        ConvertToAttributes(TYsonStringBuf("{attribute_revision=43u; type=table; sorted=%false; dynamic=%true}")),
        {
            .Queue = {.Cluster = "mamma", .Path = "mia"},
            .RowRevision = 15,
            .Revision = 43,
            .ObjectType = NObjectClient::EObjectType::Table,
            .Dynamic = true,
            .Sorted = false,
            .SynchronizationError = TError(),
        });
}

TEST(TTableRowTest, ConsumerBoilerplateSanity)
{
    CheckConversions<TConsumerTableRow>(
        {.Cluster = "mamma", .Path = "mia"},
        15,
        ConvertToAttributes(TYsonStringBuf(
            "{attribute_revision=43u; type=table; target_queue=\"cluster:path\"; treat_as_queue_consumer=%true; "
            "schema=[{name=a; type=int64; sort_order=ascending}]; vital_queue_consumer=%true; owner=nosokhvost}")),
        {
            .Consumer = {.Cluster = "mamma", .Path = "mia"},
            .RowRevision = 15,
            .TargetQueue = TCrossClusterReference{.Cluster = "cluster", .Path = "path"},
            .Revision = 43,
            .ObjectType = NObjectClient::EObjectType::Table,
            .TreatAsQueueConsumer = true,
            .Schema = TTableSchema({TColumnSchema("a", EValueType::Int64, ESortOrder::Ascending)}),
            .Vital = true,
            .Owner = "nosokhvost",
            .SynchronizationError = TError(),
        });

    // Check with optional fields absent.
    CheckConversions<TConsumerTableRow>(
        {.Cluster ="mamma", .Path ="mia"},
        15,
        ConvertToAttributes(TYsonStringBuf(
            "{attribute_revision=43u; type=table; target_queue=\"cluster:path\"; "
            "schema=[{name=a; type=int64; sort_order=ascending}]; owner=hydra}")),
        {
            .Consumer = {.Cluster = "mamma", .Path = "mia"},
            .RowRevision = 15,
            .TargetQueue = TCrossClusterReference{.Cluster = "cluster", .Path = "path"},
            .Revision = 43,
            .ObjectType = NObjectClient::EObjectType::Table,
            .TreatAsQueueConsumer = false,
            .Schema = TTableSchema({TColumnSchema("a", EValueType::Int64, ESortOrder::Ascending)}),
            .Vital = false,
            .Owner = "hydra",
            .SynchronizationError = TError(),
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NQueueAgent

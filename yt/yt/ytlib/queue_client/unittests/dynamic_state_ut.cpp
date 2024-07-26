#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/ytlib/queue_client/dynamic_state.h>

#include <yt/yt/client/api/rowset.h>

#include <yt/yt/client/table_client/comparator.h>

#include <yt/yt/client/queue_client/config.h>

#include <yt/yt/core/ytree/helpers.h>

#include <yt/yt/client/complex_types/uuid_text.h>

#include <yt/yt/client/table_client/name_table.h>

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

} // namespace
} // namespace NYT::NQueueClient

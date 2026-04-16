#include <yt/yt/ytlib/queue_client/path.h>

#include <yt/yt/client/queue_client/common.h>

#include <yt/yt/library/formats/format.h>

#include <yt/yt/core/misc/blob_output.h>
#include <yt/yt/core/misc/configurable_singleton_def.h>

#include <yt/yt/core/ytree/helpers.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NQueueClient {

namespace {

using namespace NYTree;
using namespace NYson;
using namespace NTableClient;
using namespace NObjectClient;
using namespace NYPath;
using namespace NFormats;

////////////////////////////////////////////////////////////////////////////////

TEST(TQueueClientPathTest, CrossClusterReference)
{
    TCrossClusterReference crossClusterRef{.Cluster = "cluster_1", .Path = "//my/path"};
    TRichYPath path("//my/path");
    path.SetCluster("cluster_1");
    TTablePath tablePath{path};

    EXPECT_EQ(crossClusterRef, ToCrossClusterReference(tablePath));

    path.SetQueueConsumerName("my_id");
    TConsumerReference consumerRef(path);

    EXPECT_EQ(crossClusterRef, ToCrossClusterReference(consumerRef));
}

TEST(TQueueClientPathTest, FromCrossClusterReferenceString)
{
    TRichYPath path("//my/path");
    path.SetCluster("cluster_1");
    TTablePath tablePath{path};

    EXPECT_EQ(tablePath, TTablePath(ToString(ToCrossClusterReference(tablePath))));
}

TEST(TQueueClientPathTest, StringRoundConversion)
{
    TRichYPath path("//my/path");
    path.SetCluster("cluster_1");
    TTablePath tablePath{path};
    EXPECT_EQ(ToString(tablePath), "cluster_1://my/path");
    // TODO(YT-27209): change to new serialization
    // EXPECT_EQ(ToString(tablePath), "<\"cluster\"=\"cluster_1\";>//my/path");
    EXPECT_EQ(TTablePath(ToString(tablePath)), tablePath);

    path.SetQueueConsumerName("my_id");
    TConsumerReference consumerRef(path);
    EXPECT_EQ(ToString(consumerRef), "<\"cluster\"=\"cluster_1\";\"queue_consumer_name\"=\"my_id\";>//my/path");
    EXPECT_EQ(TConsumerReference(ToString(consumerRef)), consumerRef);
}

TEST(TQueueClientPathTest, BadObjects)
{
    TRichYPath path("//my/path");
    EXPECT_THROW(Y_UNUSED(TTablePath(path)), TErrorException);
    EXPECT_THROW(Y_UNUSED(TConsumerReference(path)), TErrorException);

    path.SetCluster("cluster_1");
    EXPECT_NO_THROW(Y_UNUSED(TTablePath(path)));
    EXPECT_NO_THROW(Y_UNUSED(TConsumerReference(path)));

    path.SetForeign(true);
    EXPECT_THROW(Y_UNUSED(TTablePath(path)), TErrorException);
    EXPECT_THROW(Y_UNUSED(TConsumerReference(path)), TErrorException);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NQueueClient

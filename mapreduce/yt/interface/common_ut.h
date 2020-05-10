#include <mapreduce/yt/interface/common.h>
#include <mapreduce/yt/interface/serialize.h>

#include <library/yson/node/node_io.h>
#include <library/yson/node/node_builder.h>

#include <library/cpp/unittest/registar.h>

namespace NYT::NTesting {

template <typename T>
TString ToYson(const T& x)
{
    TNode result;
    TNodeBuilder builder(&result);
    Serialize(x, &builder);
    return NodeToYsonString(result);
}

} // namespace NYT::NTesting

#define ASSERT_SERIALIZABLES_EQUAL(a, b) \
    UNIT_ASSERT_EQUAL_C(a, b, NYT::NTesting::ToYson(a) << " != " << NYT::NTesting::ToYson(b))

#define ASSERT_SERIALIZABLES_UNEQUAL(a, b) \
    UNIT_ASSERT_UNEQUAL_C(a, b, NYT::NTesting::ToYson(a) << " == " << NYT::NTesting::ToYson(b))

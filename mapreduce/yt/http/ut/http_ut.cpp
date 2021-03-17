#include <mapreduce/yt/http/http.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NYT;

Y_UNIT_TEST_SUITE(HttpHeader)
{
    Y_UNIT_TEST(TestAddParameter) {
        THttpHeader header("POST", "/foo");
        header.AddMutationId();

        auto id1 = header.GetParameters()["mutation_id"].AsString();

        header.AddMutationId();

        auto id2 = header.GetParameters()["mutation_id"].AsString();

        UNIT_ASSERT(id1 != id2);
    }
}

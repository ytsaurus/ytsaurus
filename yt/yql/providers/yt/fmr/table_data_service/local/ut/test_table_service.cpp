#include <library/cpp/testing/unittest/registar.h>
#include <yt/yql/providers/yt/fmr/table_data_service/local/table_data_service.h>

using namespace NYql;

Y_UNIT_TEST_SUITE(TLocalTableServiceTest)
{
    Y_UNIT_TEST(GetNonexistentKey) {
        TLocalTableDataService tableDataService(3);
        auto getFuture = tableDataService.Get("key");
        UNIT_ASSERT_VALUES_EQUAL(getFuture.GetValueSync(), Nothing());
    }
    Y_UNIT_TEST(GetExistingKey) {
        TLocalTableDataService tableDataService(3);
        auto putFuture = tableDataService.Put("key", "1");
        putFuture.GetValueSync();
        auto getFuture = tableDataService.Get("key");
        UNIT_ASSERT_VALUES_EQUAL(getFuture.GetValueSync(), "1");
    }
    Y_UNIT_TEST(DeleteNonexistentKey) {
        TLocalTableDataService tableDataService(3);
        auto putFuture = tableDataService.Put("key", "1");
        putFuture.GetValueSync();
        auto deleteFuture = tableDataService.Delete("other_key");
        deleteFuture.GetValueSync();
        auto getFuture = tableDataService.Get("key");
        UNIT_ASSERT_VALUES_EQUAL(getFuture.GetValueSync(), "1");
    }
    Y_UNIT_TEST(DeleteExistingKey) {
        TLocalTableDataService tableDataService(3);
        auto putFuture = tableDataService.Put("key", "1");
        putFuture.GetValueSync();
        auto deleteFuture = tableDataService.Delete("key");
        deleteFuture.GetValueSync();
        auto getFuture = tableDataService.Get("key");
        UNIT_ASSERT_VALUES_EQUAL(getFuture.GetValueSync(), Nothing());
    }
}

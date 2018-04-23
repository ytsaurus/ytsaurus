#include "log.h"

#include <library/unittest/registar.h>

using namespace NYT;

SIMPLE_UNIT_TEST_SUITE(LogingTests) {
    SIMPLE_UNIT_TEST(TestFromString) {
        UNIT_ASSERT_EQUAL(FromString("error"), ILogger::ELevel::ERROR);
        UNIT_ASSERT_EQUAL(FromString("warning"), ILogger::ELevel::ERROR);
        UNIT_ASSERT_EQUAL(FromString("debug"), ILogger::ELevel::DEBUG);
        UNIT_ASSERT_EXCEPTION(FromString<ILogger::ELevel>("no"), yexception);
    }
}

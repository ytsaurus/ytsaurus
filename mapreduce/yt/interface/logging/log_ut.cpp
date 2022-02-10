#include "logger.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NYT;

Y_UNIT_TEST_SUITE(LogingTests) {
    Y_UNIT_TEST(TestFromString) {
        UNIT_ASSERT_EQUAL(FromString("error"), ILogger::ELevel::ERROR);
        UNIT_ASSERT_EQUAL(FromString("warning"), ILogger::ELevel::ERROR);
        UNIT_ASSERT_EQUAL(FromString("info"), ILogger::ELevel::INFO);
        UNIT_ASSERT_EQUAL(FromString("debug"), ILogger::ELevel::DEBUG);
        UNIT_ASSERT_EQUAL(FromString("ERROR"), ILogger::ELevel::ERROR);
        UNIT_ASSERT_EQUAL(FromString("WARNING"), ILogger::ELevel::ERROR);
        UNIT_ASSERT_EQUAL(FromString("INFO"), ILogger::ELevel::INFO);
        UNIT_ASSERT_EQUAL(FromString("DEBUG"), ILogger::ELevel::DEBUG);
        UNIT_ASSERT_EXCEPTION(FromString<ILogger::ELevel>("no"), yexception);
    }
}

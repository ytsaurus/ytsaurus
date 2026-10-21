package ru.yandex.devtools.test;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertTrue;

class NestedAfterAllFailedTest {

    @Nested
    class Cases {
        @ParameterizedTest
        @ValueSource(ints = {1, 2})
        void test(int value) {
            assertTrue(value > 0);
        }

        @AfterAll
        static void afterAll() {
            if (TestState.isTestOverTest()) {
                throw new IllegalStateException("Unable to clean up nested tests");
            }
        }
    }
}

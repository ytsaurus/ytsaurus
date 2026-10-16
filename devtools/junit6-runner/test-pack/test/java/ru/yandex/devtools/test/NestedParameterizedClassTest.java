package ru.yandex.devtools.test;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@Slf4j
@TestClassOrder(ClassOrderer.ClassName.class)
@TestMethodOrder(MethodOrderer.MethodName.class)
class NestedParameterizedClassTest {

    @ParameterizedClass
    @ValueSource(strings = {"test1", "test2"})
    @Nested
    class SingleTest {
        @Parameter
        String strParam;

        @Test
        void test() {
            log.info("Just a test: {}", strParam);

            assertNotNull(strParam);
        }

    }

}

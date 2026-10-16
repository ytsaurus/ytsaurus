package ru.yandex.devtools.test;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvFileSource;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
@TestMethodOrder(MethodOrderer.MethodName.class)
public class ClassMethodsParameterizedCsvTest {

    @CsvSource({
            "v1,        test1,     1, false",
            "v1,        test2,     2, true",
            "v2,        test3,     3, true"
    })
    @ParameterizedTest
    void testCsvArg(E0 e0, String name, int value, boolean bool) {
        log.info("Checking {}, {} = {}, {}", e0, name, value, bool);
        assertTrue(value > 0, name);
    }

    @CsvFileSource(resources = {
            "/csv-file-1.csv",
            "/csv-file-2.csv"
    })
    @ParameterizedTest
    void testCsvFile(E0 e0, String name, int value, boolean bool) {
        log.info("Checking {}, {} = {}, {}", e0, name, value, bool);
        assertTrue(value > 0, name);
    }

    enum E0 {
        v1, v2
    }
}

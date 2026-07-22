package ru.yandex.devtools.test;

import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import ru.yandex.devtools.log.LoggingContext;
import ru.yandex.devtools.test.Shared.Parameters;
import ru.yandex.devtools.test.TestHelper.TestResult;

import static ru.yandex.devtools.test.Shared.GSON;

@Slf4j
class RunnerTest {

    static {
        TestState.setTestOverTest(true);
    }

    @BeforeAll
    static void initOnce() {
        if (Runner.params == null) {
            Runner.params = new Parameters();
            System.setProperty(LoggingContext.YA_JTEST_DONT_SPLIT_LOGS, "true");
        }
    }

    @ParameterizedTest
    @MethodSource("testClasses")
    void testCollectAllTests(Class<?> testClass, TestResult expect) throws Exception {
        TestResult actual = TestHelper.collectTestResult(runTest(testClass));
        TestHelper.dumpMismatchesIfRequired(expect, actual);
        Assertions.assertEquals(expect, actual);
        log.info("Test class checked: {}", testClass);
    }

    @Test
    void testYaIgnoreReasonInComment() throws Exception {
        String comment = runTest(ClassMethodSingleYaIgnoredTest.class).stream()
                .filter(line -> !line.isEmpty())
                .map(line -> GSON.fromJson(line, Map.class))
                .filter(event -> "subtest-finished".equals(event.get("name")))
                .map(event -> (Map<?, ?>) event.get("value"))
                .filter(value -> "skipped".equals(value.get("status")))
                .map(value -> (String) value.get("comment"))
                .findFirst()
                .orElseThrow();
        Assertions.assertEquals("Disabled by @YaIgnore: Flaky on macOS", comment);
    }

    @Test
    void testInvalidJunitTagsExpression() {
        IllegalArgumentException error = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> new RuntimeTagFilter(new YaTestNameBase(), List.of("!"))
        );
        Assertions.assertEquals(
                "Invalid --junit-tags expression '!': exclusion tag is empty",
                error.getMessage()
        );
    }

    @Test
    void testReadableJunitTagsDescription() {
        Assertions.assertEquals(
                "tag1+tag2 tag3 !tag4+tag5 !tag6",
                RuntimeTagFilter.describeExpression(" tag1 + tag2   tag3  !tag4 + tag5 !tag6 ")
        );
    }

    @Test
    void testExclusionOnlyJunitTagsExpression() {
        Assertions.assertDoesNotThrow(
                () -> new RuntimeTagFilter(new YaTestNameBase(), List.of("!priority1"))
        );
        Assertions.assertEquals(
                "!priority1",
                RuntimeTagFilter.describeExpression("!priority1")
        );
        Assertions.assertEquals(
                "!priority1",
                RuntimeTagFilter.describeExpression("\"!priority1\"")
        );
    }

    @Test
    void testHyphenatedTagNameInJunitTagsExpression() {
        Assertions.assertEquals(
                "foo-bar",
                RuntimeTagFilter.describeExpression("foo-bar")
        );
        Assertions.assertEquals(
                "tag-tag",
                RuntimeTagFilter.describeExpression("tag-tag")
        );
        Assertions.assertEquals(
                "tag1+tag2-tag3",
                RuntimeTagFilter.describeExpression("tag1+tag2-tag3")
        );
    }

    //

    private List<String> runTest(Class<?> className) throws Exception {
        Parameters parameters = new Parameters();
        parameters.testsJar = "class:" + className.getName();

        StringWriter text = new StringWriter();
        try (LoggingContext loggingContext = LoggingContext.bypass()) {
            new Runner().run(new RunnerTask(loggingContext, new RunnerTiming(), parameters, text));
        }

        return Arrays.asList(text.getBuffer().toString().split("\n"));
    }

    //@formatter:off`
    //CHECKSTYLE:OFF
    static Arguments[] testClasses() {

        // При изменении количества параметризованных тестов - их количество всегда растет или уменьшается на 1
        // Метод, собирающий параметры для тестов, вызывается 2 раза - на этапе сборка параметризованных тестов
        // и уже на этапе реального выполнения этих тестов

        // Считаем, что если количество тестов увеличилось, что это не flaky

        return new Arguments[]{
                Arguments.of(
                        ClassFailedTest.class,
                        TestResult.failed(1,
                                "ru.yandex.devtools.test.ClassFailedTest::test()")),

                Arguments.of(
                        ClassMethodSingleTest.class,
                        TestResult.good(1,
                                "ru.yandex.devtools.test.ClassMethodSingleTest::test()")),

                Arguments.of(ClassMethodDisplayTest.class,
                        TestResult.good(2,
                                "ru.yandex.devtools.test.ClassMethodDisplayTest::Тест",
                                "ru.yandex.devtools.test.ClassMethodDisplayTest::Тест_1")),

                Arguments.of(ClassMethodSingleFailedTest.class,
                        TestResult.failed(1,
                                "ru.yandex.devtools.test.ClassMethodSingleFailedTest::test()")),

                Arguments.of(ClassMethodsNoneTest.class,
                        TestResult.good(0)),

                Arguments.of(ClassMethodsMultiTest.class,
                        new TestResult(8, 6, 1, 1, 0, Arrays.asList(
                                "ru.yandex.devtools.test.ClassMethodsMultiTest::canonTest()",
                                "ru.yandex.devtools.test.ClassMethodsMultiTest::failedTest()",
                                "ru.yandex.devtools.test.ClassMethodsMultiTest::flakyTest(long):[1] 4",
                                "ru.yandex.devtools.test.ClassMethodsMultiTest::linksTest()",
                                "ru.yandex.devtools.test.ClassMethodsMultiTest::metricsTest()",
                                "ru.yandex.devtools.test.ClassMethodsMultiTest::skippedTest()",
                                "ru.yandex.devtools.test.ClassMethodsMultiTest::test1()",
                                "ru.yandex.devtools.test.ClassMethodsMultiTest::test2()"))),

                Arguments.of(ClassParameterizedConstantFailedTest.class,
                        TestResult.failed(3,
                                "ru.yandex.devtools.test.ClassParameterizedConstantFailedTest::test(int):[1] 1",
                                "ru.yandex.devtools.test.ClassParameterizedConstantFailedTest::test(int):[2] 2",
                                "ru.yandex.devtools.test.ClassParameterizedConstantFailedTest::test(int):[3] 3")),

                Arguments.of(ClassMethodsParameterizedConstantTest.class,
                        TestResult.good(3,
                                "ru.yandex.devtools.test.ClassMethodsParameterizedConstantTest::test(int):[1] 1",
                                "ru.yandex.devtools.test.ClassMethodsParameterizedConstantTest::test(int):[2] 2",
                                "ru.yandex.devtools.test.ClassMethodsParameterizedConstantTest::test(int):[3] 3")),

                Arguments.of(ClassMethodsParameterizedConstantDisplayTest.class,
                        TestResult.good(6,
                                "ru.yandex.devtools.test.ClassMethodsParameterizedConstantDisplayTest::Тест на проверку отображения:[1] 10",
                                "ru.yandex.devtools.test.ClassMethodsParameterizedConstantDisplayTest::Тест на проверку отображения:[2] 5",
                                "ru.yandex.devtools.test.ClassMethodsParameterizedConstantDisplayTest::Тест на проверку отображения:[3] 66",
                                "ru.yandex.devtools.test.ClassMethodsParameterizedConstantDisplayTest::Тест на проверку отображения и параметров:check [10]",
                                "ru.yandex.devtools.test.ClassMethodsParameterizedConstantDisplayTest::Тест на проверку отображения и параметров:check [5]",
                                "ru.yandex.devtools.test.ClassMethodsParameterizedConstantDisplayTest::Тест на проверку отображения и параметров:check [66]")),

                Arguments.of(ClassMethodsParameterizedConstantFailedTest.class,
                        TestResult.failed(3,
                                "ru.yandex.devtools.test.ClassMethodsParameterizedConstantFailedTest::test(int):[1] 1",
                                "ru.yandex.devtools.test.ClassMethodsParameterizedConstantFailedTest::test(int):[2] 2",
                                "ru.yandex.devtools.test.ClassMethodsParameterizedConstantFailedTest::test(int):[3] 3")),

                Arguments.of(ClassMethodsParameterizedMethodSameClassTest.class,
                        TestResult.good(4,
                                "ru.yandex.devtools.test.ClassMethodsParameterizedMethodSameClassTest::test(int, String):[1] 0, \"0\"",
                                "ru.yandex.devtools.test.ClassMethodsParameterizedMethodSameClassTest::test(int, String):[2] 1, \"1\"",
                                "ru.yandex.devtools.test.ClassMethodsParameterizedMethodSameClassTest::test(int, String):[3] 2, \"2\"",
                                "ru.yandex.devtools.test.ClassMethodsParameterizedMethodSameClassTest::test(int, String):[4] 3, \"3\"")),

                Arguments.of(ClassMethodsParameterizedMethodDifferentClassTest.class,
                        TestResult.good(4,
                                "ru.yandex.devtools.test.ClassMethodsParameterizedMethodDifferentClassTest::test(int, String):[1] 0, \"0\"",
                                "ru.yandex.devtools.test.ClassMethodsParameterizedMethodDifferentClassTest::test(int, String):[2] 1, \"1\"",
                                "ru.yandex.devtools.test.ClassMethodsParameterizedMethodDifferentClassTest::test(int, String):[3] 2, \"2\"",
                                "ru.yandex.devtools.test.ClassMethodsParameterizedMethodDifferentClassTest::test(int, String):[4] 3, \"3\"")),

                Arguments.of(ClassMethodsRepeatTest.class,
                        TestResult.good(3,
                                "ru.yandex.devtools.test.ClassMethodsRepeatTest::test():repetition 1 of 3",
                                "ru.yandex.devtools.test.ClassMethodsRepeatTest::test():repetition 2 of 3",
                                "ru.yandex.devtools.test.ClassMethodsRepeatTest::test():repetition 3 of 3")),

                Arguments.of(ClassMethodsRepeatDisplayTest.class,
                        TestResult.good(4,
                                "ru.yandex.devtools.test.ClassMethodsRepeatDisplayTest::Тест с повторами:repetition 1 of 2",
                                "ru.yandex.devtools.test.ClassMethodsRepeatDisplayTest::Тест с повторами:repetition 2 of 2",
                                "ru.yandex.devtools.test.ClassMethodsRepeatDisplayTest::Тест с повторами и параметрами:тест [1]",
                                "ru.yandex.devtools.test.ClassMethodsRepeatDisplayTest::Тест с повторами и параметрами:тест [2]")),

                Arguments.of(ClassMethodsRepeatFailedTest.class,
                        TestResult.failed(5,
                                "ru.yandex.devtools.test.ClassMethodsRepeatFailedTest::test():repetition 1 of 5",
                                "ru.yandex.devtools.test.ClassMethodsRepeatFailedTest::test():repetition 2 of 5",
                                "ru.yandex.devtools.test.ClassMethodsRepeatFailedTest::test():repetition 3 of 5",
                                "ru.yandex.devtools.test.ClassMethodsRepeatFailedTest::test():repetition 4 of 5",
                                "ru.yandex.devtools.test.ClassMethodsRepeatFailedTest::test():repetition 5 of 5")),

                Arguments.of(ClassIgnoredTest.class,
                        TestResult.skipped(1,
                                "ru.yandex.devtools.test.ClassIgnoredTest::test()")),

                Arguments.of(ClassMethodSingleIgnoredTest.class,
                        TestResult.skipped(1,
                                "ru.yandex.devtools.test.ClassMethodSingleIgnoredTest::test()")),

                Arguments.of(ClassMethodSingleYaIgnoredTest.class,
                        TestResult.skipped(1,
                                "ru.yandex.devtools.test.ClassMethodSingleYaIgnoredTest::test()")),

                Arguments.of(ClassMethodsParameterizedConstantIgnoredTest.class,
                        TestResult.skipped(3,
                                "ru.yandex.devtools.test.ClassMethodsParameterizedConstantIgnoredTest::test(int):[1] 1",
                                "ru.yandex.devtools.test.ClassMethodsParameterizedConstantIgnoredTest::test(int):[2] 2",
                                "ru.yandex.devtools.test.ClassMethodsParameterizedConstantIgnoredTest::test(int):[3] 3")),

                Arguments.of(ClassMethodsParameterizedMethodIgnoreTest.class,
                        TestResult.skipped(4,
                                "ru.yandex.devtools.test.ClassMethodsParameterizedMethodIgnoreTest::test(int, String):[1] 0, \"0\"",
                                "ru.yandex.devtools.test.ClassMethodsParameterizedMethodIgnoreTest::test(int, String):[2] 1, \"1\"",
                                "ru.yandex.devtools.test.ClassMethodsParameterizedMethodIgnoreTest::test(int, String):[3] 2, \"2\"",
                                "ru.yandex.devtools.test.ClassMethodsParameterizedMethodIgnoreTest::test(int, String):[4] 3, \"3\"")),

                Arguments.of(ClassMethodsParameterizedMethodFlakyTest.class,
                        TestResult.good(4,
                                "ru.yandex.devtools.test.ClassMethodsParameterizedMethodFlakyTest::test(long, String):[1] 4, \"4\"",
                                "ru.yandex.devtools.test.ClassMethodsParameterizedMethodFlakyTest::test(long, String):[2] 5, \"5\"",
                                "ru.yandex.devtools.test.ClassMethodsParameterizedMethodFlakyTest::test(long, String):[3] 6, \"6\"",
                                "ru.yandex.devtools.test.ClassMethodsParameterizedMethodFlakyTest::test(long, String):[4] 7, \"7\"")),

                Arguments.of(ClassMethodsParameterizedMethodCountUpFlakyTest.class,
                        TestResult.notLaunched(4,
                                "ru.yandex.devtools.test.ClassMethodsParameterizedMethodCountUpFlakyTest::test(long, String):[1] 0, \"0\"",
                                "ru.yandex.devtools.test.ClassMethodsParameterizedMethodCountUpFlakyTest::test(long, String):[2] 1, \"1\"",
                                "ru.yandex.devtools.test.ClassMethodsParameterizedMethodCountUpFlakyTest::test(long, String):[3] 2, \"2\"",
                                "ru.yandex.devtools.test.ClassMethodsParameterizedMethodCountUpFlakyTest::test(long, String):[4] 3, \"3\"",
                                "ru.yandex.devtools.test.ClassMethodsParameterizedMethodCountUpFlakyTest::test(long, String):[5] 4, \"4\"")
                                .withGood(5)),

                Arguments.of(ClassMethodsParameterizedMethodCountDownFlakyTest.class,
                        TestResult.notLaunched(4,
                                "ru.yandex.devtools.test.ClassMethodsParameterizedMethodCountDownFlakyTest::test(long, String):[1] 0, \"0\"",
                                "ru.yandex.devtools.test.ClassMethodsParameterizedMethodCountDownFlakyTest::test(long, String):[2] 1, \"1\"",
                                "ru.yandex.devtools.test.ClassMethodsParameterizedMethodCountDownFlakyTest::test(long, String):[3] 2, \"2\"",
                                "ru.yandex.devtools.test.ClassMethodsParameterizedMethodCountDownFlakyTest::test(long, String):[4] 3, \"3\"")
                                .withGood(3)
                                .withFlaky(1)),

                Arguments.of(ClassMethodsParameterizedMethodFailTest.class,
                        TestResult.failed(1,
                                "ru.yandex.devtools.test.ClassMethodsParameterizedMethodFailTest::test(long, String)")
                                .withNotLaunched(0)),

                Arguments.of(ClassMethodsParameterizedCsvTest.class,
                        TestResult.good(6,
                                "ru.yandex.devtools.test.ClassMethodsParameterizedCsvTest::testCsvArg(E0, String, int, boolean):[1] \"v1\", \"test1\", \"1\", \"false\"",
                                "ru.yandex.devtools.test.ClassMethodsParameterizedCsvTest::testCsvArg(E0, String, int, boolean):[2] \"v1\", \"test2\", \"2\", \"true\"",
                                "ru.yandex.devtools.test.ClassMethodsParameterizedCsvTest::testCsvArg(E0, String, int, boolean):[3] \"v2\", \"test3\", \"3\", \"true\"",
                                "ru.yandex.devtools.test.ClassMethodsParameterizedCsvTest::testCsvFile(E0, String, int, boolean):[1] \"v2\", \"file-test1\", \"11\", \"false\"",
                                "ru.yandex.devtools.test.ClassMethodsParameterizedCsvTest::testCsvFile(E0, String, int, boolean):[2] \"v1\", \"file-test2\", \"22\", \"false\"",
                                "ru.yandex.devtools.test.ClassMethodsParameterizedCsvTest::testCsvFile(E0, String, int, boolean):[3] \"v1\", \"file-test3\", \"33\", \"true\"")),

                Arguments.of(ClassMethodsParameterizedEmptyTest.class,
                        TestResult.good(2,
                                "ru.yandex.devtools.test.ClassMethodsParameterizedEmptyTest::testEmptyArg(List):[1] []",
                                "ru.yandex.devtools.test.ClassMethodsParameterizedEmptyTest::testNullArg(String):[1] null")),

                Arguments.of(ClassMethodsParameterizedEnumTest.class,
                        TestResult.good(2,
                                "ru.yandex.devtools.test.ClassMethodsParameterizedEnumTest::testEnumArg(E1):[1] v1",
                                "ru.yandex.devtools.test.ClassMethodsParameterizedEnumTest::testEnumArg(E1):[2] v2")),

                Arguments.of(ClassMethodsParameterizedSameNameTest.class,
                        TestResult.good(4,
                                "ru.yandex.devtools.test.ClassMethodsParameterizedSameNameTest::test value: {0}:[1] \"v1\"",
                                "ru.yandex.devtools.test.ClassMethodsParameterizedSameNameTest::test value: {0}:[2] \"v2\"",
                                "ru.yandex.devtools.test.ClassMethodsParameterizedSameNameTest::test value: {0}:[1] 3",
                                "ru.yandex.devtools.test.ClassMethodsParameterizedSameNameTest::test value: {0}:[2] 4")),

                Arguments.of(NestedClassTest.class,
                        TestResult.good(12,
                                "ru.yandex.devtools.test.NestedClassTest$DisplayTest::Тест",
                                "ru.yandex.devtools.test.NestedClassTest$DisplayTest::Тест_1",
                                "ru.yandex.devtools.test.NestedClassTest$ParameterizedConstantDisplayTest::Тест на проверку отображения:[1] 10",
                                "ru.yandex.devtools.test.NestedClassTest$ParameterizedConstantDisplayTest::Тест на проверку отображения:[2] 5",
                                "ru.yandex.devtools.test.NestedClassTest$ParameterizedConstantDisplayTest::Тест на проверку отображения:[3] 66",
                                "ru.yandex.devtools.test.NestedClassTest$ParameterizedConstantDisplayTest::Тест на проверку отображения и параметров:check [10]",
                                "ru.yandex.devtools.test.NestedClassTest$ParameterizedConstantDisplayTest::Тест на проверку отображения и параметров:check [5]",
                                "ru.yandex.devtools.test.NestedClassTest$ParameterizedConstantDisplayTest::Тест на проверку отображения и параметров:check [66]",
                                "ru.yandex.devtools.test.NestedClassTest$ParameterizedConstantTest::test(int):[1] 1",
                                "ru.yandex.devtools.test.NestedClassTest$ParameterizedConstantTest::test(int):[2] 2",
                                "ru.yandex.devtools.test.NestedClassTest$ParameterizedConstantTest::test(int):[3] 3",
                                "ru.yandex.devtools.test.NestedClassTest$SingleTest::test()")),

                Arguments.of(NestedParameterizedClassTest.class,
                        TestResult.good(2,
                                "ru.yandex.devtools.test.NestedParameterizedClassTest$SingleTest::[1] strParam = \"test1\":test()",
                                "ru.yandex.devtools.test.NestedParameterizedClassTest$SingleTest::[2] strParam = \"test2\":test()")
                                .withNotLaunched(0)),

                Arguments.of(ParameterizedClassTest.class,
                        TestResult.good(48,
                                "ru.yandex.devtools.test.ParameterizedClassTest::[1] strParam = \"test1\":test()",
                                "ru.yandex.devtools.test.ParameterizedClassTest::[1] strParam = \"test1\":test(int):[1] 1",
                                "ru.yandex.devtools.test.ParameterizedClassTest::[1] strParam = \"test1\":test(int):[2] 2",
                                "ru.yandex.devtools.test.ParameterizedClassTest::[1] strParam = \"test1\":test(int):[3] 3",
                                "ru.yandex.devtools.test.ParameterizedClassTest::[1] strParam = \"test1\":Тест",
                                "ru.yandex.devtools.test.ParameterizedClassTest::[1] strParam = \"test1\":Тест_1",
                                "ru.yandex.devtools.test.ParameterizedClassTest::[1] strParam = \"test1\":Тест на проверку отображения:[1] 10",
                                "ru.yandex.devtools.test.ParameterizedClassTest::[1] strParam = \"test1\":Тест на проверку отображения:[2] 5",
                                "ru.yandex.devtools.test.ParameterizedClassTest::[1] strParam = \"test1\":Тест на проверку отображения:[3] 66",
                                "ru.yandex.devtools.test.ParameterizedClassTest::[1] strParam = \"test1\":Тест на проверку отображения и параметров:check [10]",
                                "ru.yandex.devtools.test.ParameterizedClassTest::[1] strParam = \"test1\":Тест на проверку отображения и параметров:check [5]",
                                "ru.yandex.devtools.test.ParameterizedClassTest::[1] strParam = \"test1\":Тест на проверку отображения и параметров:check [66]",
                                "ru.yandex.devtools.test.ParameterizedClassTest$DisplayTest::Тест",
                                "ru.yandex.devtools.test.ParameterizedClassTest$DisplayTest::Тест_1",
                                "ru.yandex.devtools.test.ParameterizedClassTest$ParameterizedConstantDisplayTest::Тест на проверку отображения:[1] 10",
                                "ru.yandex.devtools.test.ParameterizedClassTest$ParameterizedConstantDisplayTest::Тест на проверку отображения:[2] 5",
                                "ru.yandex.devtools.test.ParameterizedClassTest$ParameterizedConstantDisplayTest::Тест на проверку отображения:[3] 66",
                                "ru.yandex.devtools.test.ParameterizedClassTest$ParameterizedConstantDisplayTest::Тест на проверку отображения и параметров:check [10]",
                                "ru.yandex.devtools.test.ParameterizedClassTest$ParameterizedConstantDisplayTest::Тест на проверку отображения и параметров:check [5]",
                                "ru.yandex.devtools.test.ParameterizedClassTest$ParameterizedConstantDisplayTest::Тест на проверку отображения и параметров:check [66]",
                                "ru.yandex.devtools.test.ParameterizedClassTest$ParameterizedConstantTest::test(int):[1] 1",
                                "ru.yandex.devtools.test.ParameterizedClassTest$ParameterizedConstantTest::test(int):[2] 2",
                                "ru.yandex.devtools.test.ParameterizedClassTest$ParameterizedConstantTest::test(int):[3] 3",
                                "ru.yandex.devtools.test.ParameterizedClassTest$SingleTest::test()",
                                "ru.yandex.devtools.test.ParameterizedClassTest::[2] strParam = \"test2\":test()",
                                "ru.yandex.devtools.test.ParameterizedClassTest::[2] strParam = \"test2\":test(int):[1] 1",
                                "ru.yandex.devtools.test.ParameterizedClassTest::[2] strParam = \"test2\":test(int):[2] 2",
                                "ru.yandex.devtools.test.ParameterizedClassTest::[2] strParam = \"test2\":test(int):[3] 3",
                                "ru.yandex.devtools.test.ParameterizedClassTest::[2] strParam = \"test2\":Тест",
                                "ru.yandex.devtools.test.ParameterizedClassTest::[2] strParam = \"test2\":Тест_1",
                                "ru.yandex.devtools.test.ParameterizedClassTest::[2] strParam = \"test2\":Тест на проверку отображения:[1] 10",
                                "ru.yandex.devtools.test.ParameterizedClassTest::[2] strParam = \"test2\":Тест на проверку отображения:[2] 5",
                                "ru.yandex.devtools.test.ParameterizedClassTest::[2] strParam = \"test2\":Тест на проверку отображения:[3] 66",
                                "ru.yandex.devtools.test.ParameterizedClassTest::[2] strParam = \"test2\":Тест на проверку отображения и параметров:check [10]",
                                "ru.yandex.devtools.test.ParameterizedClassTest::[2] strParam = \"test2\":Тест на проверку отображения и параметров:check [5]",
                                "ru.yandex.devtools.test.ParameterizedClassTest::[2] strParam = \"test2\":Тест на проверку отображения и параметров:check [66]",
                                "ru.yandex.devtools.test.ParameterizedClassTest$DisplayTest::Тест_2",
                                "ru.yandex.devtools.test.ParameterizedClassTest$DisplayTest::Тест_3",
                                "ru.yandex.devtools.test.ParameterizedClassTest$ParameterizedConstantDisplayTest::Тест на проверку отображения:[1] 10_1",
                                "ru.yandex.devtools.test.ParameterizedClassTest$ParameterizedConstantDisplayTest::Тест на проверку отображения:[2] 5_1",
                                "ru.yandex.devtools.test.ParameterizedClassTest$ParameterizedConstantDisplayTest::Тест на проверку отображения:[3] 66_1",
                                "ru.yandex.devtools.test.ParameterizedClassTest$ParameterizedConstantDisplayTest::Тест на проверку отображения и параметров:check [10]_1",
                                "ru.yandex.devtools.test.ParameterizedClassTest$ParameterizedConstantDisplayTest::Тест на проверку отображения и параметров:check [5]_1",
                                "ru.yandex.devtools.test.ParameterizedClassTest$ParameterizedConstantDisplayTest::Тест на проверку отображения и параметров:check [66]_1",
                                "ru.yandex.devtools.test.ParameterizedClassTest$ParameterizedConstantTest::test(int):[1] 1_1",
                                "ru.yandex.devtools.test.ParameterizedClassTest$ParameterizedConstantTest::test(int):[2] 2_1",
                                "ru.yandex.devtools.test.ParameterizedClassTest$ParameterizedConstantTest::test(int):[3] 3_1",
                                "ru.yandex.devtools.test.ParameterizedClassTest$SingleTest::test()_1")
                                .withNotLaunched(0)),

                Arguments.of(ParameterizedMethodClassTest.class,
                        TestResult.good(48,
                                "ru.yandex.devtools.test.ParameterizedMethodClassTest::[1] strParam = \"test1\", intParam = 1:test()",
                                "ru.yandex.devtools.test.ParameterizedMethodClassTest::[1] strParam = \"test1\", intParam = 1:test(int):[1] 1",
                                "ru.yandex.devtools.test.ParameterizedMethodClassTest::[1] strParam = \"test1\", intParam = 1:test(int):[2] 2",
                                "ru.yandex.devtools.test.ParameterizedMethodClassTest::[1] strParam = \"test1\", intParam = 1:test(int):[3] 3",
                                "ru.yandex.devtools.test.ParameterizedMethodClassTest::[1] strParam = \"test1\", intParam = 1:Тест",
                                "ru.yandex.devtools.test.ParameterizedMethodClassTest::[1] strParam = \"test1\", intParam = 1:Тест_1",
                                "ru.yandex.devtools.test.ParameterizedMethodClassTest::[1] strParam = \"test1\", intParam = 1:Тест на проверку отображения:[1] 10",
                                "ru.yandex.devtools.test.ParameterizedMethodClassTest::[1] strParam = \"test1\", intParam = 1:Тест на проверку отображения:[2] 5",
                                "ru.yandex.devtools.test.ParameterizedMethodClassTest::[1] strParam = \"test1\", intParam = 1:Тест на проверку отображения:[3] 66",
                                "ru.yandex.devtools.test.ParameterizedMethodClassTest::[1] strParam = \"test1\", intParam = 1:Тест на проверку отображения и параметров:check [10]",
                                "ru.yandex.devtools.test.ParameterizedMethodClassTest::[1] strParam = \"test1\", intParam = 1:Тест на проверку отображения и параметров:check [5]",
                                "ru.yandex.devtools.test.ParameterizedMethodClassTest::[1] strParam = \"test1\", intParam = 1:Тест на проверку отображения и параметров:check [66]",
                                "ru.yandex.devtools.test.ParameterizedMethodClassTest$DisplayTest::Тест",
                                "ru.yandex.devtools.test.ParameterizedMethodClassTest$DisplayTest::Тест_1",
                                "ru.yandex.devtools.test.ParameterizedMethodClassTest$ParameterizedConstantDisplayTest::Тест на проверку отображения:[1] 10",
                                "ru.yandex.devtools.test.ParameterizedMethodClassTest$ParameterizedConstantDisplayTest::Тест на проверку отображения:[2] 5",
                                "ru.yandex.devtools.test.ParameterizedMethodClassTest$ParameterizedConstantDisplayTest::Тест на проверку отображения:[3] 66",
                                "ru.yandex.devtools.test.ParameterizedMethodClassTest$ParameterizedConstantDisplayTest::Тест на проверку отображения и параметров:check [10]",
                                "ru.yandex.devtools.test.ParameterizedMethodClassTest$ParameterizedConstantDisplayTest::Тест на проверку отображения и параметров:check [5]",
                                "ru.yandex.devtools.test.ParameterizedMethodClassTest$ParameterizedConstantDisplayTest::Тест на проверку отображения и параметров:check [66]",
                                "ru.yandex.devtools.test.ParameterizedMethodClassTest$ParameterizedConstantTest::test(int):[1] 1",
                                "ru.yandex.devtools.test.ParameterizedMethodClassTest$ParameterizedConstantTest::test(int):[2] 2",
                                "ru.yandex.devtools.test.ParameterizedMethodClassTest$ParameterizedConstantTest::test(int):[3] 3",
                                "ru.yandex.devtools.test.ParameterizedMethodClassTest$SingleTest::test()",
                                "ru.yandex.devtools.test.ParameterizedMethodClassTest::[2] strParam = \"test2\", intParam = 2:test()",
                                "ru.yandex.devtools.test.ParameterizedMethodClassTest::[2] strParam = \"test2\", intParam = 2:test(int):[1] 1",
                                "ru.yandex.devtools.test.ParameterizedMethodClassTest::[2] strParam = \"test2\", intParam = 2:test(int):[2] 2",
                                "ru.yandex.devtools.test.ParameterizedMethodClassTest::[2] strParam = \"test2\", intParam = 2:test(int):[3] 3",
                                "ru.yandex.devtools.test.ParameterizedMethodClassTest::[2] strParam = \"test2\", intParam = 2:Тест",
                                "ru.yandex.devtools.test.ParameterizedMethodClassTest::[2] strParam = \"test2\", intParam = 2:Тест_1",
                                "ru.yandex.devtools.test.ParameterizedMethodClassTest::[2] strParam = \"test2\", intParam = 2:Тест на проверку отображения:[1] 10",
                                "ru.yandex.devtools.test.ParameterizedMethodClassTest::[2] strParam = \"test2\", intParam = 2:Тест на проверку отображения:[2] 5",
                                "ru.yandex.devtools.test.ParameterizedMethodClassTest::[2] strParam = \"test2\", intParam = 2:Тест на проверку отображения:[3] 66",
                                "ru.yandex.devtools.test.ParameterizedMethodClassTest::[2] strParam = \"test2\", intParam = 2:Тест на проверку отображения и параметров:check [10]",
                                "ru.yandex.devtools.test.ParameterizedMethodClassTest::[2] strParam = \"test2\", intParam = 2:Тест на проверку отображения и параметров:check [5]",
                                "ru.yandex.devtools.test.ParameterizedMethodClassTest::[2] strParam = \"test2\", intParam = 2:Тест на проверку отображения и параметров:check [66]",
                                "ru.yandex.devtools.test.ParameterizedMethodClassTest$DisplayTest::Тест_2",
                                "ru.yandex.devtools.test.ParameterizedMethodClassTest$DisplayTest::Тест_3",
                                "ru.yandex.devtools.test.ParameterizedMethodClassTest$ParameterizedConstantDisplayTest::Тест на проверку отображения:[1] 10_1",
                                "ru.yandex.devtools.test.ParameterizedMethodClassTest$ParameterizedConstantDisplayTest::Тест на проверку отображения:[2] 5_1",
                                "ru.yandex.devtools.test.ParameterizedMethodClassTest$ParameterizedConstantDisplayTest::Тест на проверку отображения:[3] 66_1",
                                "ru.yandex.devtools.test.ParameterizedMethodClassTest$ParameterizedConstantDisplayTest::Тест на проверку отображения и параметров:check [10]_1",
                                "ru.yandex.devtools.test.ParameterizedMethodClassTest$ParameterizedConstantDisplayTest::Тест на проверку отображения и параметров:check [5]_1",
                                "ru.yandex.devtools.test.ParameterizedMethodClassTest$ParameterizedConstantDisplayTest::Тест на проверку отображения и параметров:check [66]_1",
                                "ru.yandex.devtools.test.ParameterizedMethodClassTest$ParameterizedConstantTest::test(int):[1] 1_1",
                                "ru.yandex.devtools.test.ParameterizedMethodClassTest$ParameterizedConstantTest::test(int):[2] 2_1",
                                "ru.yandex.devtools.test.ParameterizedMethodClassTest$ParameterizedConstantTest::test(int):[3] 3_1",
                                "ru.yandex.devtools.test.ParameterizedMethodClassTest$SingleTest::test()_1")
                                .withNotLaunched(0))
        };
    }
    //CHECKSTYLE:ON
}

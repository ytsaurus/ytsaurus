package ru.yandex.devtools.test;

public class TestState {
    private static final boolean IDE_TEST = Boolean.getBoolean("IDE_TEST");
    private static volatile boolean testOverTest;

    private TestState() {
        //
    }

    public static void setTestOverTest(boolean testOverTest) {
        TestState.testOverTest = testOverTest;
    }

    public static boolean isTestOverTest() {
        return testOverTest;
    }

    public static boolean isYaMakeTest() {
        return !IDE_TEST;
    }
}

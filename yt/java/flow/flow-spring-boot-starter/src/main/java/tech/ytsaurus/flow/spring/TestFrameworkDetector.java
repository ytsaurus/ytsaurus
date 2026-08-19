package tech.ytsaurus.flow.spring;

/**
 * Tells whether the Spring context being started belongs to a test.
 * <p>
 * Context startup runs on the thread that called {@code SpringApplication.run}, so a test framework
 * or Spring's TestContext machinery is still on the call stack. Covers JUnit, TestNG and Cucumber;
 * {@code flow.runner.enabled=false} overrides the guess for anything else.
 */
final class TestFrameworkDetector {

    private TestFrameworkDetector() {
    }

    /** Whether the current call stack contains a test-framework frame. */
    static boolean testFrameworkOnStack() {
        for (StackTraceElement element : Thread.currentThread().getStackTrace()) {
            String className = element.getClassName();
            if (className.startsWith("org.springframework.boot.test.")
                    || className.startsWith("org.springframework.test.")
                    || className.startsWith("org.junit.")
                    || className.startsWith("org.testng.")
                    || className.startsWith("cucumber.")
                    || className.startsWith("io.cucumber.")) {
                return true;
            }
        }
        return false;
    }
}

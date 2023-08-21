package tech.ytsaurus.testlib;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;

public class Matchers {
    private Matchers() {
    }

    public static <T extends Throwable> Matcher<T> isCausedBy(Class<?> errorClass) {
        return new BaseMatcher<>() {
            @Override
            public boolean matches(Object item) {
                if (!(item instanceof Throwable)) {
                    return false;
                }

                Throwable throwable = (Throwable) item;
                while (throwable != null) {
                    if (errorClass.isInstance(throwable)) {
                        return true;
                    }
                    throwable = throwable.getCause();
                }

                return false;
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("caused by ");
                description.appendValue(errorClass);
            }
        };
    }
}

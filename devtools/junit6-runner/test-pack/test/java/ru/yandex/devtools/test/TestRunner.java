package ru.yandex.devtools.test;

// Специальный тестовый класс для запуска под отладчиком со всеми необходимыми классами для анализа
public class TestRunner {

    private TestRunner() {
        //
    }

    public static void main(String[] args) throws Exception {
        Runner.main(args);
    }
}

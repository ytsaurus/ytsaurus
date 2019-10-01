package ru.yandex.allotment;

public class ConsoleColors {
    private static boolean hasConsole = System.console() != null;

    public static String RESET = hasConsole ? "\u001B[0m" : "";
    public static String RED = hasConsole ? "\u001B[31m" : "";
    public static String GREEN = hasConsole ? "\u001B[32m" : "";
    public static String YELLOW = hasConsole ? "\u001B[33m" : "";
}

package ru.yandex.abi

inline fun adaptWithJavaSamAgain(value: String, noinline transform: (String) -> String): String =
    adaptWithJavaSam(value, transform)

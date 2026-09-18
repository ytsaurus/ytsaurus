package ru.yandex.abi

class PublicApi {
    fun message(): String = implementation()
    inline fun decorate(value: String, transform: (String) -> String): String = transform("[$value]")
}

inline fun adaptWithJavaSam(value: String, noinline transform: (String) -> String): String =
    JavaApi.apply(value, transform)

private fun implementation(): String = "full implementation"

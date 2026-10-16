package ru.yandex.abi

fun main() {
    println(PublicApi().decorate(JavaApi().message()) { adaptWithJavaSamAgain(it) { value -> value } })
}

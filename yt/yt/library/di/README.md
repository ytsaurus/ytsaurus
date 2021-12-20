# DI - Dependency Injection для C++

`DI` - библиотека упрощающая dependency injection.

## Пример

```c++
struct TFoo
    : public TRefCounted
{
    // Чтобы конструктор класса можно было использовать в DI, нужно
    // обернуть сигнатуру конструктора в макрос YT_DI_CTOR.
    //
    // YT_DI_CTOR определён в yt/yt/library/di/public.h и раскрывается в один using.
    //
    // using TConstructorSignature = TFoo();
    //
    // При желании, можно определить этот typedef руками. Это позволит
    // добавить поддержку DI без зависимости на библиотеку.
    YT_DI_CTOR(TFoo()) = default;
};

struct TBar
    : public TRefCounted
{
    YT_DI_CTOR(TBar(TFooPtr foo))
        : Foo(std::move(foo))
    { }

    TFooPtr Foo;
};

void Example()
{
    // TComponent описывает часть графа объектов.
    //
    // В нашем случае, граф состоит из двух вершин: TFoo и TBar.
    //
    // TFoo не имеет зависимостей. А TBar зависит от TFoo.
    auto component = TComponent{}
        .Bind<TFoo>()
        .Bind<TBar>();

    // TInjector отвечает за создание объектов.
    //
    // По умолчанию TInjector создаёт объекты лениво, при первом обращении через Get.
    TInjector injector{component};

    // Get сначала создаст TFoo, затем создаст TBar.
    auto barPtr = injector.Get<TBarPtr>();
}
```

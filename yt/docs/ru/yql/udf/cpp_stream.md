### Работа с YQL-Stream в C++ UDF

YQL-Stream - это однопроходный итератор. В YQL его можно получить на выходе ```PROCESS``` или ```REDUCE``` (см. документацию YQL).

В сpp YQL-Stream - это класс-наследник ```TBoxedValue``` с переопределённым ```Fetch()``` методом.

```cpp
class TMyStreamProvider: public TBoxedValue {
public:
    TMyStreamProvider(...) {
        ...
    }

    TMyStreamProvider() = delete;
    TMyStreamProvider(const TMyStreamProvider&) = delete;
    virtual ~TMyStreamProvider() = default;
    void operator=(const TMyStreamProvider&) = delete;
    TMyStreamProvider(TMyStreamProvider&& other) = default;
    TMyStreamProvider& operator=(TMyStreamProvider&& other) = default;

    EFetchStatus Fetch(TUnboxedValue& result) override;
};
```

```Fetch()``` может вернуть 3 статуса:
* ```EFetchStatus::Ok```

  Символизирует успешную запись значения в result.

* ```EFetchStatus::Finish```

  Символизирует конец потока, result - не изменяется.

* ```EFetchStatus::Yield```

  Специальный статус. Если на входе у UDF-функции нет YQL-Stream'а, то ваш поток никогда не должен генерировать этот статус. Если входной поток выдал статус ```EFetchStatus::Yield```, то в выходной поток может отдать 0 или больше значений, но всё равно необходимо сгенерировать и вернуть статус ```EFetchStatus::Yield```.

Важно помнить, что после того, как udf-функция в cpp-коде вернула поток, то всё что было в теле ```Run()``` разрушается деструктором. Поэтому всё необходимое нужно прокидывать через конструктор в класс Stream'а, чтобы он был владельцем всех ресурсов.

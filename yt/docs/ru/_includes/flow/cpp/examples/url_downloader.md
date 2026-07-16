# URL Downloader в {{product-name}} Flow (C++)

[Пайплайн](../../../../flow/concepts/glossary.md#pipeline) демонстрирует выполнение внешних вызовов (подобных HTTP-запросам) из [компьютейшенов](../../../../flow/concepts/glossary.md#stream-and-computation) с управлением скоростью загрузки (троттлинг), шардированием по хостам и управлением [стейтом](../../../../flow/concepts/glossary.md#state).

[Исходный код]({{source-root}}/yt/yt/flow/examples/cpp/url_downloader)

## Компоненты пайплайна

### TUrlDownloader

Вспомогательный класс (наследник `TRefCounted`), управляющий очередями загрузки для каждого хоста. Обеспечивает:

- **Регистрацию хостов** (`RegisterHost`) &mdash; для каждого хоста создается асинхронный исполнитель, который последовательно обрабатывает URL-адреса из очереди с искусственной задержкой (эмуляция троттлинга)
- **Добавление URL** (`RegisterUrl`) &mdash; добавляет URL в очередь соответствующего хоста
- **Извлечение результатов** (`ExtractProcessedUrls`) &mdash; возвращает обработанные URL с результатами
- **Отмену хоста** (`UnregisterHost`) &mdash; останавливает обработку и очищает очередь

Ключевой паттерн &mdash; использование `AsyncVia(GetCurrentInvoker())` для запуска фоновой обработки в рамках сериализованного инвокера, что позволяет безопасно работать с общим состоянием без блокировок.

### TLimitedUrlDownloadComputation

Основной компьютейшен, наследуется от `TTransformComputation`. Координирует загрузку URL с помощью `TUrlDownloader`.

При обработке входного сообщения (`DoProcessMessage`):
1. Читает `TUrlMessage` с полями `Host` и `Url`
2. Сохраняет URL в стейт хоста
3. Регистрирует хост и URL в `TUrlDownloader`
4. Ставит [таймер](../../../../flow/concepts/glossary.md#timer) для периодической проверки результатов через `GetNextHostCheck`
5. Применяет лимит на размер стейта через `EnforceLimit`

При срабатывании таймера (`DoProcessTimer`):
1. Восстанавливает хост из стейта
2. Извлекает обработанные URL из `TUrlDownloader`
3. Удаляет обработанные URL из стейта
4. Генерирует `TProcessedUrlMessage` для каждого обработанного URL
5. Если очередь пуста &mdash; отменяет регистрацию хоста и сбрасывает стейт; иначе &mdash; ставит следующий таймер

## Типы сообщений

- **TUrlMessage** &mdash; наследник `TYsonMessage`. Содержит поля `Host` и `Url`.
- **TProcessedUrlMessage** &mdash; наследник `TYsonMessage`. Содержит поля `Host`, `Url` и `Data` (результат обработки).

## Ключевые паттерны

### Internal YsonState

Для хранения очереди URL по каждому хосту используется `TKeyStateClient<TLimitedHostState>`. Стейт `TLimitedHostState` содержит:
- `Host` &mdash; имя хоста
- `Urls` &mdash; очередь URL (`std::deque<std::string>`), ожидающих обработки

### Таймеры для периодической проверки

Метод `GetNextHostCheck` вычисляет время следующей проверки хоста. Время вычисляется с учетом:
- `CheckHostPeriod` &mdash; период проверки (по умолчанию 5 секунд)
- Хэш имени хоста &mdash; для равномерного распределения проверок разных хостов по времени

### Динамические параметры

`TDynamicLimitedUrlDownloadParameters` позволяет менять параметры без перезапуска пайплайна:
- `CheckHostPeriod` &mdash; период проверки хостов (по умолчанию 5 секунд, минимум 1 секунда)
- `PersistLimit` &mdash; максимальное количество URL, сохраняемых в стейте для одного хоста (по умолчанию 1000)

### PersistLimit

`EnforceLimit` ограничивает размер очереди URL в стейте. Если количество URL превышает `PersistLimit`, старые URL удаляются. Это необходимо, чтобы стейт не превышал ограничения на размер строки в динамической таблице.

{% note warning %}

При ребалансировке [партиций](../../../../flow/concepts/glossary.md#partition) URL, не попавшие в лимит `PersistLimit`, будут потеряны. Значение лимита следует выбирать с учетом допустимых потерь.

{% endnote %}

## Структура пайплайна

1. **Входная очередь** &rarr; поток `urls` (`TUrlMessage`)
2. Поток `urls` &rarr; **TLimitedUrlDownloadComputation** (с таймерами и стейтом) &rarr; поток `processed_urls` (`TProcessedUrlMessage`)

## Функция main

В `main` регистрируются два потока:
- `RegisterStream<TUrlMessage>("urls")` &mdash; входные URL
- `RegisterStream<TProcessedUrlMessage>("processed_urls")` &mdash; обработанные URL

## Исходный код

### TUrlDownloader

{% code '/yt/yt/flow/examples/cpp/url_downloader/main.cpp' lang='cpp' lines='[BEGIN url_downloader]-[END url_downloader]' keep-indents %}

### TLimitedUrlDownloadComputation

{% code '/yt/yt/flow/examples/cpp/url_downloader/main.cpp' lang='cpp' lines='[BEGIN limited_url_download]-[END limited_url_download]' keep-indents %}

## См. также

- [Быстрый старт (C++)](../../../../flow/cpp/getting-started.md)
- [Computation (C++)](../../../../flow/cpp/computation.md)

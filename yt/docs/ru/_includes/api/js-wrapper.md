# {{product-name}} JavaScript Wrapper (Browser + Node.js)

Враппер для запросов к кластерам {{product-name}}, который использует HTTP API {{product-name}} с наиболее приближенным к нему интерфейсом.

## Установка

`npm install @ytsaurus/javascript-wrapper`

## Подключение и использование

Библиотека написана в стиле CommonJS - можно использовать в nodejs-приложениях или собирать зависимости с помощью Webpack.

```javascript
    const yt = require('@ytsaurus/javascript-wrapper')()

    // Позволяет задать настройки по умолчанию для всех команд.
    // Если же необходимо работать с несколькими кластерами одновременно, то прокси и токен передаются каждой команде отдельно.
    yt.setup.setGlobalOption('proxy', 'cluster-name.yt.my-domain.com');
    yt.setup.setGlobalOption('secure', true);

    yt.setup.setGlobalOption('authentication', {
        type: 'domain'
    });

    yt.setup.setGlobalOption('timeout', 15000);

    // Пример 1
    yt.v3.get({ path: '//sys/users/user/@' })

    yt.v3.get({
        setup: {
            proxy: 'cluster-name.yt.my-domain.com',
            authentication: {
                type: 'none'
            }
        },
        parameters: { path: '//sys/users/user/@' }
    })
        .done(function (userAttributes) {
            // ...
        });


    // Пример 2
    yt.v3.set({ path: '//sys/users/user/@banned' }, true);

    yt.v3.set({
        setup: {
            proxy: 'cluster-name.yt.my-domain.com',
            authentication: {
                type: 'oauth',
                token: 'abcdefghijklmnopqrstuvwxyz'
            }
        },
        parameters: { path: '//sys/users/user/@banned' },
        data: true
    })
        .done(function () {
            // ...
        });
```

Кроме того, библиотеку можно использовать как глобальную переменную.

## Настройки

Враппер позволяет задавать глобальные и локальные настройки для каждого запроса.


| **Имя** | **Тип** | **Значение по умолчанию** |
| --- | --- | --- |
|`secure`  | boolean | `true` |
|`useHeavyProxy`  | boolean | - |
|`proxy`  | string | - |
|`heavyProxy`  | string | `true` |
|`timeout`  | Number | `100000` |
|`useEncodedParameters`  | boolean | `true` |
|`authentication`  | object | `{ type: 'none' }` |
|`dataType`  | string | - |
|`encodedParametersSettings`  | object |
```

{
    maxSize: 64 * 1024,
    maxCount: 2,
    encoder(string) {
        return window.btoa(utf8.encode(string));
    }
}
```




## События

Можно подписаться на события `requestStart`, `requestEnd`, `error`:

    yt.subscribe('requestStart', function () {
        console.log('requestStart');
    });


Это может быть полезно для отображения лоадера (можно считать, сколько активных запросов) и для логирования ошибок при запросе API.

## Команды

О доступных командах можно почитать в разделе [Команды](../../api/commands.md). Единственное отличие заключается в том, что имена команд пишутся в привычном стиле для языка JavaScript, а именно CamelCase.

Не все команды реализованы, но могут быть добавлены при надобности. Обычно враппер выдает соответствующую ошибку при отсутствии команды.

Общий вид команды:

```javascript
// yt.<version>.<command>(<parameters>[, <data>])
yt.v3.get({ path: "//home/user/@account" });
yt.v3.set({ path: "//home/user/@account" }, "default");
```

Общий вид команды с возможностью задать локальные настройки:

```javascript
/**
 * yt.<version>.<command>({
 *    parameters: <parameters>,
 *    data: <data>,
 *    setup: <setup>
 * })
 */
const setup = { timeout: 3000 };
yt.v3.get({ setup, parameters: { path: "//home/user/@account" } });
yt.v3.set({ setup, parameters: { path: "//home/user/@account" } }, "default");
```

## Константы для кодов ошибок

|**Константа** | **Код** |
| --- | --- |
|`yt.codes.GENERAL_ERROR`  | 1 |
|`yt.codes.NODE_DOES_NOT_EXIST` | 500 |
|`yt.codes.NODE_ALREADY_EXISTS` | 501 |
|`yt.codes.PERMISSION_DENIED`  | 901 |
|`yt.codes.USER_IS_BANNED` | 903 |
|`yt.codes.USER_EXCEEDED_RPS` | 904 |

## Полезные ссылки

[Регистрация команд по версиям на бэкенде](https://github.com/YTsaurus/YTsaurus/blob/main/yt/yt/client/driver/driver.cpp).

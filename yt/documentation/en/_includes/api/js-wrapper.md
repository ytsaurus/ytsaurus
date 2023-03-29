# {{product-name}} JavaScript Wrapper (Browser + Node.js)

A wrapper for queries to {{product-name}} clusters that uses the {{product-name}} HTTP API with the closest interface possible.

## Installation

`npm install @ytsaurus/javascript-wrapper`

## Connection and use

The library is written in CommonJS style: you can use it in a node or bundle dependencies with Webpack.

```javascript
    const yt = require('@ytsaurus/javascript-wrapper')()

    // Enables you to set default settings for all commands.
    // If you need to work with multiple clusters simultaneously, the proxy and token are sent to each command separately.
    yt.setup.setGlobalOption('proxy', 'cluster-name.yt.my-domain.com');
    yt.setup.setGlobalOption('secure', true);

    yt.setup.setGlobalOption('authentication', {
        type: 'domain'
    });

    yt.setup.setGlobalOption('timeout', 15000);

    // Example 1
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


    // Example 2
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

The library can also be used as a global variable.

## Settings

The wrapper enables you to specify global and local settings for each query.


| **Name** | **Type** | **Default value** |
| --- | --- | --- |
| `secure` | boolean | `true` |
| `useHeavyProxy` | boolean | - |
| `proxy` | string | - |
| `heavyProxy` | string | `true` |
| `timeout` | Number | `100,000` |
| `useEncodedParameters` | boolean | `true` |
| `authentication` | object | `{ type: 'none' }` |
| `dataType` | string | - |
| `encodedParametersSettings` | object |
```

{
    maxSize: 64 * 1024,
    maxCount: 2,
    encoder(string) {
        return window.btoa(utf8.encode(string));
    }
}
```




## Events

You can subscribe to `requestStart`, `requestEnd`, and `error` events:

    yt.subscribe('requestStart', function () {
        console.log('requestStart');
    });


This can be useful for displaying the loader (you can count how many active queries there are) and for logging errors when requesting the API.

## Commands

For information about the available commands, see the [Commands](../../api/commands.md) section. The only difference is that the command names are written in the usual style for the JavaScript language, namely CamelCase.

Not all commands have been implemented, but they can be added if needed. If there is no command, the wrapper usually displays a corresponding error.

General format of the command:

```javascript
// yt.<version>.<command>(<parameters>[, <data>])
yt.v3.get({ path: "//home/user/@account" });
yt.v3.set({ path: "//home/user/@account" }, "default");
```

The general view of the command with the ability to configure local settings:

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

## Constants for error codes

| **Constant** | **Code** |
| --- | --- |
| `yt.codes.GENERAL_ERROR` | 1 |
| `yt.codes.NODE_DOES_NOT_EXIST` | 500 |
| `yt.codes.NODE_ALREADY_EXISTS` | 501 |
| `yt.codes.PERMISSION_DENIED` | 901 |
| `yt.codes.USER_IS_BANNED` | 903 |
| `yt.codes.USER_EXCEEDED_RPS` | 904 |

## Useful links

[Registering commands by version on the backend](https://github.com/YTsaurus/YTsaurus/blob/main/yt/yt/client/driver/driver.cpp).

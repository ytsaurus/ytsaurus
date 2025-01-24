# Настройка SSO

В веб-интерфейсе {{product-name}} реализовано два типа аутентификации пользователей: по паролю и через [SSO](https://en.wikipedia.org/wiki/Single_sign-on). Как работать с паролями — написано в [Руководстве пользователя](../../user-guide/storage/auth). В данной статье описано, как сконфигурировать SSO.

{% note warning %}

{{product-name}} поддерживает SSO-аутентификацию только по протоколу [OAuth 2.0](https://oauth.net/2/). При настройке SSO выбирайте такой Identity Server, который поддерживает работу с OAuth.

{% endnote %}

## Конфигурация

Для корректной работы аутентификации необходимо настроить две компоненты: прокси и веб-интерфейс.

- Чтобы настроить прокси, следует заполнить поле `oauthService` в [спецификации ресурса](https://github.com/ytsaurus/ytsaurus-k8s-operator/blob/main/docs/api.md#ytsaurusspec) ytsaurus.

- Для конфигурации веб-интерфейса необходимо задать настройку [ytOAuthSettings](https://github.com/ytsaurus/ytsaurus-ui/blob/main/packages/ui/docs/configuration.md#oauth) в конфиге веб-интерфейса. Сделать это можно через [ui-helm-chart](https://github.com/ytsaurus/ytsaurus-ui/blob/main/packages/ui-helm-chart/values.yaml#L80-L89), заполнив поле `settings.oauth` в `values.yaml`.

### Пример

Ниже приведён пример — как настроить SSO-аутентификацию в {{product-name}} с помощью сервиса [Microsoft Identity Platform](https://learn.microsoft.com/en-us/entra/identity-platform/v2-oauth2-auth-code-flow).

1. Сначала необходимо завести OAuth-приложение в MS Identity Platform. В качестве RedirectURIs следует задать `https://<HOST_NAME_OF_YOUR_YT_CLUSTER>/api/oauth/callback`.

    При создании приложения будут получены `CLIENT_ID` и `CLIENT_SECRET`. Также потребуется определить `TENANT_ID` — узнать его можно у администратора OAuth-сервера. Полученные параметры необходимо будет указывать в спецификации веб-интерфейса.

2. Далее следует настроить сервер:

    ```yaml
    # ytsaurus.yaml
    apiVersion: cluster.ytsaurus.tech/v1
    kind: Ytsaurus
    metadata:
      name: ytdemo
    spec:
      oauthService:
        host: graph.microsoft.com
        port: 443
        secure: true
        userInfoHandler:
          endpoint: oidc/userinfo
          loginField: email
      # ...
    ```

3. Настройка веб-интерфейса будет выглядеть следующим образом:

    ```yaml
    # ui-helm.values.yaml
    ui:
      image:
        repository: ghcr.io/ytsaurus/ui
    # ...
    settings:
      oauth:
        enabled: false
        baseURL: "https://login.microsoftonline.com/mycompany.onmicrosoft.com/oauth2/v2.0/" # mycompany.onmicrosoft.com is a tenant ID example
        authPath: "authorize"
        logoutPath: "logout"
        tokenPath: "token"
        clientIdEnvName: "CLIENT_ID"
        clientSecretEnvName: "CLIENT_SECRET"
        scope: "openid offline_access" # offline_access scope is required for api to respond with refresh_token
        buttonLabel: "Login via SSO"
    ```

## Детали реализации

Ниже описано, как устроен флоу работы с OAuth. Это понимание поможет при локализации возможных проблем конфигурации.

1. Пользователь открывает веб-интерфейс {{product-name}} и нажимает "Login via SSO".
2. Веб-интерфейс {{product-name}} перенаправляет пользователя на URL стороннего OAuth Identity Server с пробросом сконфигурированных `scope`. URL стороннего сервера формируется на основе `baseURL` и `authPath`.
3. Пользователь соглашается с запрошенными разрешениями, и OAuth Identity Server перенаправляет пользователя на адрес, который был задан в настройках OAuth-приложения — `https://<HOST_NAME_OF_YOUR_YT_CLUSTER>/api/oauth/callback`. В query-параметре `code` передаётся код авторизации.
4. Веб-интерфейс делает запрос в OAuth Identity Server по URL из `baseURL` и `tokenPath`. В запросе передаются код авторизации и секрет приложения. В ответе приходят `access_token` и `refresh_token`.
5. Веб-интерфейс выставляет токены в cookie браузера пользователя: `yt_oauth_access_token` и `yt_oauth_refresh_token`.
6. Когда `yt_oauth_access_token` истекает — веб-интерфейс обновляет его, используя `yt_oauth_refresh_token`.
7. Веб-интерфейс отправляет запросы в прокси, передавая значение cookie `yt_oauth_access_token`. Прокси делает запрос в OAuth Identity Server на URL, который формируется на основе `oauthService.host`, `oauthService.port` и `oauthService.userInfoHandler.endpoint`.
8. Из полученного ответа достаётся поле, сконфигурированное в `oauthService.userInfoHandler.loginField` — оно используется в качестве username пользователя {{product-name}}.
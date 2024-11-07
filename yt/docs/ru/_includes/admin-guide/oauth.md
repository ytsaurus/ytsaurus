## OAuth для аутентификации пользователей
В WEB UI YTsaurus реализовано два типа аутентификации пользователей: по паролю и через [OAuth2](https://oauth.net/2/).
В этой статье описано как сконфигурить аутентификацию через OAuth.

### Конфигурация
Для корректной работы нужно сконфигурить Proxies и UI.

Если для деплоя YTsaurus изпользуется [ytsaurus-k8s-operator](https://github.com/ytsaurus/ytsaurus-k8s-operator), то
нужно заполнить поле `oauthService` в [спецификации ресурса](https://github.com/ytsaurus/ytsaurus-k8s-operator/blob/main/docs/api.md#ytsaurusspec) 
ytsaurus. 

Для конфигурации UI нужно заполнить поле [ytOAuthSettings](https://github.com/ytsaurus/ytsaurus-ui/blob/main/packages/ui/docs/configuration.md#oauth) 
в конфиге UI. Поле можно задать в values для [ui-helm-chart](https://github.com/ytsaurus/ytsaurus-ui/blob/main/packages/ui-helm-chart/values.yaml#L80-L89).

### Пример
Здесь приведен пример конфигурации OAuth для интеграции с [Microsoft Identity Platform](https://learn.microsoft.com/en-us/entra/identity-platform/v2-oauth2-auth-code-flow).

Сначала нужно завести OAuth приложение в MS Identity Platform. 
В качестве RedirectURIs нужно задать `https://<HOST NAME OF YOUR YT CLUSTER>/api/oauth/callback`.
При создании приложения будут получены `CLIENT_ID` и `CLIENT_SECRET`.
Также необходимо узнать `TENANT_ID` у администратора OAuth-сервера.

Настройка сервера:
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

Настройка UI:
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

### Детали реализации
В этом разделе описано как устроен флоу работы с OAuth для локализации возможных проблем конфигурации.

1. Пользователь открывает web-интерфейс YTsaurus и нажимает "Login via SSO".
2. UI редиректит пользователя в интерфейс стороннего OAuth Identity Server (урл формируется из `baseURL` и `authPath`) 
с пробросом сконфигурированных `scope`
3. Пользователь соглашается с запрошенными разрешениями и OAuth Identity Server редиректит его на 
`https://<HOST NAME OF YOUR YT CLUSTER>/api/oauth/callback`, который был задан в настройках OAuth-приложения, 
передавая в урле код авторизации в поле `code`.
4. UI делает запрос в OAuth Identity Server (урл формируется из `baseURL` и `tokenPath`), передавая код авторизации 
и секрет приложения и получает `access_token` и `refresh_token`.
5. UI выставляет токены в cookie браузера пользоввателя `yt_oauth_access_token` и `yt_oauth_refresh_token`.
6. Когда `yt_oauth_access_token` истекает — UI обновляет ее используя `yt_oauth_refresh_token`.
7. UI отправляет запросы в Proxy с cookie `yt_oauth_access_token`, Proxy делает запрос в OAuth Identity Server  
(урл формируется из `oauthService.host`, `oauthService.port` и `oauthService.userInfoHandler.endpoint`), из ответа 
достается поле, сконфигурированное в `oauthService.userInfoHandler.loginField` и используется в качестве username 
пользователя YTSaurus. 

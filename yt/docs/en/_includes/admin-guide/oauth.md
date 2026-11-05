# Setting up SSO

The {{product-name}} web interface offers two types of user authentication: password-based authentication and single sign-on ([SSO](https://en.wikipedia.org/wiki/Single_sign-on)). To learn more about using passwords, see the [User manual](../../user-guide/storage/auth). This article explains how to configure SSO.

{% note warning %}

{{product-name}} supports SSO authentication exclusively through the [OAuth 2.0](https://oauth.net/2/) protocol. When configuring SSO, choose an identity server that supports OAuth.

{% endnote %}

## Configuration

To implement proper authentication, you need to configure two components: a proxy and a web interface.

- To set up a proxy, fill in the `oauthService` field in the ytsaurus [resource specification](https://github.com/ytsaurus/ytsaurus-k8s-operator/blob/main/docs/api.md#ytsaurusspec).

- To configure the web interface, set the [ytOAuthSettings](https://github.com/ytsaurus/ytsaurus-ui/blob/main/packages/ui/docs/configuration.md#oauth) parameter in the web interface config file. You can do this via [ui-helm-chart](https://github.com/ytsaurus/ytsaurus-ui/blob/main/packages/ui-helm-chart/values.yaml#L80-L89) by filling in the `settings.oauth` field in `values.yaml`.

### Example

#### Microsoft Identity Platform

Below is an example demonstrating how to set up SSO authentication in {{product-name}} using [Microsoft Identity Platform](https://learn.microsoft.com/en-us/entra/identity-platform/v2-oauth2-auth-code-flow).

1. First, create an OAuth app in MS Identity Platform. For RedirectURIs, specify `https://<HOST_NAME_OF_YOUR_YT_CLUSTER>/api/oauth/callback`.

    When creating the app, you'll receive a `CLIENT_ID` and a `CLIENT_SECRET`. You'll also need to specify the `TENANT_ID`. You can get it from your OAuth server administrator. You'll need to enter the received parameters in the web interface specification.

2. Next, configure the server:

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

3. Your web interface settings should look like this:

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

#### Keycloak

Here's an example showing how to configure SSO authentication using [Keycloak](https://www.keycloak.org/).

In this example, we use the realm `ytsaurus.tech`, the Keycloak address `https://keycloak.example.com`, and the web interface address `https://ui.example.com`. Keycloak must be accessible both from the user's browser and from the HTTP proxy.

1. Create an OpenID Connect client in Keycloak. Enable **Client authentication** and **Standard flow**, and set the following parameters:

    ```text
    Valid redirect URIs: https://ui.example.com/api/oauth/callback
    Valid post logout redirect URIs: https://ui.example.com/api/oauth/logout/callback
    Web origins: https://ui.example.com
    ```

    Save the obtained `CLIENT_ID` and `CLIENT_SECRET` in a Kubernetes Secret:

    ```bash
    kubectl create secret generic ytsaurus-ui-keycloak \
      --from-literal=client-id='<CLIENT_ID>' \
      --from-literal=client-secret='<CLIENT_SECRET>'
    ```

2. Configure the server:

    ```yaml
    # ytsaurus.yaml
    apiVersion: cluster.ytsaurus.tech/v1
    kind: Ytsaurus
    metadata:
      name: ytdemo
    spec:
      oauthService:
        host: keycloak.example.com
        port: 443
        secure: true
        userInfoHandler:
          endpoint: "realms/ytsaurus.tech/protocol/openid-connect/userinfo"
          loginField: "preferred_username"
          errorField: "error"
        disableUserCreation: false
      # ...
    ```

    With `disableUserCreation: false`, the system automatically creates a user in `//sys/users` after their first successful login. You need to assign permissions to the user separately.

3. Configure the web interface:

    ```yaml
    # ui-helm.values.yaml
    ui:
      clusterConfig:
        clusters:
          - id: my-cluster
            # Other cluster parameters are omitted.
            authentication: basic
      env:
        - name: KEYCLOAK_CLIENT_ID
          valueFrom:
            secretKeyRef:
              name: ytsaurus-ui-keycloak
              key: client-id
        - name: KEYCLOAK_CLIENT_SECRET
          valueFrom:
            secretKeyRef:
              name: ytsaurus-ui-keycloak
              key: client-secret
    settings:
      oauth:
        enabled: true
        baseURL: "https://keycloak.example.com"
        authPath: "realms/ytsaurus.tech/protocol/openid-connect/auth"
        logoutPath: "realms/ytsaurus.tech/protocol/openid-connect/logout"
        tokenPath: "realms/ytsaurus.tech/protocol/openid-connect/token"
        clientIdEnvName: "KEYCLOAK_CLIENT_ID"
        clientSecretEnvName: "KEYCLOAK_CLIENT_SECRET"
        scope: "openid profile"
        buttonLabel: "Login via Keycloak"
    ```

    The `authentication: basic` setting allows the web interface to pass authentication data to the HTTP proxy and doesn't require enabling password-based login. The secret with the OAuth client secret doesn't replace the [interface secret](https://github.com/ytsaurus/ytsaurus-ui/blob/main/packages/ui-helm-chart/values.yaml): the interface secret stores the {{product-name}} token for the web interface's internal requests.

4. Apply the cluster specification and install or update the web interface with the prepared `values.yaml`. After logging in, Keycloak will redirect the user to `/api/oauth/callback`, and the HTTP proxy will request `userinfo` and retrieve the username from `preferred_username`.

## Implementation details

Below is an overview of the OAuth workflow. Understanding it will help you identify potential issues in your configuration.

1. The user opens the {{product-name}} web interface and clicks "Login via SSO".
2. The {{product-name}} web interface redirects them to the URL of a third-party OAuth identity server, forwarding the configured `scopes`. The URL of the third-party server is generated from `baseURL` and `authPath`.
3. The user agrees to grant the requested permissions, and the OAuth identity server redirects them to the address specified in the OAuth app settings: `https://<HOST_NAME_OF_YOUR_YT_CLUSTER>/api/oauth/callback`. The authorization code is passed in the `code` query parameter.
4. The web interface makes a request to the OAuth identity server using the URL generated from `baseURL` and `tokenPath`. The request passes the authorization code and the app secret. The response contains `access_token` and `refresh_token`.
5. The web interface stores the tokens in the user's browser cookies: `yt_oauth_access_token` and `yt_oauth_refresh_token`.
6. When `yt_oauth_access_token` expires, the web interface updates it using `yt_oauth_refresh_token`.
7. The web interface sends a request to the proxy, passing the value of the `yt_oauth_access_token` cookie. The proxy makes a request to the OAuth identity server via a URL generated from `oauthService.host`, `oauthService.port`, and `oauthService.userInfoHandler.endpoint`.
8. The received response contains the field configured in `oauthService.userInfoHandler.loginField`, which is used as the username in {{product-name}}.
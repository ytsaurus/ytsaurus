Some {{product-name}} SDK settings are specific to the cluster configuration. To simplify your work, the SDKs get them from the cluster, from a predefined location in Cypress: `//sys/client_config/default`.

{% cut "**Creating a client config**" %}

The client config is a Document located at the path `//sys/client_config/default`.

```(bash)
yt create map_node //sys/client_config
yt create document //sys/client_config/default
```

Initial config setup (example):
```(bash)
yt set //sys/client_config/default '{enable_proxy_discovery=%false}'
```

{% endcut %}

##### Config field descriptions

Note: not all fields are supported in all SDKs.

- `enable_proxy_discovery`, `bool`, enabled by default. Determines whether to use “heavy proxies” for “heavy” requests (write/read/…). This is relevant for small clusters.
- `http_proxy_discovery_url`, `str`, deprecated, default is "hosts". You can override the address for getting proxies (for example, to add a role).
- `operation_link_template`, `str`. A template for generating links to operations. This is useful if the UI is deployed on a different domain or path.
- `query_link_template`, `str`. Similar to the previous field, but it’s used for generating links to the query tracker.
- `strawberry_ctl_address`, `str`. A template for the Strawberry controller address.
- `strawberry_cluster_name`, `str`, default is None. The cluster name in the Strawberry controller, if it differs from the proxy.
- `max_replication_factor`, `int`, default is None (SDK defaults are used). The maximum replication_factor when uploading tables or files. This is relevant for small clusters.
- `python_enable_password_strength_validation`, `str`, default is False. Determines whether to validate the password length when you run `set_user_password`.
- `python_pickling_ignore_system_modules`, `bool`, default is False. Controls dependency collection when you run operations. It disables the collection of all packages installed in the “system Python”.
- `python_pickling_dynamic_libraries_enable_auto_collection`, `bool`. Like the previous field, it affects dependency collection. It disables the collection of binary libraries from packages.
- `python_encrypt_pickle_files`, `bool`. Determines whether to encrypt files with the operation code state. This simplifies dependency collection.

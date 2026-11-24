## NYT::NFlow::TYdbTopicDriverConfig {#NYT_NFlow_TYdbTopicDriverConfig}
<!-- This page is generated. -->
Source: [yt/yt/flow/extensions/ydb/cpp/ydb_topic_driver.h]({{source-root}}/yt/yt/flow/extensions/ydb/cpp/ydb_topic_driver.h)

Settings for the `TYdbTopicDriver` resource used by public YDB connectors. The built-in resource uses plaintext transport with `host:port` endpoints. It has no TLS or CA certificate setting; `grpcs://` does not enable TLS. Token and login authentication do not encrypt the connection.

#|
|| **Parameter** | **Description** ||
|| `auth_mode` | **Type**: [NYT::NFlow::EYdbAuthMode](./all_yson_structs#NYT_NFlow_EYdbAuthMode)
**Default value**: `auto`
Authentication mode: `auto`, `token`, `login`, or `anonymous`. A credentials factory injected through C++ takes precedence over configuration and environment variables. ||
|| `token_env_var` | **Type**: `std::string`
**Default value**: `YDB_TOKEN`
Name of the environment variable holding the token for `auto` and `token` modes. Specify the variable name, not the token. ||
|| `username` | **Type**: `std::string`
Username for `login` mode; must be nonempty in that mode. Defaults to the empty string (`""`). ||
|| `password_env_var` | **Type**: `std::string`
Name of the environment variable holding the password for `login` mode. Both the variable name and its nonempty value are required in that mode. Do not specify the password itself. Defaults to the empty string (`""`). ||
|| `network_thread_count` | **Type**: `int`
**Default value**: `2`
Number of YDB driver network threads handling SDK I/O. ||
|| `client_thread_count` | **Type**: `int`
**Default value**: `0`
Number of threads in the YDB SDK client pool. A value of `0` enables the SDK adaptive pool. ||
|| `stop_driver_before_destroying` | **Type**: `bool`
**Default value**: `true`
Stop the YDB driver before destroying the resource and wait for its background work and handlers to finish. ||
|#


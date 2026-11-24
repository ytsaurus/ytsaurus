## NYT::NFlow::EYdbAuthMode {#NYT_NFlow_EYdbAuthMode}
<!-- This page is generated. -->

#|
|| Possible values | **Description** ||
|| `auto` | Use the token from `token_env_var`, or connect without authentication if it is empty. Login and password authentication is not selected automatically. ||
|| `token` | Use the token from `token_env_var`. An empty variable causes an error. ||
|| `login` | Use `username` and the password from `password_env_var` without reading the token variable. An empty password causes an error. ||
|| `anonymous` | Connect without authentication without reading token or password variables. ||
|#


# Error codes

This section presents a collection of error codes that you may encounter when working with {{product-name}}.

The error code is a non-negative integer:

- `0` means a successful execution `NYT::EErrorCode::OK`
- `1` means generic errors `NYT::EErrorCode::Generic`

Other values are split between subsystems.

You can view registered error codes by [searching](https://github.com/search?q=repo%3Aytsaurus%2Fytsaurus+path%3A%2F%5Eyt%5C%2F%2F+YT_DEFINE_ERROR_ENUM&type=code) for the `YT_DEINE_ERROR_ENUM` construct in the code base.

To exactly construe certain errors, you need quite a deep knowledge of the system architecture. The list of errors is continuously expanded (mostly in a backward-compatible way).

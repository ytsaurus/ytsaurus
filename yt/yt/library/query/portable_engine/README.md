# Portable query expression engine

`portable_engine` is an LLVM-free backend for a deliberately small subset of the YTsaurus query expression engine. It provides a limited implementation of `query/engine_api` and is currently being developed for the YQL over YTFlow worker, which needs MiniKQL's LLVM JIT and cannot also link the incompatible LLVM version used by `query/engine`.

This is not a general-purpose replacement for `query/engine`, which remains the default backend. The name "portable" refers to the absence of an LLVM dependency, not to complete query-language coverage.

Only explicitly supported typed expression nodes and operation signatures are accepted. Unsupported expressions are rejected when an evaluator is constructed; the portable backend does not fall back to `query/engine`.

The YQL over YTFlow team currently maintains this backend and defines its supported operation set. Other consumers are possible, but before depending on it they must agree with the team on the required operation coverage, maintenance responsibilities, and expected lifetime. The backend may be removed after YQL execution moves out of the Flow worker unless broader use justifies retaining it.

## Supported operations

The following operations are available through the internal expression program. Public `engine_api` adapters are not implemented yet.

| Operation | Supported signature | Null handling |
|---|---|---|
| `farm_hash` | One or more arguments, each `Int64`, `Uint64`, `Boolean`, or `String`; returns `Uint64` | NULL values participate in the fingerprint. |
| `%` | Two `Int64` arguments returning `Int64`, or two `Uint64` arguments returning `Uint64` | Returns NULL if either value is NULL. |
| `uint64` | `Int64` argument; returns `Uint64` | Preserves NULL; otherwise preserves the integer bits. |

Typed literals and schema-position column references are also supported. Static argument types are matched exactly: a NULL value in a supported type does not require a signature with the static type `Null`. Such signatures, `farm_hash()` without arguments, and other operations are not supported.

The append-only [capability manifest](capabilities.yson) records the active nodes and signatures. Each entry must be covered by portable semantic tests with explicit expected values or errors.

## Extending the engine

1. Implement the operation and register its exact typed signature. Use variadic registration only for a genuinely variadic function whose arguments share the registry's single allowed type set.
2. Add semantic tests with explicit expected values and errors, including null handling, relevant boundary cases, and rejection of unsupported signatures. The LLVM backend may be used for differential checks, but it is not the semantic specification.
3. Update [Supported operations](#supported-operations), the capability manifest, and its semantic coverage.

### Important considerations

- Add explicit compiler support if the operation requires a new AST node or lazy evaluation; do not hide control flow in an eager callback.
- The portable compiler consumes an already typed AST. Language-level type inference belongs in `query/base` and must not be duplicated here.

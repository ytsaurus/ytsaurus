# YTsaurus C++ Style Guide

<!--================================================================================-->

## Scope

This document summarizes the coding conventions used in most of our C++ code base. The rules described here apply at least to the following top-level directories:

* `yt/yt` — the root directory for YT server code, native client code, public RPC API and core libraries
* `yt/chyt` — the root directory for CHYT, which serves as an integration between ClickHouse engine and YT acting as storage and execution layer
* `yt/yql` — the root directory for YQL agent, which hosts the YQL logic within YT server process
* `library/cpp/yt` — the collection of libraries that were made separate from the rest of YT code to simplify their re-use in other projects

This document does not cover the following top-level directories:

* `yt/cpp` — despite being the official C++ SDK, it is written in a slightly different coding style than the rest of the code due to historical reasons
* `util` — the set of foundation libraries inherited from Yandex code base with some very basic primitives (like streams, filesystem helpers, etc)
* `library/cpp` (except `library/cpp/yt`) — various libraries inherited from Yandex code base

For the code from the directories above, as well as any other C++ code in our repository, try to follow the conventions used in the code around the files you are working on. Changing code in the two latter directories is not recommended, but is technically possible.

<!--================================================================================-->

## General

We use C\+\+20. As soon as the most of our toolchains support C\+\+23, we aim to switch to it.

As of today, we do not provide clang-format/clang-tidy configuration due to significant complications while trying to express our coding style in terms of these tools. Still, it may happen in the future; there is an issue #75 for that.

Most of the code follows the rules from this document, but some older parts may not. If you are working on such a part, you may refactor it to follow the rules from this document, but it is not required. Having a PR that introduces 3 new lines and changes whitespace around 300 lines is not very convenient; try to separate such changes into separate PRs.

<!--================================================================================-->

## Code Organization

### Classes and Structs

Class and struct names are always in PascalCase and are prepended either with capital `T`, `I` or `E`.

```cpp
class TInputChunk;
struct TConnectionOptions;
```

Base classes (i.e. reusable bases for other classes) are often suffixed with `Base` suffix.

```cpp
class TOperationControllerBase;
```

Classes are used for most of the code; structs are used for PODs and similar types. Structs must not contain private fields or methods.

Interfaces are structs prefixed with capital `I`. Their virtual methods must be pure virtual; non-virtual helpers expressed in terms of those methods are also allowed.

```cpp
struct IOperationController;
struct IInvoker;
```

Enums are always in PascalCase and are prepended with capital `E`. They are defined using the specialized DEFINE_ENUM macro.

```cpp
DEFINE_ENUM(ETransactionType,
    ((Master)          (0)) // Accepted by both masters and tablets
    ((Tablet)          (1)) // Accepted by tablets only
);
```

#### Order within a visibility block

Each `public`, `protected`, and `private` block must contain declarations and definitions in the following strict order:

1. Type declarations (`using` aliases, nested `class`, `struct`, `enum`).
2. Constant fields (`constexpr`, `static const`, and `inline static const` members).
3. Data fields and `DEFINE_BY*_*_PROPERTY` macros.
4. Methods (including constructors, destructors, virtual methods, and signal macros).
5. Static methods.

This order applies independently to every visibility block in the class and recursively to all classes and structs defined within a parent class.

```cpp
class TSomeClass
    : public TBase
{
public:
    // 1. Type declarations.
    using TKey = int;
    using TValueMap = THashMap<TKey, TValue>;

    // 2. Constant fields.
    constexpr int DefaultRetryCount = 3;
    inline static const std::string DefaultPrefix = "default";

    // 3. Data fields and properties.
    DEFINE_BYVAL_RW_PROPERTY(int, TabletCount);
    DEFINE_BYREF_RO_PROPERTY(std::string, Name);

    // 4. Methods.
    explicit TSomeClass(TKey key);

    void DoSomething();
    TFuture<void> DoSomethingAsync();

    // 5. Static methods.
    static TSomeClassPtr Create(const TOptions& options);
    static TKey MakeKey(int value);

private:
    // 1. Type declarations.
    using TBase = TSomeOtherClass;

    struct TReplicaEntry
    {
        // 1. Type declarations.
        NApi::NNative::IClientPtr Client;
        TFuture<NApi::NNative::IClientPtr> AsyncClient;

        // 4. Methods.
        bool IsTerminated() const
    };

    // 3. Data fields.
    TKey Key_;
    TValueMap ValueMap_;
    THashMap<std::string, TIntrusivePtr<TReplicaEntry>> ReplicaEntries_;

    // 4. Methods.
    void DoSomethingInternal();
};
```

#### Short type aliases inside a class

When a class uses heavily templated or deeply namespaced types repeatedly, `using` declarations (placed at the beginning of the relevant visibility block) create short local aliases that reduce noise in method signatures and field declarations.

```cpp
class TDistributedWriteTableStartFacade
    : public TDistributedWriteStartFacadeBase<TDistributedWriteTableStartFacade, TDistributedTableSessionTraits>
{
    // Short aliases declared first inside the private block.
    using TBase = TDistributedWriteStartFacadeBase<TDistributedWriteTableStartFacade, TDistributedTableSessionTraits>;
    using TTraits = TDistributedTableSessionTraits;

public:
    // Short public aliases.
    using TStartOptions = TDistributedWriteSessionStartOptions;
    using TSession = TDistributedWriteSession;

    // Methods — signatures now use the short names.
    void StartSession(const TStartOptions& options);
    TSession& GetSession();
};
```

<!------------------------------------------------------------------------------------>

### Functions

Function names are always in PascalCase regardless of whether they are free functions or methods.

```cpp

void DoSomething();

class TSomeClass
{
public:
    void DoSomethingElse();

private:
    void DoSomethingInternal();
};
```

<!------------------------------------------------------------------------------------>

### Variables and Enums

Global variables, consts and enum values as well as static consts and variables are always in PascalCase.

```cpp
constexpr i64 MaxValueSize = 128_KB;

static const TLogger Logger("SomeLogger");

DEFINE_ENUM(ENodeFactoryState,
    (Active)
    (Committed)
    (RolledBack)
);

void DoSomething()
{
    static const auto VariableName = DoSomethingElse();
}
```

Local variables and parameters are always in camelCase.

```cpp
void DoSomething(const TFooBar& fooBar)
{
    const auto& foo = fooBar.GetFoo();
    const auto& bar = fooBar.GetBar();
}
```

<!------------------------------------------------------------------------------------>

### Namespaces

Namespaces are always in PascalCase and are prepended with capital `N`.

Nested namespaces are a common practice. The topmost namespace is usually `NYT`, but may be different for some libraries.

The last line of the namespace repeats the namespace name to make it easier to find the end of the namespace.

```cpp
namespace NYT::NChunkPools {

// Something

} // namespace NYT::NChunkPools
```

<!------------------------------------------------------------------------------------>

### Separator Lines

We use separator lines to separate logical blocks of code. The separator line is a line with 80 `/` characters.

Separator lines are used in two situations:

- Between a namespace boundary and its content. Every opening and closing of a namespace is wrapped in separator lines. `using` directives that immediately follow the namespace opening are not separated from it.
- Between groups of top-level declarations within a namespace: between classes, structs, enums, groups of free functions, etc. Use a separator line only if a file could be split into different files across a separator line.

As a consequence of the second rule, separator lines are never placed within a class, struct, or function.

```cpp
namespace NYT::NChunkPools {

using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

struct IFoo
{ };

IFooPtr CreateFoo();

////////////////////////////////////////////////////////////////////////////////

class TBar
{ };

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
```

When a separator section contains both type definitions and free functions that operate on them, types come first and functions after.

```cpp
// OK.
DEFINE_ENUM(EMode,
    (Foo)
    (Bar)
);

struct TSomeStruct
{
    EMode Mode = EMode::Foo;

    static TSomeStruct Create(EMode mode);
};

TAnotherStructPtr GetAnotherStruct();

// Not OK.
TAnotherStructPtr GetAnotherStruct();

struct TSomeStruct
{
    EMode Mode = EMode::Foo;
};
```

<!------------------------------------------------------------------------------------>

### Properties

We use a syntax similar to C# for properties. Properties are defined using the specialized `DEFINE_BYXXX_YY_PROPERTY` macro.

```cpp
class TSomeClass
{
public:
    DEFINE_BYREF_RO_PROPERTY(TFoo, Foo); // Produces const TFoo& Foo() const;
    DEFINE_BYREF_RW_PROPERTY(TBar, Bar); // Produces TBar& Bar(); and const TBar& Bar() const;

    DEFINE_BYVAL_RO_PROPERTY(TBaz, Baz); // Produces TBaz GetBaz() const;
    DEFINE_BYVAL_RW_PROPERTY(TQux, Qux); // Produces TQux GetQux(); and void SetQux(TQux value);

    // In all cases the internal field is named as <name>_, e.g. Foo_.
};
```

See more complex property macros in [property.h](/library/cpp/yt/misc/property.h).

<!------------------------------------------------------------------------------------>

### Include Guards

We use `#pragma once` for include guards in `.h` files.

<!------------------------------------------------------------------------------------>

### Include Directives

We use quote-style include directives for the headers located in the same directory as the including file. We use angle-bracket-style include directives for the rest of included headers, including system ones.

Included headers are grouped by the base directory they are located in. Groups are separated by a blank line. Within a group, headers are sorted alphabetically.

We order groups from the most specific to the most general. The motivation is that we would like to reduce the number of situations where a later-included header already includes an earlier one, as it may hide dependency issues.

We do not care about including everything that is used by the current file.

For .cpp files, the corresponding header is always the first included header and is separated by a blank line.

For .h files, usually the private.h/public.h header is the first included.

Very common standard includes (like `<vector>` or `<util/string.h>`) are collected in `yt/yt/core/misc/common.h`, which is usually included in any .cpp or .h file in `yt/yt`.`

```cpp
// Example for yt/yt/server/foo/bar.cpp.
#include "bar.h"

#include "config.h"
#include "helpers.h"

#include <yt/yt/server/foo/baz/baz.h>

#include <yt/yt/server/other_server/public.h>

#include <yt/yt/server/some_server/something.h>
#include <yt/yt/server/some_server/something_more.h>

#include <yt/yt/server/lib/some_server_lib/server_lib_staff.h>

#include <yt/yt/ytlib/some_client/stuff.h>

#include <yt/yt/ytlib/some_other_client/config.h>
#include <yt/yt/ytlib/some_other_client/public.h>

#include <yt/yt/client/very_basic_client/proto/stuff.pb.h>

#include <yt/yt/library/basic_staff/specific.h>

#include <yt/yt/core/certain/primitive.h>

#include <library/cpp/yt/common_library/library.h>

#include <util/string/split.h>

#include <any>
```

<!------------------------------------------------------------------------------------>

### inl.h Files

When a template is declared in a .h file, its implementation is usually placed in a corresponding -inl.h file. The -inl.h file is included at the end of the .h file.

Another occasion to use -inl.h files is when you want to force-inline a function declared in the header.

The -inl.h itself must be protected from erroneous inclusion by a boilerplate include guard.

```cpp
// trace_context.h

template <class TFn>
void AnnotateTraceContext(TFn&& fn);

TTraceContext* GetCurrentTraceContext();

#define TRACE_CONTEXT_INL_H_
#include "trace_context-inl.h"
#undef TRACE_CONTEXT_INL_H_


// trace_context-inl.h

#ifndef TRACE_CONTEXT_INL_H_
#error "Direct inclusion of this file is not allowed, include trace_context.h"
// For the sake of sane code completion.
#include "trace_context.h"
#endif

template <class TFn>
void AnnotateTraceContext(TFn&& fn)
{ ... }

Y_FORCE_INLINE TTraceContext* GetCurrentTraceContext()
{ ... }

```

<!--================================================================================-->

## Code Practices

<!------------------------------------------------------------------------------------>

### Integer types

We use `int` when we do not care about the exact size of the integer and for most loop variables.

When the arity of the integer is important, we use `i8`, `i16`, `i32`, `i64` and `ui8`, `ui16`, `ui32`, `ui64` for signed and unsigned integers respectively.

We sometimes use `size_t` and `ssize_t` when interoperability with the standard library requires them, but in general we try not to use them for local variables or loop variables.

We do not use `long`, `long long` or `short` types, as well as C++11 fixed-width types like `int8_t` or `uint32_t` or non-standard extensions like `__int64` or `unsigned __int64`.

We favor signed types instead of unsigned types. We acknowledge that this is a controversial topic, but we believe that the benefits of signed types outweigh the benefits of unsigned types. We do not use unsigned types for loop variables. In general, we use unsigned types only for bit manipulations, wire protocols and other cases where unsignedness is justified by the nature of manipulations.
<details>
<summary>Some reasoning behind this decision by maxim.babenko</summary>
<blockquote>
Generally, I try to adhere to the point of view from here:
<a href="https://google.github.io/styleguide/cppguide.html#Integer_Types">https://google.github.io/styleguide/cppguide.html#Integer_Types</a>. Interestingly, modern C++ seems to be evolving towards this viewpoint.

For instance, std::span::size is signed: https://en.cppreference.com/w/cpp/container/span/size

You can also read: http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2019/p1227r2.html

The committee itself, represented by quite respected comrades, by the way, has long and quite unequivocally expressed their opinion on this topic: https://youtu.be/Puio5dly9N8?t=2558
</blockquote>
</details>

```cpp
// Variant 1.
int tableCount = static_cast<int>(InputTables_.size());
for (int tableIndex = 0; tableIndex < tableCount; ++tableIndex) {
    ...
}

// Variant 2.
for (ssize_t tableIndex = 0; tableIndex < std::ssize(InputTables_); ++tableIndex) {
    ...
}

// Not OK.
for (size_t tableIndex = 0; tableIndex < InputTables_.size(); ++tableIndex) {
    ...
}

// OK.
for (const auto& table : InputTables_) {
    ...
}
for (auto&& table : InputTables_) {
    ...
}
```

<!------------------------------------------------------------------------------------>

### `auto` vs Explicit Types

We tend to use `auto` for local variables, but not for fields, function parameters or return types. Using `auto` for the latter cases is allowed, but must be justified by the code readability or the template nature of the code.

Prefer explicit types for primitive values such as `int`, `i64`, `char`, and `bool`, but `auto` is also allowed.

Use `std::string` for strings. Use `TString` only when interoperability with existing code requires it.

We encourage using structured bindings with auto whenever possible.

```c++
// Not OK.
std::vector<TInputChunkPtr> chunks = CollectChunks();
// OK.
auto chunks = CollectChunks();

// OK.
auto canTeleportChunk = chunk.IsTrivial() && OutputTable_.CanTeleport();
// Preferred.
bool canTeleportChunk = chunk.IsTrivial() && OutputTable_.CanTeleport();

// Not OK.
auto GetDebugString(TChunkInfo info);
// OK.
std::string GetDebugString(TChunkInfo info);

// Not OK.
auto pair = GetSomePair();
// OK.
auto [lhs, rhs] = GetSomePair();

// OK because de-facto this is a template function.
auto GetRandomElement(auto collection)
{
    return collection[Rand() % collection.size()];
}

// OK because auto replaces cumbersome const std::pair<TInputCookie, TInputChunk>&.
BuildYsonFluently(consumer)
    .BeginList()
        .DoFor(cookieToChunk, [&] (TFluentMap fluent, const auto& pair) {
            fluent
                .BeginMap()
                    .Item("input_cookie").Value(pair.first)
                    .Item("output_cookie").Value(pair.second)
                .EndMap();
        })
    .EndList();
```

<!------------------------------------------------------------------------------------>

### Forward Declarations

We strongly rely on forward declarations, preferring them over includes whenever possible. We extract them into separate files named `public.h`/`private.h` which serve as a collection of forward declarations for the whole directory. Forward declarations that may be used outside of the directory are placed into `public.h`, the rest are placed into `private.h` (which usually includes `public.h`).

```cpp

// something.h

#include <foobar/public.h>

void DoSomethingElse(const TFoo& foo);
void DoSomething(TBarPtr bar);


// something.cpp

#include <foobar/foo.h>
#include <foobar/bar.h>

void DoSomethingElse(const TFoo& foo)
{
    // Use full definition of TFoo.
}

void DoSomething(TBarPtr bar)
{
    // Use full definition of TBar.
}


// foobar/public.h

struct TFoo;
DECLARE_REFCOUNTED_CLASS(TBar);
```

<!------------------------------------------------------------------------------------>

### Raw/Intrusive/Weak/Unique/Shared Ptrs

The most common pointer type used across our code base is `TIntrusivePtr<T>`. Semantically it is very close to `std::shared_ptr<T>`, but it has a slightly different memory layout, and it is tightly integrated with our reference counting mechanism and asynchronous framework. In a typical case, we prefer `TIntrusivePtr<T>` over `std::shared_ptr<T>`.

An intrusive pointer requires the class to be either derived from `TRefCounted` or to be a final class.

After a full definition of such a class, the `DEFINE_REFCOUNTED_TYPE` macro must be used to provide implementations of reference counting methods.

The forward declaration of a class itself and a fully defined alias `TPtr = TIntrusivePtr<T>` are provided by `DECLARE_REFCOUNTED_CLASS`/`DECLARE_REFCOUNTED_STRUCT` macros and are usually placed in a `public.h`/`private.h` file.

```cpp
// something.h

class TSomething
    : public TRefCounted
{
    explicit TSomething(TSomeArg arg);
};

DEFINE_REFCOUNTED_TYPE(TSomething);


// public.h

DECLARE_REFCOUNTED_CLASS(TSomething);


// consumer.cpp

#include "public.h"

void DoSomething(TSomethingPtr something)
{
    PassSomewhere(std::move(something));
}
```

Weak pointers (`TWeakPtr<T>`) accompany intrusive pointers in asynchronous contexts to mitigate lifetime issues. Their use outside of asynchronous code is usually not justified.

Unique pointers (`std::unique_ptr<T>`) are frequently used with non-refcounted or standard classes. Shared pointers are somewhat rare, but may occur when interoperability with the standard library is required, or the ownership promotion from the unique pointer is used.

Raw pointers are quite often used when the ownership is guaranteed to be somewhere else, and the pointer is used as a non-owning mutable reference. Other usages of raw pointers are not encouraged. In particular, raw pointers are never used for memory allocation (except deep inside some low-level libraries).

<!------------------------------------------------------------------------------------>

### Special Base Classes

#### TRefCounted base

`TRefCounted` is the base class for every object managed by `TIntrusivePtr`. Inherit from it when an object has shared ownership — i.e. when it will be held by more than one pointer at a time, or when the lifetime must extend beyond the creating scope.

```cpp
class TTabletManager
    : public TRefCounted
{
    ...
};

DEFINE_REFCOUNTED_TYPE(TTabletManager)
```

Do not inherit from `TRefCounted` for stack-allocated objects or objects with a single clear owner — prefer `std::unique_ptr` or a plain value in those cases. It is also possible for a final class (e.g. one defined with the `final` keyword) not to inherit from `TRefCounted` and still be used with `TIntrusivePtr`.

```cpp
struct TFinalClass final
{
    ...
};

DEFINE_REFCOUNTED_TYPE(TFinalClass)
```

Use `virtual TRefCounted` on interfaces so that a class implementing multiple interfaces (e.g. `class TFoo : public IBar, public IBaz`) has a single shared ref-count rather than two. Use plain (non-virtual) `TRefCounted` only when a class directly inherits from it and is not part of a diamond hierarchy.

Because `TRefCounted` deletes the copy and move constructors, any class that inherits from it is automatically non-copyable and non-movable. There is no need to additionally inherit from `TNonCopyable`.

#### TNonCopyable base

`TNonCopyable` disables copying (copy constructor and copy assignment are deleted) while leaving move semantics intact. Use it as a `private` base for objects that have an identity or manage a resource that must not be duplicated: singletons, managers, non-trivial RAII handles, objects that hold raw OS resources.

```cpp
class TDiskWriteQueue
    : private TNonCopyable
{
    ...
};
```

It is acceptable to delete the copy constructor explicitly instead of inheriting from `TNonCopyable`.

Do not use `TNonCopyable` together with `TRefCounted` — the latter already implies non-copyability.

#### TMoveOnly base

`TMoveOnly` explicitly enables move while deleting copy. Use it as a `private` base for RAII guards, scope sentinels, and ownership-transfer wrappers — objects that must be transferable but must not be duplicated.

```cpp
class TFiberMinLogLevelGuard
    : private TMoveOnly
{
public:
    explicit TFiberMinLogLevelGuard(ELogLevel minLogLevel);
    ~TFiberMinLogLevelGuard();

private:
    const ELogLevel OldMinLogLevel_;
};
```

Similar to `TNonCopyable`, it is acceptable to delete the move constructor explicitly.

<!------------------------------------------------------------------------------------>

### Interfaces vs Pimpl

We discourage heavy class declarations in header files, preferring to hide the implementation details by either using (most common) interfaces or [pimpl](https://en.cppreference.com/w/cpp/language/pimpl) idiom.

In the case of an interface with a single implementation, the factory method to construct the instance of the interface (called `Create...`) is usually placed in the same header file as the interface itself, and returns either an intrusive pointer, unique pointer or shared pointer to the class. The implementation is put into the corresponding .cpp file.

```cpp

// tree_traverser.h

struct ITreeTraverser
    : public virtual TRefCounted
{
    virtual void Traverse(const TTree& tree) = 0;
    virtual void Reset() = 0;
};

ITreeTraverserPtr CreateTreeTraverser(TTreeTraverserOptions options);


// tree_traverser.cpp

class TTreeTraverser
    : public ITreeTraverser
{
public:
    TTreeTraverser(TTreeTraverserOptions options)
        : Options_(std::move(options))
    { ... }

    void Traverse(const TTree& tree) override
    { ... }

    void Reset() override
    { ... }

private:
    const TTreeTraverserOptions Options_;
    // ...arbitrarily complex private fields and methods.
};

ITreeTraverserPtr CreateTreeTraverser(TTreeTraverserOptions options)
{
    return New<TTreeTraverser>(std::move(options));
}
```

Pimpl is currently considered a legacy idiom, but may be used sometimes. A notable case is when CoW semantics are involved, or the pointer implementation may be changed for other reasons.

<!------------------------------------------------------------------------------------>

### Passing Arguments

When passing arguments we pick the most performant way whenever possible. In general, the following rules apply:
- We pass objects of basic types and small classes (such as `TGuid`) by value.
- We pass shared pointers (e.g. `TIntrusivePtr` derivatives) by reference to avoid doing atomic increment for reference counter.
- We use const references when we are sure that there is no context switch in the callee (e.g. `WaitFor`).
- In other cases we pass objects by shared/unique pointers.

Below are some special cases.

#### Pass large objects (vectors, etc) by value in a class constructor

Take an argument by value when you intend to `std::move` it into a class field. This lets the caller choose to copy (pass an lvalue) or move (pass an rvalue) without requiring separate overloads.

```cpp
class TClient
{
public:
    TClient(IConnectionPtr connection, INodeMemoryTrackerPtr memoryTracker)
        : Connection_(std::move(connection))
        , MemoryTracker_(std::move(memoryTracker))
    { }

private:
    const IConnectionPtr Connection_;
    const INodeMemoryTrackerPtr MemoryTracker_;
};

// Caller decides whether to copy or move.
auto client = New<TClient>(connection, memoryTracker);            // copies both
auto client = New<TClient>(std::move(connection), memoryTracker); // moves connection, copies tracker
```

Taking `const IConnectionPtr&` instead would force an extra copy inside the constructor whenever the caller wants to transfer ownership.

#### Pass mutable object by pointer

When a function writes to a caller-owned variable, pass it as a raw pointer. The `&` at the call site makes the mutation explicit and visible.

```cpp
// Declaration.
bool TryInvalidateAndRetry(const IConnectionPtr& connection, const TError& error, int* retryCount);

// Call site — mutation of retryCount is visible.
if (TryInvalidateAndRetry(connection, error, &retryCount)) {
    ...
}
```

Do not use non-const references for output parameters; they hide the mutation at the call site.

If possible, use return values instead of modifying mutable arguments. It is preferable to use tuples and specific structs to pass complex values instead of mutable arguments.

#### Guards

`TGuard` and its variants (`TReaderGuard`, `TWriterGuard`) have three distinct passing conventions depending on the intended semantics.

*By const reference — for lock annotation.* When a function requires the caller to already hold a lock, declare the guard as an unnamed `const TGuard<NThreading::TSpinLock>&` parameter. The argument is never used inside the function body; it exists solely to document and statically enforce the locking requirement.

```cpp
IClientResponseHandlerPtr Finalize(const TGuard<NThreading::TSpinLock>& /*guard*/)
{
    // Code in this function assumes lock is held by caller.
}
```

*By rvalue reference — for passing ownership.* When the function takes ownership of the guard (e.g. to ensure the lock is released when the function returns, or to release it at a controlled point inside), pass by rvalue reference so the guard is moved in.

```cpp
void Process(TGuard<NThreading::TSpinLock>&& guard)
{
    // Code in this function takes ownership for lock.
}
```

*By pointer — when the callee may release it early.* When the callee must conditionally release the lock mid-execution while the caller continues after the call, pass a pointer to the guard.

```cpp
TFuture<void> DoAbort(TGuard<NThreading::TSpinLock>* guard)
{
    ...
    guard->Release(); // Releases the lock before kicking off async work.
    ...
}
```

<!------------------------------------------------------------------------------------>

### The `explicit` Keyword

Mark constructors with a single non-default argument `explicit` when implicit conversion is undesirable. Omit `explicit` when implicit conversion is intentional and documented.

```cpp
// OK.
class TFooBar
{
public:
    explicit TFooBar(int bar);
};

class TFooBaz
{
public:
    explicit TFooBaz(int bar, int baz = 0);
};

// Not OK — implicit construction from int is possible.
class TFooBar
{
public:
    TFooBar(int bar);
};

class TFooBaz
{
public:
    TFooBaz(int bar, int baz = 0);
};
```

Do not use `explicit` on constructors that take zero non-default arguments, two or more non-default arguments, or on copy/move constructors.

<!------------------------------------------------------------------------------------>

### `std::optional`

We use `std::optional<T>` for types that may be absent and do not have well-typed sentinel values in our codebase. In particular, we prefer `std::nullopt` over a zero integer or an empty string — this makes the code less error-prone. We use `std::optional<T>` for structs which are passed by value. We don't use `std::optional<T>` for smart pointers.

Prefer the contextual bool conversion and `*opt` over `opt.has_value()` and `opt.value()`. Use `has_value()` when an explicit presence check is clearer, and `value_or()` for defaults. For `std::optional<bool>`, both the bool conversion and `has_value()` test presence, not the contained boolean value.

```cpp
std::optional<int> optionalValue = DoSomething();
if (optionalValue) {
    ProcessResult(*optionalValue);
}
```

When constructing a conditional optional, use the ternary form with `std::optional<T>(value)` and `std::nullopt` so both branches are visually balanced.

```cpp
// OK — ternary with explicit std::optional<T> on the value branch.
auto poolTag = dynamicOptions->EnableTabletDynamicMemoryLimit
    ? std::optional<std::string>(slot->GetTabletCellBundleName())
    : std::nullopt;

// OK — same pattern for proto fields.
auto enginesInfo = rpcResponse.has_engines_info()
    ? std::optional(TYsonString(rpcResponse.engines_info()))
    : std::nullopt;

// OK - shorter variant for proto fields.
auto enginesInfo = YT_OPTIONAL_FROM_PROTO(rpcResponse, engines_info);
```

Do not wrap types that already carry a null state, in particular types like `TGuid`, `TTransactionId`, `TIntrusivePtr` and `TTimestamp`. Use that null value directly (e.g. `NullTransactionId`, `nullptr`, and `NullTimestamp`) instead of `std::optional`.

```cpp
// Not OK.
std::optional<TTransactionId> transactionId;
std::optional<TIntrusivePtr<TTablet>> tablet;

// OK.
TTransactionId transactionId = NullTransactionId;
if (transactionId != NullTransactionId) {
    ...
}

// OK.
TIntrusivePtr<TTablet> tablet = nullptr;
if (!tablet) {
    ...
}
```

<!------------------------------------------------------------------------------------>

### `std::variant`

When checking which alternative a variant holds, always use `std::holds_alternative<T>()`. Do not use `.index()`.

```cpp
// OK.
bool HasBoundaryKeys() const
{
    return std::holds_alternative<TBoundaryKeys>(Key_);
}

// Not OK.
bool IsInitialized() const
{
    return Key_.index() != 0;
}
```

<!------------------------------------------------------------------------------------>

### The `static` keyword

Use `static` only for class members (static methods and static data members) and fields. Never use `static` on free functions — put them in an anonymous namespace instead.

```cpp
// OK — file-scope helper in anonymous namespace.
namespace {

TError MakeOperationRevivalError()
{
    return TError("Operation of job is reviving");
}

} // namespace

// Not OK — static free function.
static TError MakeOperationRevivalError()
{
    return TError("Operation of job is reviving");
}
```

Static class methods and static data members are fine:

```cpp
class TTabletManager
{
public:
    static TClientOptions FromUser(std::string user);

private:
    static constexpr auto BatchSizeLimit = 1000;
    static void SetTableConfigErrors(TTablet* tablet, const std::vector<TError>& errors);
};
```

File-scope `static const` and `constexpr` variables outside of classes are allowed when they annotate a single translation unit's constant just after the includes and the global using section. It is preferable to use `constexpr` whenever possible. It is preferable to use an anonymous namespace for translation-unit scoped constants.

```cpp

using namespace NYT;

////////////////////////////////////////////////////////////////////////////////

// OK.
constexpr int MinSampledBlockCountPerChunk = 3;
static const auto ExpiredRegistryEvictionPeriod = TDuration::Minutes(1);
```

<!------------------------------------------------------------------------------------>

### Init Statements in if and for

Init-statements in `if` and `for` are preferred whenever the init-expression produces a variable whose scope should be limited to the conditional block.

```cpp
// OK.
if (auto it = ReplicaClients_.find(clusterName); it != ReplicaClients_.end()) {
    return it->second;
}

// Not OK.
auto it = ReplicaClients_.find(clusterName);
if (it != ReplicaClients_.end()) {
    return it->second;
}
// 'it' is not used after the block
```

The same pattern applies to `for` loops that need a one-time setup expression with a scope limited to the loop.

<!------------------------------------------------------------------------------------>

### Collection Helpers

Prefer the helpers from [collection_helpers.h](yt/core/misc/collection_helpers.h) over raw STL operations when the operation must succeed. The `*OrCrash` family asserts the postcondition and crashes immediately on violation, making failures loud and unambiguous instead of silently returning a value that the caller is expected to check.

| Helper | Replaces |
|--------|----------|
| `GetOrCrash(map, key)` | `map.find(key)` + `YT_VERIFY(it != map.end())` + `it->second` |
| `GetIteratorOrCrash(map, key)` | `map.find(key)` + `YT_VERIFY(it != map.end())` |
| `EraseOrCrash(map, key)` | `YT_VERIFY(map.erase(key) > 0)` |
| `InsertOrCrash(container, value)` | `YT_VERIFY(container.insert(value).second)` |
| `EmplaceOrCrash(container, args...)` | `YT_VERIFY(container.emplace(args...).second)` |

```cpp
// Not OK — raw erase silently succeeds even if the key is absent.
TabletMap_.erase(tabletId);

// OK — crashes if the key is absent, which is the expected invariant.
EraseOrCrash(TabletMap_, tabletId);

// Not OK.
auto it = TabletMap_.find(tabletId);
YT_VERIFY(it != TabletMap_.end());
auto* tablet = it->second.Get();

// OK.
auto* tablet = GetOrCrash(TabletMap_, tabletId).Get();
```

Other helpers worth knowing:

- `GetOrDefault(map, key, defaultValue)` — returns a copy of the mapped value or `defaultValue` when absent; avoids manual find-or-default patterns.
- `GetOrDefaultReference(map, key, defaultValue)` — same but returns a `const` reference.
- `GetOrInsert(map, key, ctor)` — inserts a default-constructed value on miss and returns a reference; avoids find-then-emplace patterns.
- `Contains(range, value)` — readable shorthand for `range.find(value) != range.end()`.
- `GetKeys(map)` / `GetValues(map)` — extract keys or values into a `std::vector` without manual iteration.
- `SortHashMapByKeys(map)` — returns a sorted `std::vector<std::pair<K, V>>` for deterministic iteration.

<!------------------------------------------------------------------------------------>

### Logging

Log messages follow quite strict rules. This results in extreme convenience for debugging and searching for relevant log messages once you get used to it.

Use `YT_TLOG_*` macros for logging. Keep the message fixed and put variable values in tags using `.With("Key", value)`. Place each `.With(...)` call on a separate line, indented by 4 spaces. Tag keys use PascalCase. This keeps messages searchable and tags machine-readable. The older `YT_LOG_*` macros are legacy syntax.

Log messages are usually in past simple, present continuous or do not contain a verb at all. They do not end with a period. Separate multiple clauses with a comma or semicolon.

```cpp
YT_TLOG_DEBUG("Starting fetching columnar statistics");
YT_TLOG_DEBUG("Finished fetching columnar statistics");
YT_TLOG_FATAL("Unknown write command")
    .With("Command", command);
YT_TLOG_INFO("Replaying encountered skip record, multiplexed suffix is ignored");
```

Attach a caught exception or a received error with `.With(ex)`, which uses the predefined `Error` tag. These single-argument calls must come after all named tags. Preserve the error itself rather than formatting `ex.what()` into the message.

```cpp
try {
    EndpointHostName_ = FqdnHostName();
} catch (const std::exception& ex) {
    YT_TLOG_ERROR("Failed to resolve local host name")
        .With(ex);

    EndpointHostName_ = "localhost";
}

if (!commandResultOrError.IsOK()) {
    YT_TLOG_ERROR("Failed to apply change replica mode command")
        .With("Command", command)
        .With(commandResultOrError);

    failed = true;
}
```

In legacy `YT_LOG_*` calls, variable values go in parentheses after the message as comma-separated `Key: Value` pairs. An exception or error precedes the format string.

```cpp
// OK.
YT_TLOG_DEBUG("Tables fetched")
    .With("TableCount", tableCount)
    .With("WallTime", timer.GetElapsedTime());

// Legacy syntax.
YT_LOG_DEBUG("Tables fetched (TableCount: %v)",
    tableCount);
YT_LOG_DEBUG("Fetched current offset (Offset: %v, WallTime: %v)",
    currentOffset,
    timer.GetElapsedTime());

// Not OK.
YT_TLOG_DEBUG("Tables fetched").With("TableCount", tableCount);
YT_TLOG_DEBUG("Tables fetched")
    .With("TableCount", tableCount).With("WallTime", timer.GetElapsedTime());
YT_LOG_DEBUG("Fetched %v tables",
   tableCount);
YT_LOG_DEBUG("Tables fetched (tableCount: %v)",
   tableCount);
```

There are several severity categories for logging which correspond to different actions. The most common is `YT_TLOG_DEBUG`, but other categories should be used whenever appropriate.

| Severity | Description |
|:-------: | :-----------|
|`YT_TLOG_DEBUG` | Most common category for saving debugging information. Used for debugging when code behaves in an unexpected way.|
|`YT_TLOG_INFO` | The same as above, but of limited size. Only very general logs, for example system initialization or state transition, should be placed here.|
|`YT_TLOG_TRACE` | Used for heavy debugging. Typically logs of this type are disabled, but may be turned on if necessary.|
|`YT_TLOG_WARNING` | A recoverable problem that deserves attention but does not prevent the operation from continuing.|
|`YT_TLOG_ERROR` | Non-critical errors that are usually self-healed. Everything works according to design, but user experience is disrupted and sometimes requires investigation by administrator.|
|`YT_TLOG_ALERT` | An encountered error doesn't damage overall system integrity but is unexpected and calls for investigation.|
|`YT_TLOG_FATAL` | An unexpected failure occurred and the system can't continue. Print everything useful for debugging and crash.|

For conditional logging, use the `_IF` or `_UNLESS` variant. In the debug examples below, tag expressions are evaluated only when the event is enabled and the condition allows logging.

```cpp
// OK.
YT_TLOG_DEBUG_IF(!fetched, "Chunk fetch failed")
    .With("ChunkId", chunkId);

YT_TLOG_DEBUG_UNLESS(fetched, "Chunk fetch failed")
    .With("ChunkId", chunkId);

// Not OK.
if (!fetched) {
    YT_TLOG_DEBUG("Chunk fetch failed")
        .With("ChunkId", chunkId);
}
```

### Errors

All error and exception texts must be written as if they were addressed to an end user. They must also be written in grammatically correct English; typically they also do not contain dots, using semicolons instead.

#### Message arguments and attributes

Include values needed to understand an error directly in its message. Both `TError` and `THROW_ERROR_EXCEPTION` accept a format string followed by its arguments; these arguments are substituted into the message. In general, we assume that the end user will read the error text and will not read the attributes of the error.

Add additional details with `.With("attribute_name", value)`, using snake_case keys and one call per line. This applies to both `TError` and `THROW_ERROR_EXCEPTION`. Attributes can help diagnose the error, for example by including the faulty key range, GUIDs, or surrounding text.

```cpp
THROW_ERROR_EXCEPTION(EErrorCode::MaxDataWeightPerJobExceeded, "Maximum allowed data weight per sorted job exceeds the limit: %v > %v",
    job->GetDataWeight(),
    JobSizeConstraints_->GetMaxDataWeightPerJob())
    .With("lower_key", job->LowerPrimaryKey())
    .With("upper_key", job->UpperPrimaryKey());

RequestAttachmentsStream_->Abort(TError("Client request control is finalized")
    .With("request_id", GetRequestId()));
```

```cpp
// Legacy.
THROW_ERROR_EXCEPTION("Failed to update table")
    << TErrorAttribute("request_id", requestId);
```

#### Propagating errors

Preserve an underlying failure with `.With(error)` instead of flattening it into text. For a caught exception, use `.With(TError(ex))`. Unlike logging, this attaches an inner error, not a tag named `Error`.

```cpp
try {
    UpdateTable(tablePath);
} catch (const std::exception& ex) {
    THROW_ERROR_EXCEPTION("Failed to update table")
        .With(TError(ex));
}
```

When propagating a `TError` or `TErrorOr<T>`, use `THROW_ERROR_EXCEPTION_IF_FAILED(error)`; pass a message as an additional argument when the caller needs more context. This macro wraps a failed error and leaves a successful result alone. It does not return a fluent error, so do not append `.With(...)` to it.

```cpp
// OK.
THROW_ERROR_EXCEPTION_IF_FAILED(error, "Failed to update table");

// Not OK.
if (!error.IsOK()) {
    THROW_ERROR_EXCEPTION("Failed to update table")
        .With(error);
}
```

#### Format specifiers

Error messages use the same format mini-language as `Format()` (see [format.h](/library/cpp/yt/string/format.h)). The choice of specifier determines how a value is quoted:

| Specifier | Output | When to use |
|:---------:|--------|-------------|
| `%v` | Default | IDs, numbers, timestamps, paths |
| `%Qv` | Double-quoted (`"value"`) | Names - clusters, users, etc. |
| `%Qlv` | Lowercase + double-quoted (`"some_enum"`) | Enum values - types, modes, states, etc. |

```cpp
// Path → %v; enum → %Qlv.
THROW_ERROR_EXCEPTION("Cannot backup table %v of type %Qlv",
    tablePath,
    tableType);

// Cluster name → %Qv.
THROW_ERROR_EXCEPTION("Backups are disabled at cluster %Qv",
    clusterName);
```

#### Expected/actual pattern

When a type mismatch or constraint violation is reported, use the fixed phrasing `expected X, actual Y` (or `got Y`):

```cpp
THROW_ERROR_EXCEPTION("Invalid type of %v: expected %Qlv, actual %Qlv",
    path,
    expectedType,
    actualType);

THROW_ERROR_EXCEPTION("Destination table %Qv has invalid backup state: expected %Qlv, got %Qlv",
    tablePath,
    EBackupState::BackupCompleted,
    actualState);

THROW_ERROR_EXCEPTION("Invalid number of subresponses: expected %v, actual %v",
    expectedCount,
    actualCount);
```

<!--================================================================================-->

## Persistence

Some server components (master, tablet node, chaos node, etc.) use the Hydra consensus
framework, which encapsulates data manipulation into the concept of specific *mutations*
(logged operations). Hydra periodically takes a *snapshot* of all in-memory state and
replays logged mutations on top of it after a restart. Code that participates in this
machinery is called *persistent code* and requires special discipline: any structural
change must be paired with a snapshot version bump and backward-compatibility handling.

<!------------------------------------------------------------------------------------>

### Snapshots, Mutations, and Reign

The in-memory data structures that survive across restarts are called *persistent state*. They
are saved to a snapshot via `Save` / `SaveKeys` / `SaveValues` methods and restored via the
matching `Load` / `LoadKeys` / `LoadValues` methods. Methods that mutate persistent state are
called *mutation handlers* and are named with the `Hydra*` prefix.

The snapshot format version is called the *Reign*. Each component defines its own enum in
`serialize.h`:

- master: `yt/server/master/cell_master/serialize.h`
- tablet node: `yt/server/node/tablet_node/serialize.h`
- chaos node: `yt/server/node/chaos_node/serialize.h`

The enum follows a strict convention — one entry per logical change, with the author's login
appended as a comment. A special entry denotes a new major version:

```cpp
DEFINE_ENUM(ETabletReign,
    // 25.1 starts here.
    ((Start_25_1)                      (101100)) // ponasenko-rs
    ((SmoothMovementForwardWrites)     (101102)) // ifsmirnov
    // 25.2 starts here.
    ((Start_25_2)                      (101200)) // ponasenko-rs
    ((AddTabletMountTime)              (101204)) // alexelexa
);
```

Every structural change to persistent state or mutation handler logic must add a new reign
entry. Use the existing numeric pattern (100-unit increments between releases, 1-unit increments
per change). Entries must remain monotonically increasing inside a single major version.
Typically, new entries are added to the newest major version. In case the persistent state should be
modified in previous versions, new entries are added to each major version up to the latest.

<!------------------------------------------------------------------------------------>

### Save and Load

Each persistent object implements `Save(TSaveContext&)` and `Load(TLoadContext&)`.
Use `NYT::Save` and `NYT::Load` free functions to serialize individual fields.

```cpp
void THunkTablet::Save(TSaveContext& context) const
{
    using NYT::Save;

    Save(context, State_);
    Save(context, HunkStoragePath_);

    TSizeSerializer::Save(context, IdToStore_.size());
    for (auto it : GetSortedIterators(IdToStore_)) {  // stable order — required
        const auto& [storeId, store] = *it;
        Save(context, storeId);
        store->Save(context);
    }
}

void THunkTablet::Load(TLoadContext& context)
{
    using NYT::Load;

    Load(context, State_);
    // COMPAT(akozhikhov)
    if (context.GetVersion() >= ETabletReign::HunkTabletSensors) {
        Load(context, HunkStoragePath_);
    }

    auto storeCount = TSizeSerializer::Load(context);
    for (int index = 0; index < static_cast<int>(storeCount); ++index) {
        auto storeId = Load<TStoreId>(context);
        auto store = New<THunkStore>(storeId, this);
        store->Load(context);
        EmplaceOrCrash(IdToStore_, storeId, std::move(store));
    }
}
```

The `Load` function always reads fields in the same order as `Save` wrote them. Adding a new
field: append it to `Save` and guard the corresponding `Load` behind a reign version check.
Removing a field: remove it from `Save` and replace the `Load` with a version-guarded temporary
read that discards the data.

For components that split serialization into keys and values (used by entity maps), implement
`SaveKeys` / `LoadKeys` and `SaveValues` / `LoadValues` as separate pairs.

<!------------------------------------------------------------------------------------>

### Persist

Smaller structs that have no per-field versioning needs may use a single `Persist` function
instead of separate `Save` and `Load`. The `TStreamPersistenceContext` carries a mode flag:

```cpp
void TLockEntry::Persist(const TStreamPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, Transactions_);

    if (context.IsLoad()) {
        // Rebuild transient state that is derived from persistent state.
        SharedQueue_.clear();
        for (auto [transactionId, timestamp] : Transactions_) {
            SharedQueue_.emplace(timestamp, NewPromise<void>());
        }
    }
}
```

Use `Persist` for simple value types and small structs. Use separate `Save`/`Load` when the
struct needs fine-grained version checks on individual fields.

<!------------------------------------------------------------------------------------>

### Compatibility Code and COMPAT Comments

Any time a `Load` path is conditional on a reign version, mark it with a `// COMPAT(login)`
comment immediately above the check. The login is the author responsible for the compatibility
shim. A longer explanation may follow on the same line.

```cpp
// COMPAT(ifsmirnov)
if (context.GetVersion() >= ETabletReign::SmoothMovementForwardWrites) {
    PersistentExternalizedTransactionMap_.LoadKeys(context);
}
```

The same convention applies inside `Hydra*` mutation handlers, where the reign is read from the
mutation context rather than the snapshot context.

```cpp
void HydraMountTablet(TReqMountTablet* request)
{
    // COMPAT(alexelexa)
    TInstant mountTime;
    if (mutationContext->Request().Reign >= static_cast<int>(ETabletReign::AddTabletMountTime)) {
        // New code should only be executed for mutations created after the new version is deployed.
        ...
    }
}
```

When a feature was backported to two release branches before being promoted to a single canonical
reign, the check may need to cover both the backport and the mainline reigns:

```cpp
// COMPAT(savrus)
int reign = GetCurrentMutationContext()->Request().Reign;
if (reign >= static_cast<int>(ETabletReign::CheckChaosTransactionsInPrepare) ||
    (reign < static_cast<int>(ETabletReign::Start_25_2) &&
     reign >= static_cast<int>(ETabletReign::CheckChaosTransactionsInPrepare_25_1)))
{
    // New behavior for both the 25.1 backport and the 25.2+ mainline.
}
```

Compat shims are permanent until the minimum supported snapshot version passes the guarded
reign. Add a note `// COMPAT(login): remove when everything is X.Y.` when you know the cleanup
target.

<!------------------------------------------------------------------------------------>

### Hive Mutations and Stable Iteration

*Hive* is the cross-cell messaging layer. `Hive*` mutation handlers process messages that were
sent from another cell via `hiveManager->PostMessage(mailbox, message)`. Because Hive delivers
messages in order and mutations may be re-executed during log replay, a `Hive*` handler must
produce exactly the same sequence of outgoing messages on every execution.

This means that any iteration over an unordered collection (hash map, hash set) inside a `Hive*`
handler — or inside any mutation handler that posts Hive messages — must use a stable accessor.
Use `GetSortedIterators` for `THashMap` and `THashSet`, and `GetIteratorsSortedByKey` for maps
keyed by `TObjectPtr`:

```cpp
// Not OK — hash map iteration order is non-deterministic.
for (const auto& [replicaId, replica] : tablet->Replicas()) {
    hiveManager->PostMessage(GetMailbox(replicaId), BuildUpdateMessage(replica));
}

// OK — sorted by key, same order on every replay.
for (auto it : GetIteratorsSortedByKey(tablet->Replicas())) {
    hiveManager->PostMessage(GetMailbox(it->first), BuildUpdateMessage(it->second));
}
```

The same rule applies to `Save` functions — snapshot bytes must be identical across runs of the
same logical state. Always use `GetSortedIterators` when serializing hash collections:

```cpp
// OK — deterministic snapshot output.
TSizeSerializer::Save(context, StoreIdMap_.size());
for (auto it : GetSortedIterators(StoreIdMap_)) {
    const auto& [storeId, store] = *it;
    Save(context, storeId);
    store->Save(context);
}
```

`GetSortedIterators` and `GetIteratorsSortedByKey` are defined in
`yt/core/misc/collection_helpers.h`.

<!--================================================================================-->

## Concurrency

<!------------------------------------------------------------------------------------>

### Thread-Local Storage

Avoid thread-local state in fiber code when possible. A fiber can resume on another thread after a context switch, while the compiler may cache the address of a `thread_local` variable across that switch. Accessing the cached address then uses the previous thread's storage.

When thread-local state is necessary, use `YT_DEFINE_THREAD_LOCAL` from `<library/cpp/yt/misc/tls.h>` and access it through the generated accessor. Use `YT_DECLARE_THREAD_LOCAL` for a declaration in a header. A hand-written accessor must use `YT_PREVENT_TLS_CACHING`; a bare `thread_local` variable is not a safe replacement for these helpers.

```cpp
YT_DEFINE_THREAD_LOCAL(int, RequestCount, 0);

void OnRequest()
{
    ++RequestCount();
}
```

Do not retain a pointer or reference returned by a thread-local accessor across a possible context switch. Call the accessor again after the switch. These helpers prevent compiler caching; they do not turn thread-local state into fiber-local state.

<!------------------------------------------------------------------------------------>

### Thread Affinity

Use `DECLARE_THREAD_AFFINITY_SLOT` in the `private` section to name a thread that a class is bound to, then assert it at the top of every method that must run on that thread.

```cpp
class THunkLockManager
    : public IHunkLockManager
{
public:
    void RegisterHunkStore(...) override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        // ...
    }

    void ReconfigureTablet(...) override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
        // ...
    }

private:
    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);
};
```

Use `YT_ASSERT_THREAD_AFFINITY_ANY()` for methods that are explicitly designed to be called from any thread. This is documentation — it says "this method is thread-safe by design".

```cpp
TAsyncSemaphoreGuard TryGetConfigLockGuard() override
{
    YT_ASSERT_THREAD_AFFINITY_ANY();
    return TAsyncSemaphoreGuard::TryAcquire(ConfigurationLock_);
}

void RefreshEra(TReplicationEra newEra) override
{
    YT_ASSERT_THREAD_AFFINITY_ANY();
    // ...
}
```

Place the assertion on the first line of a function body, before any logic.

<!------------------------------------------------------------------------------------>

### Spinlocks and Reader-Writer Locks

`TSpinLock` provides simple mutual exclusion. Use it for short critical sections protecting mutable state that has no read-heavy workload. `TReaderWriterSpinLock` allows concurrent readers and an exclusive writer. Use it when reads dominate.

Declare spinlocks with `YT_DECLARE_SPIN_LOCK` in the private section of the class.

```cpp
private:
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, ChannelsLock_);
```

Acquire locks with `Guard()`, `ReaderGuard()`, or `WriterGuard()` and hold the result as a local `auto guard` — the lock is released when `guard` goes out of scope:

```cpp
// Exclusive (TSpinLock).
{
    auto guard = Guard(SpinLock_);
    DoSomethingExclusive();
}

// Read-lock (TReaderWriterSpinLock).
{
    auto guard = ReaderGuard(ChannelsLock_);
    return Channels_.contains(cellTag) ? GetOrCrash(Channels_, cellTag) : nullptr;
}

// Write-lock (TReaderWriterSpinLock).
{
    auto guard = WriterGuard(ChannelsLock_);
    Channels_[kind][cellTag] = std::move(channel);
}
```

Keep the locked scope as short as possible. Never hold a spinlock across `WaitFor`, `Sleep`, or any other fiber-suspension point. Spinlocks are not re-entrant.

Use `YT_ASSERT_SPINLOCK_AFFINITY(Lock_)` at the top of private helpers that must only be called under a lock.

```cpp
void EnsureBundleDataCreated(const ITabletSlotPtr& slot)
{
    YT_ASSERT_SPINLOCK_AFFINITY(SpinLock_);
    // ...
}
```

<!------------------------------------------------------------------------------------>

### TFuture and TPromise

`TFuture<T>` is a handle to an asynchronous result. `TPromise<T>` is the write end — it is created with `NewPromise<T>()` and fulfilled with `.Set(value)` or `.TrySet(value)`. Use `.TrySet()` when multiple code paths might complete the promise.

```cpp
const TPromise<void> FlushedPromise = NewPromise<void>();

// Producer side.
FlushedPromise.TrySet(TError());  // error case
FlushedPromise.TrySet(TError());  // idempotent — safe to call again

// Consumer side.
TFuture<void> future = FlushedPromise.ToFuture();
WaitFor(future)
    .ThrowOnError();
```

Obtain a `TFuture` from a running task by holding the return value of `.Run()` or `.Apply()`. Cancel with `.Cancel(TError(...))`, reset with `.Reset()`:

```cpp
TFuture<void> FiberFuture_;

FiberFuture_ = BIND(&TMyClass::Run, MakeWeak(this))
    .AsyncVia(Invoker_)
    .Run();

// On shutdown.
FiberFuture_.Cancel(TError("Component disabled"));
FiberFuture_.Reset();
```

To block the current fiber until a future is ready, use `WaitFor()`.
The calling code must be running in an interruptible context (typically code runs in interruptible fibers in the thread pool invoker, but there are some exceptions). Use `WaitForFast()` when the future is likely already set and you want to avoid a fiber switch.
When the future is set, use `.ThrowOnError()` to validate success for `void` futures or `.ValueOrThrow()` to validate and get the value.

```cpp
// Blocking wait — suspends the fiber.
auto value = WaitFor(asyncFuture)
    .ValueOrThrow();

// Fast path — avoids context switch if already set.
WaitForFast(AllSucceeded(std::move(enqueuedFutures)))
    .ThrowOnError();
```

Chain async transformations with `.Apply(BIND(...))` instead of blocking.

```cpp
return asyncSchemalessWriter.AsUnique().Apply(BIND([] (IWriterPtr&& writer) {
    return CreateAdapterWriter(std::move(writer));
}));
```

<!------------------------------------------------------------------------------------>

### Future Combinators

Use `AllSucceeded` when all sub-operations must succeed — the combined future fails as soon as any one of them fails.

```cpp
WaitFor(AllSucceeded(std::move(lookupRowsFutures)))
    .ThrowOnError();
```

Use `AllSet` when you need to process each result individually, even if some failed:

```cpp
auto rspsOrErrors = WaitFor(AllSet(asyncRsps))
    .ValueOrThrow();

for (const auto& rspOrError : rspsOrErrors) {
    if (!rspOrError.IsOK()) {
        YT_TLOG_WARNING("Sub-request failed")
            .With(rspOrError);

        continue;
    }

    Process(rspOrError.Value());
}
```

Use `AnySucceeded` when any one success is sufficient (e.g. waiting for the first available replica):

```cpp
WaitFor(AnySucceeded(std::vector{future1, future2}))
    .ThrowOnError();
```

Summary:

| Combinator | Succeeds when | Fails when |
|------------|---------------|------------|
| `AllSucceeded` | all inputs succeed | any input fails |
| `AllSet` | always (wraps each result in `TErrorOr`) | never |
| `AnySucceeded` | any input succeeds | all inputs fail |
| `AnySet` | any input is set | never |

Pass futures by `std::move` into combinators to avoid extra reference-count overhead.

<!------------------------------------------------------------------------------------>

### BIND and Invokers

Use `BIND(&TClass::Method, MakeWeak(this))` to schedule a method on an invoker. `MakeWeak` prevents the object from being kept alive by the callback if it is destroyed before the callback fires (in this case the callback is not executed).

```cpp
FiberFuture_ = BIND(&TChaosAgent::FiberMain, MakeWeak(this), period)
    .AsyncVia(epochAutomatonInvoker)
    .Run();
```

Use `MakeStrong(this)` when you explicitly want the object to stay alive until the callback completes (e.g. for one-shot fire-and-forget tasks).

```cpp
slotManager->SubscribeScanSlot(BIND(&TBackingStoreCleaner::OnScanSlot, MakeStrong(this)));
```

Use `BIND_NO_PROPAGATE` for inner callbacks passed as arguments to avoid unintended propagation of the caller's tracing or cancellation context.

```cpp
FiberFuture_ = BIND(
    &TChaosAgent::FiberMain,
    MakeWeak(this),
    BIND_NO_PROPAGATE(&TChaosAgent::FiberIteration, MakeWeak(this)),
    tickPeriod)
    .AsyncVia(epochAutomatonInvoker)
    .Run();
```

Dispatch work to a specific invoker by calling `Invoke()` or chaining `.Via(invoker)` when the caller does not need to wait for completion.

```cpp
// Dispatch to invoker and do not wait for completion (preferred).
invoker->Invoke(BIND(&TClass::TMethod, MakeWeak(this)));

// Dispatch to invoker and do not wait for completion (for code not on a hot path).
BIND(&TClass::TMethod, MakeWeak(this))
    .Via(invoker)
    .Run();
```

Use `.AsyncVia(invoker)` followed by `.Run()` to dispatch work to an invoker and wait for completion.

```cpp
// Dispatch to invoker and wait for completion.
WaitFor(BIND(&TClass::TMethod, MakeWeak(this))
    .AsyncVia(invoker)
    .Run())
    .ThrowOnError();
```

<!--================================================================================-->

## Cosmetics

Rules in this section define the fancy stuff not affecting the code behavior. They are quite strictly followed throughout our code base, making its visual appearance notably consistent even in completely unrelated parts of the code.

<!------------------------------------------------------------------------------------>

### Naming Conventions

We discourage the use of abbreviations in names and one-letter/two-letter names. Somewhat commonly recognized exceptions to the previous rule are `it` for iterators and `Impl` as a suffix for implementations.

```cpp

// Not OK.
int oc = GetOperationCount();
std::vector<TTablet*> vec;
for (int i = 0; i < tableCount; ++i) {
    ...
}

// OK.
int operationCount = GetOperationCount();
std::vector<TTablet*> tablets;
for (int tableIndex = 0; tableIndex < tableCount; ++tableIndex) {
    ...
}
```

Well-known acronyms are OK (e.g. TCP, IP, IO, MPSC, CH for ClickHouse). YT is not an acronym, but works like a two-letter acronym.

 Two-letter acronyms are treated as acronyms (e.g. both letters are capitalized or not capitalized simultaneously), while acronyms with three or more letters are treated as words (e.g. only the first letter is capitalized).

```cpp
namespace NYT;
class TIOTracker;
auto ioEngine = CreateIOEngine();
class TIP6Address;

class TTcpConnection;
class TMpscQueueImpl;
```

#### Types naming for common concepts

Collection type names carry a suffix reflecting the container kind: `*Map`, `*Set`, `*List`. Plural names are also acceptable for sequences.

```cpp
class TTagSet;
using TChunkList = std::vector<TChunkPtr>;
using TJobMap = THashMap<TJobId, TJobPtr>;
```

Types denoting function signatures are named with the suffix `Signature`. Typed specializations of `std::function` or `TCallback` are usually suffixed with `Callback` or `Handler`, but other variants are possible.

```cpp
// OK.
using TFooSignature = int(const TBar&);
using TFooCallback = TCallback<TFooSignature>;
// Also OK.
using TFooHandler = std::function<TFooSignature>;
```

Template type parameters follow the single-letter and short-word convention: `T`, `U`, `TKey`, `TValue`.

#### Variable naming for common concepts

When a variable represents a specific well-known concept, qualify it with the domain name rather than using a bare or abbreviated identifier.

- Object identifiers must be qualified: `tabletId`, `chunkId`, not bare `id`.
- Loop counters and indices must be descriptive: `tableIndex`, `chunkIndex`, not `i`. For small loops, or when the counter corresponds to different object types, use `index`.
- `ref` as an abbreviation for reference is not allowed; use the full descriptive name.

Short names are accepted by convention: `ex` in catch clauses, `it` for iterators when the iterated collection is evident from context.

```cpp
// Not OK.
TObjectId id = ...;
const TFoo& ref = GetFoo();
for (int i = 0; i < tableCount; ++i) {
    ...
}

// OK.
TObjectId tableId = ...;
const TFoo& foo = GetFoo();
for (int tableIndex = 0; tableIndex < tableCount; ++tableIndex) {
     ...
}

// OK — accepted abbreviations.
template <class T, class TKey, class TValue>
catch (const std::exception& ex) {
    ...
}
for (auto it = tablets.begin(); it != tablets.end(); ++it) {
    ...
}
```

When the same concept appears on both sides of a binary operation (comparators, binary operators, merges), prefix variables with `lhs` and `rhs`.

```cpp
bool operator==(const TKey& lhsKey, const TKey& rhsKey);
TChunk Merge(const TChunk& lhsChunk, const TChunk& rhsChunk);
```

Variables of type `TErrorOr<T>` are suffixed with `OrError`. Variables of type `TFuture<T>` are prefixed with `future`.

```cpp
auto futureRowset = client->LookupRows(...);
auto rowsetOrError = WaitFor(futureRowset);
auto rowset = rowsetOrError
    .ValueOrThrow();
```

Collection variables should use plural names.

```cpp
std::vector<TTablet*> tablets;  // OK.
std::vector<TTablet*> tablet;   // Not OK.
```

For map variables, avoid the `FirstToSecondMap` pattern; name the map by what it represents.

```cpp
// Not OK.
THashMap<TTabletId, TTabletPtr> TabletIdToTabletMap_;
// OK.
THashMap<TTabletId, TTabletPtr> TabletMap_;
```

Constants representing expected or typical cardinalities are prefixed with `Typical`.

```cpp
constexpr int TypicalTabletCount = 16;
constexpr int TypicalInputChunkCount = 64;
```

Variables tracking the current item when iterating a sequence are prefixed with `current`.

```cpp
for (const auto& tablet : tablets) {
    auto currentOffset = tablet.GetStartingOffset();
    ...
}
```

Loggers are always named `Logger`, without a trailing underscore, regardless of scope. This is a deliberate exception to the private field naming convention: `YT_TLOG_*` and legacy `YT_LOG_*` macros resolve the logger by looking up the name `Logger` in the enclosing scope, so a class member must be spelled exactly `Logger`. In free functions that need logging, a local alias is created at the top of the function.

String literal constants (attribute names, RPC action names, etc.) are declared as `constexpr TStringBuf` or `static const char*`.

```cpp
// Namespace-scope logger.
static const TLogger Logger("TabletManager");

// String literal constant.
constexpr TStringBuf OriginalTabletStateAttribute = "original_tablet_state";

// Class member logger — no underscore, even in private:.
class TTabletManager {
    ...
private:
    const TLogger Logger;
};

// Free function — create a local alias so YT_TLOG_* macros work.
void DoSomething(const IConnectionPtr& connection)
{
    const auto& Logger = connection->GetLogger();

    YT_TLOG_DEBUG("Doing something");
}
```

#### Function naming patterns

Several naming prefixes communicate the behavioral contract of a function.

Functions which mimic [properties](#properties) (but do more than just property macros):

- `Get*` — returns a property. May return `nullptr` or a default value if the property is absent. It is a good practice to have an additional `const`-qualified implementation which returns a `const&`, not a copy of the property.
- `Set*` - sets a property.

Retrieval functions are named according to what they do when the result is absent:

- `Get*` — asserts that the object is present and returns it directly.
- `Find*` — returns `nullptr`, `std::nullopt` or type-dependent null value if the object is absent.
- `Get*OrThrow` — throws if the object is absent.

```cpp
TTablet* GetTablet(TTabletId tabletId);           // Asserts presence.
TTablet* FindTablet(TTabletId tabletId);          // Returns nullptr if absent.
TTabletPtr GetTabletOrThrow(TTabletId tabletId);  // Throws if absent.
```

Boolean predicates are prefixed with `Is`. Validation functions are prefixed with `Check` (if the function returns `bool`, `TError` or a custom `TSmthResult`) or `Validate` (when the function asserts or throws an exception).

```cpp
bool IsOK() const;
bool IsTabletMounted(TTabletId tabletId) const;
bool CheckPermission(const TUser& user, EPermission permission); // returns false if the condition is not passed
void ValidateNodeType(ENodeType nodeType);                       // throws an exception if the condition is not passed
```

Event handler methods are prefixed with `On`.

```cpp
void OnTimeout();
void OnReplicaCreated(TReplicaId replicaId);
```

Toggle operations are named as symmetric pairs starting with `Enable` and `Disable`.

```cpp
void EnableSync();
void DisableSync();
```

When a single logical object travels through multiple functions and is stored in multiple variables, use the same root name throughout. If your code works with a row set of type `TRowset`, variables should be named `rowset` or `...Rowset`, and functions designed to work with it should include `Rowset` in their names.

```cpp
// OK — consistent naming throughout.
TRowsetPtr FetchRowset(...);
void ProcessRowset(TRowsetPtr rowset);
void FlushRowset(TRowsetPtr rowset);

auto rowset = FetchRowset(...);
ProcessRowset(rowset);
FlushRowset(rowset);

// Not OK — same object, different names across the call chain.
auto data = FetchRowset(...);
ProcessTable(data);
FlushRows(data);
```

<!------------------------------------------------------------------------------------>

### Braces

Opening braces of classes and functions are placed on the next line.

```cpp
class TSomething
{
    ...
};

void DoSomething()
{
    ...
}
```

Opening braces of namespaces, lambda functions and control blocks are placed on the same line.

```cpp
namespace NYT::NChunkPools {
...
} // namespace NYT::NChunkPools

auto doSomething = [&] (auto arg) {
    ...
};

if (condition) {
    ...
} else {
    ...
}
```

The only exception to the latter rule is when the control block statement is too long to fit on a single line.

```cpp
for (int tableIndex = 0;
     tableIndex < tableCount && InputTables_[tableIndex].IsTrivial();
     ++tableIndex)
{
    ...
}

if (auto it = map.find(key);
    it != map.end() && it->second.SomeCondition())
{
    ...
}
```

Braces around single-statement control blocks are never omitted.

```cpp
// Not OK.
if (condition)
    DoSomething();

// OK.
if (condition) {
    DoSomething();
}
```

<!------------------------------------------------------------------------------------>

### Whitespace

Only a single space is used between tokens. No double spaces are allowed, except for alignment inside indentation or for tabular-looking code formatting: enums, macros and (very rarely) struct field initialization.

Pointer and reference declarators are attached to the type, not to the variable name.

```cpp
// OK.
const TFoo* foo;
const auto* bar;
TBar& ref;

// Not OK.
const TFoo *foo;
const auto *bar;
TBar &ref;
```

Default argument values are always written with spaces around `=`, matching the style of other assignments. This contrasts with Python conventions.

```cpp
// OK.
virtual TFuture<ITransactionPtr> StartNativeTransaction(
    ETransactionType type,
    const TTransactionStartOptions& options = {}) = 0;

// Not OK.
virtual TFuture<ITransactionPtr> StartNativeTransaction(
    ETransactionType type,
    const TTransactionStartOptions& options={}) = 0;
```

Commented argument names (magic constants annotated with the parameter name, as described in the [Functions](#functions-1) section) have no spaces inside the comment delimiters.

```cpp
// OK.
GetChaosTableInSyncReplicas(
    tableInfo,
    replicationCard,
    /*nameTable*/ nullptr,
    /*allKeys*/ true);

// Not OK.
GetChaosTableInSyncReplicas(
    tableInfo,
    replicationCard,
    /* nameTable */ nullptr,
    /* allKeys */ true);
```

Lambda capture lists are followed by a single space before the argument list `(` or the body `{`. Items inside `[...]` are separated by `, `.

```cpp
// OK.
auto lambda = [&] { ... };
auto lambda = [&] (int arg) { ... };
auto lambda = [&, this] { ... };

// Not OK.
auto lambda = [&]{ ... };
auto lambda = [&](int arg) { ... };
auto lambda = [& , this] { ... };
```

Template declarations have a space between the `template` keyword and `<`. Type instantiations have no spaces inside the angle brackets.

```cpp
// OK.
template <class T>
template <class TKey, class TValue>

TFuture<void> future;
THashMap<TKey, TValue> items;

// Not OK.
template<class T>
std::vector< int > items;
THashMap< TKey, TValue > items;
```

<!------------------------------------------------------------------------------------>

### Indents

We use 4 spaces for indents. We do not use tabs.

The body of the namespace is not indented.

The list of class bases is indented by 4 spaces; each entry starts on a new line either with a comma or a colon.

```cpp
class TSomething
    : public TBase
    , public TAnotherBase
{
    ...
};
```

The list of designators in a constructor initializer list is indented by 4 spaces; each entry starts on a new line either with a comma or a colon.

```cpp
// Inside class definition.
class TSomething
    : public TBase
{
public:
    TSomething(TSomeArg arg, TAnotherArg anotherArg)
        : Base(arg)
        , AnotherArg_(std::move(anotherArg))
    { ... }
};

// Or outside class definition.
TSomething::TSomething(TSomeArg arg, TAnotherArg anotherArg)
    : Base(arg)
    , AnotherArg_(std::move(anotherArg))
{ ... }
```

If the argument list of a function signature or call is too long to fit on a single line, we split it into multiple lines, each argument occupying a separate line. The whole argument list is indented by 4 spaces.

```cpp
// OK.
SomeFunction(
    veryLongArgument1,
    veryLongArgument2,
    veryLongArgument3);

// Not OK.
SomeFunction(veryLongArgument1,
             veryLongArgument2,
             veryLongArgument3);

// Also not OK.
SomeFunction(veryLongArgument1,
    veryLongArgument2,
    veryLongArgument3);
```

Tagged logging (`YT_TLOG_*`) takes a fixed message, followed by a chain of tags, one call per line. It has no format-argument list; see [Logging](#logging).

For `THROW_ERROR_EXCEPTION` and its `_IF`/`_UNLESS` variants, start the format string on the same line as the macro, after any error code or condition. Place each format argument on its own subsequent line, indented by 4 spaces, even when the entire call would fit on one line. Split a long format string into adjacent string literals when necessary, keeping the first literal on the macro's line. Attribute and inner-error `.With(...)` calls follow the closing parenthesis, each on a new line at the same indentation as the format arguments. The same format-string layout applies to legacy `YT_LOG_*` calls.

```cpp
// OK.
THROW_ERROR_EXCEPTION("Unknown medium %Qv",
    mediumName);
THROW_ERROR_EXCEPTION(NTabletClient::EErrorCode::BundleResourceLimitExceeded, "Tablet cell bundle %Qv violates tablet static memory limit",
    tabletCellBundle);
THROW_ERROR_EXCEPTION_IF(limitExceeded, "Tablet cell bundle %Qv violates tablet static memory limit",
    tabletCellBundle);

// Not OK.
THROW_ERROR_EXCEPTION(
    "Unknown medium %Qv",
    mediumName);
THROW_ERROR_EXCEPTION("Unknown medium %Qv", mediumName);
THROW_ERROR_EXCEPTION("Unknown medium %Qv for table %v",
    mediumName, tablePath);

```

For chained function calls, we split the chain into multiple lines, each call occupying a separate line. The whole chain is indented by 4 spaces.

```cpp
BIND(&TSomeClass::DoSomething, MakeStrong(this))
    .AsyncVia(SomeInvoker_)
    .Run();
```

There is no single strict rule for chained invocation splitting. Chained `.ThrowOnError()`, `.ValueOrThrow()` should always appear on the next line. Chained `.Via()`, `.AsyncVia()`, `.Run()` could appear on the same line, in particular if they chain a lambda. Chained `.WithTag()` or `.WithTags()` usually appear on the same line.

```cpp

// OK.
callback()
    .ThrowOnError();

// Not OK.
callback().ThrowOnError();

// OK.
auto value = callback()
    .ValueOrThrow();

// Not OK.
return callback().ValueOrThrow();

// OK.
callback()
    .Via(Invoker_)
    .Run();

// OK.
BIND([] {
    ...
}).AsyncVia(Invoker_).Run();

// OK.
auto logger = Logger.WithTag("Cluster: %v", clusterName);

// OK.
auto profiler = Profiler_.WithTag("bucket", name);

```

If an arithmetic or boolean expression is too long, we split it right after the operator and indent the whole chain by 4 spaces.

```cpp
// OK.
int nanosecondDuration = durationInDays * hourPerDay * minutesPerHour *
    secondsPerMinute * microsecondsPerSecond *
    nanosecondsPerMicrosecond;

// Not OK.
int nanosecondDuration = durationInDays * hourPerDay * minutesPerHour
    * secondsPerMinute * microsecondsPerSecond
    * nanosecondsPerMicrosecond;

// OK.
if ((someLooseCondition1 && conditionForLooseCondition1) ||
    (someLooseCondition2 && conditionForLooseCondition2))
{ ... }

// Not OK.
if ((someLooseCondition1 && conditionForLooseCondition1)
    || (someLooseCondition2 && conditionForLooseCondition2))
{ ... }
```

The ternary operator is different: in case it is too long to fit on a single line, `?` and `:` should be placed on new lines and indented by 4 spaces.
```cpp
// OK.
auto option = request.has_option() ? std::make_optional(request.option()) : std::nullopt;

// OK.
auto optionalLongOption = request.has_long_option()
    ? std::make_optional(request.long_option())
    : std::nullopt;

// Not OK.
auto optionalLongOption = request.has_long_option() ?
    request.long_option() : nullopt;
// Also not OK.
auto optionalLongOption = request.has_long_option() ?
    request.long_option() :
    nullopt;
```

If a lambda capture list is too long, it should be split. Each captured parameter appears on a new line and is indented by 4 spaces. The closing `]` should appear on a new line, without indentation, followed by the lambda argument list. The lambda argument list follows the same rules as functions. Since lambdas have both a capture list, an argument list and a function body, one should be careful with indentation.

```cpp

// OK.
auto lambda = [
    &,
    this,
    this_ = MakeStrong(this),
    captured = std::move(captured)
] {
    // lambda body.
};

// Not OK.
auto lambda = [
    &, this,
    this_ = MakeStrong(this), captured = std::move(captured)
] {
    // lambda body.
};

// OK.
auto lambda = [
    this,
    this_ = MakeStrong(this),
] (auto& argument1, auto& argument2) {
    // lambda body.
};

// OK.
auto lambda = [
    this,
    this_ = MakeStrong(this),
] (
    auto& argument1,
    auto& argument2
) {
    // lambda body.
};

// Not OK.
auto lambda =
    [
        this,
    ] {
        // lambda body.
    };

// Not OK.
auto lambda = [this,
    this_ = MakeStrong(this),
] {
    // lambda body.
};

// Not OK.
auto lambda = [
    this,
    this_ = MakeStrong(this),
] (auto& argument1, auto& argument2)
{
    // lambda body.
};

// Not OK.
auto lambda = [
    this,
    this_ = MakeStrong(this),
] (auto& argument1,
    auto& argument2)
{
    // lambda body.
};
```

<!------------------------------------------------------------------------------------>

### Empty Lines

We use empty lines to split code into comprehensible parts. In general, only a single line should be empty, but there are a few cases when two-line breaks are possible. We never place three or more empty lines in a row.

In the include section, empty lines split different include directories (cf. [Include Directives](#include-directives)). A header with the same name as the .cpp should also be separated by an empty line if placed at the beginning of the include section.

```cpp
// OK.
#include <yt/yt/server/other_server/public.h>

#include <yt/yt/server/misc/something.h>
#include <yt/yt/server/misc/something_more.h>

// Not OK.
#include <yt/yt/server/other_server/public.h>
#include <yt/yt/server/misc/something.h>

// Also not OK.
#include <yt/yt/server/misc/something.h>

#include <yt/yt/server/misc/something_more.h>
```

We place an empty line before and after the `//////` separator.
```cpp
// OK.
namespace NYT::NChunkPools {

////////////////////////////////////////////////////////////////////////////////

struct IFoo;

// Not OK.

namespace NYT::NChunkPools {
////////////////////////////////////////////////////////////////////////////////
struct IFoo;
```

We always separate all definitions with a body (classes, structs, and functions) by an empty line.

```cpp
// OK.
struct TRow
{
    ...
};

struct TKey
{
    ...
};

// Not OK.
struct TRow
{
    ...
};
struct TKey
{
    ...
};
```

Similar empty line separation applies to other blocks of code in curly braces which are on the same indentation level. As a rule of thumb: `}` is followed by an empty line if and only if the next code line is on the same indentation level.

```cpp
// OK.
if (condition1) {
    ...
}

if (condition2) {
    ...
}

// Not OK.
if (condition1) {
    ...
}
if (condition2) {
    ...
}

// OK.
if (condition1) {
    ...
    if (condition2) {
        ...
    }
}

// Not OK.
if (condition1) {
    ...
    if (condition2) {
        ...
    }

}
```

In general, code preceding `if`, `for` and `while` statements should be separated by an empty line. However, the initialisation of variables used inside code blocks could be glued to the `if`, `for` or `while` statement.

```cpp
// OK.
DoSomething();

if (condition) {
    ...
}

// Not OK.
DoSomething();
if (condition) {
    ...
}

// OK.
auto variable1 = ...;
auto variable2 = ...;
if (condition) {
    // Some code that uses variable1 and variable2.
}
```

We separate cases in `switch` statements by an empty line. Sometimes we embrace a case inside curly braces — to limit the scope of a new variable declaration to this case — then the closing `}` should appear on a new line followed by an empty line. If a case doesn't have a body (e.g. code falls through to the next case), the empty line is omitted.

```cpp

// OK.
switch (expression) {
    case 1:
        ...
        break;

    case 2: {
        ...
        break;
    }

    case 3:
    case 4:
        ...
        break;

    default:
        ...
}

// Not OK.
switch (expression) {
    case 1:
        ...
        break;
    case 2: {
        ...
        break;
    }
    case 3:

    case 4:
        ...
    default:
        ...
}

```

Declarations may be separated by an empty line if they group into logical blocks.

```cpp
// OK.
struct TRow;
struct TKey;

struct TMessage;

// OK.
virtual void ValidateBundleUsePermission(TTabletCellBundle* bundle) const = 0;

virtual TTabletBase* GetTabletOrThrow(TTabletId tabletId) = 0;
virtual TTabletCell* GetTabletCellOrThrow(TTabletCellId tabletId) = 0;

// Not OK.
virtual void ValidateBundleUsePermission(TTabletCellBundle* bundle) const = 0;
virtual TTabletBase* GetTabletOrThrow(TTabletId tabletId) = 0;
virtual TTabletCell* GetTabletCellOrThrow(TTabletCellId tabletId) = 0;

// Also not OK.
virtual void ValidateBundleUsePermission(TTabletCellBundle* bundle) const = 0;

virtual TTabletBase* GetTabletOrThrow(TTabletId tabletId) = 0;

virtual TTabletCell* GetTabletCellOrThrow(TTabletCellId tabletId) = 0;
```

Class member definitions may be arranged in groups separated by a single empty line. In particular, const and mutable members should be separated by an empty line.

```cpp
// OK.
class TSomeClass
{
...
private:
    const int SomeConstMember1;
    const int SomeConstMember2;

    int SomeMutableMember1;
    int SomeMutableMember2;
};

// Not OK.
class TSomeClass
{
...
private:
    const int SomeConstMember1;
    const int SomeConstMember2;
    int SomeMutableMember1;
    int SomeMutableMember2;
};
```

Sometimes two empty lines are used to separate class fields from class methods. In rare cases, groups of class method definitions are separated by two empty lines — but only when the groups are large and clearly separated from each other by code semantics. These are the only cases when two empty lines are allowed.

```cpp
// OK.
class TSomeClass
{
...
private:
    ... // Long list of members, split into several groups
    int LastMember;


    void DoSomething()
    {
        ...
    }
};
```

Macro invocations — `YT_TLOG_*`, legacy `YT_LOG_*`, `THROW_ERROR_EXCEPTION`, `YT_VERIFY` — should be separated by an empty line from the other code at the same indentation level, both above and below the macro invocation. A macro and its `.With(...)` chain form one statement; do not insert blank lines inside the chain.

```cpp
// OK.
YT_TLOG_DEBUG("Doing something");

DoSomething();

YT_TLOG_DEBUG("Did something");

// Not OK.
YT_TLOG_DEBUG("Doing something");
DoSomething();
YT_TLOG_DEBUG("Did something");

// OK.
if (condition) {
    YT_TLOG_DEBUG("Doing something");

    DoSomething();

    YT_TLOG_DEBUG("Did something");
}

// OK.
YT_VERIFY(condition1);
YT_VERIFY(condition2);

DoSomething();

// Not OK.
YT_VERIFY(condition1);
YT_VERIFY(condition2);
DoSomething();

// Not OK.
YT_VERIFY(condition1);

YT_VERIFY(condition2);

DoSomething();
```

<!------------------------------------------------------------------------------------>

### Comments

For code commenting we use `//` comments. We do not use `/* */` comments. Comments should be comprised of full English sentences; capital letters and dots are mandatory.
```cpp
// OK.

// The last chunk serves as a sentinel.
for (int chunkIndex = 0; chunkIndex < chunkCount - 1; ++chunkIndex) {
    ...
}

int lockBalance = 0; // The number of locks acquired minus the number of locks released.
```

We often use doxygen-style comments for documenting classes, methods and functions. We almost never use doxygen-style comments for documenting variables. We use `|parameter|` to refer to a parameter in the text. We use `#Function()`, `#TClass`, etc., following the doxygen style.

```cpp
// Some real-life examples.

//! Creates an invoker that executes all callbacks in the
//! context of |underlyingInvoker| (possibly in different threads)
//! but in a serialized fashion (i.e. all queued callbacks are executed
//! in the proper order and no two callbacks are executed in parallel).
//! |invokerName| is used as a profiling tag.
//! |registry| is needed for testing purposes only.
IInvokerPtr CreateSerializedInvoker(
    IInvokerPtr underlyingInvoker,
    const std::string& invokerName = "default",
    NProfiling::IRegistryPtr registry = nullptr);

struct IInvoker
    : public virtual TRefCounted
{
    //! Schedules invocation of a given callback.
    virtual void Invoke(TClosure callback) = 0;
};

struct ISuspendableInvoker
{
    //! Puts invoker into suspended mode.
    /*
     *  Warning: This function is not thread-safe.
     *  When all currently executing callbacks will be finished, returned future will be set.
     *  All incoming callbacks will be queued until Resume is called.
     */
    virtual TFuture<void> Suspend() = 0;
};

struct IThroughputThrottler
{
    //! Unconditionally acquires |amount| units for utilization.
    //! This request could easily lead to an overdraft.
    /*!
     *  \note Thread affinity: any
     */
    virtual void Acquire(i64 amount) = 0;
};
```

We use special types of comments to denote some work in progress: COMPAT, TODO, XXX, and NB. We **do not** use FIXME, HACK, and other common types of comment marks. COMPAT, TODO and XXX should be followed by the author's username in parentheses.

```cpp
// OK.

// COMPAT(savrus)
if (reign < static_cast<int>(ETabletReign::Start_25_2)) {
    ...
}

// TODO(babenko): Rename to "limit".
registrar.ParameterWithUniversalAccessor<std::optional<i64>>(
    "max_size",
    ...
)

// NB: This enum is part of the persistent state.
DEFINE_ENUM(EDictionaryCompressionPolicy,
    ...
)

// Not OK.

// TODO(babenko): rename to "limit"
// NB(levysotsky) We don't use TYsonStruct here.
```

| Type | Description |
|:---: | :---------- |
| TODO | A message that something should be done in the future; however, the system is fine as it is. |
| XXX  | Similar to the above, but the author is not happy with the code and calls for an urgent rewrite. We do not favor this kind of mark in code review. |
| NB   | Nota bene — a remark that calls for attention. Something one should know about the code, but that is not clear from the context. |
| COMPAT | Marks code that exists solely for backward compatibility and should be removed in the future. |

<!------------------------------------------------------------------------------------>

### Functions

Recall that function declaration and invocation follow a strict indentation rule: either it fits on a single line or each argument should be on a separate line.

```cpp
// OK.
SomeFunction(shortArgument1, shortArgument2, shortArgument3);

// OK.
SomeFunction(
    veryLongArgument1,
    veryLongArgument2,
    veryLongArgument3);
```

Unused method arguments are commented in the method definition and are not commented in the declaration.

```cpp
void DoSomething(int usedArg, int unusedArg, int anotherUsedArg);

void DoSomething(int usedArg, int /*unusedArg*/, int anotherUsedArg)
{ ... }
```

Magic constants within function calls must be annotated by the corresponding argument name.

```cpp
DoSomething(
    chunkCount,
    /*maxDataWeightPerJob*/ std::numeric_limits<i64>::max(),
    InputTables_[tableIndex],
    /*enableJobSplitting*/ false);
```

Empty function bodies are written as `{ }` on a separate line.

```cpp
// OK.
void DoNothing()
{ }

// Not OK.
void DoNothing() { }
void DoNothing()
{
}
void DoNothing()
{}
```

In general, lambdas are treated similarly to functions. The exceptions are lambdas without arguments and immediate invocation. In the former case, `()` is omitted. In the latter case, `()` follows `}` on the same line, separated by a single space.

```cpp

// OK.
auto lambda = [&] {
    ...
};

// Not OK.
auto lambda = [&] () {
    ...
};

// OK.
auto lambda = [] {
    ...
} ();

// Not OK.
auto lambda = [] {
    ...
}
( );
```

<!------------------------------------------------------------------------------------>

### Misc

Multiple statements or declarations are not allowed on a single line.

```cpp
// Not OK.
int a = 0, b = 1;
// OK.
int a = 0;
int b = 1;
```

<!------------------------------------------------------------------------------------>

## TODO

Here go topics that are not yet covered.

### Fluents

### Yson Struct

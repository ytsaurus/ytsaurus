# YTsaurus C++ Style Guide

<!--================================================================================-->

## Scope

This document summarizes the coding conventions used in the most part of our C++ code base. The rules described here apply at least to the following top-level directories:

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

We use C++20. As soon as the most of our toolchains supoprt C++23, we aim to switch to it.

As of today, we do not provide clang-format/clang-tidy configuration due to significant complications while trying to express our coding style in terms of these tools. Still, it may happen in the future; there is an issue #75 for that.

Most part of the code follows the rules from this document, but some older parts may not. If you are working on such a part, you may refactor it to follow the rules from this document, but it is not required. Having a PR which introduces 3 new lines and changes whitespaces around 300 lines is not very convenient, try to separate such changes into separate PRs.

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

Classes are used for the most part of the code, structs are used for PODs and similar types. Structs must not contain private fields or methods.

We define the convention of an interface, which is a struct with only pure virtual methods. Such structs are prefixed with capital `I`.

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

<!------------------------------------------------------------------------------------>

### Functions

Function names are always in PascalCase independent of whether they are free functions or methods.

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

Global variables, consts and enum values are always in PascalCase.

```cpp
constexpr i64 MaxValueSize = 128_KB;

static const TLogger Logger("SomeLogger");

DEFINE_ENUM(ENodeFactoryState,
    (Active)
    (Committed)
    (RolledBack)
);
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

Nested namespaces are a common practice. Topmost namespace is usually `NYT`, but may be different for some libraries.

The last line of the namespace repeats the namespace name to make it easier to find the end of the namespace.

```cpp
namespace NYT::NChunkPools
{

// Something

} // namespace NYT::NChunkPools
```

<!------------------------------------------------------------------------------------>

### Separator Lines

We use separator lines to separate logical blocks of code. The separator line is a line with 80 `/` characters.

The only strict place to always use separator lines is to distinguish the content of a namespace from its surrounding code. The use of separator lines between classes, structs, functions, etc is optional. We never use separator lines within a class, struct or function.

```cpp
namespace NYT::NChunkPools
{

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

We use a quote-style include directives for the headers located in the same directory as the including file. We use angle-bracket-style include directives for the rest of included headers, including system ones.

Included headers are grouped by the base directory they are located in. Groups are separated by a blank line. Within a group, headers are sorted alphabetically.

We order groups from the most specific to the most general. Motivation is that we would like to reduce the number of situations when the latter included header already includes the former one as it may hide some dependency issues. 

We do not care about including everything which is used by the current file.

For .cpp files, the corresponding header is always the first included header and is separated by a blank line. 

For .h files, usually the private.h/public.h header is the first included.

Very common standard includes (like `<vector>` or `<util/string.h>`) are collected in `yt/yt/core/misc/common.h`, which is usually included in any .cpp or .h file in `yt/yt`.`

```cpp
// Example for yt/yt/server/foo/bar.cpp.
#include "bar.h"

#include "helpers.h"
#include "config.h"

#include <yt/yt/server/misc/something.h>
#include <yt/yt/server/misc/something_more.h>

#include <yt/yt/ytlib/some_client/stuff.h>

#include <yt/yt/ytlib/some_other_client/config.h>
#include <yt/yt/ytlib/some_other_client/public.h>

#include <yt/yt/client/very_basic_client/proto/stuff.pb.h>

#include <yt/yt/core/certain/primitive.h>

#include <library/cpp/yt/common_library/library.h>

#include <util/string/split.h>

#include <any>
```

<!------------------------------------------------------------------------------------>

### inl.h Files

When a template is declared in a .h file, its implementation is usually placed in a corresponding -inl.h file. The -inl.h file is included at the end of the .h file.

Another occasion to use -inl.h files is when you want to force inline a function declared in the header.

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

We use `int` when we do not care about the exact size of the integer and for the most loop variables.

When arity of the integer is important, we use `i8`, `i16`, `i32`, `i64` and `ui8`, `ui16`, `ui32`, `ui64` for signed and unsigned integers respectively. 

We sometimes use `size_t` and `ssize_t` when interoperability with standard library requires them, but in general we try not to use them for local variables or loop variables. 

We do not use `long`, `long long` or `short` types, as well as C++11 fixed width types like `int8_t` or `uint32_t` or non-standard extensions like `__int64` or `unsigned __int64`.

We favor signed types instead of unsigned types. We acknowledge this being a controversial topic, but we believe that the benefits of signed types outweigh the benefits of unsigned types. We do not use unsigned types for loop variables. In general, we use unsigned types only for bit manipulations, wire protocols and other cases where unsignedness is justified by the nature of manipulations.
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

// Not ok.
for (size_t tableIndex = 0; tableIndex < InputTables_.size(); ++tableIndex) {
    ...
}

// Also not ok.
for (auto it = InputTables_.begin(); it != InputTables_.end(); ++it) {
    ...
}
```

</details>

<!------------------------------------------------------------------------------------>

### `auto` vs Explicit Types

We tend to use `auto` for local variables, but not for fields, function parameters or return types. Using `auto` for latter cases is allowed, but must be justified by the code readability or the template nature of the code.

We do not use auto with short primitive types like `int`, `i64`, `char`, `bool`.

```c++
// Not ok.
std::vector<TInputChunkPtr> chunks = CollectChunks();
// Ok.
auto chunks = CollectChunks();

// Not ok.
auto canTeleportChunk = chunk.IsTrivial() && OutputTable_.CanTeleport();
// Ok.
bool canTeleportChunk = chunk.IsTrivial() && OutputTable_.CanTeleport();

// Not ok.
auto GetDebugString(TChunkInfo info);
// Ok.
TString GetDebugString(TChunkInfo info);

// Ok because de-facto this is a template function.
auto GetRandomElement(auto collection)
{
    return collection[Rand() % collection.size()];
}

// Ok because auto replaces cumbersome const std::pair<TInputCookie, TInputChunk>&.
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

#include <foorbar/foo.h>
#include <foorbar/bar.h>

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

The most common pointer type used across our code base is `TIntrusivePtr<T>`. Semantically it is very close to `std::shared_ptr<T>`, but it has a slightly different memory layout, and it is tightly integrated with our reference counting mechanism and asynchronous framework.

Intrusive pointer requires the class to be either derived from `TRefCounted`, or be a final class. 

After a full definition of such class, a `DEFINE_REFCOUNTED_TYPE` macro must be used to provide implementations of reference counting methods. 

The forward declaration of a class itself and a fully defined alias `TPtr = TIntrusivePtr<T>` are provided by `DECLARE_REFCOUNTED_CLASS`/`DECLARE_REFCOUNTED_STRUCT` macros and are usually placed in `public.h`/`private.h` file.

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

Unique pointers are frequently used with non-refcounted or standard classes. Shared pointers or somewhat rare, but may occur when interoperability with standard library is required, or the ownership promotion from the unique pointer is used.

Raw pointers are quite often used when the ownership is guaranteed to be somewhere else, and the pointer is used as a non-owning reference. Raw pointers are never used for memory allocation (except deep inside some low-level libraries).

<!------------------------------------------------------------------------------------>

### Interfaces vs Pimpl

We discourage heavy class declarations in header files, preferring to hide the implementation details by either using (most common) interfaces or [pimpl](https://en.cppreference.com/w/cpp/language/pimpl) idiom.

In case of interface with a single implementation, the factory method to construct the instance of the interface (called `Create...`) is usually placed in the same header file as the interface itself, and returns either the intrusive pointer, unique pointer or shared pointer to the class. The implementation is put into the corresponding .cpp file.

```cpp

// tree_traverser.h

struct ITreeTraverser
    : public TRefCounted
{
    virtual void Traverse(const TTree& tree) = 0;
    virtual void Reset() = 0;
};

ITreeTraverserPtr CreateTreeTraverser(const TTreeTraverserOptions& options);


// tree_traverser.cpp

class TTreeTraverser
    : public ITreeTraverser
{
public:
    TTreeTraverser(const TTreeTraverserOptions& options)
        : Options_(options)
    { ... }
    void Traverse(const TTree& tree) override
    { ... }
    void Reset() override
    { ... }

private:
    const TTreeTraverserOptions Options_;
    // ...arbitrarily complex private fields and methods.
}

ITreeTraverserPtr CreateTreeTraverser(const TTreeTraverserOptions& options)
{
    return New<TTreeTraverser>(options);
}
```

Pimpl is currently considered a legacy idiom, but may be used sometimes. A notable case is when the CoW-semantics is involved, or the pointer implementation may be changed due to other reasons.

<!------------------------------------------------------------------------------------>

### Logging

Log messages follow quite strict rules. This results in an extreme convenience for debugging and searching for relevant log messages whenever you get used to it.

Logging is done only using the `YT_LOG_XXX` macros. 

Log messages are usually in past simple, present continuous or do not contain a verb at all. Log messages are not completed English sentences, they do not finish with period. In case when multiple "sentences" must be present within a single message, separate them with a semicolon.

```cpp
YT_LOG_DEBUG("Starting fetching columnar statistics");
YT_LOG_DEBUG("Finished fetching columnar statistics");
YT_LOG_DEBUG("Fetched current offset (Offset: %v, WallTime: %v)", currentOffset, timer.GetElapsedTime());
YT_LOG_FATAL("Unknown write command (Command: %v)", command);
YT_LOG_INFO("Concurrent cache head statistics (ElementCount: %v)", cacheHead->GetElementCount());
YT_LOG_INFO("Replay encountered skip record; multiplexed suffix is ignored (ChunkId: %v, RecordId: %v, RecordCount: %v)", chunkId, recordId, recordCount);
```

In context of a caught exception or received error, it is usually logged as a first argument before the message.

```cpp
try {
    EndpointHostName_ = FqdnHostName();
} catch (const std::exception& ex) {
    YT_LOG_ERROR(ex, "Failed to resolve local host name");
    EndpointHostName_ = "localhost";
}

if (!commandResultOrError.IsOK()) {
    YT_LOG_ERROR(commandResultOrError, "Failed to apply change replica mode command");
    failed = true;
}
```

If the logged message contains variable parts, they are always put in the brackets after the message in form of `Key: Value` pairs separated by commas. This is important for many reasons: readability, searchability, ability to parse the log messages automatically, ability to group log messages by the same key, etc. Finally, the logger may be configured to add some fixed tags to all log messages, and this logic also depends on the fixed form of the log message.

```cpp
// Ok.
YT_LOG_DEBUG("Tables fetched (TableCount: %v)", tableCount);
// Not ok.
YT_LOG_DEBUG("Fetched %v tables", tableCount);
YT_LOG_DEBUG("Tables fetched (tableCount: %v)", tableCount);
```

### Errors

All error and exception text must be written as if they were dedicated to an end user. They must also be written in a gramatically correct English; typically they also do not contain dots, having semicolons instead. 

In contrast to the log messages, variable parts of an error are encouraged to be in included directly in the error text. In general, we assume that the end user will read the error text and will not read the attributes of an error.

Finally, the attributes of an error are machine readable, and may contain wider debug information, such as the faulty key range, guids or string contexts.

```cpp
THROW_ERROR_EXCEPTION("Column %v has type %Qlv that is not currently supported by Arrow encoder", column.GetName(), column.GetType());

THROW_ERROR_EXCEPTION(
    EErrorCode::MaxDataWeightPerJobExceeded, "Maximum allowed data weight per sorted job exceeds the limit: %v > %v",
    job->GetDataWeight(),
    JobSizeConstraints_->GetMaxDataWeightPerJob())
    << TErrorAttribute("lower_key", job->LowerPrimaryKey())
    << TErrorAttribute("upper_key", job->UpperPrimaryKey());

RequestAttachmentsStream_->Abort(TError("Client request control is finalized")
    << TErrorAttribute("request_id", GetRequestId()));
```

<!--================================================================================-->

## Cosmetics

Rules in this section define the fancy stuff not affecting the code behavior. They are quite strictly followed throughout our code base, making its visual appearance notably consistent even in completely unrelated parts of the code.

<!------------------------------------------------------------------------------------>

### Naming Conventions

We discourage the use of abbreviations in names and one-letter/two-letter names. Somewhat commonly recognized exceptions from the previous rule are using `it` for iterators, `Impl` as a suffix for implementation.

```cpp

// Not ok.
int oc = GetOperationCount();
std::vector<TTablet*> vec;
for (int i = 0; i < tableCount; ++i) {
    ...
}

// Ok.
int operationCount = GetOperationCount();
std::vector<TTablet*> tablets;
for (int tableIndex = 0; tableIndex < tableCount; ++tableIndex) {
    ...
}
```

Well-known acronyms are ok (e.g. TCP, IP, IO, MPSC, CH for ClickHouse). YT is not an acronym, but works like a two-letter acronym.

 Two-letter acronyms are treated as acronyms (e.g. both letters are capitalized or not captialized simultaneously), three-letter acronyms are treated as words (e.g. only the first letter is capitalized).

```cpp
namespace NYT;
class TIOTracker;
auto ioEngine = CreateIOEngine();
class TIP6Address;

class TTcpConnection;
class TMpscQueueImpl;
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
}

auto doSomething = [&] (auto arg) {
    ...
};

if (condition) {
    ...
} else {
    ...
}
```

The only exception for the latter rule is when the control block statement is too long to fit into a single line.

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

Parentheses around single-statement control blocks are never omitted.

```cpp
// Not ok.
if (condition)
    DoSomething();

// Ok.
if (condition) {
    DoSomething();
}
```

<!------------------------------------------------------------------------------------>

### Indents

We use 4 spaces for indents. We do not use tabs.

The body of the namespace is not indented.

List of class bases is indented by 4 spaces; each entry starts on a new line either with a comma or a colon.

```cpp
class TSomething
    : public TBase
    , public TAnotherBase
{
    ...
};
```

List of designators in a constructor initializer list is indented by 4 spaces; each entry starts on a new line either with a comma or a colon.

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
}

// Or outside class definition.
TSomething::TSomething(TSomeArg arg, TAnotherArg anotherArg)
    : Base(arg)
    , AnotherArg_(std::move(anotherArg))
{ ... }
```

If the argument list of a function signature or call is too long to fit into a single line, we split it into multiple lines, each argument occupying a separate line. The whole argument list is indented by 4 spaces.

```cpp
// Ok.
SomeFunction(
    veryLongArgument1,
    veryLongArgument2,
    veryLongArgument3);

// Not ok.
SomeFunction(veryLongArgument1,
             veryLongArgument2,
             veryLongArgument3);

// Also not ok.
SomeFunction(veryLongArgument1,
    veryLongArgument2,
    veryLongArgument3);
```

In case of chained function calls, we split the chain into multiple lines, each call occupying a separate line. The whole chain is indented by 4 spaces.

```cpp
BIND(&TSomeClass::DoSomething, MakeStrong(this))
    .AsyncVia(SomeInvoker_)
    .Run();
```

<!------------------------------------------------------------------------------------>

### Comments

For code commenting we use `//` comments. We do not use `/* */` comments. Comment should be comprised of full English sentences; capital letters and dots are mandatory.
```cpp
// Ok.

// The last chunk serves as a sentinel.
for (int chunkIndex = 0; chunkIndex < chunkCount - 1; ++chunkIndex) {
    ...
}

int lockBalance = 0; // The number of locks acquired minus the number of locks released.
```

We often use doxygen-style comments for documenting classes, methods and functions. We almost do not use doxygen-style comments for documenting variables.

```cpp
// Some real-life examples.

//! Creates an invoker that executes all callbacks in the
//! context of #underlyingInvoker (possibly in different threads)
//! but in a serialized fashion (i.e. all queued callbacks are executed
//! in the proper order and no two callbacks are executed in parallel).
//! #invokerName is used as a profiling tag.
//! #registry is needed for testing purposes only.
IInvokerPtr CreateSerializedInvoker(
    IInvokerPtr underlyingInvoker,
    const TString& invokerName = "default",
    NProfiling::IRegistryImplPtr registry = nullptr);

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
    //! Unconditionally acquires #amount units for utilization.
    //! This request could easily lead to an overdraft.
    /*!
     *  \note Thread affinity: any
     */
    virtual void Acquire(i64 amount) = 0;
};
```

<!------------------------------------------------------------------------------------>

### Misc

Multiple statements or declarations are not allowed on a single line.

```cpp
// Not ok.
int a = 0, b = 1;
// Ok.
int a = 0;
int b = 1;
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
// Ok.
void DoNothing()
{ }

// Not ok.
void DoNothing() { }
void DoNothing()
{
}
void DoNothing()
{}
```

<!------------------------------------------------------------------------------------>

## TODO

Here go yet uncovered topics.

### Fluents

### Asynchronous Idioms

### Yson Struct

### String Formatting

# Generating documentation

The script generates HTML documentation for the MapReduce C++ SDK. It requires Python 3.8 or newer and `tar`. Doxygen must be installed and available in `PATH`.

To create an archive, run the script from the source root:

```bash
python3 yt/cpp/mapreduce/doc
```

The `c++_doc.tar.gz` archive is created in the current directory and contains HTML in the `c++/` directory. The script locates the source root by looking for `yt/cpp/mapreduce/interface`; it can also run from nested directories.

To preview the documentation locally, start the HTTP server:

```bash
python3 yt/cpp/mapreduce/doc serve 8080
```

Open `http://localhost:8080/`. The default port is `8080`.

# Writing documentation

Write documentation in English using Doxygen comments.

Use only `///` for Doxygen comments.

Use only `@` for commands, such as `@brief` (not `\brief`).

For a simple function that needs only a one-line description, use:

```cpp
/// Add two integers.
int Sum(int, int);
```

For a function that needs more detail, use:

```cpp
///
/// @brief Add two integers.
///
/// Invoke mathematical operation of addition to compute an integer.
/// This function is thread safe.
///
/// @return result of computation
int Sum(int, int);
```

Use [Markdown](https://daringfireball.net/projects/markdown/syntax) for links, emphasis, and other formatting.

## Common Doxygen commands

See the [Doxygen command reference](http://www.doxygen.nl/manual/commands.html) for the complete list. Commonly used commands include:

- `@brief`: a short description of the documented entity.
- `@ref`: a link to another documented entity. Use the fully qualified name, including the namespace, and add parentheses for functions: `@ref NYT::IClient`, `@ref NYT::CreateClient()`.
- `@file`: a description of a file. Doxygen does not generate documentation for global functions unless their file is documented.
- `@cond`: exclude a section of code from the documentation and suppress warnings about it. For example:

  ```cpp
  /// @cond Doxygen_Suppress
  using TSelf = TAutoMergeSpec;
  /// @endcond
  ```

- `@copydoc`: copy documentation from another method. This is useful for groups of similar methods. For example:

  ```cpp
  /// Set foo
  TFoo& SetFoo(int a) &;
  /// @copydoc TFoo SetFoo(int) &;
  TFoo SetFoo(int a) &&;
  ```

# Publishing documentation

The `.github/workflows/docs.yaml` workflow in `ytsaurus/ytsaurus-cpp-sdk` builds documentation on every push to `main` and supports manual runs through `workflow_dispatch`.

Documentation is published at [cppdoc.ytsaurus.tech](https://cppdoc.ytsaurus.tech/).

# Editor configuration

## Vim

To make editing Doxygen comments easier in Vim, add these lines to `.vimrc`:

```vim
autocmd FileType c,cpp set comments-=://
autocmd FileType c,cpp set comments+=:///
autocmd FileType c,cpp set comments+=://
```

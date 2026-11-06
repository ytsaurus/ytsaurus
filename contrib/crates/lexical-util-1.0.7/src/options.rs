//! Shared traits for the options API.
//!
//! The following constants have the following signifiers:
//!
//! - `${X}_LITERAL`: Applies to all literal values for that language (for
//!   example, [`RUST_LITERAL`]).
//! - `${X}_STRING`: Applies to all string values for that language (for
//!   example, [`ERLANG_STRING`]).
//! - `${X}`: Applies to all values for that language (for example, [`KAWA`]).
//! - `${X}_(NAN|INF|INFINITY)`: Applies to only a single special value (for
//!   example, [`PHP_LITERAL_NAN`], [`PHP_LITERAL_INF`], and
//!   [`PHP_LITERAL_INFINITY`]).
//!
//! If it's not defined, all values are the default. The default options
//! are:
//! - NaN: (`*_NAN`): `NaN`
//! - Short infinity: (`*_INF`): `Inf` (including `+Inf` and `-Inf`)
//! - Long infinity: (`*_INFINITY`): `Infinity` (including `+Infinity` and
//!   `-Infinity`)

#[cfg(any(feature = "write-floats", feature = "write-integers"))]
use crate::constants::FormattedSize;

// TRAITS
// ------

#[doc(hidden)]
#[macro_export]
macro_rules! write_options_doc {
    () => {
        "
Get an upper bound on the required buffer size.

<div class=\"warning\">

This method is soft-deprecated and meant for internal use.
You should always use [`buffer_size_const`] so you can get
the required buffer size at compile time to determine the
buffer size required.

</div>

[`buffer_size_const`]: Self::buffer_size_const

This is used when custom formatting options, such as significant
digits specifiers or custom exponent breaks, are used, which
can lead to more or less significant digits being written than
expected. If using the default formatting options, then this will
always be [`FORMATTED_SIZE`][FormattedSize::FORMATTED_SIZE] or
[`FORMATTED_SIZE_DECIMAL`][FormattedSize::FORMATTED_SIZE_DECIMAL],
depending on the radix.
"
    };
}

/// Shared trait for all writer options.
#[cfg(any(feature = "write-floats", feature = "write-integers"))]
pub trait WriteOptions: Default {
    /// Determine if the options are valid.
    fn is_valid(&self) -> bool;

    /// Get an upper bound on the required buffer size.
    ///
    /// <div class="warning">
    ///
    /// This method is soft-deprecated and meant for internal use.
    /// You should always use `buffer_size_const` for either [`integer`] or
    /// [`float`] writer so you can get the required buffer size at compile time
    /// to determine the buffer size required.
    ///
    /// </div>
    ///
    /// [`integer`]: https://docs.rs/lexical-write-integer/latest/lexical_write_integer/struct.Options.html#method.buffer_size_const
    /// [`float`]: https://docs.rs/lexical-write-float/latest/lexical_write_float/struct.Options.html#method.buffer_size_const
    ///
    /// This is used when custom formatting options, such as significant
    /// digits specifiers or custom exponent breaks, are used, which
    /// can lead to more or less significant digits being written than
    /// expected. If using the default formatting options, then this will
    /// always be [`FORMATTED_SIZE`][FormattedSize::FORMATTED_SIZE] or
    /// [`FORMATTED_SIZE_DECIMAL`][FormattedSize::FORMATTED_SIZE_DECIMAL],
    /// depending on the radix.
    ///
    /// Using `buffer_size_const` lets you create static arrays at compile time,
    /// rather than dynamically-allocate memory or know the value ahead of time.
    #[deprecated = "Use `buffer_size_const` instead. Will be removed in 2.0."]
    fn buffer_size<T: FormattedSize, const FORMAT: u128>(&self) -> usize;
}

/// Shared trait for all parser options.
#[cfg(any(feature = "parse-floats", feature = "parse-integers"))]
pub trait ParseOptions: Default {
    /// Determine if the options are valid.
    fn is_valid(&self) -> bool;
}

// PRE-DEFINED CONSTANTS
// ---------------------

// The following constants have the following signifiers:
//  ${X}_LITERAL - Applies to all literal values for that language.
//  ${X}_STRING - Applies to all string values for that language.
//  ${X} - Applies to all values for that language.
//  ${X}_(NAN|INF|INFINITY) - Applies to only a single special value.
//  IF it's not defined, all values are the default.

macro_rules! literal {
    ($name:ident, $value:ident $(, $doc:literal)?) => {
        $(#[doc = $doc])?
        pub const $name: Option<&[u8]> = $value;
    };
    ($name:ident, $value:literal $(, $doc:literal)?) => {
        $(#[doc = $doc])?
        pub const $name: Option<&[u8]> = Some($value);
    };
}

literal!(RUST_LITERAL, None, "A `Rust` literal number (uses default options).");
// RUST_STRING
literal!(PYTHON_LITERAL, None, "A `Python` literal number (uses default options).");
// PYTHON_STRING
literal!(CXX_LITERAL_NAN, b"NAN", "A `C++` literal NaN (`NAN`).");
literal!(CXX_LITERAL_INF, b"INFINITY", "A `C++` literal short infinity (`INFINITY`).");
literal!(CXX_LITERAL_INFINITY, b"INFINITY", "A `C++` literal long infinity (`INFINITY`).");
// CXX_STRING
literal!(C_LITERAL_NAN, b"NAN", "A `C` literal NaN (`NAN`).");
literal!(C_LITERAL_INF, b"INFINITY", "A `C` literal short infinity (`INFINITY`).");
literal!(C_LITERAL_INFINITY, b"INFINITY", "A `C` literal long infinity (`INFINITY`).");
// RUBY_LITERAL
literal!(RUBY_LITERAL_NAN, b"NaN", "A `Ruby` literal NaN (`NaN`).");
literal!(RUBY_LITERAL_INF, b"Infinity", "A `Ruby` literal short infinity (`Infinity`).");
literal!(RUBY_STRING_NONE, None, "A `Ruby` string (uses default options).");
// C_STRING
literal!(SWIFT_LITERAL, None, "A `Swift` literal number (uses default options).");
// SWIFT_STRING
literal!(GO_LITERAL, None, "A `Golang` literal number (uses default options).");
// GO_STRING
literal!(HASKELL_LITERAL, None, "A `Haskell` literal number (uses default options).");
literal!(HASKELL_STRING_INF, b"Infinity", "A `Haskell` string short infinity (`Infinity`).");
literal!(HASKELL_STRING_INFINITY, b"Infinity", "A `Haskell` string long infinity (`Infinity`).");
literal!(JAVASCRIPT_INF, b"Infinity", "A `JavaScript` string short infinity (`Infinity`).");
literal!(JAVASCRIPT_INFINITY, b"Infinity", "A `JavaScript` string long infinity (`Infinity`).");
literal!(PERL_LITERAL, None, "A `Perl` literal literal (uses default options).");
// PERL_STRING
literal!(PHP_LITERAL_NAN, b"NAN", "A `PHP` literal NaN (`NAN`).");
literal!(PHP_LITERAL_INF, b"INF", "A `PHP` literal short infinity (`INF`).");
literal!(PHP_LITERAL_INFINITY, b"INF", "A `PHP` literal long infinity (`INF`).");
// PHP_STRING
literal!(JAVA_LITERAL, None, "A `Java` literal number (uses default options).");
literal!(JAVA_STRING_INF, b"Infinity", "A `Java` string short infinity (`Infinity`).");
literal!(JAVA_STRING_INFINITY, b"Infinity", "A `Java` string long infinity (`Infinity`).");
literal!(R_LITERAL_INF, b"Inf", "An `R` literal short infinity (`Inf`).");
literal!(R_LITERAL_INFINITY, b"Inf", "An `R` literal long infinity (`Inf`).");
// R_STRING
literal!(KOTLIN_LITERAL, None, "A `Kotlin` literal number (uses default options).");
literal!(KOTLIN_STRING_INF, b"Infinity", "A `Kotlin` string short infinity (`Infinity`).");
literal!(KOTLIN_STRING_INFINITY, b"Infinity", "A `Kotlin` string long infinity (`Infinity`).");
literal!(JULIA_LITERAL_INF, b"Inf", "A `Julia` string short infinity (`Inf`).");
literal!(JULIA_LITERAL_INFINITY, b"Inf", "A `Julia` string long infinity (`Inf`).");
// JULIA_STRING
literal!(CSHARP_LITERAL, None, "A `C#` literal number (uses default options).");
literal!(CSHARP_STRING_INF, b"Infinity", "A `C#` string short infinity (`Infinity`).");
literal!(CSHARP_STRING_INFINITY, b"Infinity", "A `C#` string long infinity (`Infinity`).");
literal!(KAWA, None, "A `Kawa` (Lisp) literal number (uses default options).");
literal!(GAMBITC, None, "A `Gambit-C` (Lisp) literal number (uses default options).");
literal!(GUILE, None, "A `Guile` (Lisp) literal number (uses default options).");
literal!(CLOJURE_LITERAL, None, "A `Clojure` (Lisp) literal number (uses default options).");
literal!(CLOJURE_STRING_INF, b"Infinity", "A `Clojure` string short infinity (`Infinity`).");
literal!(CLOJURE_STRING_INFINITY, b"Infinity", "A `Clojure` string long infinity (`Infinity`).");
literal!(ERLANG_LITERAL_NAN, b"nan", "An `Erlang` literal NaN (`nan`).");
literal!(ERLANG_STRING, None, "An `Erlang` string number (uses default options).");
literal!(ELM_LITERAL, None, "An `Elm` literal number (uses default options).");
literal!(ELM_STRING_NAN, None, "An `Elm` strong NaN (uses default options).");
literal!(ELM_STRING_INF, b"Infinity", "An `Elm` string short infinity (`Infinity`).");
literal!(ELM_STRING_INFINITY, b"Infinity", "An `Elm` string long infinity (`Infinity`).");
literal!(SCALA_LITERAL, None, "A `Scala` literal number (uses default options).");
literal!(SCALA_STRING_INF, b"Infinity", "A `Scala` string short infinity (`Infinity`).");
literal!(SCALA_STRING_INFINITY, b"Infinity", "A `Scala` string long infinity (`Infinity`).");
literal!(ELIXIR, None, "An `Elixir` number (uses default options).");
literal!(FORTRAN_LITERAL, None, "A `FORTRAN` literal number (uses default options).");
// FORTRAN_STRING
literal!(D_LITERAL, None, "A `D-Lang` literal number (uses default options).");
// D_STRING
literal!(COFFEESCRIPT_INF, b"Infinity", "A `CoffeeScript` string short infinity (`Infinity`).");
literal!(COFFEESCRIPT_INFINITY, b"Infinity", "A `CoffeeScript` string long infinity (`Infinity`).");
literal!(COBOL, None, "A `COBOL` literal number (uses default options).");
literal!(FSHARP_LITERAL_NAN, b"nan", "An `F#` literal NaN (`nan`).");
literal!(FSHARP_LITERAL_INF, b"infinity", "An `F#` literal short infinity (`infinity`).");
literal!(FSHARP_LITERAL_INFINITY, b"infinity", "An `F#` literal long infinity (`infinity`).");
// FSHARP_STRING
literal!(VB_LITERAL, None, "A `Visual Basic` literal number (uses default options)");
literal!(VB_STRING_INF, None, "A `Visual Basic` short string infinity (uses default options)");
literal!(VB_STRING_INFINITY, None, "A `Visual Basic` long string number (uses default options)");
literal!(OCAML_LITERAL_NAN, b"nan", "An `OCAML` literal NaN (`nan`).");
literal!(OCAML_LITERAL_INF, b"infinity", "An `OCAML` literal short infinity (`infinity`).");
literal!(OCAML_LITERAL_INFINITY, b"infinity", "An `OCAML` literal long infinity (`infinity`).");
// OCAML_STRING
literal!(OBJECTIVEC, None, "An `Objective-C` number (uses default options).");
literal!(REASONML_LITERAL_NAN, b"nan", "A `ReasonML` literal NaN (`nan`).");
literal!(REASONML_LITERAL_INF, b"infinity", "A `ReasonML` literal short infinity (`infinity`).");
literal!(
    REASONML_LITERAL_INFINITY,
    b"infinity",
    "A `ReasonML` literal long infinity (`infinity`)."
);
// REASONML_STRING
literal!(MATLAB_LITERAL_INF, b"inf", "A `MATLAB` literal short infinity (`inf`).");
literal!(MATLAB_LITERAL_INFINITY, b"Inf", "A `MATLAB` literal long infinity (`Inf`).");
// MATLAB_STRING
literal!(ZIG_LITERAL, None, "A `Zig` literal number (uses default options).");
// ZIG_STRING
literal!(SAGE_LITERAL_INF, b"infinity", "A `SageMath` literal short infinity (`infinity`).");
literal!(SAGE_LITERAL_INFINITY, b"Infinity", "A `SageMath` literal long infinity (`Infinity`).");
// SAGE_STRING
literal!(JSON, None, "A `JSON` number (uses default options).");
literal!(TOML, None, "A `TOML` number (uses default options).");
literal!(YAML, None, "A `YAML` number (uses default options).");
literal!(XML_INF, None, "An `XML` short infinity (uses default options).");
literal!(XML_INFINITY, None, "An `XML` short infinity (uses default options).");
literal!(SQLITE, None, "A `SQLite` number (uses default options).");
literal!(POSTGRESQL, None, "A `PostgreSQL` number (uses default options).");
literal!(MYSQL, None, "A `MySQL` number (uses default options).");
literal!(MONGODB_INF, b"Infinity", "A `MongoDB` short infinity (`Infinity`).");
literal!(MONGODB_INFINITY, b"Infinity", "A `MongoDB` long infinity (`Infinity`).");

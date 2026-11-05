//! Implement string conversion routines in a single trait.

// NOTE:
//  We use macros to define the traits, rather than implement here
//  since we can't define traits for types when both are defined outside
//  the current crate, including in workspaces.

// FROM LEXICAL

/// Define the [`FromLexical`] trait.
///
/// * `name`: The name of the crate calling the function.
/// * `value`: A numerical value to use for the example.
/// * `t`: The type of the number for the example.
/// * `len`: The length of the string form of `value`.
///
/// # Examples
///
/// ```rust,ignore
/// from_lexical!("lexical_core", 1234, u64, 4);
/// ```
///
/// [`FromLexical`]: https://docs.rs/lexical-core/latest/lexical_core/trait.FromLexical.html
#[macro_export]
#[cfg(any(feature = "parse-floats", feature = "parse-integers"))]
macro_rules! from_lexical {
    ($name:literal, $value:literal, $t:ty, $len:literal $(, #[$attr:meta])? $(,)?) => {
        /// Trait for numerical types that can be parsed from bytes.
        $(#[$attr])?
        pub trait FromLexical: lexical_util::num::Number {
            /// Checked parser for a string-to-number conversion.
            ///
            /// This method parses the entire string, returning an error if
            /// any invalid digits are found during parsing. Returns a [`Result`]
            /// containing either the parsed value, or an error containing
            /// any errors that occurred during parsing.
            ///
            /// * `bytes`   - Slice containing a numeric string.
            ///
            /// # Examples
            ///
            /// ```rust
            #[doc = concat!("# assert_eq!(", stringify!($len), ", \"", stringify!($value), "\".len());")]
            #[doc = concat!("use ", $name, "::FromLexical;")]
            ///
            #[doc = concat!("let value = \"", stringify!($value), "\";")]
            #[doc = concat!("let parsed = ", stringify!($t), "::from_lexical(value.as_bytes());")]
            #[doc = concat!("assert_eq!(parsed, Ok(", stringify!($value), "));")]
            /// ```
            fn from_lexical(bytes: &[u8]) -> lexical_util::result::Result<Self>;

            /// Checked parser for a string-to-number conversion.
            ///
            /// This method parses until an invalid digit is found (or the end
            /// of the string), returning the number of processed digits
            /// and the parsed value until that point. Returns a [`Result`]
            /// containing either the parsed value and the number of processed
            /// digits, or an error containing any errors that occurred during
            /// parsing.
            ///
            /// * `bytes`   - Slice containing a numeric string.
            ///
            /// # Examples
            ///
            /// ```rust
            #[doc = concat!("# assert_eq!(", stringify!($len), ", \"", stringify!($value), "\".len());")]
            #[doc = concat!("use ", $name, "::FromLexical;")]
            ///
            #[doc = concat!("let value = \"", stringify!($value), "\";")]
            #[doc = concat!("let parsed = ", stringify!($t), "::from_lexical_partial(value.as_bytes());")]
            #[doc = concat!("assert_eq!(parsed, Ok((", stringify!($value), ", ", stringify!($len), ")));")]
            /// ```
            fn from_lexical_partial(bytes: &[u8]) -> lexical_util::result::Result<(Self, usize)>;
        }
    };
}

/// Define the [`FromLexicalWithOptions`] trait.
///
/// * `name`: The name of the crate calling the function.
/// * `value`: A numerical value to use for the example.
/// * `t`: The type of the number for the example.
/// * `len`: The length of the string form of `value`.
/// * `ops_t`: The options type.
///
/// # Examples
///
/// ```rust,ignore
/// from_lexical_with_options!("lexical_core", 1234, u64, 4, ParseIntegerOptions);
/// ```
///
/// [`FromLexicalWithOptions`]: https://docs.rs/lexical-core/latest/lexical_core/trait.FromLexicalWithOptions.html
#[macro_export]
#[cfg(any(feature = "parse-floats", feature = "parse-integers"))]
macro_rules! from_lexical_with_options {
    ($name:literal, $value:literal, $t:ty, $len:literal, $ops_t:ty $(, #[$attr:meta])? $(,)?) => {
        /// Trait for numerical types that can be parsed from bytes with custom options.
        ///
        /// The [`Options`][Self::Options] type specifies the configurable
        /// options to provide.
        $(#[$attr])?
        pub trait FromLexicalWithOptions: lexical_util::num::Number {
            /// Custom formatting options for parsing a number.
            type Options: lexical_util::options::ParseOptions;

            /// Checked parser for a string-to-number conversion.
            ///
            /// This method parses the entire string, returning an error if
            /// any invalid digits are found during parsing. The parsing
            /// is dictated by the options, which specifies special
            /// float strings, required float components, digit separators,
            /// exponent characters, and more. Returns a [`Result`] containing
            /// either the parsed value, or an error containing any errors
            /// that occurred during parsing.
            ///
            /// * `FORMAT`  - Flags and characters designating the number grammar.
            /// * `bytes`   - Slice containing a numeric string.
            /// * `options` - Options to dictate number parsing.
            ///
            /// The `FORMAT` packed struct is built using [`NumberFormatBuilder`].
            /// Any invalid number format will prevent parsing, returning
            /// the appropriate format error. If you are unsure which format
            /// to use, use [`STANDARD`].
            ///
            /// # Examples
            ///
            /// ```rust
            #[doc = concat!("# assert_eq!(", stringify!($len), ", \"", stringify!($value), "\".len());")]
            #[doc = concat!("use ", $name, "::{format, FromLexicalWithOptions, ", stringify!($ops_t), "};")]
            ///
            /// const FORMAT: u128 = format::STANDARD;
            #[doc = concat!("const OPTIONS: ", stringify!($ops_t), " = ", stringify!($ops_t), "::new();")]
            #[doc = concat!("let value = \"", stringify!($value), "\";")]
            #[doc = concat!("let parsed = ", stringify!($t), "::from_lexical_with_options::<FORMAT>(value.as_bytes(), &OPTIONS);")]
            #[doc = concat!("assert_eq!(parsed, Ok(", stringify!($value), "));")]
            /// ```
            ///
            /// [`NumberFormatBuilder`]: lexical_util::format::NumberFormatBuilder
            /// [`STANDARD`]: lexical_util::format::STANDARD
            fn from_lexical_with_options<const FORMAT: u128>(
                bytes: &[u8],
                options: &Self::Options,
            ) -> lexical_util::result::Result<Self>;

            /// Checked parser for a string-to-number conversion.
            ///
            /// This method parses until an invalid digit is found (or the end
            /// of the string), returning the number of processed digits
            /// and the parsed value until that point. Returns a [`Result`]
            /// containing either the parsed value and the number of
            /// processed digits, or an error containing any errors that
            /// occurred during parsing.
            ///
            /// * `FORMAT`  - Flags and characters designating the number grammar.
            /// * `bytes`   - Slice containing a numeric string.
            /// * `options` - Options to dictate number parsing.
            ///
            /// The `FORMAT` packed struct is built using [`NumberFormatBuilder`].
            /// Any invalid number format will prevent parsing, returning
            /// the appropriate format error. If you are unsure which format
            /// to use, use [`STANDARD`].
            ///
            /// # Examples
            ///
            /// ```rust
            #[doc = concat!("# assert_eq!(", stringify!($len), ", \"", stringify!($value), "\".len());")]
            #[doc = concat!("use ", $name, "::{format, FromLexicalWithOptions, ", stringify!($ops_t), "};")]
            ///
            /// const FORMAT: u128 = format::STANDARD;
            #[doc = concat!("const OPTIONS: ", stringify!($ops_t), " = ", stringify!($ops_t), "::new();")]
            ///
            #[doc = concat!("let value = \"", stringify!($value), "\";")]
            #[doc = concat!(
                "let parsed = ",
                stringify!($t),
                "::from_lexical_partial_with_options::<FORMAT>(value.as_bytes(), &OPTIONS);"
            )]
            #[doc = concat!("assert_eq!(parsed, Ok((", stringify!($value), ", ", stringify!($len), ")));")]
            /// ```
            ///
            /// [`NumberFormatBuilder`]: lexical_util::format::NumberFormatBuilder
            /// [`STANDARD`]: lexical_util::format::STANDARD
            fn from_lexical_partial_with_options<const FORMAT: u128>(
                bytes: &[u8],
                options: &Self::Options,
            ) -> lexical_util::result::Result<(Self, usize)>;
        }
    };
}

// TO LEXICAL

/// Define the [`ToLexical`] trait.
///
/// * `name`: The name of the crate calling the function.
/// * `value`: A numerical value to use for the example.
/// * `t`: The type of the number for the example.
///
/// # Examples
///
/// ```rust,ignore
/// to_lexical!("lexical_core", 1234, u64);
/// ```
///
/// [`ToLexical`]: https://docs.rs/lexical-core/latest/lexical_core/trait.ToLexical.html
#[macro_export]
#[cfg(any(feature = "write-floats", feature = "write-integers"))]
macro_rules! to_lexical {
    ($name:literal, $value:literal, $t:ty $(, #[$attr:meta])? $(,)?) => {
        /// Trait for numerical types that can be serialized to bytes.
        ///
        /// To determine the number of bytes required to serialize a value to
        /// string, check the associated constants from a required trait:
        /// - [`FORMATTED_SIZE`]: The number of bytes required for any number for any
        ///   radix, that is, `2` to `36`.
        /// - [`FORMATTED_SIZE_DECIMAL`]: The number of bytes required for decimal (base
        ///   10) numbers.
        ///
        /// [`FORMATTED_SIZE`]: crate::FormattedSize::FORMATTED_SIZE
        /// [`FORMATTED_SIZE_DECIMAL`]: crate::FormattedSize::FORMATTED_SIZE_DECIMAL
        $(#[$attr])?
        pub trait ToLexical:
            lexical_util::constants::FormattedSize + lexical_util::num::Number
        {
            /// Serializer for a number-to-string conversion.
            ///
            /// Returns a subslice of the input buffer containing the written bytes,
            /// starting from the same address in memory as the input slice. That
            /// is, the `bytes` provided to the function and the returned buffer
            /// reference the same buffer, just with the number of elements truncated
            /// to the written digits.
            ///
            /// * `value`   - Number to serialize.
            /// * `bytes`   - Buffer to write number to.
            ///
            /// # Examples
            ///
            /// ```rust
            /// use core::str;
            ///
            #[doc = concat!("use ", $name, "::{format, FormattedSize, ToLexical};")]
            ///
            #[doc = concat!("let value: ", stringify!($t), " = ", stringify!($value), ";")]
            #[doc = concat!("let mut buffer = [0u8; ", stringify!($t), "::FORMATTED_SIZE_DECIMAL];")]
            /// let digits = value.to_lexical(&mut buffer);
            #[doc = concat!("assert_eq!(str::from_utf8(digits), Ok(\"", stringify!($value), "\"));")]
            /// ```
            ///
            /// # Panics
            ///
            /// Panics if the buffer is not of sufficient size. The caller
            /// must provide a slice of sufficient size. In order to ensure
            /// the function will not panic, ensure the buffer has at least
            /// [`FORMATTED_SIZE_DECIMAL`] elements.
            ///
            /// [`FORMATTED_SIZE_DECIMAL`]: lexical_util::constants::FormattedSize::FORMATTED_SIZE_DECIMAL
            fn to_lexical<'a>(self, bytes: &'a mut [u8]) -> &'a mut [u8];
        }
    };
}

/// Define the [`ToLexicalWithOptions`] trait.
///
/// * `name`: The name of the crate calling the function.
/// * `value`: A numerical value to use for the example.
/// * `t`: The type of the number for the example.
/// * `ops_t`: The options type.
///
/// # Examples
///
/// ```rust,ignore
/// to_lexical_with_options!("lexical_core", 1234, u64, WriteIntegerOptions);
/// ```
///
/// [`ToLexicalWithOptions`]: https://docs.rs/lexical-core/latest/lexical_core/trait.ToLexicalWithOptions.html
#[macro_export]
#[cfg(any(feature = "write-floats", feature = "write-integers"))]
macro_rules! to_lexical_with_options {
    ($name:literal, $value:literal, $t:ty, $ops_t:ty $(, #[$attr:meta])? $(,)?) => {
        /// Trait for numerical types that can be serialized to bytes with custom
        /// options.
        ///
        /// To determine the number of bytes required to serialize a value to
        /// string, check the associated constants from a required trait:
        /// - [`FORMATTED_SIZE`]: The number of bytes required for any number for any
        ///   radix, that is, `2` to `36`.
        /// - [`FORMATTED_SIZE_DECIMAL`]: The number of bytes required for decimal (base
        ///   10) numbers.
        ///
        /// The [`Options`][Self::Options] type specifies the configurable options to provide.
        ///
        /// [`FORMATTED_SIZE`]: crate::FormattedSize::FORMATTED_SIZE
        /// [`FORMATTED_SIZE_DECIMAL`]: crate::FormattedSize::FORMATTED_SIZE_DECIMAL
        $(#[$attr])?
        pub trait ToLexicalWithOptions:
            lexical_util::constants::FormattedSize + lexical_util::num::Number
        {
            /// Custom formatting options for writing a number.
            type Options: lexical_util::options::WriteOptions;

            /// Serializer for a number-to-string conversion.
            ///
            /// Returns a subslice of the input buffer containing the written bytes,
            /// starting from the same address in memory as the input slice. That
            /// is, the `bytes` provided to the function and the returned buffer
            /// reference the same buffer, just with the number of elements truncated
            /// to the written digits.
            ///
            /// * `FORMAT`  - Flags and characters designating the number grammar.
            /// * `value`   - Number to serialize.
            /// * `bytes`   - Buffer to write number to.
            /// * `options` - Options for number formatting.
            ///
            /// `FORMAT` should be built using [`NumberFormatBuilder`] and includes
            /// options such as the numerical radix for writing the value to string.
            /// `options` specificies extra, additional configurations such as
            /// special values like `NaN` or `+Infinity` for how to serialize
            /// the number.
            ///
            /// [`NumberFormatBuilder`]: crate::NumberFormatBuilder
            ///
            /// # Examples
            ///
            /// ```rust
            /// use core::str;
            ///
            #[doc = concat!(
                "use ",
                $name,
                "::{format, FormattedSize, ",
                stringify!($ops_t),
                ", ToLexicalWithOptions};"
            )]
            ///
            /// const FORMAT: u128 = format::STANDARD;
            #[doc = concat!("const OPTIONS: ", stringify!($ops_t), " = ", stringify!($ops_t), "::new();")]
            #[doc = concat!(
                "const BUFFER_SIZE: usize = OPTIONS.buffer_size_const::<",
                stringify!($t),
                ", FORMAT>();"
            )]
            ///
            #[doc = concat!("let value: ", stringify!($t), " = ", stringify!($value), ";")]
            /// let mut buffer = [0u8; BUFFER_SIZE];
            /// let digits = value.to_lexical_with_options::<FORMAT>(&mut buffer, &OPTIONS);
            #[doc = concat!("assert_eq!(str::from_utf8(digits), Ok(\"", stringify!($value), "\"));")]
            /// ```
            ///
            /// # Panics
            ///
            /// Panics if the buffer is not of sufficient size. The caller
            /// must provide a slice of sufficient size. In order to ensure
            /// the function will not panic, ensure the buffer has at least
            /// [`Options::buffer_size_const`] elements. This is required
            /// only when changing the number of significant digits, the
            /// exponent break point, or disabling scientific notation.
            ///
            /// If you are not using [`min_significant_digits`] (floats only),
            /// 1200 bytes is always enough to hold the the output for a custom
            /// radix, and `400` is always enough for decimal strings.
            ///
            /// **Floats Only**
            ///
            /// These panics are only when using uncommon features for float
            /// writing, represent configuration errors, so runtime error
            /// handling is not provided.
            ///
            /// Also panics if the provided number format is invalid, or
            /// if the mantissa radix is not equal to the exponent base
            /// and the mantissa radix/exponent base combinations are
            /// not in the following list:
            ///
            /// - `4, 2`
            /// - `8, 2`
            /// - `16, 2`
            /// - `32, 2`
            /// - `16, 4`
            ///
            /// Panics as well if `the` NaN or `Inf` string provided to the writer
            /// is disabled, but the value provided is `NaN` or `Inf`, respectively.
            ///
            #[doc = concat!(
                "[`Options::buffer_size_const`]: crate::",
                stringify!($ops_t),
                "::buffer_size_const"
            )]
            /// [`FORMATTED_SIZE`]: crate::FormattedSize::FORMATTED_SIZE
            /// [`min_significant_digits`]: https://docs.rs/lexical-core/latest/lexical_core/struct.WriteFloatOptionsBuilder.html#method.min_significant_digits
            fn to_lexical_with_options<'a, const FORMAT: u128>(
                self,
                bytes: &'a mut [u8],
                options: &Self::Options,
            ) -> &'a mut [u8];
        }
    };
}

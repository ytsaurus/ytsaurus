//! Pre-built formats for each programming language,

#![cfg(feature = "format")]

use core::num;

use crate::format::NumberFormatBuilder;

// FIXME

// Sample test code for each language used:
//
//  Rust
//  ----
//
//  Setup:
//      Save to `main.rs` and run `rustc main.rs -o main`.
//
//  Code:
//      ```text
//      pub fn main() {
//          println!("{:?}", 3_.0f32);
//          println!("{:?}", "3_.0".parse::<f32>());
//      }
//      ```
//
// Python
// ------
//
//  Setup:
//      Run `python` to enter the interpreter.
//
//  Code:
//      ```text
//      print(3_.0)
//      print(float("3_.0"))
//      ```
//
//  C++
//  ---
//
//  Setup:
//      Save to `main.cc` and run `g++ main.cc -o main -std=c++XX`,
//      where XX is one of the following values:
//          - 98
//          - 03
//          - 11
//          - 14
//          - 17
//
//  Code:
//      ```text
//      #include <cstdlib>
//      #include <cstring>
//      #include <iostream>
//      #include <iterator>
//      #include <stdexcept>
//
//      double parse(const char* string) {
//          char* end;
//          double result = strtod(string, &end);
//          auto endp = reinterpret_cast<const char*>(end);
//          if (std::distance(string, endp) != strlen(string)) {
//              throw std::invalid_argument("did not consume entire string.");
//          }
//          return result;
//      }
//
//      int main() {
//          std::cout << 3'.0 << std::endl;
//          std::cout << parse("3'.0") << std::endl;
//      }
//      ```
//
//  C
//  -
//
//  Setup:
//      Save to `main.c` and run `gcc main.c -o main -std=cXX`,
//      where XX is one of the following values:
//          - 89
//          - 90
//          - 99
//          - 11
//          - 18
//
//  Code:
//      ```text
//      #include <stdint.h>
//      #include <stdlib.h>
//      #include <string.h>
//      #include <stdio.h>
//
//      size_t distance(const char* first, const char* last) {
//          uintptr_t x = (uintptr_t) first;
//          uintptr_t y = (uintptr_t) last;
//          return (size_t) (y - x);
//      }
//
//      double parse(const char* string) {
//          char* end;
//          double result = strtod(string, &end);
//          if (distance(string, (const char*) end) != strlen(string)) {
//              abort();
//          }
//          return result;
//      }
//
//      int main() {
//          printf("%f\n", 3'.);
//          printf("%f\n", parse("3'."));
//      }
//      ```
//
// Ruby
// ----
//
//  Setup:
//      Run `irb` to enter the interpreter.
//
//  Code:
//      ```text
//      puts 3.0_1;
//      puts "3.0_1".to_f;
//      ```
// Swift
// -----
//
//  Setup:
//      Run `swift` to enter the interpreter.
//
//  Code:
//      ```text
//      print(3.0);
//      print(Float("3.0"));
//      ```
// Golang
// ------
//
// Setup:
//      Save to `main.go` and run `go run main.go`
//
// Code:
//      ```text
//      package main
//
//      import (
//          "fmt"
//          "strconv"
//      )
//
//      func main() {
//          fmt.Println(3.0)
//          fmt.Println(strconv.ParseFloat("3.0", 64))
//      }
//      ```
//
// Haskell
// -------
//
// Setup:
//      Run `ghci` to enter the interpreter.
//
// Code:
//      ```text
//      :m Numeric
//      showFloat 3.0 ""
//      let x = "3.0"
//      read x :: Float
//      ```
//
// Javascript
// ----------
//
// Setup:
//      Run `nodejs` (or `node`) to enter the interpreter.
//
// Code:
//      ```text
//          console.log(3.0)
//          console.log(parseFloat("3.0"))
//      ```
//
// Perl
// ----
//
// Setup:
//      Run `perl -de1` to enter the interpret.
//
// Code:
//      ```text
//      print 3.01;
//      print '3.01' * 1;
//      ```
//
// PHP
// ---
//
// Setup:
//      Run `php -a` to enter the interpret.
//
// Code:
//      ```text
//      printf("%f\n", 3.0);
//      printf("%f\n", floatval("3.0"));
//      ```
//
// Java
// ----
//
// Setup:
//      Save to `main.java` and run `javac main.java`, then run `java Main`.
//
// Code:
//      ```text
//      class Main {
//          public static void main(String args[]) {
//              System.out.println(3.0);
//              System.out.println(Float.parseFloat("3.0"));
//          }
//      }
//      ```
//
// R
// -
//
// Setup:
//      Run `R` to enter the interpret.
//
// Code:
//      ```text
//      print(3.0);
//      print(as.numeric("3.0"));
//      ```
//
// Kotlin
// ------
//
// Setup:
//      Save file to `main.kt` and run `kotlinc main.kt -d main.jar`,
//      then run `java -jar main.jar`.
//
// Code:
//      ```text
//      fun main() {
//          println(3.0)
//          println("3.0".toDouble())
//      }
//      ```
//
// Julia
// -----
//
// Setup:
//      Run `julia` to enter the interpret.
//
// Code:
//      ```text
//      print(3.0);
//      print(parse(Float64, "3.0"));
//      ```
//
// C#
// --
//
// Note:
//      Mono accepts both integer and fraction decimal separators, Mono is
//      just buggy, see https://github.com/dotnet/csharplang/issues/55#issuecomment-574902516.
//
// Setup:
//      Run `csharp -langversion:X` to enter the interpret,
//      where XX is one of the following values:
//          - ISO-1
//          - ISO-2
//          - 3
//          - 4
//          - 5
//          - 6
//          - 7
//
// Code:
//      ```text
//      Console.WriteLine("{0}", 3.0);
//      Console.WriteLine("{0}", float.Parse("3.0"));
//      ```
//
// Kawa
// ----
//
// Setup:
//      Run `kawa` to enter the interpreter.
//
// Code:
//      ```text
//      3.0
//      (string->number "3.0")
//      ```
//
// Gambit-C
// --------
//
// Setup:
//      Run `gsc` to enter the interpreter.
//
// Code:
//      ```text
//      3.0
//      (string->number "3.0")
//      ```
//
// Guile
// -----
//
// Setup:
//      Run `guile` to enter the interpreter.
//
// Code:
//      ```text
//      3.0
//      (string->number "3.0")
//      ```
//
// Clojure
// -------
//
// Setup:
//      Run `clojure` to enter the interpreter.
//
// Code:
//      ```text
//      3.0
//      (Float/parseFloat "3.0")
//      ```
//
// Erlang
// ------
//
// Setup:
//      Run `erl` to enter the interpreter.
//
// Code:
//      ```text
//      io:format("~p~n", [3.0]).
//      string:to_float("3.0").
//      ```
//
// Elm
// ---
//
// Setup:
//      Run `elm repl` to enter the interpreter.
//
// Code:
//      ```text
//      3.0
//      String.toFloat "3.0"
//      ```
//
// Scala
// -----
//
// Setup:
//      Run `scala` to enter the interpreter.
//
// Code:
//      ```text
//      3.0
//      "3.0".toFloat
//      ```
//
// Elixir
// ------
//
// Setup:
//      Run `iex` to enter the interpreter.
//
// Code:
//      ```text
//      3.0;
//      String.to_float("3.0");
//      ```
//
// FORTRAN
// -------
//
// Setup:
//      Save to `main.f90` and run `gfortran -o main main.f90`
//
// Code:
//      ```text
//      program main
//        real :: x
//        character (len=30) :: word
//        word = "3."
//        read(word, *) x
//        print *, 3.
//        print *, x
//      end program main
//      ```
//
// D
// -
//
// Setup:
//      Save to `main.d` and run `dmd -run main.d`
//
// Code:
//      ```text
//      import std.conv;
//      import std.stdio;
//
//      void main()
//      {
//          writeln(3.0);
//          writeln(to!double("3.0"));
//      }
//      ```
//
// Coffeescript
// ------------
//
// Setup:
//      Run `coffee` to enter the interpreter.
//
// Code:
//      ```text
//      3.0;
//      parseFloat("3.0");
//      ```
//
// Cobol
// -----
//
// Setup:
//      Save to `main.cbl` and run `cobc main.cbl` then `cobcrun main`.
//
// Code:
//      ```text
//                IDENTIFICATION DIVISION.
//                PROGRAM-ID. main.
//
//                DATA DIVISION.
//                   WORKING-STORAGE SECTION.
//                   01 R PIC X(20)   VALUE "3.0".
//                   01 TOTAL        USAGE IS COMP-2.
//
//                PROCEDURE DIVISION.
//                   COMPUTE TOTAL = FUNCTION NUMVAL(R).
//                   Display 3.0.
//                   Display TOTAL.
//                   STOP RUN.
//      ```
//
// F#
// --
//
// Setup:
//      Run `dotnet fsi` to enter the interpreter.
//
// Code:
//      ```text
//      printfn "%f" 3.0;;
//      let f = float "3.0";;
//      printfn "%f" f;;
//      ```
//
// Visual Basic
// ------------
//
// Setup:
//      Save to `main.vb` and run `vbnc main.vb`.
//
// Code:
//      ```text
//      Imports System
//
//      Module Module1
//          Sub Main()
//              Console.WriteLine(Format$(3.0, "0.0000000000000"))
//              Console.WriteLine(Format$(CDbl("3.0"), "0.0000000000000"))
//          End Sub
//      End Module
//      ```
//
// OCaml
// -----
//
// Setup:
//      Save to `main.ml` and run `ocamlc -o main main.ml`.
//
// Code:
//      ```text
//      Printf.printf "%f\n" 3.0
//      let () =
//          let f = float_of_string "3.0" in
//          Printf.printf "%f\n" f
//      ```
//
// Objective-C
// -----------
//
// Setup:
//      Save to `main.m` and run `gcc -o main -lobjc -lgnustep-base main.m
// -fconstant-string-class=NSConstantString`.
//
// Code:
//      ```text
//      #import <Foundation/Foundation.h>
//      #import <stdio.h>
//
//      int main(int argv, char* argc[])
//      {
//          printf("%f\n", 3.0);
//          NSString *s = @"3.0";
//          double f = [s doubleValue];
//          printf("%f\n", f);
//      }
//      ```
//
// ReasonML
// --------
//
// Setup:
//      Run `rtop` to enter the interpreter.
//
// Code:
//      ```text
//      Printf.printf("%f\n", 3.0);
//      Printf.printf("%f\n", float_of_string("3.0"));
//      ```
//
// Zig
// ---
//
// Setup:
//      Save to `main.zig` and run `zig build-exe main.zig`
//
// Code:
//      ```text
//      const std = @import("std");
//
//      pub fn main() void {
//          const f: f64 = 3.0;
//          std.debug.warn("{}\n", f);
//          const x: f64 = std.fmt.parseFloat(f64, "3.0") catch unreachable;
//          std.debug.warn("{}\n", x);
//      }
//      ```
//
//
// Octave (and Matlab)
// -------------------
//
// Setup:
//      Run `octave` to enter the interpreter, or
//      run `octave --traditional` to enter the Matlab interpret.
//
// Code:
//      ```text
//      3.0
//      str2double("3.0")
//      ```
//
// Sage
// ----
//
// Setup:
//      Run `sage` to enter the interpreter.
//
// Code:
//      ```text
//      3.0
//      float("3.0")
//      ```
//
// JSON
// ----
//
// Setup:
//      Run `node` (or `nodejs`) to enter the JS interpreter.
//
// Code:
//      ```text
//      JSON.parse("3.0")
//      ```
//
// TOML
// ----
//
// Setup:
//      Run `python` to enter the Python interpreter.
//
// Code:
//      ```text
//      import tomlkit
//      tomlkit.parse("a = 3.0")
//      ```
//
// XML
// ---
//
// Setup:
//      Run `python` to enter the Python interpreter.
//
// Code:
//      ```text
//      from lxml import etree
//
//      def validate_xml(xsd, xml):
//          '''Validate XML file against schema'''
//
//          schema = etree.fromstring(xsd)
//          doc = etree.fromstring(xml)
//          xmlschema = etree.XMLSchema(schema)
//
//          return xmlschema.validate(doc)
//
//
//      xsd = b'''<?xml version="1.0" encoding="UTF-8"?>
//      <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
//          <xs:element name="prize" type="xs:float"/>
//      </xs:schema>'''
//
//      xml = b'''<?xml version="1.0" encoding="UTF-8"?>
//      <prize>3.0</prize>
//      '''
//
//      validate_xml(xsd, xml)
//      ```
//
// SQLite
// ------
//
// Setup:
//      Run `sqlite3 :memory:` to enter the sqlite3 interpreter
//      with an in-memory database.
//
// Code:
//      ```text
//      CREATE TABLE stocks (price real);
//      INSERT INTO stocks VALUES (3.0);
//      SELECT * FROM stocks;
//      ```
//
// PostgreSQL
// ----------
//
// Setup:
//      Run `initdb -D db` to create a database data direction,
//      then run `pg_ctl -D db start` to start the server, then run
//      `createdb` to create a user database and `psql` to start the
//      interpreter.
//
// Code:
//      ```text
//      CREATE TABLE stocks (price real);
//      INSERT INTO stocks VALUES (3.0);
//      SELECT * FROM stocks;
//      ```
//
// MySQL
// -----
//
// Setup:
//      Run `mysqld` to start the server, then run `mysql` to start the
//      interpreter.
//
// Code:
//      ```text
//      USE mysql;
//      CREATE TABLE stocks (price real);
//      INSERT INTO stocks VALUES (3.0);
//      SELECT * FROM stocks;
//      ```
//
// MongoDB
// -------
//
// Setup:
//      Run `mongod --dbpath data/db` to start the server, then run
//      `mongo` to start the interpreter.
//
// Code:
//      ```text
//      use mydb
//      db.movie.insert({"name": 3.0})
//      db.movie.find()
//      ```

// PRE-DEFINED CONSTANTS
// ---------------------
//
// Sample Format Shorthand:
// ------------------------
//
// The format shorthand lists the test cases, and if applicable,
// the digit separator character. For example, the shorthand
// `[134-_]` specifies it passes tests 1, 3, and 4, and uses
// `'_'` as a digit-separator character. Meanwhile, `[0]` means it
// passes test 0, and has no digit separator.

// RUST LITERAL [4569ABFGHIJKMN-_]
/// Number format for a [`Rust`] literal floating-point number.
///
/// [`Rust`]: https://www.rust-lang.org/
#[rustfmt::skip]
pub const RUST_LITERAL: u128 = NumberFormatBuilder::new()
    .digit_separator(num::NonZeroU8::new(b'_'))
    .required_digits(true)
    .no_positive_mantissa_sign(true)
    .no_special(true)
    .internal_digit_separator(true)
    .trailing_digit_separator(true)
    .consecutive_digit_separator(true)
    .build_strict();

// RUST STRING [0134567MN]
/// Number format to parse a [`Rust`] float from string.
///
/// [`Rust`]: https://www.rust-lang.org/
#[rustfmt::skip]
pub const RUST_STRING: u128 = NumberFormatBuilder::new().build_strict();

/// Number format for a [`Python`] literal floating-point number.
///
/// [`Python`]: https://www.python.org/
pub const PYTHON_LITERAL: u128 = PYTHON3_LITERAL;

/// Number format to parse a [`Python`] float from string.
///
/// [`Python`]: https://www.python.org/
pub const PYTHON_STRING: u128 = PYTHON3_STRING;

/// Number format for a [`Python3`] literal floating-point number.
///
/// [`Python3`]: https://www.python.org/
pub const PYTHON3_LITERAL: u128 = PYTHON36_LITERAL;

// PYTHON3 STRING [0134567MN]
/// Number format to parse a [`Python3`] float from string.
///
/// [`Python3`]: https://www.python.org/
#[rustfmt::skip]
pub const PYTHON3_STRING: u128 = NumberFormatBuilder::new().build_strict();

// PYTHON3.6+ LITERAL [013456N-_]
/// Number format for a [`Python3.6`] or higher literal floating-point number.
///
/// [`Python3.6`]: https://www.python.org/downloads/release/python-360/
#[rustfmt::skip]
pub const PYTHON36_LITERAL: u128 = NumberFormatBuilder::new()
    .digit_separator(num::NonZeroU8::new(b'_'))
    .no_special(true)
    .no_integer_leading_zeros(true)
    .internal_digit_separator(true)
    .build_strict();

// PYTHON3.5- LITERAL [013456N]
/// Number format for a [`Python3.5`] or lower literal floating-point number.
///
/// [`Python3.5`]: https://www.python.org/downloads/release/python-350/
#[rustfmt::skip]
pub const PYTHON35_LITERAL: u128 = NumberFormatBuilder::new()
    .no_special(true)
    .no_integer_leading_zeros(true)
    .build_strict();

// PYTHON2 LITERAL [013456MN]
/// Number format for a [`Python2`] literal floating-point number.
///
/// [`Python2`]: https://www.python.org/downloads/release/python-270/
#[rustfmt::skip]
pub const PYTHON2_LITERAL: u128 = NumberFormatBuilder::new()
    .no_special(true)
    .build_strict();

// PYTHON2 STRING [0134567MN]
/// Number format to parse a [`Python2`] float from string.
///
/// [`Python2`]: https://www.python.org/downloads/release/python-270/
#[rustfmt::skip]
pub const PYTHON2_STRING: u128 = NumberFormatBuilder::new().build_strict();

/// Number format for a [`C++`] literal floating-point number.
///
/// [`C++`]: https://en.cppreference.com/w/
pub const CXX_LITERAL: u128 = CXX20_LITERAL;

/// Number format to parse a [`C++`] float from string.
///
/// [`C++`]: https://en.cppreference.com/w/
pub const CXX_STRING: u128 = CXX20_STRING;

/// Number format for a [`C++`] literal hexadecimal floating-point number.
///
/// [`C++`]: https://en.cppreference.com/w/
#[cfg(feature = "power-of-two")]
pub const CXX_HEX_LITERAL: u128 = CXX20_HEX_LITERAL;

/// Number format to parse a [`C++`] hexadecimal float from string.
///
/// [`C++`]: https://en.cppreference.com/w/
#[cfg(feature = "power-of-two")]
pub const CXX_HEX_STRING: u128 = CXX20_HEX_STRING;

// C++20 LITERAL [013456789ABMN-']
/// Number format for a [`C++20`] literal floating-point number.
///
/// [`C++20`]: https://en.cppreference.com/w/cpp/20
#[rustfmt::skip]
pub const CXX20_LITERAL: u128 = NumberFormatBuilder::new()
    .digit_separator(num::NonZeroU8::new(b'\''))
    .case_sensitive_special(true)
    .internal_digit_separator(true)
    .build_strict();

// C++20 STRING [0134567MN]
/// Number format for a [`C++20`] string floating-point number.
///
/// [`C++20`]: https://en.cppreference.com/w/cpp/20
#[rustfmt::skip]
pub const CXX20_STRING: u128 = NumberFormatBuilder::new().build_strict();

// C++20 HEX LITERAL [013456789ABMN-']
/// Number format for a [`C++20`] literal hexadecimal floating-point number.
///
/// [`C++20`]: https://en.cppreference.com/w/cpp/20
#[rustfmt::skip]
#[cfg(feature = "power-of-two")]
pub const CXX20_HEX_LITERAL: u128 = NumberFormatBuilder::new()
    .required_exponent_notation(true)
    .digit_separator(num::NonZeroU8::new(b'\''))
    .mantissa_radix(16)
    .exponent_base(num::NonZeroU8::new(2))
    .exponent_radix(num::NonZeroU8::new(10))
    .case_sensitive_special(true)
    .internal_digit_separator(true)
    .build_strict();

// C++20 HEX STRING [0134567MN]
/// Number format for a [`C++20`] string hexadecimal floating-point number.
///
/// [`C++20`]: https://en.cppreference.com/w/cpp/20
#[rustfmt::skip]
#[cfg(feature = "power-of-two")]
pub const CXX20_HEX_STRING: u128 = NumberFormatBuilder::new()
    .mantissa_radix(16)
    .exponent_base(num::NonZeroU8::new(2))
    .exponent_radix(num::NonZeroU8::new(10))
    .build_strict();

// C++17 LITERAL [013456789ABMN-']
/// Number format for a [`C++17`] literal floating-point number.
///
/// [`C++17`]: https://en.cppreference.com/w/cpp/17
#[rustfmt::skip]
pub const CXX17_LITERAL: u128 = NumberFormatBuilder::new()
    .digit_separator(num::NonZeroU8::new(b'\''))
    .case_sensitive_special(true)
    .internal_digit_separator(true)
    .build_strict();

// C++17 STRING [0134567MN]
/// Number format for a [`C++17`] string floating-point number.
///
/// [`C++17`]: https://en.cppreference.com/w/cpp/17
#[rustfmt::skip]
pub const CXX17_STRING: u128 = NumberFormatBuilder::new().build_strict();

// C++17 HEX LITERAL [013456789ABMN-']
/// Number format for a [`C++17`] literal hexadecimal floating-point number.
///
/// [`C++17`]: https://en.cppreference.com/w/cpp/17
#[rustfmt::skip]
#[cfg(feature = "power-of-two")]
pub const CXX17_HEX_LITERAL: u128 = NumberFormatBuilder::new()
    .required_exponent_notation(true)
    .digit_separator(num::NonZeroU8::new(b'\''))
    .mantissa_radix(16)
    .exponent_base(num::NonZeroU8::new(2))
    .exponent_radix(num::NonZeroU8::new(10))
    .case_sensitive_special(true)
    .internal_digit_separator(true)
    .build_strict();

// C++17 HEX STRING [0134567MN]
/// Number format for a [`C++17`] string hexadecimal floating-point number.
///
/// [`C++17`]: https://en.cppreference.com/w/cpp/17
#[rustfmt::skip]
#[cfg(feature = "power-of-two")]
pub const CXX17_HEX_STRING: u128 = NumberFormatBuilder::new()
    .mantissa_radix(16)
    .exponent_base(num::NonZeroU8::new(2))
    .exponent_radix(num::NonZeroU8::new(10))
    .build_strict();

// C++14 LITERAL [013456789ABMN-']
/// Number format for a [`C++14`] literal floating-point number.
///
/// [`C++14`]: https://en.cppreference.com/w/cpp/14
#[rustfmt::skip]
pub const CXX14_LITERAL: u128 = NumberFormatBuilder::new()
    .digit_separator(num::NonZeroU8::new(b'\''))
    .case_sensitive_special(true)
    .internal_digit_separator(true)
    .build_strict();

// C++14 STRING [0134567MN]
/// Number format for a [`C++14`] string floating-point number.
///
/// [`C++14`]: https://en.cppreference.com/w/cpp/14
#[rustfmt::skip]
pub const CXX14_STRING: u128 = NumberFormatBuilder::new().build_strict();

// C++14 HEX STRING [0134567MN]
/// Number format for a [`C++14`] string hexadecimal floating-point number.
///
/// [`C++14`]: https://en.cppreference.com/w/cpp/14
#[rustfmt::skip]
#[cfg(feature = "power-of-two")]
pub const CXX14_HEX_STRING: u128 = NumberFormatBuilder::new()
    .mantissa_radix(16)
    .exponent_base(num::NonZeroU8::new(2))
    .exponent_radix(num::NonZeroU8::new(10))
    .build_strict();

// C++11 LITERAL [01345678MN]
/// Number format for a [`C++11`] literal floating-point number.
///
/// [`C++11`]: https://en.cppreference.com/w/cpp/11
#[rustfmt::skip]
pub const CXX11_LITERAL: u128 = NumberFormatBuilder::new()
    .case_sensitive_special(true)
    .build_strict();

// C++11 STRING [0134567MN]
/// Number format for a [`C++11`] string floating-point number.
///
/// [`C++11`]: https://en.cppreference.com/w/cpp/11
#[rustfmt::skip]
pub const CXX11_STRING: u128 = NumberFormatBuilder::new().build_strict();

// C++11 HEX STRING [0134567MN]
/// Number format for a [`C++11`] string hexadecimal floating-point number.
///
/// [`C++11`]: https://en.cppreference.com/w/cpp/11
#[rustfmt::skip]
#[cfg(feature = "power-of-two")]
pub const CXX11_HEX_STRING: u128 = NumberFormatBuilder::new()
    .mantissa_radix(16)
    .exponent_base(num::NonZeroU8::new(2))
    .exponent_radix(num::NonZeroU8::new(10))
    .build_strict();

// C++03 LITERAL [01345678MN]
/// Number format for a [`C++03`] literal floating-point number.
///
/// [`C++03`]: https://en.wikipedia.org/wiki/C%2B%2B03
#[rustfmt::skip]
pub const CXX03_LITERAL: u128 = NumberFormatBuilder::new()
    .case_sensitive_special(true)
    .build_strict();

// C++03 STRING [0134567MN]
/// Number format for a [`C++03`] string floating-point number.
///
/// [`C++03`]: https://en.wikipedia.org/wiki/C%2B%2B03
#[rustfmt::skip]
pub const CXX03_STRING: u128 = NumberFormatBuilder::new().build_strict();

// C++98 LITERAL [01345678MN]
/// Number format for a [`C++98`] literal floating-point number.
///
/// [`C++98`]: https://en.cppreference.com/w/
#[rustfmt::skip]
pub const CXX98_LITERAL: u128 = NumberFormatBuilder::new()
    .case_sensitive_special(true)
    .build_strict();

// C++98 STRING [0134567MN]
/// Number format for a [`C++98`] string floating-point number.
///
/// [`C++98`]: https://en.cppreference.com/w/
#[rustfmt::skip]
pub const CXX98_STRING: u128 = NumberFormatBuilder::new().build_strict();

/// Number format for a [`C`] literal floating-point number.
///
/// [`C`]: https://en.cppreference.com/w/c
pub const C_LITERAL: u128 = C18_LITERAL;

/// Number format to parse a [`C`] float from string.
///
/// [`C`]: https://en.cppreference.com/w/c
pub const C_STRING: u128 = C18_STRING;

/// Number format for a [`C`] literal hexadecimal floating-point number.
///
/// [`C`]: https://en.cppreference.com/w/c
#[cfg(feature = "power-of-two")]
pub const C_HEX_LITERAL: u128 = C18_HEX_LITERAL;

/// Number format to parse a [`C`] hexadecimal float from string.
///
/// [`C`]: https://en.cppreference.com/w/c
#[cfg(feature = "power-of-two")]
pub const C_HEX_STRING: u128 = C18_HEX_STRING;

// C18 LITERAL [01345678MN]
/// Number format for a [`C18`] literal floating-point number.
///
/// [`C18`]: https://en.cppreference.com/w/c/17
#[rustfmt::skip]
pub const C18_LITERAL: u128 = NumberFormatBuilder::new()
    .case_sensitive_special(true)
    .build_strict();

// C18 STRING [0134567MN]
/// Number format for a [`C18`] string floating-point number.
///
/// [`C18`]: https://en.cppreference.com/w/c/17
#[rustfmt::skip]
pub const C18_STRING: u128 = NumberFormatBuilder::new().build_strict();

// C18 HEX LITERAL [01345678MN]
/// Number format for a [`C18`] literal hexadecimal floating-point number.
///
/// [`C18`]: https://en.cppreference.com/w/c/17
#[rustfmt::skip]
#[cfg(feature = "power-of-two")]
pub const C18_HEX_LITERAL: u128 = NumberFormatBuilder::new()
    .case_sensitive_special(true)
    .required_exponent_notation(true)
    .mantissa_radix(16)
    .exponent_base(num::NonZeroU8::new(2))
    .exponent_radix(num::NonZeroU8::new(10))
    .build_strict();

// C18 HEX STRING [0134567MN]
/// Number format for a [`C18`] string hexadecimal floating-point number.
///
/// [`C18`]: https://en.cppreference.com/w/c/17
#[rustfmt::skip]
#[cfg(feature = "power-of-two")]
pub const C18_HEX_STRING: u128 = NumberFormatBuilder::new()
    .mantissa_radix(16)
    .exponent_base(num::NonZeroU8::new(2))
    .exponent_radix(num::NonZeroU8::new(10))
    .build_strict();

// C11 LITERAL [01345678MN]
/// Number format for a [`C11`] literal floating-point number.
///
/// [`C11`]: https://en.cppreference.com/w/c/11
#[rustfmt::skip]
pub const C11_LITERAL: u128 = NumberFormatBuilder::new()
    .case_sensitive_special(true)
    .build_strict();

// C11 STRING [0134567MN]
/// Number format for a [`C11`] string floating-point number.
///
/// [`C11`]: https://en.cppreference.com/w/c/11
#[rustfmt::skip]
pub const C11_STRING: u128 = NumberFormatBuilder::new().build_strict();

// C11 HEX LITERAL [01345678MN]
/// Number format for a [`C11`] literal hexadecimal floating-point number.
///
/// [`C11`]: https://en.cppreference.com/w/c/11
#[rustfmt::skip]
#[cfg(feature = "power-of-two")]
pub const C11_HEX_LITERAL: u128 = NumberFormatBuilder::new()
    .case_sensitive_special(true)
    .required_exponent_notation(true)
    .mantissa_radix(16)
    .exponent_base(num::NonZeroU8::new(2))
    .exponent_radix(num::NonZeroU8::new(10))
    .build_strict();

// C11 HEX STRING [0134567MN]
/// Number format for a [`C11`] string hexadecimal floating-point number.
///
/// [`C11`]: https://en.cppreference.com/w/c/11
#[rustfmt::skip]
#[cfg(feature = "power-of-two")]
pub const C11_HEX_STRING: u128 = NumberFormatBuilder::new()
    .mantissa_radix(16)
    .exponent_base(num::NonZeroU8::new(2))
    .exponent_radix(num::NonZeroU8::new(10))
    .build_strict();

// C99 LITERAL [01345678MN]
/// Number format for a [`C99`] literal floating-point number.
///
/// [`C99`]: https://en.cppreference.com/w/c/99
#[rustfmt::skip]
pub const C99_LITERAL: u128 = NumberFormatBuilder::new()
    .case_sensitive_special(true)
    .build_strict();

// C99 STRING [0134567MN]
/// Number format for a [`C99`] string floating-point number.
///
/// [`C99`]: https://en.cppreference.com/w/c/99
#[rustfmt::skip]
pub const C99_STRING: u128 = NumberFormatBuilder::new().build_strict();

// C99 HEX LITERAL [01345678MN]
/// Number format for a [`C99`] literal hexadecimal floating-point number.
///
/// [`C99`]: https://en.cppreference.com/w/c/99
#[rustfmt::skip]
#[cfg(feature = "power-of-two")]
pub const C99_HEX_LITERAL: u128 = NumberFormatBuilder::new()
    .case_sensitive_special(true)
    .required_exponent_notation(true)
    .mantissa_radix(16)
    .exponent_base(num::NonZeroU8::new(2))
    .exponent_radix(num::NonZeroU8::new(10))
    .build_strict();

// C99 HEX STRING [0134567MN]
/// Number format for a [`C99`] string hexadecimal floating-point number.
///
/// [`C99`]: https://en.cppreference.com/w/c/99
#[rustfmt::skip]
#[cfg(feature = "power-of-two")]
pub const C99_HEX_STRING: u128 = NumberFormatBuilder::new()
    .mantissa_radix(16)
    .exponent_base(num::NonZeroU8::new(2))
    .exponent_radix(num::NonZeroU8::new(10))
    .build_strict();

// C90 LITERAL [013456MN]
/// Number format for a [`C90`] literal floating-point number.
///
/// [`C90`]: https://en.cppreference.com/w/c
#[rustfmt::skip]
pub const C90_LITERAL: u128 = NumberFormatBuilder::new()
    .no_special(true)
    .build_strict();

// C90 STRING [0134567MN]
/// Number format for a [`C90`] string floating-point number.
///
/// [`C90`]: https://en.cppreference.com/w/c
#[rustfmt::skip]
pub const C90_STRING: u128 = NumberFormatBuilder::new().build_strict();

// C90 HEX STRING [0134567MN]
/// Number format for a [`C90`] string hexadecimal floating-point number.
///
/// [`C90`]: https://en.cppreference.com/w/c
#[rustfmt::skip]
#[cfg(feature = "power-of-two")]
pub const C90_HEX_STRING: u128 = NumberFormatBuilder::new()
    .mantissa_radix(16)
    .exponent_base(num::NonZeroU8::new(2))
    .exponent_radix(num::NonZeroU8::new(10))
    .build_strict();

// C89 LITERAL [013456MN]
/// Number format for a [`C89`] literal floating-point number.
///
/// [`C89`]: https://en.cppreference.com/w/c
#[rustfmt::skip]
pub const C89_LITERAL: u128 = NumberFormatBuilder::new()
    .no_special(true)
    .build_strict();

// C89 STRING [0134567MN]
/// Number format for a [`C89`] string floating-point number.
///
/// [`C89`]: https://en.cppreference.com/w/c
#[rustfmt::skip]
pub const C89_STRING: u128 = NumberFormatBuilder::new().build_strict();

// C89 HEX STRING [0134567MN]
/// Number format for a [`C89`] string hexadecimal floating-point number.
///
/// [`C89`]: https://en.cppreference.com/w/c
#[rustfmt::skip]
#[cfg(feature = "power-of-two")]
pub const C89_HEX_STRING: u128 = NumberFormatBuilder::new()
    .mantissa_radix(16)
    .exponent_base(num::NonZeroU8::new(2))
    .exponent_radix(num::NonZeroU8::new(10))
    .build_strict();

// RUBY LITERAL [345689AMN-_]
/// Number format for a [`Ruby`] literal floating-point number.
///
/// [`Ruby`]: https://www.ruby-lang.org/en/
#[rustfmt::skip]
pub const RUBY_LITERAL: u128 = NumberFormatBuilder::new()
    .digit_separator(num::NonZeroU8::new(b'_'))
    .required_exponent_sign(true)
    .required_digits(true)
    .no_special(true)
    .no_integer_leading_zeros(true)
    .no_float_leading_zeros(true)
    .internal_digit_separator(true)
    .build_strict();

// RUBY OCTAL LITERAL [345689AN-_]
/// Number format for an octal [`Ruby`] literal floating-point number.
///
/// [`Ruby`]: https://www.ruby-lang.org/en/
#[rustfmt::skip]
#[cfg(feature = "power-of-two")]
pub const RUBY_OCTAL_LITERAL: u128 = NumberFormatBuilder::new()
    .digit_separator(num::NonZeroU8::new(b'_'))
    .mantissa_radix(8)
    .required_digits(true)
    .no_special(true)
    .internal_digit_separator(true)
    .build_strict();

// RUBY STRING [01234569ABMN-_]
// Note: Amazingly, Ruby 1.8+ do not allow parsing special values.
/// Number format to parse a [`Ruby`] float from string.
///
/// [`Ruby`]: https://www.ruby-lang.org/en/
#[rustfmt::skip]
pub const RUBY_STRING: u128 = NumberFormatBuilder::new()
    .digit_separator(num::NonZeroU8::new(b'_'))
    .no_special(true)
    .internal_digit_separator(true)
    .build_strict();

// SWIFT LITERAL [34569ABFGHIJKMN-_]
/// Number format for a [`Swift`] literal floating-point number.
///
/// [`Swift`]: https://developer.apple.com/swift/
#[rustfmt::skip]
pub const SWIFT_LITERAL: u128 = NumberFormatBuilder::new()
    .digit_separator(num::NonZeroU8::new(b'_'))
    .required_digits(true)
    .no_special(true)
    .internal_digit_separator(true)
    .trailing_digit_separator(true)
    .consecutive_digit_separator(true)
    .build_strict();

// SWIFT STRING [134567MN]
/// Number format to parse a [`Swift`] float from string.
///
/// [`Swift`]: https://developer.apple.com/swift/
#[rustfmt::skip]
pub const SWIFT_STRING: u128 = NumberFormatBuilder::new()
    .required_fraction_digits(true)
    .build_strict();

// GO LITERAL [13456MN]
/// Number format for a [`Golang`] literal floating-point number.
///
/// [`Golang`]: https://go.dev/
#[rustfmt::skip]
pub const GO_LITERAL: u128 = NumberFormatBuilder::new()
    .required_fraction_digits(true)
    .no_special(true)
    .build_strict();

// GO STRING [134567MN]
/// Number format to parse a [`Golang`] float from string.
///
/// [`Golang`]: https://go.dev/
#[rustfmt::skip]
pub const GO_STRING: u128 = NumberFormatBuilder::new()
    .required_fraction_digits(true)
    .build_strict();

// HASKELL LITERAL [456MN]
/// Number format for a [`Haskell`] literal floating-point number.
///
/// [`Haskell`]: https://www.haskell.org/
#[rustfmt::skip]
pub const HASKELL_LITERAL: u128 = NumberFormatBuilder::new()
    .required_digits(true)
    .no_positive_mantissa_sign(true)
    .no_special(true)
    .build_strict();

// HASKELL STRING [45678MN]
/// Number format to parse a [`Haskell`] float from string.
///
/// [`Haskell`]: https://www.haskell.org/
#[rustfmt::skip]
pub const HASKELL_STRING: u128 = NumberFormatBuilder::new()
    .required_digits(true)
    .no_positive_mantissa_sign(true)
    .case_sensitive_special(true)
    .build_strict();

// JAVASCRIPT LITERAL [01345678M]
/// Number format for a [`Javascript`] literal floating-point number.
///
/// [`Javascript`]: https://developer.mozilla.org/en-US/docs/Web/JavaScript
#[rustfmt::skip]
pub const JAVASCRIPT_LITERAL: u128 = NumberFormatBuilder::new()
    .case_sensitive_special(true)
    .no_float_leading_zeros(true)
    .build_strict();

// JAVASCRIPT STRING [012345678MN]
/// Number format to parse a [`Javascript`] float from string.
///
/// [`Javascript`]: https://developer.mozilla.org/en-US/docs/Web/JavaScript
#[rustfmt::skip]
pub const JAVASCRIPT_STRING: u128 = NumberFormatBuilder::new()
    .required_exponent_digits(false)
    .case_sensitive_special(true)
    .build_strict();

// PERL LITERAL [0134569ABDEFGHIJKMN-_]
/// Number format for a [`Perl`] literal floating-point number.
///
/// [`Perl`]: https://www.perl.org/
#[rustfmt::skip]
pub const PERL_LITERAL: u128 = NumberFormatBuilder::new()
    .digit_separator(num::NonZeroU8::new(b'_'))
    .no_special(true)
    .internal_digit_separator(true)
    .fraction_leading_digit_separator(true)
    .exponent_leading_digit_separator(true)
    .trailing_digit_separator(true)
    .consecutive_digit_separator(true)
    .build_strict();

// PERL STRING [01234567MN]
/// Number format to parse a [`Perl`] float from string.
///
/// [`Perl`]: https://www.perl.org/
pub const PERL_STRING: u128 = PERMISSIVE;

// PHP LITERAL [01345678MN]
/// Number format for a [`PHP`] literal floating-point number.
///
/// [`PHP`]: https://www.php.net/
#[rustfmt::skip]
pub const PHP_LITERAL: u128 = NumberFormatBuilder::new()
    .case_sensitive_special(true)
    .build_strict();

// PHP STRING [0123456MN]
/// Number format to parse a [`PHP`] float from string.
///
/// [`PHP`]: https://www.php.net/
#[rustfmt::skip]
pub const PHP_STRING: u128 = NumberFormatBuilder::new()
    .required_exponent_digits(false)
    .no_special(true)
    .build_strict();

// JAVA LITERAL [0134569ABIJKMN-_]
/// Number format for a [`Java`] literal floating-point number.
///
/// [`Java`]: https://www.java.com/en/
#[rustfmt::skip]
pub const JAVA_LITERAL: u128 = NumberFormatBuilder::new()
    .digit_separator(num::NonZeroU8::new(b'_'))
    .no_special(true)
    .internal_digit_separator(true)
    .consecutive_digit_separator(true)
    .build_strict();

// JAVA STRING [01345678MN]
/// Number format to parse a [`Java`] float from string.
///
/// [`Java`]: https://www.java.com/en/
#[rustfmt::skip]
pub const JAVA_STRING: u128 = NumberFormatBuilder::new()
    .case_sensitive_special(true)
    .build_strict();

// R LITERAL [01345678MN]
/// Number format for an [`R`] literal floating-point number.
///
/// [`R`]: https://www.r-project.org/
#[rustfmt::skip]
pub const R_LITERAL: u128 = NumberFormatBuilder::new()
    .case_sensitive_special(true)
    .build_strict();

// R STRING [01234567MN]
/// Number format to parse an [`R`] float from string.
///
/// [`R`]: https://www.r-project.org/
pub const R_STRING: u128 = PERMISSIVE;

// KOTLIN LITERAL [0134569ABIJKN-_]
/// Number format for a [`Kotlin`] literal floating-point number.
///
/// [`Kotlin`]: https://kotlinlang.org/
#[rustfmt::skip]
pub const KOTLIN_LITERAL: u128 = NumberFormatBuilder::new()
    .digit_separator(num::NonZeroU8::new(b'_'))
    .no_special(true)
    .no_integer_leading_zeros(true)
    .internal_digit_separator(true)
    .consecutive_digit_separator(true)
    .build_strict();

// KOTLIN STRING [0134568MN]
/// Number format to parse a [`Kotlin`] float from string.
///
/// [`Kotlin`]: https://kotlinlang.org/
#[rustfmt::skip]
pub const KOTLIN_STRING: u128 = NumberFormatBuilder::new()
    .case_sensitive_special(true)
    .build_strict();

// JULIA LITERAL [01345689AMN-_]
/// Number format for a [`Julia`] literal floating-point number.
///
/// [`Julia`]: https://julialang.org/
#[rustfmt::skip]
pub const JULIA_LITERAL: u128 = NumberFormatBuilder::new()
    .digit_separator(num::NonZeroU8::new(b'_'))
    .case_sensitive_special(true)
    .integer_internal_digit_separator(true)
    .fraction_internal_digit_separator(true)
    .build_strict();

// JULIA STRING [01345678MN]
/// Number format to parse a [`Julia`] float from string.
///
/// [`Julia`]: https://julialang.org/
#[rustfmt::skip]
pub const JULIA_STRING: u128 = NumberFormatBuilder::new().build_strict();

// JULIA HEX LITERAL [01345689AMN-_]
/// Number format for a [`Julia`] literal floating-point number.
///
/// [`Julia`]: https://julialang.org/
#[rustfmt::skip]
#[cfg(feature = "power-of-two")]
pub const JULIA_HEX_LITERAL: u128 = NumberFormatBuilder::new()
    .digit_separator(num::NonZeroU8::new(b'_'))
    .mantissa_radix(16)
    .exponent_base(num::NonZeroU8::new(2))
    .exponent_radix(num::NonZeroU8::new(10))
    .case_sensitive_special(true)
    .integer_internal_digit_separator(true)
    .fraction_internal_digit_separator(true)
    .build_strict();

// JULIA HEX STRING [01345678MN]
/// Number format to parse a [`Julia`] float from string.
///
/// [`Julia`]: https://julialang.org/
#[rustfmt::skip]
#[cfg(feature = "power-of-two")]
pub const JULIA_HEX_STRING: u128 = NumberFormatBuilder::new()
    .mantissa_radix(16)
    .exponent_base(num::NonZeroU8::new(2))
    .exponent_radix(num::NonZeroU8::new(10))
    .build_strict();

/// Number format for a [`C#`] literal floating-point number.
///
/// [`C#`]: https://learn.microsoft.com/en-us/dotnet/csharp/
pub const CSHARP_LITERAL: u128 = CSHARP7_LITERAL;

/// Number format to parse a [`C#`] float from string.
///
/// [`C#`]: https://learn.microsoft.com/en-us/dotnet/csharp/
pub const CSHARP_STRING: u128 = CSHARP7_STRING;

// CSHARP7 LITERAL [034569ABIJKMN-_]
/// Number format for a [`C#7`] literal floating-point number.
///
/// [`C#7`]: https://learn.microsoft.com/en-us/dotnet/csharp/whats-new/csharp-version-history#c-version-73
#[rustfmt::skip]
pub const CSHARP7_LITERAL: u128 = NumberFormatBuilder::new()
    .digit_separator(num::NonZeroU8::new(b'_'))
    .required_fraction_digits(true)
    .no_special(true)
    .internal_digit_separator(true)
    .consecutive_digit_separator(true)
    .build_strict();

// CSHARP7 STRING [0134568MN]
/// Number format to parse a [`C#7`] float from string.
///
/// [`C#7`]: https://learn.microsoft.com/en-us/dotnet/csharp/whats-new/csharp-version-history#c-version-73
#[rustfmt::skip]
pub const CSHARP7_STRING: u128 = NumberFormatBuilder::new()
    .case_sensitive_special(true)
    .build_strict();

// CSHARP6 LITERAL [03456MN]
/// Number format for a [`C#6`] literal floating-point number.
///
/// [`C#6`]: https://learn.microsoft.com/en-us/dotnet/csharp/whats-new/csharp-version-history#c-version-60
#[rustfmt::skip]
pub const CSHARP6_LITERAL: u128 = NumberFormatBuilder::new()
    .required_fraction_digits(true)
    .no_special(true)
    .build_strict();

// CSHARP6 STRING [0134568MN]
/// Number format to parse a [`C#6`] float from string.
///
/// [`C#6`]: https://learn.microsoft.com/en-us/dotnet/csharp/whats-new/csharp-version-history#c-version-60
#[rustfmt::skip]
pub const CSHARP6_STRING: u128 = NumberFormatBuilder::new()
    .case_sensitive_special(true)
    .build_strict();

// CSHARP5 LITERAL [03456MN]
/// Number format for a [`C#5`] literal floating-point number.
///
/// [`C#5`]: https://learn.microsoft.com/en-us/dotnet/csharp/whats-new/csharp-version-history#c-version-50
#[rustfmt::skip]
pub const CSHARP5_LITERAL: u128 = NumberFormatBuilder::new()
    .required_fraction_digits(true)
    .no_special(true)
    .build_strict();

// CSHARP5 STRING [0134568MN]
/// Number format to parse a [`C#5`] float from string.
///
/// [`C#5`]: https://learn.microsoft.com/en-us/dotnet/csharp/whats-new/csharp-version-history#c-version-50
#[rustfmt::skip]
pub const CSHARP5_STRING: u128 = NumberFormatBuilder::new()
    .case_sensitive_special(true)
    .build_strict();

// CSHARP4 LITERAL [03456MN]
/// Number format for a [`C#4`] literal floating-point number.
///
/// [`C#4`]: https://learn.microsoft.com/en-us/dotnet/csharp/whats-new/csharp-version-history#c-version-40
#[rustfmt::skip]
pub const CSHARP4_LITERAL: u128 = NumberFormatBuilder::new()
    .required_fraction_digits(true)
    .no_special(true)
    .build_strict();

// CSHARP4 STRING [0134568MN]
/// Number format to parse a [`C#4`] float from string.
///
/// [`C#4`]: https://learn.microsoft.com/en-us/dotnet/csharp/whats-new/csharp-version-history#c-version-40
#[rustfmt::skip]
pub const CSHARP4_STRING: u128 = NumberFormatBuilder::new()
    .case_sensitive_special(true)
    .build_strict();

// CSHARP3 LITERAL [03456MN]
/// Number format for a [`C#3`] literal floating-point number.
///
/// [`C#3`]: https://learn.microsoft.com/en-us/dotnet/csharp/whats-new/csharp-version-history#c-version-30
#[rustfmt::skip]
pub const CSHARP3_LITERAL: u128 = NumberFormatBuilder::new()
    .required_fraction_digits(true)
    .no_special(true)
    .build_strict();

// CSHARP3 STRING [0134568MN]
/// Number format to parse a [`C#3`] float from string.
///
/// [`C#3`]: https://learn.microsoft.com/en-us/dotnet/csharp/whats-new/csharp-version-history#c-version-30
#[rustfmt::skip]
pub const CSHARP3_STRING: u128 = NumberFormatBuilder::new()
    .case_sensitive_special(true)
    .build_strict();

// CSHARP2 LITERAL [03456MN]
/// Number format for a [`C#2`] literal floating-point number.
///
/// [`C#2`]: https://learn.microsoft.com/en-us/dotnet/csharp/whats-new/csharp-version-history#c-version-20
#[rustfmt::skip]
pub const CSHARP2_LITERAL: u128 = NumberFormatBuilder::new()
    .required_fraction_digits(true)
    .no_special(true)
    .build_strict();

// CSHARP2 STRING [0134568MN]
/// Number format to parse a [`C#2`] float from string.
///
/// [`C#2`]: https://learn.microsoft.com/en-us/dotnet/csharp/whats-new/csharp-version-history#c-version-20
#[rustfmt::skip]
pub const CSHARP2_STRING: u128 = NumberFormatBuilder::new()
    .case_sensitive_special(true)
    .build_strict();

// CSHARP1 LITERAL [03456MN]
/// Number format for a [`C#1`] literal floating-point number.
///
/// [`C#1`]: https://learn.microsoft.com/en-us/dotnet/csharp/whats-new/csharp-version-history#c-version-12-1
#[rustfmt::skip]
pub const CSHARP1_LITERAL: u128 = NumberFormatBuilder::new()
    .required_fraction_digits(true)
    .no_special(true)
    .build_strict();

// CSHARP1 STRING [0134568MN]
/// Number format to parse a [`C#1`] float from string.
///
/// [`C#1`]: https://learn.microsoft.com/en-us/dotnet/csharp/whats-new/csharp-version-history#c-version-12-1
#[rustfmt::skip]
pub const CSHARP1_STRING: u128 = NumberFormatBuilder::new()
    .case_sensitive_special(true)
    .build_strict();

// KAWA LITERAL [013456MN]
/// Number format for a [`Kawa`] literal floating-point number.
///
/// [`Kawa`]: https://www.gnu.org/software/kawa/
#[rustfmt::skip]
pub const KAWA_LITERAL: u128 = NumberFormatBuilder::new()
    .no_special(true)
    .build_strict();

// KAWA STRING [013456MN]
/// Number format to parse a [`Kawa`] float from string.
///
/// [`Kawa`]: https://www.gnu.org/software/kawa/
#[rustfmt::skip]
pub const KAWA_STRING: u128 = NumberFormatBuilder::new()
    .no_special(true)
    .build_strict();

// GAMBITC LITERAL [013456MN]
/// Number format for a [`Gambit-C`] literal floating-point number.
///
/// [`Gambit-C`]: https://gambitscheme.org/
#[rustfmt::skip]
pub const GAMBITC_LITERAL: u128 = NumberFormatBuilder::new()
    .no_special(true)
    .build_strict();

// GAMBITC STRING [013456MN]
/// Number format to parse a [`Gambit-C`] float from string.
///
/// [`Gambit-C`]: https://gambitscheme.org/
#[rustfmt::skip]
pub const GAMBITC_STRING: u128 = NumberFormatBuilder::new()
    .no_special(true)
    .build_strict();

// GUILE LITERAL [013456MN]
/// Number format for a [`Guile`] literal floating-point number.
///
/// [`Guile`]: https://www.gnu.org/software/guile/
#[rustfmt::skip]
pub const GUILE_LITERAL: u128 = NumberFormatBuilder::new()
    .no_special(true)
    .build_strict();

// GUILE STRING [013456MN]
/// Number format to parse a [`Guile`] float from string.
///
/// [`Guile`]: https://www.gnu.org/software/guile/
#[rustfmt::skip]
pub const GUILE_STRING: u128 = NumberFormatBuilder::new()
    .no_special(true)
    .build_strict();

// CLOJURE LITERAL [13456MN]
/// Number format for a [`Clojure`] literal floating-point number.
///
/// [`Clojure`]: https://clojure.org/
#[rustfmt::skip]
pub const CLOJURE_LITERAL: u128 = NumberFormatBuilder::new()
    .required_integer_digits(true)
    .no_special(true)
    .build_strict();

// CLOJURE STRING [01345678MN]
/// Number format to parse a [`Clojure`] float from string.
///
/// [`Clojure`]: https://clojure.org/
#[rustfmt::skip]
pub const CLOJURE_STRING: u128 = NumberFormatBuilder::new()
    .case_sensitive_special(true)
    .build_strict();

// ERLANG LITERAL [34578MN]
/// Number format for an [`Erlang`] literal floating-point number.
///
/// [`Erlang`]: https://www.erlang.org/
#[rustfmt::skip]
pub const ERLANG_LITERAL: u128 = NumberFormatBuilder::new()
    .required_digits(true)
    .no_exponent_without_fraction(true)
    .case_sensitive_special(true)
    .build_strict();

// ERLANG STRING [345MN]
/// Number format to parse an [`Erlang`] float from string.
///
/// [`Erlang`]: https://www.erlang.org/
#[rustfmt::skip]
pub const ERLANG_STRING: u128 = NumberFormatBuilder::new()
    .required_digits(true)
    .no_exponent_without_fraction(true)
    .no_special(true)
    .build_strict();

// ELM LITERAL [456]
/// Number format for an [`Elm`] literal floating-point number.
///
/// [`Elm`]: https://elm-lang.org/
#[rustfmt::skip]
pub const ELM_LITERAL: u128 = NumberFormatBuilder::new()
    .required_digits(true)
    .no_positive_mantissa_sign(true)
    .no_integer_leading_zeros(true)
    .no_float_leading_zeros(true)
    .no_special(true)
    .build_strict();

// ELM STRING [01345678MN]
// Note: There is no valid representation of NaN, just Infinity.
/// Number format to parse an [`Elm`] float from string.
///
/// [`Elm`]: https://elm-lang.org/
#[rustfmt::skip]
pub const ELM_STRING: u128 = NumberFormatBuilder::new()
    .case_sensitive_special(true)
    .build_strict();

// SCALA LITERAL [3456]
/// Number format for a [`Scala`] literal floating-point number.
///
/// [`Scala`]: https://www.scala-lang.org/
#[rustfmt::skip]
pub const SCALA_LITERAL: u128 = NumberFormatBuilder::new()
    .required_digits(true)
    .no_special(true)
    .no_integer_leading_zeros(true)
    .no_float_leading_zeros(true)
    .build_strict();

// SCALA STRING [01345678MN]
/// Number format to parse a [`Scala`] float from string.
///
/// [`Scala`]: https://www.scala-lang.org/
#[rustfmt::skip]
pub const SCALA_STRING: u128 = NumberFormatBuilder::new()
    .case_sensitive_special(true)
    .build_strict();

// ELIXIR LITERAL [3459ABMN-_]
/// Number format for an [`Elixir`] literal floating-point number.
///
/// [`Elixir`]: https://elixir-lang.org/
#[rustfmt::skip]
pub const ELIXIR_LITERAL: u128 = NumberFormatBuilder::new()
    .digit_separator(num::NonZeroU8::new(b'_'))
    .required_digits(true)
    .no_exponent_without_fraction(true)
    .no_special(true)
    .internal_digit_separator(true)
    .build_strict();

// ELIXIR STRING [345MN]
/// Number format to parse an [`Elixir`] float from string.
///
/// [`Elixir`]: https://elixir-lang.org/
#[rustfmt::skip]
pub const ELIXIR_STRING: u128 = NumberFormatBuilder::new()
    .required_digits(true)
    .no_exponent_without_fraction(true)
    .no_special(true)
    .build_strict();

// FORTRAN LITERAL [013456MN]
/// Number format for a [`FORTRAN`] literal floating-point number.
///
/// [`FORTRAN`]: https://fortran-lang.org/
#[rustfmt::skip]
pub const FORTRAN_LITERAL: u128 = NumberFormatBuilder::new()
    .no_special(true)
    .build_strict();

// FORTRAN STRING [0134567MN]
/// Number format to parse a [`FORTRAN`] float from string.
///
/// [`FORTRAN`]: https://fortran-lang.org/
#[rustfmt::skip]
pub const FORTRAN_STRING: u128 = NumberFormatBuilder::new().build_strict();

// D LITERAL [0134569ABFGHIJKN-_]
/// Number format for a [`D`] literal floating-point number.
///
/// [`D`]: https://dlang.org/
#[rustfmt::skip]
pub const D_LITERAL: u128 = NumberFormatBuilder::new()
    .digit_separator(num::NonZeroU8::new(b'_'))
    .no_special(true)
    .no_integer_leading_zeros(true)
    .internal_digit_separator(true)
    .trailing_digit_separator(true)
    .consecutive_digit_separator(true)
    .build_strict();

// D STRING [01345679AFGMN-_]
/// Number format to parse a [`D`] float from string.
///
/// [`D`]: https://dlang.org/
#[rustfmt::skip]
pub const D_STRING: u128 = NumberFormatBuilder::new()
    .digit_separator(num::NonZeroU8::new(b'_'))
    .integer_internal_digit_separator(true)
    .fraction_internal_digit_separator(true)
    .integer_trailing_digit_separator(true)
    .fraction_trailing_digit_separator(true)
    .build_strict();

// COFFEESCRIPT LITERAL [01345678]
/// Number format for a [`Coffeescript`] literal floating-point number.
///
/// [`Coffeescript`]: https://coffeescript.org/
#[rustfmt::skip]
pub const COFFEESCRIPT_LITERAL: u128 = NumberFormatBuilder::new()
    .case_sensitive_special(true)
    .no_integer_leading_zeros(true)
    .no_float_leading_zeros(true)
    .build_strict();

// COFFEESCRIPT STRING [012345678MN]
/// Number format to parse a [`Coffeescript`] float from string.
///
/// [`Coffeescript`]: https://coffeescript.org/
#[rustfmt::skip]
pub const COFFEESCRIPT_STRING: u128 = NumberFormatBuilder::new()
    .case_sensitive_special(true)
    .build_strict();

// COBOL LITERAL [0345MN]
/// Number format for a [`Cobol`] literal floating-point number.
///
/// [`Cobol`]: https://www.ibm.com/think/topics/cobol
#[rustfmt::skip]
pub const COBOL_LITERAL: u128 = NumberFormatBuilder::new()
    .required_fraction_digits(true)
    .no_exponent_without_fraction(true)
    .no_special(true)
    .build_strict();

// COBOL STRING [012356MN]
/// Number format to parse a [`Cobol`] float from string.
///
/// [`Cobol`]: https://www.ibm.com/think/topics/cobol
#[rustfmt::skip]
pub const COBOL_STRING: u128 = NumberFormatBuilder::new()
    .required_exponent_sign(true)
    .no_special(true)
    .build_strict();

// FSHARP LITERAL [13456789ABIJKMN-_]
/// Number format for a [`F#`] literal floating-point number.
///
/// [`F#`]: https://fsharp.org/
#[rustfmt::skip]
pub const FSHARP_LITERAL: u128 = NumberFormatBuilder::new()
    .digit_separator(num::NonZeroU8::new(b'_'))
    .required_integer_digits(true)
    .required_exponent_digits(true)
    .case_sensitive_special(true)
    .internal_digit_separator(true)
    .consecutive_digit_separator(true)
    .build_strict();

// FSHARP STRING [013456789ABCDEFGHIJKLMN-_]
/// Number format to parse a [`F#`] float from string.
///
/// [`F#`]: https://fsharp.org/
#[rustfmt::skip]
pub const FSHARP_STRING: u128 = NumberFormatBuilder::new()
    .digit_separator(num::NonZeroU8::new(b'_'))
    .internal_digit_separator(true)
    .leading_digit_separator(true)
    .trailing_digit_separator(true)
    .consecutive_digit_separator(true)
    .special_digit_separator(true)
    .build_strict();

// VB LITERAL [03456MN]
/// Number format for a [`Visual Basic`] literal floating-point number.
///
/// [`Visual Basic`]: https://learn.microsoft.com/en-us/dotnet/visual-basic/
#[rustfmt::skip]
pub const VB_LITERAL: u128 = NumberFormatBuilder::new()
    .required_fraction_digits(true)
    .no_special(true)
    .build_strict();

// VB STRING [01345678MN]
/// Number format to parse a [`Visual Basic`] float from string.
///
/// [`Visual Basic`]: https://learn.microsoft.com/en-us/dotnet/visual-basic/
// Note: To my knowledge, Visual Basic cannot parse infinity.
#[rustfmt::skip]
pub const VB_STRING: u128 = NumberFormatBuilder::new()
    .case_sensitive_special(true)
    .build_strict();

// OCAML LITERAL [1456789ABDFGHIJKMN-_]
/// Number format for an [`OCaml`] literal floating-point number.
///
/// [`OCaml`]: https://ocaml.org/
#[rustfmt::skip]
pub const OCAML_LITERAL: u128 = NumberFormatBuilder::new()
    .digit_separator(num::NonZeroU8::new(b'_'))
    .required_integer_digits(true)
    .required_exponent_digits(true)
    .no_positive_mantissa_sign(true)
    .case_sensitive_special(true)
    .internal_digit_separator(true)
    .fraction_leading_digit_separator(true)
    .trailing_digit_separator(true)
    .consecutive_digit_separator(true)
    .build_strict();

// OCAML STRING [01345679ABCDEFGHIJKLMN-_]
/// Number format to parse an [`OCaml`] float from string.
///
/// [`OCaml`]: https://ocaml.org/
#[rustfmt::skip]
pub const OCAML_STRING: u128 = NumberFormatBuilder::new()
    .digit_separator(num::NonZeroU8::new(b'_'))
    .internal_digit_separator(true)
    .leading_digit_separator(true)
    .trailing_digit_separator(true)
    .consecutive_digit_separator(true)
    .special_digit_separator(true)
    .build_strict();

// OBJECTIVEC LITERAL [013456MN]
/// Number format for an [`Objective-C`] literal floating-point number.
///
/// [`Objective-C`]: https://en.wikipedia.org/wiki/Objective-C
#[rustfmt::skip]
pub const OBJECTIVEC_LITERAL: u128 = NumberFormatBuilder::new()
    .no_special(true)
    .build_strict();

// OBJECTIVEC STRING [013456MN]
/// Number format to parse an [`Objective-C`] float from string.
///
/// [`Objective-C`]: https://en.wikipedia.org/wiki/Objective-C
#[rustfmt::skip]
pub const OBJECTIVEC_STRING: u128 = NumberFormatBuilder::new()
    .no_special(true)
    .build_strict();

// REASONML LITERAL [13456789ABDFGHIJKMN-_]
/// Number format for a [`ReasonML`] literal floating-point number.
///
/// [`ReasonML`]: https://reasonml.github.io/
#[rustfmt::skip]
pub const REASONML_LITERAL: u128 = NumberFormatBuilder::new()
    .digit_separator(num::NonZeroU8::new(b'_'))
    .required_integer_digits(true)
    .required_exponent_digits(true)
    .case_sensitive_special(true)
    .internal_digit_separator(true)
    .fraction_leading_digit_separator(true)
    .trailing_digit_separator(true)
    .consecutive_digit_separator(true)
    .build_strict();

// REASONML STRING [01345679ABCDEFGHIJKLMN-_]
/// Number format to parse a [`ReasonML`] float from string.
///
/// [`ReasonML`]: https://reasonml.github.io/
#[rustfmt::skip]
pub const REASONML_STRING: u128 = NumberFormatBuilder::new()
    .digit_separator(num::NonZeroU8::new(b'_'))
    .internal_digit_separator(true)
    .leading_digit_separator(true)
    .trailing_digit_separator(true)
    .consecutive_digit_separator(true)
    .special_digit_separator(true)
    .build_strict();

// OCTAVE LITERAL [013456789ABDFGHIJKMN-_]
// Note: Octave accepts both NaN and nan, Inf and inf.
/// Number format for an [`Octave`] literal floating-point number.
///
/// [`Octave`]: https://octave.org/
#[rustfmt::skip]
pub const OCTAVE_LITERAL: u128 = NumberFormatBuilder::new()
    .digit_separator(num::NonZeroU8::new(b'_'))
    .case_sensitive_special(true)
    .internal_digit_separator(true)
    .fraction_leading_digit_separator(true)
    .trailing_digit_separator(true)
    .consecutive_digit_separator(true)
    .build_strict();

// OCTAVE STRING [01345679ABCDEFGHIJKMN-,]
/// Number format to parse an [`Octave`] float from string.
///
/// [`Octave`]: https://octave.org/
#[rustfmt::skip]
pub const OCTAVE_STRING: u128 = NumberFormatBuilder::new()
    .digit_separator(num::NonZeroU8::new(b','))
    .internal_digit_separator(true)
    .leading_digit_separator(true)
    .trailing_digit_separator(true)
    .consecutive_digit_separator(true)
    .build_strict();

// MATLAB LITERAL [013456789ABDFGHIJKMN-_]
// Note: Matlab accepts both NaN and nan, Inf and inf.
/// Number format for an [`Matlab`] literal floating-point number.
///
/// [`Matlab`]: https://www.mathworks.com/products/matlab.html
#[rustfmt::skip]
pub const MATLAB_LITERAL: u128 = NumberFormatBuilder::new()
    .digit_separator(num::NonZeroU8::new(b'_'))
    .case_sensitive_special(true)
    .internal_digit_separator(true)
    .fraction_leading_digit_separator(true)
    .trailing_digit_separator(true)
    .consecutive_digit_separator(true)
    .build_strict();

// MATLAB STRING [01345679ABCDEFGHIJKMN-,]
/// Number format to parse an [`Matlab`] float from string.
///
/// [`Matlab`]: https://www.mathworks.com/products/matlab.html
#[rustfmt::skip]
pub const MATLAB_STRING: u128 = NumberFormatBuilder::new()
    .digit_separator(num::NonZeroU8::new(b','))
    .internal_digit_separator(true)
    .leading_digit_separator(true)
    .trailing_digit_separator(true)
    .consecutive_digit_separator(true)
    .build_strict();

// ZIG LITERAL [1456MN]
/// Number format for a [`Zig`] literal floating-point number.
///
/// [`Zig`]: https://ziglang.org/
#[rustfmt::skip]
pub const ZIG_LITERAL: u128 = NumberFormatBuilder::new()
    .required_integer_digits(true)
    .no_positive_mantissa_sign(true)
    .no_special(true)
    .build_strict();

// ZIG STRING [01234567MN]
/// Number format to parse a [`Zig`] float from string.
///
/// [`Zig`]: https://ziglang.org/
pub const ZIG_STRING: u128 = PERMISSIVE;

// SAGE LITERAL [012345678MN]
// Note: Both Infinity and infinity are accepted.
/// Number format for a [`Sage`] literal floating-point number.
///
/// [`Sage`]: https://www.sagemath.org/
#[rustfmt::skip]
pub const SAGE_LITERAL: u128 = NumberFormatBuilder::new()
    .case_sensitive_special(true)
    .build_strict();

// SAGE STRING [01345679ABMN-_]
/// Number format to parse a [`Sage`] float from string.
///
/// [`Sage`]: https://www.sagemath.org/
#[rustfmt::skip]
pub const SAGE_STRING: u128 = NumberFormatBuilder::new()
    .digit_separator(num::NonZeroU8::new(b'_'))
    .internal_digit_separator(true)
    .build_strict();

// JSON [456]
/// Number format for a [`JSON`][`JSON-REF`] literal floating-point number.
///
/// [`JSON-REF`]: https://www.json.org/json-en.html
#[rustfmt::skip]
pub const JSON: u128 = NumberFormatBuilder::new()
    .required_digits(true)
    .no_positive_mantissa_sign(true)
    .no_special(true)
    .no_integer_leading_zeros(true)
    .no_float_leading_zeros(true)
    .build_strict();

// TOML [34569AB]
/// Number format for a [`TOML`][`TOML-REF`] literal floating-point number.
///
/// [`TOML-REF`]: https://toml.io/en/
#[rustfmt::skip]
pub const TOML: u128 = NumberFormatBuilder::new()
    .digit_separator(num::NonZeroU8::new(b'_'))
    .required_digits(false)
    .no_special(true)
    .no_integer_leading_zeros(true)
    .no_float_leading_zeros(true)
    .internal_digit_separator(true)
    .build_strict();

// YAML (defined in-terms of JSON schema).
/// Number format for a [`YAML`][`YAML-REF`] literal floating-point number.
///
/// [`YAML-REF`]: https://yaml.org/
pub const YAML: u128 = JSON;

// XML [01234578MN]
/// Number format for an [`XML`][`XML-REF`] literal floating-point number.
///
/// [`XML-REF`]: https://en.wikipedia.org/wiki/XML
#[rustfmt::skip]
pub const XML: u128 = NumberFormatBuilder::new()
    .required_exponent_digits(false)
    .case_sensitive_special(true)
    .build_strict();

// SQLITE [013456MN]
/// Number format for a [`SQLite`] literal floating-point number.
///
/// [`SQLite`]: https://www.sqlite.org/
#[rustfmt::skip]
pub const SQLITE: u128 = NumberFormatBuilder::new()
    .no_special(true)
    .build_strict();

// POSTGRESQL [013456MN]
/// Number format for a [`PostgreSQL`] literal floating-point number.
///
/// [`PostgreSQL`]: https://www.postgresql.org/
#[rustfmt::skip]
pub const POSTGRESQL: u128 = NumberFormatBuilder::new()
    .no_special(true)
    .build_strict();

// MYSQL [013456MN]
/// Number format for a [`MySQL`] literal floating-point number.
///
/// [`MySQL`]: https://www.mysql.com/
#[rustfmt::skip]
pub const MYSQL: u128 = NumberFormatBuilder::new()
    .no_special(true)
    .build_strict();

// MONGODB [01345678M]
/// Number format for a [`MongoDB`] literal floating-point number.
///
/// [`MongoDB`]: https://www.mongodb.com/
#[rustfmt::skip]
pub const MONGODB: u128 = NumberFormatBuilder::new()
    .case_sensitive_special(true)
    .no_float_leading_zeros(true)
    .build_strict();

// HIDDEN DEFAULTS AND INTERFACES

/// Number format when no flags are set.
#[doc(hidden)]
#[rustfmt::skip]
pub const PERMISSIVE: u128 = NumberFormatBuilder::new()
    .required_exponent_digits(false)
    .required_mantissa_digits(false)
    .build_strict();

/// Number format when all digit separator flags are set.
#[doc(hidden)]
#[rustfmt::skip]
pub const IGNORE: u128 = NumberFormatBuilder::new()
    .digit_separator(num::NonZeroU8::new(b'_'))
    .digit_separator_flags(true)
    .required_exponent_digits(false)
    .required_mantissa_digits(false)
    .build_strict();

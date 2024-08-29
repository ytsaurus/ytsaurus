Boost Multiprecision Library
============================

|                  |  Master  |   Develop   |
|------------------|----------|-------------|
| Drone            | [![Build Status](https://drone.cpp.al/api/badges/boostorg/multiprecision/status.svg?ref=refs/heads/master)](https://drone.cpp.al/boostorg/multiprecision)          | [![Build Status](https://drone.cpp.al/api/badges/boostorg/multiprecision/status.svg)](https://drone.cpp.al/boostorg/multiprecision) |
| Github Actions   | [![Build Status](https://github.com/boostorg/multiprecision/workflows/multiprecision/badge.svg?branch=master)](https://github.com/boostorg/multiprecision/actions) | [![Build Status](https://github.com/boostorg/multiprecision/workflows/multiprecision/badge.svg?branch=develop)](https://github.com/boostorg/multiprecision/actions) |
| Codecov          | [![codecov](https://codecov.io/gh/boostorg/multiprecision/branch/master/graph/badge.svg)](https://codecov.io/gh/boostorg/multiprecision/branch/master)             | [![codecov](https://codecov.io/gh/boostorg/multiprecision/branch/develop/graph/badge.svg)](https://codecov.io/gh/boostorg/multiprecision/branch/develop) |


`Boost.Multiprecision` is a C++ library that provides integer, rational, floating-point, complex and interval number types
having more range and precision than the language's ordinary built-in types.

Language adherence:
  - `Boost.Multiprecision` requires a compliant C++14 compiler.
  - It is compatible with C++14, 17, 20, 23 and beyond.

The big number types in `Boost.Multiprecision` can be used with a wide selection of basic
mathematical operations, elementary transcendental functions as well as the functions in Boost.Math. The Multiprecision types can
also interoperate with the built-in types in C++ using clearly defined conversion rules. This allows `Boost.Multiprecision` to be
used for all kinds of mathematical calculations involving integer, rational and floating-point types requiring extended range and precision.

Multiprecision consists of a generic interface to the mathematics of large numbers as well as a selection of big number back ends, with
support for integer, rational and floating-point types. `Boost.Multiprecision` provides a selection of back ends provided off-the-rack in
including interfaces to GMP, MPFR, MPIR, TomMath as well as its own collection of Boost-licensed, header-only back ends for integers,
rationals, floats and complex. In addition, user-defined back ends can be created and used with the interface of Multiprecision,
provided the class implementation adheres to the necessary concepts.

Depending upon the number type, precision may be arbitrarily large (limited only by available memory), fixed at compile time
(for example $50$ or $100$ decimal digits), or a variable controlled at run-time by member functions.
The types are expression-template-enabled by default. This usually provides better performance than naive user-defined types.
If not needed, expression templates can be disabled when configuring the `number` type with its backend.

The full documentation is available on [boost.org](http://www.boost.org/doc/libs/release/libs/multiprecision/index.html).

## Using Multiprecision ##

<p align="center">
  <a href="https://godbolt.org/z/hj75jEqcz" alt="godbolt">
    <img src="https://img.shields.io/badge/try%20it%20on-godbolt-green" /></a>
</p>

In the following example, we use Multiprecision's Boost-licensed binary
floating-point backend type `cpp_bin_float` to compute ${\sim}100$ decimal digits of

$$\sqrt{\pi} = \Gamma \left( \frac{1}{2} \right)~{\approx}~1.772453850905516027298{\ldots}\text{,}$$

where we also observe that Multiprecision can seemlesly interoperate with
[Boost.Math](https://github.com/boostorg/math).

```cpp
#include <iomanip>
#include <iostream>
#include <sstream>

#include <boost/multiprecision/cpp_bin_float.hpp>
#include <boost/math/special_functions/gamma.hpp>

auto main() -> int
{
  using big_float_type = boost::multiprecision::cpp_bin_float_100;

  const big_float_type sqrt_pi { sqrt(boost::math::constants::pi<big_float_type>()) };

  const big_float_type half { big_float_type(1) / 2 };

  const big_float_type gamma_half { boost::math::tgamma(half) }; 

  std::stringstream strm { };

  strm << std::setprecision(std::numeric_limits<big_float_type>::digits10) << "sqrt_pi   : " << sqrt_pi << '\n';
  strm << std::setprecision(std::numeric_limits<big_float_type>::digits10) << "gamma_half: " << gamma_half;

  std::cout << strm.str() << std::endl;
}
```

## Standalone ##

Defining `BOOST_MP_STANDALONE` allows `Boost.Multiprecision`
to be used with the only dependency being [Boost.Config](https://github.com/boostorg/config).

Our [package on this page](https://github.com/boostorg/multiprecision/releases)
already includes a copy of Boost.Config so no other downloads are required.
Some functionality is reduced in this mode.
A static_assert message will alert you if a particular feature has been disabled by standalone mode.
[Boost.Math](https://github.com/boostorg/math) standalone mode is compatiable,
and recommended if special functions are required for the floating point types.

## Support, bugs and feature requests ##

Bugs and feature requests can be reported through the [Gitub issue tracker](https://github.com/boostorg/multiprecision/issues)
(see [open issues](https://github.com/boostorg/multiprecision/issues) and
[closed issues](https://github.com/boostorg/multiprecision/issues?utf8=%E2%9C%93&q=is%3Aissue+is%3Aclosed)).

You can submit your changes through a [pull request](https://github.com/boostorg/multiprecision/pulls).

There is no mailing-list specific to `Boost Multiprecision`,
although you can use the general-purpose Boost [mailing-list](http://lists.boost.org/mailman/listinfo.cgi/boost-users)
using the tag [multiprecision].


## Development ##

Clone the whole boost project, which includes the individual Boost projects as submodules
([see boost+git doc](https://github.com/boostorg/boost/wiki/Getting-Started)):

```sh
  git clone https://github.com/boostorg/boost
  cd boost
  git submodule update --init
```

The Boost Multiprecision Library is located in `libs/multiprecision/`.

### Running tests ###
First, build the `b2` engine by running `bootstrap.sh` in the root of the boost directory. This will generate `b2` configuration in `project-config.jam`.

```sh
  ./bootstrap.sh
```

Now make sure you are in `libs/multiprecision/test`. You can either run all the tests listed in `Jamfile.v2` or run a single test:

```sh
  ../../../b2                        <- run all tests
  ../../../b2 test_complex           <- single test
```

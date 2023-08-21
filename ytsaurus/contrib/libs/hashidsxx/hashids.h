/**
 *  Hashidsxx (c) 2014 Toon Schoenmakers
 *
 *  https://github.com/schoentoon/hashidsxx
 *  hashidsxx may be freely distributed under the MIT license.
 */

#pragma once

#if __cplusplus >= 201103
#  include <initializer_list>
#  include <type_traits>
#endif

#include <string>
#include <vector>
#include <stdexcept>
#include <cmath>

// For C++11 we would use cstdint here instead of stdint.h but we want to be able
// to link to this file with non C++11 as well
#include <stdint.h>

#define DEFAULT_ALPHABET                                                       \
  "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"

namespace hashidsxx {

class Hashids {
private:
  /**
   *  The salt that will be used to generate the hashes
   */
  const std::string _salt;

  /**
   *  The alphabet that will be used to generate the hashes
   */
  std::string _alphabet;

  /**
   *  Minimum length of the hash, 0 means that there's not minimum
   */
  const unsigned int _min_length;

  std::string _separators;
  std::string _guards;

public:
  Hashids(const std::string &salt = "", unsigned int min_length = 0,
          const std::string &alphabet = DEFAULT_ALPHABET);
  Hashids(const Hashids &that);

#if __cplusplus >= 201103
  Hashids(Hashids &&that);
#endif

  virtual ~Hashids();

#if __cplusplus >= 201103
  template <typename Number, typename std::enable_if<std::is_integral<Number>::value, int>::type* = nullptr>
  std::string encode(const std::initializer_list<Number> &input) const {
    return encode(input.begin(), input.end());
  }

  template <typename... Number>
  std::string encode(Number... numbers) const {
    return encode({numbers...});
  }
#endif

  template <typename Iterator>
  std::string encode(const Iterator begin, const Iterator end) const {
    // Encrypting nothing makes no sense
    if (begin == end)
      return "";

    // Make a copy of our alphabet so we can reorder it on the fly etc
    std::string alphabet(_alphabet);

    int values_hash = 0;
    int i = 0;
    for (Iterator iter = begin; iter != end; ++iter) {
      values_hash += (*iter % (i + 100));
      ++i;
    };

    char encoded = _alphabet[values_hash % _alphabet.size()];
    char lottery = encoded;

    std::string output;
    if (_min_length > 0)
      output.reserve(_min_length); // reserve if we have a minimum length
    output.push_back(encoded);

    i = 0;
    for (Iterator iter = begin; iter != end; ++iter) {
      uint64_t number = *iter;

      std::string alphabet_salt;
      alphabet_salt.push_back(lottery);
      alphabet_salt.append(_salt).append(alphabet);

      alphabet = _reorder(alphabet, alphabet_salt);

      std::string last = _hash(number, alphabet);
      output.append(last);

      number %= last[0] + i;
      output.push_back(_separators[number % _separators.size()]);
      ++i;
    };

    // pop_back() is only available with C++11
    output.erase(output.end() - 1);

    if (output.size() < _min_length)
      _ensure_length(output, alphabet, values_hash);

    return output;
  }

  std::vector<uint64_t> decode(const std::string &input) const;

  std::string encodeHex(const std::string &input) const;

  std::string decodeHex(const std::string &input) const;

private:
  std::string &_reorder(std::string &input, const std::string &salt) const;
  std::string _reorder_norewrite(const std::string &input,
                                 const std::string &salt) const;
  std::string _hash(uint64_t number, const std::string &alphabet) const;
  uint64_t _unhash(const std::string &input, const std::string &alphabet) const;
  void _ensure_length(std::string &output, std::string &alphabet,
                      int values_hash) const;
  std::vector<std::string> _split(const std::string &hash,
                                  const std::string &splitters) const;
};
};

/**
 *  Hashidsxx (c) 2014 Toon Schoenmakers
 *
 *  https://github.com/schoentoon/hashidsxx
 *  hashidsxx may be freely distributed under the MIT license.
 */

#include "hashids.h"

#include <algorithm>

#define RATIO_SEPARATORS 3.5
#define RATIO_GUARDS 12

#include <iterator>
#include <iostream>
#include <sstream>
#include <numeric>

namespace hashidsxx {

const static std::string separators("cfhistuCFHISTU");

Hashids::Hashids(const std::string &salt, unsigned int min_length,
                 const std::string &alphabet)
    : _salt(salt), _alphabet(alphabet), _min_length(min_length), _separators(),
      _guards() {
  std::for_each(separators.begin(), separators.end(), [this](char c) {
    if (_alphabet.find(c) != std::string::npos)
      _separators.push_back(c);
  });
  _alphabet.erase(
      std::remove_if(_alphabet.begin(), _alphabet.end(), [this](char c) {
        return _separators.find(c) != std::string::npos;
      }), _alphabet.end());
  if (_alphabet.size() + _separators.size() < 16)
    throw std::runtime_error(
        "Alphabet must contain at least 16 unique characters");

  _separators = _reorder(_separators, _salt);

  std::size_t min_separators =
      (int)std::ceil((float)_alphabet.length() / RATIO_SEPARATORS);

  if (_separators.empty() || _separators.length() < min_separators) {
    if (min_separators == 1)
      min_separators = 2;
    if (min_separators > _separators.length()) {
      int split_at = min_separators - _separators.length();
      _separators.append(_alphabet.substr(0, split_at));
      _alphabet = _alphabet.substr(split_at);
    };
  };

  _alphabet = _reorder(_alphabet, _salt);
  int num_guards = (int)std::ceil((float)_alphabet.length() / RATIO_GUARDS);

  if (_alphabet.length() < 3) {
    _guards = _separators.substr(0, num_guards);
    _separators = _separators.substr(num_guards);
  } else {
    _guards = _alphabet.substr(0, num_guards);
    _alphabet = _alphabet.substr(num_guards);
  };
}

Hashids::Hashids(const Hashids &that)
    : _salt(that._salt), _alphabet(that._alphabet),
      _min_length(that._min_length), _separators(that._separators),
      _guards(that._guards) {}

#if __cplusplus >= 201103
Hashids::Hashids(Hashids &&that)
    : _salt(std::move(that._salt)), _alphabet(std::move(that._alphabet)),
      _min_length(that._min_length), _separators(std::move(that._separators)),
      _guards(std::move(that._guards)) {}
#endif

Hashids::~Hashids() {}

std::string &Hashids::_reorder(std::string &input,
                               const std::string &salt) const {
  if (salt.empty())
    return input;

  int i = input.length() - 1;
  int index = 0;
  int integer_sum = 0;

  while (i > 0) {
    index %= salt.length();
    int integer = salt[index];
    integer_sum += integer;
    unsigned int j = (integer + index + integer_sum) % i;

    std::swap(input[i], input[j]);

    --i;
    ++index;
  };

  return input;
}

std::string Hashids::_reorder_norewrite(const std::string &input,
                                        const std::string &salt) const {
  std::string output(input);
  return _reorder(output, salt);
}

std::string Hashids::_hash(uint64_t number, const std::string &alphabet) const {
  std::string output;
  do {
    output.push_back(alphabet[number % alphabet.size()]);
    number /= alphabet.size();
  } while(number != 0);
  return std::string(output.rbegin(), output.rend());
}

std::string Hashids::encodeHex(const std::string &input) const {
  std::vector<uint64_t> numbers;
  std::string buffer;
  std::string hex("0123456789abcdefABCDEF");

  for (char c : input) {
    if (hex.find_first_of(c) != std::string::npos)
      buffer.push_back(c);
    if (buffer.size() == 12) {
      numbers.push_back(std::stoull("1" + buffer, nullptr, 16));
      buffer.clear();
    }
  }
  if (!buffer.empty())
    numbers.push_back(std::stoull("1" + buffer, nullptr, 16));

  return encode(numbers.begin(), numbers.end());
}

std::string Hashids::decodeHex(const std::string &input) const {
  std::stringstream output;
  std::stringstream hexbuf;
  for (uint64_t number : decode(input)) {
    hexbuf << std::hex << number;
    output << hexbuf.str().substr(1);
    hexbuf.str(std::string());
  }
  return output.str();
}

uint64_t Hashids::_unhash(const std::string &input,
                          const std::string &alphabet) const {
  return std::accumulate(
    input.begin(),
    input.end(),
    static_cast<uint64_t>(0),
    [&alphabet](const uint64_t &carry, const char &item){
      return carry * alphabet.size() + alphabet.find(item);
    }
  );
}

void Hashids::_ensure_length(std::string &output, std::string &alphabet,
                             int values_hash) const {
  int guard_index = (values_hash + output[0]) % _guards.size();
  output.insert(output.begin(), _guards[guard_index]);

  if (output.size() < _min_length) {
    guard_index = (values_hash + output[2]) % _guards.size();
    output.push_back(_guards[guard_index]);
  };

  int split_at = alphabet.size() / 2;
  while (output.size() < _min_length) {
    alphabet = _reorder_norewrite(alphabet, alphabet);

    output = alphabet.substr(split_at) + output + alphabet.substr(0, split_at);

    int excess = output.size() - _min_length;
    if (excess > 0) {
      int from_index = excess / 2;
      output = output.substr(from_index, _min_length);
    };
  };
}

std::vector<std::string> Hashids::_split(const std::string &input,
                                         const std::string &splitters) const {
  std::vector<std::string> parts;
  std::string tmp;

  for (char c : input) {
    if (splitters.find(c) != std::string::npos) {
      parts.push_back(tmp);
      tmp.clear();
    } else
      tmp.push_back(c);
  };
  if (!tmp.empty())
    parts.push_back(tmp);

  return parts;
}

std::vector<uint64_t> Hashids::decode(const std::string &input) const {
  std::vector<uint64_t> output;

  std::vector<std::string> parts = _split(input, _guards);

  std::string hashid = parts[0];
  if (parts.size() >= 2)
    hashid = parts[1];

  if (hashid.empty())
    return output;

  output.reserve(parts.size());

  char lottery = hashid[0];
  std::string alphabet(_alphabet);

  hashid.erase(hashid.begin());

  std::vector<std::string> hash_parts = _split(hashid, _separators);
  for (const std::string &part : hash_parts) {
    std::string alphabet_salt = (lottery + _salt + alphabet);
    alphabet_salt = alphabet_salt.substr(0, alphabet.size());

    alphabet = _reorder(alphabet, alphabet_salt);

    output.push_back(_unhash(part, alphabet));
  };

  return output;
}
};

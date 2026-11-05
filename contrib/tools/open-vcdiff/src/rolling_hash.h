// Copyright 2007, 2008 The open-vcdiff Authors. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef OPEN_VCDIFF_ROLLING_HASH_H_
#define OPEN_VCDIFF_ROLLING_HASH_H_

#include <config.h>
#include <stdint.h>  // uint32_t
#include "compile_assert.h"
#include "logging.h"

#include <array>

namespace open_vcdiff {

// Rabin-Karp hasher module -- this is a faster version with different
// constants, so it's not quite Rabin-Karp fingerprinting, but its behavior is
// close enough for most applications.

// Definitions common to all hash window sizes.
class RollingHashUtil {
 public:
  // Multiplier for incremental hashing.  The compiler should be smart enough to
  // convert (val * kMult) into ((val << 8) + val).
  static const uint32_t kMult = 257;

  // Here's the heart of the hash algorithm.  Start with a partial_hash value of
  // 0, and run this HashStep once against each byte in the data window to be
  // hashed.  The result will be the hash value for the entire data window.  The
  // Hash() function, below, does exactly this, albeit with some refinements.
  static inline uint32_t HashStep(uint32_t partial_hash,
                                  unsigned char next_byte) {
    return (partial_hash * kMult) + next_byte;
  }

  // Use this function to start computing a new hash value based on the first
  // two bytes in the window.  It is equivalent to calling
  //     HashStep(HashStep(0, ptr[0]), ptr[1])
  static inline uint32_t HashFirstTwoBytes(const char* ptr) {
    return (static_cast<unsigned char>(ptr[0]) * kMult)
        + static_cast<unsigned char>(ptr[1]);
  }
};

// window_size must be >= 2.
template<int window_size>
class RollingHash : public RollingHashUtil {
 public:
  // Compute a hash of the window "ptr[0, window_size - 1]".
  static uint32_t Hash(const char* ptr) {
    uint32_t h = HashFirstTwoBytes(ptr);
    for (int i = 2; i < window_size; ++i) {
      h = HashStep(h, ptr[i]);
    }
    return h;
  }

  // Update a hash by removing the oldest byte and adding a new byte.
  //
  // UpdateHash takes the hash value of buffer[0] ... buffer[window_size -1]
  // along with the value of buffer[0] (the "old_first_byte" argument)
  // and the value of buffer[window_size] (the "new_last_byte" argument).
  // It quickly computes the hash value of buffer[1] ... buffer[window_size]
  // without having to run Hash() on the entire window.
  //
  // The larger the window, the more advantage comes from using UpdateHash()
  // (which runs in time independent of window_size) instead of Hash().
  // Each time window_size doubles, the time to execute Hash() also doubles,
  // while the time to execute UpdateHash() remains constant.  Empirical tests
  // have borne out this statement.
  //
  // The Hash formula is linear upon all its elements (modulo 2^^32), and the
  // first element of the old buffer has weight
  // (old_first_byte * pow(kMult, window_size - 1)) % (1 << 32)
  // The hash value of buffer[1] ... buffer[window_size - 1] is obtained by
  // substracting that value, then we add new_last_byte.
  static const uint32_t RollingHashMultiplier =
      RollingHash<window_size - 1>::RollingHashMultiplier * kMult;

  uint32_t UpdateHash(uint32_t old_hash,
                      unsigned char old_first_byte,
                      unsigned char new_last_byte) const {
    return HashStep(old_hash -
                      old_first_byte * RollingHashMultiplier,
                    new_last_byte);
  }
};

template<>
class RollingHash<1> {
 public:
  static const uint32_t RollingHashMultiplier = 1;
};

}  // namespace open_vcdiff

#endif  // OPEN_VCDIFF_ROLLING_HASH_H_

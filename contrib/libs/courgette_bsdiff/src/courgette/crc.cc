// Copyright (c) 2011 The Chromium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "courgette/crc.h"

#include <stdint.h>
#include <stddef.h>

#include "zlib.h"

namespace courgette {

uint32_t CalculateCrc(const uint8_t* buffer, size_t size) {
  uint32_t crc;

  // Calculate Crc by calling CRC method in zlib
  crc = crc32(0, buffer, size);

  return ~crc;
}

}  // namespace

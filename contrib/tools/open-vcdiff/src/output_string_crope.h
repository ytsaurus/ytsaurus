// Copyright 2008 The open-vcdiff Authors. All Rights Reserved.
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
//
// Contains a class that demonstrates how to specialize OutputString for the
// crope type, so that cropes can be passed as output arguments to the encoder
// and decoder.

#ifndef OPEN_VCDIFF_OUTPUT_STRING_CROPE_H_
#define OPEN_VCDIFF_OUTPUT_STRING_CROPE_H_
#include <config.h>
#ifdef HAVE_EXT_ROPE
#error #include <ext/rope>
#include "google/output_string.h"

namespace open_vcdiff {

// *** OutputString interface for crope (OutputCrope)

// crope::reserve(), if defined, does nothing
template <>
void OutputString<__gnu_cxx::crope>::ReserveAdditionalBytes(
    size_t /*res_arg*/) { }

typedef OutputString<__gnu_cxx::crope> OutputCrope;

}  // namespace open_vcdiff
#endif  // HAVE_EXT_ROPE
#endif  // OPEN_VCDIFF_OUTPUT_STRING_CROPE_H_

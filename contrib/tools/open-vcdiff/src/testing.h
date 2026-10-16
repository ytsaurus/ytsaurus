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

#ifndef OPEN_VCDIFF_TESTING_H_
#define OPEN_VCDIFF_TESTING_H_

#include <config.h>
#include <assert.h>
#include <stdint.h>  // int64_t
#include <stdlib.h>  // rand
#include <time.h>  // gettimeofday
#include <gtest/gtest.h>

#ifdef HAVE_SYS_TIME_H
#include <sys/time.h>  // struct timeval
#endif  // HAVE_SYS_TIME_H

#ifdef HAVE_WINDOWS_H
#include <windows.h>  // QueryPerformanceCounter
#endif  // HAVE_WINDOWS_H

// CHECK is used for assertions that verify the consistency of the test itself,
// rather than correctness of the code that is being tested.
//
// It is better to use a preprocessor macro for CHECK
// than an inline function, because assert() may report
// the source file and line where the failure occurred.
//
// Putting parentheses around the macro arguments
// (e.g. "assert((X) == (Y))") would be good practice
// but would produce error messages that are inconsistent
// with those expected in the unit tests.

#define CHECK(CONDITION) assert(CONDITION)
#define CHECK_EQ(X, Y) assert(X == Y)
#define CHECK_NE(X, Y) assert(X != Y)
#define CHECK_GE(X, Y) assert(X >= Y)
#define CHECK_GT(X, Y) assert(X > Y)
#define CHECK_LE(X, Y) assert(X <= Y)
#define CHECK_LT(X, Y) assert(X < Y)

namespace open_vcdiff {

// Support for timing tests
#if defined(HAVE_GETTIMEOFDAY)
class CycleTimer {
 public:
  inline CycleTimer() {
    Reset();
  }

  inline void Reset() {
    start_time_.tv_sec = 0;
    start_time_.tv_usec = 0;
    cumulative_time_in_usec_ = 0;
  }

  inline void Start() {
    CHECK(!IsStarted());
    gettimeofday(&start_time_, NULL);
  }

  inline void Restart() {
    Reset();
    Start();
  }

  inline void Stop() {
    struct timeval end_time;
    gettimeofday(&end_time, NULL);
    CHECK(IsStarted());
    cumulative_time_in_usec_ +=
        (1000000 * (end_time.tv_sec - start_time_.tv_sec))
        + end_time.tv_usec - start_time_.tv_usec;
    start_time_.tv_sec = 0;
    start_time_.tv_usec = 0;
  }

  inline int64_t GetInUsec() {
    return cumulative_time_in_usec_;
  }

 private:
  inline bool IsStarted() {
    return (start_time_.tv_usec > 0) || (start_time_.tv_sec > 0);
  }

  struct timeval start_time_;
  int64_t cumulative_time_in_usec_;
};
#elif defined(HAVE_WINDOWS_H)
class CycleTimer {
 public:
  inline CycleTimer() {
    LARGE_INTEGER frequency;
    QueryPerformanceFrequency(&frequency);  // counts per second
    usecs_per_count_ = 1000000.0 / static_cast<double>(frequency.QuadPart);
    Reset();
  }

  inline void Reset() {
    start_time_.QuadPart = 0;
    cumulative_time_in_usec_ = 0;
  }

  inline void Start() {
    CHECK(!IsStarted());
    QueryPerformanceCounter(&start_time_);
  }

  inline void Restart() {
    Reset();
    Start();
  }

  inline void Stop() {
    LARGE_INTEGER end_time;
    QueryPerformanceCounter(&end_time);
    CHECK(IsStarted());
    double count_diff = static_cast<double>(
        end_time.QuadPart - start_time_.QuadPart);
    cumulative_time_in_usec_ +=
        static_cast<int64_t>(count_diff * usecs_per_count_);
    start_time_.QuadPart = 0;
  }

  inline int64_t GetInUsec() {
    return cumulative_time_in_usec_;
  }

 private:
  inline bool IsStarted() {
    return start_time_.QuadPart > 0;
  }

  LARGE_INTEGER start_time_;
  int64_t cumulative_time_in_usec_;
  double usecs_per_count_;
};
#else
#error CycleTimer needs an implementation that does not use gettimeofday or QueryPerformanceCounter
#endif  // HAVE_GETTIMEOFDAY

// This function returns a pseudo-random value of type IntType between 0 and
// limit.  It uses the standard rand() function to produce the value, and makes
// as many calls to rand() as needed to ensure that the values returned can fall
// within the full range specified.  It is slow, so don't include calls to this
// function when calculating the execution time of tests.
//
template<typename IntType>
inline IntType PortableRandomInRange(IntType limit) {
  uint64_t value = rand();
  double rand_limit = RAND_MAX;  // The maximum possible value
  while (rand_limit < limit) {
    // value is multiplied by (RAND_MAX + 1) each iteration. This factor will be
    // canceled out when we divide by rand_limit to get scaled_value, below.
    value = (value * (static_cast<uint64_t>(RAND_MAX) + 1)) + rand();
    rand_limit = (rand_limit * (RAND_MAX + 1.0)) + RAND_MAX;
  }
  // Translate the random 64-bit integer into a floating-point value between
  // 0.0 (inclusive) and 1.0 (inclusive).
  const double scaled_value = value / rand_limit;
  return static_cast<IntType>(limit * scaled_value);
}

}  // namespace open_vcdiff

#endif  // OPEN_VCDIFF_TESTING_H_

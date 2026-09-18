#ifndef OPEN_VCDIFF_BUFFER_EDGE_TEST_H_
#define OPEN_VCDIFF_BUFFER_EDGE_TEST_H_

#include <config.h>
#include <stdlib.h>  // free, posix_memalign
#include <string.h>  // memcpy
#include "testing.h"

#ifdef HAVE_MALLOC_H
#include <malloc.h>
#endif  // HAVE_MALLOC_H

#ifdef HAVE_SYS_MMAN_H
#if !defined(_XOPEN_SOURCE) || _XOPEN_SOURCE < 600
#undef  _XOPEN_SOURCE
#define _XOPEN_SOURCE 600  // posix_memalign
#endif
#include <sys/mman.h>  // mprotect
#endif  // HAVE_SYS_MMAN_H

#ifdef HAVE_UNISTD_H
#include <unistd.h>  // getpagesize
#endif  // HAVE_UNISTD_H

#if defined(HAVE_MPROTECT) && \
   (defined(HAVE_MEMALIGN) || defined(HAVE_POSIX_MEMALIGN))
#define HAVE_BUFFER_EDGE_TEST

// Verifies that the memory is not accessed at addresses before the start, or
// after the end, of the data buffer.
// TODO(melkov): use in vcencoder_test.cc and vcdecoder3_test.cc
class BufferEdgeTestHelper {
 public:
  enum Mode {
    BEFORE_BEGIN,
    AFTER_END,
  };

  BufferEdgeTestHelper(const char* data, size_t size, Mode mode) {
    // Allocate memory pages to fit "size" bytes plus one extra page.
    page_size_ = getpagesize();
    CHECK(page_size_ > 1);
    const size_t num_pages_for_data = (size + page_size_ - 1) / page_size_;
    const size_t alloc_size = (num_pages_for_data + 1) * page_size_;

  #ifdef HAVE_POSIX_MEMALIGN
    posix_memalign(&allocated_, page_size_, alloc_size);
  #else  // !HAVE_POSIX_MEMALIGN
    allocated_ = memalign(page_size_, alloc_size);
  #endif  // HAVE_POSIX_MEMALIGN
    char* const first_page = reinterpret_cast<char*>(allocated_);

    if (mode == BEFORE_BEGIN) {
      unreadable_page_ = first_page;
      data_ = unreadable_page_ + page_size_;
    } else {
      unreadable_page_ = first_page + num_pages_for_data * page_size_;
      data_ = unreadable_page_ - size;
    }

    memcpy(data_, data, size);
    // Make page unreadable.
    mprotect(unreadable_page_, page_size_, PROT_NONE);
  }

  ~BufferEdgeTestHelper() {
    // Undo the mprotect.
    mprotect(unreadable_page_, page_size_, PROT_READ|PROT_WRITE);
    free(allocated_);
  }

  char* data() { return data_; }

 private:
  char* data_;
  char* unreadable_page_;
  int page_size_;
  void* allocated_ = NULL;
};

#endif  // HAVE_MPROTECT && (HAVE_MEMALIGN || HAVE_POSIX_MEMALIGN)

#endif  // OPEN_VCDIFF_BUFFER_EDGE_TEST_H_

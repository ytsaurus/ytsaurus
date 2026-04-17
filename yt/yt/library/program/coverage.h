#pragma once

#if defined(_linux_) && defined(CLANG_COVERAGE)
extern "C" int __llvm_profile_write_file(void);
extern "C" void __llvm_profile_set_filename(const char* name);
#endif

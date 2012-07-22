set(UCONTEXT_INCLUDES CACHE INTERNAL "")

if (HAVE_UCONTEXT_H)
  set(UCONTEXT_INCLUDES "${UCONTEXT_INCLUDES}\n#include <ucontext.h>")
endif()

if (HAVE_SYS_UCONTEXT_H)
  set(UCONTEXT_INCLUDES "${UCONTEXT_INCLUDES}\n#include <sys/ucontext.h>")
endif()

foreach (_pc_field
  "uc_mcontext.gregs[REG_EIP]"
  "uc_mcontext.gregs[REG_RIP]"
  "uc_mcontext.sc_ip"
  "uc_mcontext.uc_regs->gregs[PT_NIP]"
  "uc_mcontext.gregs[R15]"
  "uc_mcontext.arm_pc"
  "uc_mcontext.mc_eip"
  "uc_mcontext.mc_rip"
  "uc_mcontext.__gregs[_REG_EIP]"
  "uc_mcontext.__gregs[_REG_RIP]"
  "uc_mcontext->ss.eip"
  "uc_mcontext->__ss.__eip"
  "uc_mcontext->ss.rip"
  "uc_mcontext->__ss.__rip"
  "uc_mcontext->ss.srr0"
  "uc_mcontext->__ss.__srr0")

  message(STATUS "Checking PC fetch from ucontext: ${_pc_field}")

  unset(_pc_from_ucontext_compiled)
  unset(_pc_from_ucontext_compiled CACHE)
  check_c_source_compiles("
#define _GNU_SOURCE 1
${UCONTEXT_INCLUDES}
int main(int argc, char** argv)
{
  ucontext_t uc;
  return uc.${_pc_field} == 0;
}
" _pc_from_ucontext_compiled)

  if (_pc_from_ucontext_compiled)
    message(STATUS "Found PC ucontext field: ${_pc_field}")
    set(PC_FROM_UCONTEXT ${_pc_field})
    break()
  endif()
endforeach()

if (NOT PC_FROM_UCONTEXT)
  message(FATAL_ERROR "Failed to find PC in ucontext structure.")
endif()

unset(_pc_field CACHE)
unset(_pc_from_ucontext_compiled CACHE)

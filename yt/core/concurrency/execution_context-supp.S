#if !defined(__x86_64__)
#error Unsupported platform
#endif

#define CONCAT_AUX(x, y) x ## y
#define CONCAT(x, y) CONCAT_AUX(x, y)

/* Define prologue and epilogue for various assemblers. */
#if defined(__APPLE__)
.section __TEXT,__text,regular,pure_instructions
#   define BEGIN(name) \
    .text ; \
    .globl CONCAT(_, name) ; \
    .align 4, 0x90 /* 0x90 = nop */ ; \
CONCAT(_, name):
#   define END(name)
#else
.section .note.GNU-stack, "", %progbits
#   define BEGIN(name) \
    .text ; \
    .global name ; \
    .align 4, 0x90 /* 0x90 = nop */ ; \
    .p2align 4, 0x90, 15 ; \
    .type name, @function ; \
name:
#   define END(name) \
    .size name, .-name
#endif

#define SWITCH_EXECUTION_CONTEXT_IMPL SwitchExecutionContextImpl

/**
 * x86_64 Calling Convention:
 * Arguments are passed via:
 *   - GP: %rdi, %rsi, %rdx, %rcx, %r8, %r9
 *   - FP: %xmm0, %xmm1, ..., %xmm7
 * Return value is passed via:
 *   - GP: %rax
 *   - FP: %xmm0, %xmm1
 * Callee preserves:
 *   - GP: %rbx, %rbp, %rsp, %r12, %r13, %r14, %r15
 * Others:
 *   - %r10 - Static Chain Pointer (?)
 *   - %r11 - Scratch Register
 *   - %xmm8, %xmm9, ..., %xmm15 - Scratch Registers
 */

BEGIN(SWITCH_EXECUTION_CONTEXT_IMPL)
    pushq %rbp
    pushq %rbx
    pushq %r12
    pushq %r13
    pushq %r14
    pushq %r15
    movq %rsp, (%rdi) /* Save old SP. */
    movq (%rsi), %rsp /* Load new SP. */
    popq %r15
    popq %r14
    popq %r13
    popq %r12
    popq %rbx
    popq %rbp
    popq %r8 /* Extract return address. */
    mov %rdx, %rax /* Use third argument as a return value. */
    mov %rdx, %rdi /* Use third argument as a first argument. */
    mov %r15, %rcx /* Use top value from the stack as fourth argument to pass
                      a real trampoline through the sanitizer trampoline.
                      Real trampolines just ignore it.  */
    jmpq *%r8
END(SWITCH_EXECUTION_CONTEXT_IMPL)


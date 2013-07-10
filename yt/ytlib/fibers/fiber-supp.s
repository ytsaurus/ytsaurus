#if !defined(__x86_64__)
#error Unsupported platform
#endif

.file "fiber-supp.s"
#ifdef __APPLE__
.section __TEXT,__text,regular,pure_instructions
#define FIBER_CONTEXT_TRAMPOLINE __ZN3NYT13TFiberContext10TrampolineEv
#define FIBER_CONTEXT_TRANSFER_TO __ZN3NYT13TFiberContext10TransferToEPS0_S1_
#else
.section .note.GNU-stack, "", %progbits
#define FIBER_CONTEXT_TRAMPOLINE _ZN3NYT13TFiberContext10TrampolineEv
#define FIBER_CONTEXT_TRANSFER_TO _ZN3NYT13TFiberContext10TransferToEPS0_S1_
#endif

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

/* void TFiberContext::Trampoline(); */
.text
.global FIBER_CONTEXT_TRAMPOLINE
.align 16, 0x90 /* 0x90 = nop */
#ifndef __APPLE__
.type FIBER_CONTEXT_TRAMPOLINE, @function
.p2align 4,,15
#endif
FIBER_CONTEXT_TRAMPOLINE:
    .cfi_startproc
#ifndef __APPLE__
    .cfi_undefined rip
#endif
    movq %r12, %rdi
    .cfi_endproc
    jmpq *%rbx
#ifndef __APPLE__
.size FIBER_CONTEXT_TRAMPOLINE, .-FIBER_CONTEXT_TRAMPOLINE
#endif

/* void TFiberContext::TransferTo(TFiberContext* previous, TFiberContext* next); */
.text
.global FIBER_CONTEXT_TRANSFER_TO
.align 16, 0x90 /* 0x90 = nop */
#ifndef __APPLE__
.type FIBER_CONTEXT_TRANSFER_TO, @function
.p2align 4,,15
#endif
FIBER_CONTEXT_TRANSFER_TO:
    .cfi_startproc
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
    popq %rcx
    .cfi_endproc
    jmpq *%rcx
#ifndef __APPLE__
.size FIBER_CONTEXT_TRANSFER_TO, .-FIBER_CONTEXT_TRANSFER_TO
#endif


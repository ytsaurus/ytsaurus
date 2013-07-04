#if !defined(__x86_64__)
#error Unsupported platform
#endif

.file "fiber-supp.s"
.section .note.GNU-stack, "", %progbits

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
.global _ZN3NYT13TFiberContext10TrampolineEv
.type _ZN3NYT13TFiberContext10TrampolineEv, @function
.align 16
.p2align 4,,15
_ZN3NYT13TFiberContext10TrampolineEv:
    .cfi_startproc
    .cfi_undefined rip
    movq %r12, %rdi
    callq *%rbx
    callq abort@PLT
    .cfi_endproc
.size _ZN3NYT13TFiberContext10TrampolineEv, .-_ZN3NYT13TFiberContext10TrampolineEv

/* void TFiberContext::TransferTo(TFiberContext* previous, TFiberContext* next); */
.text
.global _ZN3NYT13TFiberContext10TransferToEPS0_S1_
.type _ZN3NYT13TFiberContext10TransferToEPS0_S1_, @function
.align 16
.p2align 4,,15
_ZN3NYT13TFiberContext10TransferToEPS0_S1_:
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
.size _ZN3NYT13TFiberContext10TransferToEPS0_S1_, .-_ZN3NYT13TFiberContext10TransferToEPS0_S1_


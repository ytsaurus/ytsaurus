/**
*** Copyright (C) 1985-2005 Intel Corporation.  All rights reserved.
***
*** The information and source code contained herein is the exclusive
*** property of Intel Corporation and may not be disclosed, examined
*** or reproduced in whole or in part without explicit written authorization
*** from the company.
***
*** @(#)mmintrin.h	1.5 02/12/99 08:14:16
**/
/*
 * Definitions and declarations for use with compiler intrinsics.
 */
#ifndef _MMINTRIN_H_INCLUDED
#define _MMINTRIN_H_INCLUDED

#if defined __cplusplus
extern "C" { /* Begin "C" */
/* Intrinsics use C name-mangling.
 */
#endif /* __cplusplus */

#if defined(__INTEL_COMPILER)
#ifdef __QMSPP_
typedef union __declspec(align(8)) {__int64 __m;} __m64;
#else
typedef unsigned long long __m64; 
#endif /* __QMSPP_ */
#elif _MSC_FULL_VER < 12008804
/*
 * In versions of MSC that don't support the __m64 type, provide a "dummy"
 * definition here so that MSC can at least do a syntax check on files that
 * use this type.
 */
typedef struct __m64 {int i[2];} __m64;
#elif _MSC_VER >= 1300
typedef union __declspec(intrin_type) __declspec(align(8)) __m64
{
    __int64 __m;
} __m64;
#endif

/* General support intrinsics */
void  _m_empty(void);
__m64 _m_from_int(int i);
int   _m_to_int(__m64 m);
__m64 _m_packsswb(__m64 m1, __m64 m2);
__m64 _m_packssdw(__m64 m1, __m64 m2);
__m64 _m_packuswb(__m64 m1, __m64 m2);
__m64 _m_punpckhbw(__m64 m1, __m64 m2);
__m64 _m_punpckhwd(__m64 m1, __m64 m2);
__m64 _m_punpckhdq(__m64 m1, __m64 m2);
__m64 _m_punpcklbw(__m64 m1, __m64 m2);
__m64 _m_punpcklwd(__m64 m1, __m64 m2);
__m64 _m_punpckldq(__m64 m1, __m64 m2);

/* Packed arithmetic intrinsics */
__m64 _m_paddb(__m64 m1, __m64 m2);
__m64 _m_paddw(__m64 m1, __m64 m2);
__m64 _m_paddd(__m64 m1, __m64 m2);
__m64 _m_paddsb(__m64 m1, __m64 m2);
__m64 _m_paddsw(__m64 m1, __m64 m2);
__m64 _m_paddusb(__m64 m1, __m64 m2);
__m64 _m_paddusw(__m64 m1, __m64 m2);
__m64 _m_psubb(__m64 m1, __m64 m2);
__m64 _m_psubw(__m64 m1, __m64 m2);
__m64 _m_psubd(__m64 m1, __m64 m2);
__m64 _m_psubsb(__m64 m1, __m64 m2);
__m64 _m_psubsw(__m64 m1, __m64 m2);
__m64 _m_psubusb(__m64 m1, __m64 m2);
__m64 _m_psubusw(__m64 m1, __m64 m2);
__m64 _m_pmaddwd(__m64 m1, __m64 m2);
__m64 _m_pmulhw(__m64 m1, __m64 m2);
__m64 _m_pmullw(__m64 m1, __m64 m2);

/* Shift intrinsics */
__m64 _m_psllw(__m64 m, __m64 count);
__m64 _m_psllwi(__m64 m, int count);
__m64 _m_pslld(__m64 m, __m64 count);
__m64 _m_pslldi(__m64 m, int count);
__m64 _m_psllq(__m64 m, __m64 count);
__m64 _m_psllqi(__m64 m, int count);
__m64 _m_psraw(__m64 m, __m64 count);
__m64 _m_psrawi(__m64 m, int count);
__m64 _m_psrad(__m64 m, __m64 count);
__m64 _m_psradi(__m64 m, int count);
__m64 _m_psrlw(__m64 m, __m64 count);
__m64 _m_psrlwi(__m64 m, int count);
__m64 _m_psrld(__m64 m, __m64 count);
__m64 _m_psrldi(__m64 m, int count);
__m64 _m_psrlq(__m64 m, __m64 count);
__m64 _m_psrlqi(__m64 m, int count);

/* Logical intrinsics */
__m64 _m_pand(__m64 m1, __m64 m2);
__m64 _m_pandn(__m64 m1, __m64 m2);
__m64 _m_por(__m64 m1, __m64 m2);
__m64 _m_pxor(__m64 m1, __m64 m2);

/* Comparison intrinsics */
__m64 _m_pcmpeqb(__m64 m1, __m64 m2);
__m64 _m_pcmpeqw(__m64 m1, __m64 m2);
__m64 _m_pcmpeqd(__m64 m1, __m64 m2);
__m64 _m_pcmpgtb(__m64 m1, __m64 m2);
__m64 _m_pcmpgtw(__m64 m1, __m64 m2);
__m64 _m_pcmpgtd(__m64 m1, __m64 m2);

/* Utility intrinsics */
__m64 _mm_setzero_si64();
__m64 _mm_set_pi32(int i1, int i0);
__m64 _mm_set_pi16(short s3, short s2, short s1, short s0);
__m64 _mm_set_pi8(char b7, char b6, char b5, char b4,
		  char b3, char b2, char b1, char b0);
__m64 _mm_set1_pi32(int i);
__m64 _mm_set1_pi16(short s);
__m64 _mm_set1_pi8(char b);
__m64 _mm_setr_pi32(int i1, int i0);
__m64 _mm_setr_pi16(short s3, short s2, short s1, short s0);
__m64 _mm_setr_pi8(char b7, char b6, char b5, char b4,
		   char b3, char b2, char b1, char b0);

#if defined(__x86_64) || defined(_M_X64)
__m64   _m_from_int64(__int64 a);
__int64 _m_to_int64(__m64 a);
#endif

/* Alternate intrinsic name definitions */
#define _mm_cvtsi64_m64   _m_from_int64
#define _mm_cvtm64_si64   _m_to_int64
#define _mm_empty         _m_empty
#define _mm_cvtsi32_si64  _m_from_int
#define _mm_cvtsi64_si32  _m_to_int
#define _mm_packs_pi16    _m_packsswb
#define _mm_packs_pi32    _m_packssdw
#define _mm_packs_pu16    _m_packuswb
#define _mm_unpackhi_pi8  _m_punpckhbw
#define _mm_unpackhi_pi16 _m_punpckhwd
#define _mm_unpackhi_pi32 _m_punpckhdq
#define _mm_unpacklo_pi8  _m_punpcklbw
#define _mm_unpacklo_pi16 _m_punpcklwd
#define _mm_unpacklo_pi32 _m_punpckldq
#define _mm_add_pi8       _m_paddb
#define _mm_add_pi16      _m_paddw
#define _mm_add_pi32      _m_paddd
#define _mm_adds_pi8      _m_paddsb
#define _mm_adds_pi16     _m_paddsw
#define _mm_adds_pu8      _m_paddusb
#define _mm_adds_pu16     _m_paddusw
#define _mm_sub_pi8       _m_psubb
#define _mm_sub_pi16      _m_psubw
#define _mm_sub_pi32      _m_psubd
#define _mm_subs_pi8      _m_psubsb
#define _mm_subs_pi16     _m_psubsw
#define _mm_subs_pu8      _m_psubusb
#define _mm_subs_pu16     _m_psubusw
#define _mm_madd_pi16     _m_pmaddwd
#define _mm_mulhi_pi16    _m_pmulhw
#define _mm_mullo_pi16    _m_pmullw
#define _mm_sll_pi16      _m_psllw
#define _mm_slli_pi16     _m_psllwi
#define _mm_sll_pi32      _m_pslld
#define _mm_slli_pi32     _m_pslldi
#define _mm_sll_si64      _m_psllq
#define _mm_slli_si64     _m_psllqi
#define _mm_sra_pi16      _m_psraw
#define _mm_srai_pi16     _m_psrawi
#define _mm_sra_pi32      _m_psrad
#define _mm_srai_pi32     _m_psradi
#define _mm_srl_pi16      _m_psrlw
#define _mm_srli_pi16     _m_psrlwi
#define _mm_srl_pi32      _m_psrld
#define _mm_srli_pi32     _m_psrldi
#define _mm_srl_si64      _m_psrlq
#define _mm_srli_si64     _m_psrlqi
#define _mm_and_si64      _m_pand
#define _mm_andnot_si64   _m_pandn
#define _mm_or_si64       _m_por
#define _mm_xor_si64      _m_pxor
#define _mm_cmpeq_pi8     _m_pcmpeqb
#define _mm_cmpeq_pi16    _m_pcmpeqw
#define _mm_cmpeq_pi32    _m_pcmpeqd
#define _mm_cmpgt_pi8     _m_pcmpgtb
#define _mm_cmpgt_pi16    _m_pcmpgtw
#define _mm_cmpgt_pi32    _m_pcmpgtd


#if defined __cplusplus
}; /* End "C" */
#endif /* __cplusplus */

#endif /* _MMINTRIN_H_INCLUDED */

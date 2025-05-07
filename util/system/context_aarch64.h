#pragma once

typedef unsigned long __myjmp_buf[22];

#define MJB_X19 0
#define MJB_X20 1

#define FRAME_CNT 10
#define PROGR_CNT 11
#define STACK_CNT 13
#define EXTRA_PUSH_ARGS 2

#define JUMP_FUNCTION MJB_X19
#define JUMP_ARGUMENT MJB_X20

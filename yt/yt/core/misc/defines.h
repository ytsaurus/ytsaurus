#pragma once

#define CONCAT_IMPL(x, y) x##y
#define CONCAT(x, y) CONCAT_IMPL(x, y)

//! This define can be used for generating unique variable names.
//! For example, int UNIQUE_NAME(a) = 2; int UNIQUE_NAME(a) = 3; is a valid code.
#define UNIQUE_NAME(x) CONCAT(x, __COUNTER__)

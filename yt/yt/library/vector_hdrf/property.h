#pragma once

//! Defines a trivial public read-write property that is passed by value.
//! All arguments after name are used as default value (via braced-init-list).
#define DEFINE_BYVAL_RW_PROPERTY(type, name, ...) \
protected: \
    type name##_ { __VA_ARGS__ }; \
    \
public: \
    Y_FORCE_INLINE type Get##name() const \
    { \
        return name##_; \
    } \
    \
    Y_FORCE_INLINE void Set##name(type value) \
    { \
        name##_ = value; \
    } \

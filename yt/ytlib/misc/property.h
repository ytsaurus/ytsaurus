#pragma once

////////////////////////////////////////////////////////////////////////////////

//! Defines a trivial public property that is accessed by reference.
#define DECLARE_BYREF_RW_PROPERTY(name, type) \
private: \
    type name##_; \
    \
public: \
    FORCED_INLINE type& name() \
    { \
        return name##_; \
    } \
    \
    FORCED_INLINE const type& name() const \
    { \
        return name##_; \
    }

//! Defines a trivial public property that is accessed by value.
#define DECLARE_BYVAL_RW_PROPERTY(name, type) \
private: \
    type name##_; \
    \
public: \
    FORCED_INLINE type Get##name() const \
    { \
        return name##_; \
    } \
    \
    FORCED_INLINE void Set##name(type value) \
    { \
        name##_ = value; \
    }

////////////////////////////////////////////////////////////////////////////////

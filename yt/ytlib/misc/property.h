#pragma once

////////////////////////////////////////////////////////////////////////////////

//! Defines a public read-write property.
#define DECLARE_RW_PROPERTY(name, type) \
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

////////////////////////////////////////////////////////////////////////////////

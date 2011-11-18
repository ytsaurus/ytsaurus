#pragma once

////////////////////////////////////////////////////////////////////////////////

//! Defines a trivial public read-write property that is passed by reference.
#define DECLARE_BYREF_RW_PROPERTY(type, name) \
protected: \
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

//! Defines a trivial public read-only property that is passed by reference.
#define DECLARE_BYREF_RO_PROPERTY(type, name) \
protected: \
    type name##_; \
    \
public: \
    FORCED_INLINE const type& name() const \
    { \
        return name##_; \
    }

//! Defines a trivial public read-write property that is passed by value.
#define DECLARE_BYVAL_RW_PROPERTY(type, name) \
protected: \
    type name##_; \
    \
public: \
    FORCED_INLINE type Get##name() const \
    { \
        return name##_; \
    } \
    \
    FORCED_INLINE void Set##name(const type& value) \
    { \
        name##_ = value; \
    } \
    \
    FORCED_INLINE void Set##name(type&& value) \
    { \
        name##_ = MoveRV(value); \
    }

//! Defines a trivial public read-only property that is passed by value.
#define DECLARE_BYVAL_RO_PROPERTY(type, name) \
protected: \
    type name##_; \
    \
public: \
    FORCED_INLINE type Get##name() const \
    { \
        return name##_; \
    }

////////////////////////////////////////////////////////////////////////////////

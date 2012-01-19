#pragma once

////////////////////////////////////////////////////////////////////////////////

//! Declares a trivial public read-write property that is passed by reference.
#define DECLARE_BYREF_RW_PROPERTY(type, name) \
public: \
    type& name(); \
    const type& name() const;

//! Defines a trivial public read-write property that is passed by reference.
#define DEFINE_BYREF_RW_PROPERTY(type, name) \
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

//! Forwards a trivial public read-write property that is passed by reference.
#define DELEGATE_BYREF_RW_PROPERTY(declaringType, type, name, delegateTo) \
    type& declaringType::name() \
    { \
        return (delegateTo).name(); \
    } \
    \
    const type& declaringType::name() const \
    { \
        return (delegateTo).name(); \
    }

////////////////////////////////////////////////////////////////////////////////

//! Declares a trivial public read-only property that is passed by reference.
#define DECLARE_BYREF_RO_PROPERTY(type, name) \
public: \
    FORCED_INLINE const type& name() const;

//! Defines a trivial public read-only property that is passed by reference.
#define DEFINE_BYREF_RO_PROPERTY(type, name) \
protected: \
    type name##_; \
    \
public: \
    FORCED_INLINE const type& name() const \
    { \
        return name##_; \
    }

//! Forwards a trivial public read-only property that is passed by reference.
#define DELEGATE_BYREF_RO_PROPERTY(declaringType, type, name, delegateTo) \
    const type& declaringType::name() const \
    { \
        return (delegateTo).name(); \
    }

////////////////////////////////////////////////////////////////////////////////

//! Declares a trivial public read-write property that is passed by value.
#define DECLARE_BYVAL_RW_PROPERTY(type, name) \
public: \
    type Get##name() const; \
    void Set##name(const type& value); \
    void Set##name(type&& value);

//! Defines a trivial public read-write property that is passed by value.
#define DEFINE_BYVAL_RW_PROPERTY(type, name) \
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

//! Forwards a trivial public read-write property that is passed by value.
#define DELEGATE_BYVAL_RW_PROPERTY(declaringType, type, name, delegateTo) \
    type declaringType::Get##name() \
    { \
        return (delegateTo).Get##name(); \
    } \
    \
    void declaringType::Set##name(const type& value) \
    { \
        (delegateTo).Set##name(value); \
    } \
    \
    void declaringType::Set##name(type&& value) const \
    { \
        (delegateTo).Set##name(ForwardRV<type>(value)); \
    }

////////////////////////////////////////////////////////////////////////////////

//! Declares a trivial public read-only property that is passed by value.
#define DECLARE_BYVAL_RO_PROPERTY(type, name) \
public: \
    type Get##name() const;

//! Defines a trivial public read-only property that is passed by value.
#define DEFINE_BYVAL_RO_PROPERTY(type, name) \
protected: \
    type name##_; \
    \
public: \
    FORCED_INLINE type Get##name() const \
    { \
        return name##_; \
    }

//! Forwards a trivial public read-only property that is passed by value.
#define DELEGATE_BYVAL_RO_PROPERTY(declaringType, type, name, delegateTo) \
    type declaringType::Get##name() \
    { \
        return (delegateTo).Get##name(); \
    }

////////////////////////////////////////////////////////////////////////////////

#pragma once

////////////////////////////////////////////////////////////////////////////////

//! Declares a trivial public read-write property that is passed by reference.
#define DECLARE_BYREF_RW_PROPERTY(type, name) \
public: \
    type& name(); \
    const type& name() const;

//! Defines a trivial public read-write property that is passed by reference.
//! All arguments after name are used as default value (via braced-init-list).
#define DEFINE_BYREF_RW_PROPERTY(type, name, ...) \
protected: \
    type name##_ { __VA_ARGS__ }; \
    \
public: \
    Y_FORCE_INLINE type& name() \
    { \
        return name##_; \
    } \
    \
    Y_FORCE_INLINE const type& name() const \
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
    const type& name() const;

//! Defines a trivial public read-only property that is passed by reference.
//! All arguments after name are used as default value (via braced-init-list).
#define DEFINE_BYREF_RO_PROPERTY(type, name, ...) \
protected: \
    type name##_ { __VA_ARGS__ }; \
    \
public: \
    Y_FORCE_INLINE const type& name() const \
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
    void Set##name(type value);

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

//! Forwards a trivial public read-write property that is passed by value.
#define DELEGATE_BYVAL_RW_PROPERTY(declaringType, type, name, delegateTo) \
    type declaringType::Get##name() \
    { \
        return (delegateTo).Get##name(); \
    } \
    \
    void declaringType::Set##name(type value) \
    { \
        (delegateTo).Set##name(value); \
    }

////////////////////////////////////////////////////////////////////////////////

//! Declares a trivial public read-only property that is passed by value.
#define DECLARE_BYVAL_RO_PROPERTY(type, name) \
public: \
    type Get##name() const;

//! Defines a trivial public read-only property that is passed by value.
//! All arguments after name are used as default value (via braced-init-list).
#define DEFINE_BYVAL_RO_PROPERTY(type, name, ...) \
protected: \
    type name##_ { __VA_ARGS__ }; \
    \
public: \
    Y_FORCE_INLINE type Get##name() const \
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

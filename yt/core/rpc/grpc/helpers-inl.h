#pragma once
#ifndef HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include helpers.h"
// For the sake of sane code completion.
#include "helpers.h"
#endif
#undef HELPERS_INL_H_

namespace NYT {
namespace NRpc {
namespace NGrpc {

////////////////////////////////////////////////////////////////////////////////

template <class T, void(*Deletor)(T*)>
TGrpcObjectPtr<T, Deletor>::TGrpcObjectPtr()
    : Ptr_(nullptr)
{ }

template <class T, void(*Deletor)(T*)>
TGrpcObjectPtr<T, Deletor>::~TGrpcObjectPtr()
{
    Reset();
}

template <class T, void(*Deletor)(T*)>
T* TGrpcObjectPtr<T, Deletor>::Unwrap()
{
    return Ptr_;
}

template <class T, void(*Deletor)(T*)>
const T* TGrpcObjectPtr<T, Deletor>::Unwrap() const
{
    return Ptr_;
}

template <class T, void(*Deletor)(T*)>
TGrpcObjectPtr<T, Deletor>::operator bool() const
{
    return Ptr_ != nullptr;
}

template <class T, void(*Deletor)(T*)>
T** TGrpcObjectPtr<T, Deletor>::GetPtr()
{
    Reset();
    return &Ptr_;
}

template <class T, void(*Deletor)(T*)>
void TGrpcObjectPtr<T, Deletor>::Reset()
{
    if (Ptr_) {
        Deletor(Ptr_);
        Ptr_ = nullptr;
    }
}

template <class T, void(*Deletor)(T*)>
TGrpcObjectPtr<T, Deletor>::TGrpcObjectPtr(T* obj)
    : Ptr_(obj)
{ }

template <class T, void(*Deletor)(T*)>
TGrpcObjectPtr<T, Deletor>::TGrpcObjectPtr(TGrpcObjectPtr&& other)
    : Ptr_(other.Ptr_)
{
    other.Ptr_ = nullptr;
}

template <class T, void(*Deletor)(T*)>
TGrpcObjectPtr<T, Deletor>& TGrpcObjectPtr<T, Deletor>::operator=(TGrpcObjectPtr&& other)
{
    if (this != &other) {
        Reset();
        Ptr_ = other.Ptr_;
        other.Ptr_ = nullptr;
    }
    return *this;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NGrpc
} // namespace NRpc
} // namespace NYT

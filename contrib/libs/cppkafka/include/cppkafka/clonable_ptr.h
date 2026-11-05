/*
 * Copyright (c) 2017, Matias Fontanini
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 * * Redistributions of source code must retain the above copyright
 *   notice, this list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above
 *   copyright notice, this list of conditions and the following disclaimer
 *   in the documentation and/or other materials provided with the
 *   distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

#ifndef CPPKAFKA_CLONABLE_PTR_H
#define CPPKAFKA_CLONABLE_PTR_H

#include <memory>

namespace cppkafka {

/**
 * Smart pointer which allows copying via a clone functor
 */
template <typename T, typename Deleter, typename Cloner>
class ClonablePtr {
public:
    /**
     * \brief Creates an instance
     *
     * \param ptr The pointer to be wrapped
     * \param deleter The deleter functor
     * \param cloner The clone functor
     */
    ClonablePtr(T* ptr, const Deleter& deleter, const Cloner& cloner)
    : handle_(ptr, deleter), cloner_(cloner) {

    }

    /**
     * \brief Copies the given ClonablePtr
     *
     * Cloning will be done by invoking the Cloner type
     *
     * \param rhs The pointer to be copied
     */
    ClonablePtr(const ClonablePtr& rhs)
    : handle_(std::unique_ptr<T, Deleter>(rhs.try_clone(), rhs.get_deleter())),
      cloner_(rhs.get_cloner()) {

    }

    /** 
     * \brief Copies and assigns the given pointer
     *
     * \param rhs The pointer to be copied
     */
    ClonablePtr& operator=(const ClonablePtr& rhs) {
        if (this != &rhs) {
            handle_ = std::unique_ptr<T, Deleter>(rhs.try_clone(), rhs.get_deleter());
            cloner_ = rhs.get_cloner();
        }
        return *this;
    }

    ClonablePtr(ClonablePtr&&) = default;
    ClonablePtr& operator=(ClonablePtr&&) = default;
    ~ClonablePtr() = default;

    /**
     * \brief Getter for the internal pointer
     */
    T* get() const {
        return handle_.get();
    }
    
    /**
     * \brief Releases ownership of the internal pointer
     */
    T* release() {
        return handle_.release();
    }
    
    /**
     * \brief Reset the internal pointer to a new one
     */
    void reset(T* ptr) {
        handle_.reset(ptr);
    }
    
    /**
     * \brief Get the deleter
     */
    const Deleter& get_deleter() const {
        return handle_.get_deleter();
    }
    
    /**
     * \brief Get the cloner
     */
    const Cloner& get_cloner() const {
        return cloner_;
    }
    
    /**
     * \brief Indicates whether this ClonablePtr instance is valid (not null)
     */
    explicit operator bool() const {
        return static_cast<bool>(handle_);
    }
private:
    T* try_clone() const {
        return cloner_ ? cloner_(get()) : get();
    }
    
    std::unique_ptr<T, Deleter> handle_;
    Cloner cloner_;
};

} // cppkafka

#endif // CPPKAFKA_CLONABLE_PTR_H

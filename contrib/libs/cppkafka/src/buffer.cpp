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

#include <algorithm>
#include <iostream>
#include <iomanip>
#include "buffer.h"

using std::string;
using std::equal;
using std::lexicographical_compare;
using std::ostream;
using std::hex;
using std::dec;

namespace cppkafka {

Buffer::Buffer() 
: data_(nullptr), size_(0) {

}

Buffer::Buffer(const string& data) 
: Buffer(data.data(), data.size()) {

}

const Buffer::DataType* Buffer::get_data() const {
    return data_;
}

size_t Buffer::get_size() const {
    return size_;
}

Buffer::const_iterator Buffer::begin() const {
    return data_;
}

Buffer::const_iterator Buffer::end() const {
    return data_ + size_;
}

Buffer::operator bool() const {
    return size_ != 0;
}

Buffer::operator string() const {
    return string(data_, data_ + size_);
}

ostream& operator<<(ostream& output, const Buffer& rhs) {
    for (const uint8_t value : rhs) {
        if (value >= 0x20 && value < 0x7f) {
            output << value;
        }
        else {
            output << "\\x";
            if (value < 16) {
                output << '0';
            }
            output << hex << static_cast<int>(value) << dec;
        }
    }
    return output;
}

bool operator==(const Buffer& lhs, const Buffer& rhs) {
    if (lhs.get_size() != rhs.get_size()) {
        return false;
    }
    return equal(lhs.get_data(), lhs.get_data() + lhs.get_size(), rhs.get_data());
}

bool operator!=(const Buffer& lhs, const Buffer& rhs) { 
    return !(lhs == rhs);
}

bool operator<(const Buffer& lhs, const Buffer& rhs) {
    return lexicographical_compare(lhs.get_data(), lhs.get_data() + lhs.get_size(),
                                   rhs.get_data(), rhs.get_data() + rhs.get_size());
}

bool operator>(const Buffer& lhs, const Buffer& rhs) {
    return lexicographical_compare(rhs.get_data(), rhs.get_data() + rhs.get_size(),
                                   lhs.get_data(), lhs.get_data() + lhs.get_size());
}

bool operator<=(const Buffer& lhs, const Buffer& rhs) {
    return !(lhs > rhs);
}

bool operator>=(const Buffer& lhs, const Buffer& rhs) {
    return !(lhs < rhs);
}

} // cppkafka

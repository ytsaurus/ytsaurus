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

#ifndef CPPKAFKA_MACROS_H
#define CPPKAFKA_MACROS_H

// If cppkafka was built into a shared library
#if defined(_WIN32) && !defined(CPPKAFKA_STATIC)
    // Export/import symbols, depending on whether we're compiling or consuming the lib
    #ifdef cppkafka_EXPORTS
        #define CPPKAFKA_API __declspec(dllexport)
    #else
        #define CPPKAFKA_API __declspec(dllimport)
    #endif // cppkafka_EXPORTS
#else 
    // Otherwise, default this to an empty macro
    #define CPPKAFKA_API
#endif // _WIN32 && !CPPKAFKA_STATIC

// See: https://github.com/edenhill/librdkafka/issues/1792
#define RD_KAFKA_QUEUE_REFCOUNT_BUG_VERSION 0x000b0500 //v0.11.5.00
#define RD_KAFKA_HEADERS_SUPPORT_VERSION 0x000b0402 //v0.11.4.02
#define RD_KAFKA_ADMIN_API_SUPPORT_VERSION 0x000b0500 //v0.11.5.00
#define RD_KAFKA_MESSAGE_LATENCY_SUPPORT_VERSION 0x000b0000 //v0.11.0.00
#define RD_KAFKA_EVENT_STATS_SUPPORT_VERSION 0x000b0000 //v0.11.0.00
#define RD_KAFKA_MESSAGE_STATUS_SUPPORT_VERSION 0x01000002 //v1.0.0.02
#define RD_KAFKA_STORE_OFFSETS_SUPPORT_VERSION 0x00090501 //v0.9.5.01
#define RD_KAFKA_DESTROY_FLAGS_SUPPORT_VERSION 0x000b0600 //v0.11.6

#endif // CPPKAFKA_MACROS_H

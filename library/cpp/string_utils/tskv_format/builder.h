#pragma once

#include "escape.h"

#include <util/generic/explicit_type.h>
#include <util/stream/str.h>

namespace NTskvFormat {
    class TLogBuilder {
    private:
        TStringStream Out_;

    public:
        TLogBuilder() = default;

        TLogBuilder(TStringBuf logType, ui32 unixtime) {
            Begin(logType, unixtime);
        }

        TLogBuilder(TStringBuf logType) {
            Begin(logType);
        }

        TLogBuilder& Add(TStringBuf fieldName, TStringBuf fieldValue) {
            return AddKey(fieldName).AddValue(fieldValue);
        }

        TLogBuilder& AddUnescaped(TStringBuf fieldName, TStringBuf fieldValue) {
            return AddUnescapedKey(fieldName).AddUnescapedValue(fieldValue);
        }

        // Must be followed by Add(Unescaped)Value
        TLogBuilder& AddKey(TStringBuf fieldName) {
            if (!Out_.Empty()) {
                *this << '\t';
            }
            Escape(fieldName, Out_.Str());
            *this << '=';
            return *this;
        }

        // Must be followed by Add(Unescaped)Value
        TLogBuilder& AddUnescapedKey(TStringBuf fieldName) {
            if (!Out_.Empty()) {
                *this << '\t';
            }
            *this << fieldName << '=';
            return *this;
        }

        // Must be called only after Add(Unescaped)Key
        TLogBuilder& AddValue(TStringBuf fieldValue) {
            Escape(fieldValue, Out_.Str());
            return *this;
        }

        // Must be called only after Add(Unescaped)Key
        TLogBuilder& AddUnescapedValue(TStringBuf fieldValue) {
            *this << fieldValue;
            return *this;
        }

        // Adds unescaped string to output.
        // `part` must be a valid tskv substring, as if created by another TLogBuilder.
        // Examples: "tskv\tk1=v1", "k1=v1\n", "k1=v1\tk2=v2". Incorrect examples: "k1=", "=v1", "k1=v1\t", "\tk1=v1".
        TLogBuilder& AddUnescapedPart(TStringBuf part) {
            if (!Out_.Empty()) {
                *this << '\t';
            }
            *this << part;
            return *this;
        }

        TLogBuilder& AddUnescapedPairs(TStringBuf pairs) {
            return AddUnescapedPart(pairs);
        }

        TLogBuilder& Begin(TStringBuf logType, ui32 unixtime) {
            *this << "tskv\ttskv_format="_sb << logType << "\tunixtime="_sb << unixtime;
            return *this;
        }

        TLogBuilder& Begin(TStringBuf logType) {
            *this << "tskv\ttskv_format="_sb << logType;
            return *this;
        }

        TLogBuilder& Begin() {
            *this << "tskv"_sb;
            return *this;
        }

        TLogBuilder& End() {
            *this << '\n';
            return *this;
        }

        TLogBuilder& Clear() {
            Out_.Clear();
            return *this;
        }

        TString& Str() {
            return Out_.Str();
        }

    private:
        TLogBuilder& operator<<(char c) {
            Str().push_back(c);
            return *this;
        }

        TLogBuilder& operator<<(TExplicitType<TStringBuf> s) {
            Str().append(s);
            return *this;
        }

        template <typename T>
        requires (!std::convertible_to<T, TStringBuf>)  // Use TStringBufs instead of string literals.
        TLogBuilder& operator<<(const T& value) {
            Out_ << value;
            return *this;
        }
    };

}

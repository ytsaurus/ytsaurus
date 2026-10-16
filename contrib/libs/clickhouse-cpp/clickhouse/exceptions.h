#pragma once

#include "server_exception.h"

#include <stdexcept>

namespace clickhouse {

class Error : public std::runtime_error {
    using std::runtime_error::runtime_error;
};

// Caused by any user-related code, like invalid column types or arguments passed to any method.
class ValidationError : public Error {
    using Error::Error;
};

// Buffers+IO errors, failure to serialize/deserialize, checksum mismatches, etc.
class ProtocolError : public Error {
    using Error::Error;
};

class UnimplementedError : public Error {
    using Error::Error;
};

// Internal validation error.
class AssertionError : public Error {
    using Error::Error;
};

class OpenSSLError : public Error {
    using Error::Error;
};

class CompressionError : public Error {
    using Error::Error;
};

// Exception received from server.
class ServerException : public Error {
public:
    ServerException(std::shared_ptr<Exception> e)
        : Error(std::string())
        , exception_(e)
    {
    }

    int GetCode() const {
        return exception_->code;
    }

    const Exception& GetException() const {
        return *exception_;
    }

    const char* what() const noexcept override {
        return exception_->display_text.c_str();
    }

private:
    std::shared_ptr<Exception> exception_;
};
using ServerError = ServerException;

}

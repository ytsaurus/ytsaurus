#include <yt/cpp/roren/library/save_load_wrapper/save_load_wrapper.h>

class TVirtual {
public:
    virtual ~TVirtual() = default;

    virtual int32_t GetData() const = 0;
};
using TVirtualPtr = NRoren::TSaveLoadWrapper<TVirtual>;

class TImpl1 : public TVirtual {
public:
    TImpl1(int8_t data = 0)
        : Data_(data)
    { }

    int32_t GetData() const override {
        return Data_ + 1;
    }

    Y_SAVELOAD_DEFINE(Data_);
private:
    int8_t Data_;
};

class TImpl2 : public TVirtual {
public:
    TImpl2(int16_t data = 0)
        : Data_(data)
    { }

    int32_t GetData() const override {
        return Data_ + 2;
    }

    Y_SAVELOAD_DEFINE(Data_);
private:
    int16_t Data_;
};

class TImpl3 : public TVirtual {
public:
    TImpl3(int32_t data = 0)
        : Data_(data)
    { }

    int32_t GetData() const override {
        return Data_ + 3;
    }

    Y_SAVELOAD_DEFINE(Data_);
private:
    int32_t Data_;
};

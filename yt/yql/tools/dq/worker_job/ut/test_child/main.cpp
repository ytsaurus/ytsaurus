#include <iconv.h>

int main()
{
    auto descriptor = iconv_open("UTF-8", "UTF-8");
    if (descriptor == reinterpret_cast<iconv_t>(-1)) {
        return 1;
    }
    return iconv_close(descriptor);
}

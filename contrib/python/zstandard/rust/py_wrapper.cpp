struct _object;
#undef PyInit_backend_rust
extern "C" _object* PyInit_backend_rust();
extern "C" _object* PyInit_9zstandard12backend_rust() {
    return PyInit_backend_rust();
}

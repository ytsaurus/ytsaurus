// Keep the upstream Rust initializer and the Arcadia import name.
struct _object;
#undef PyInit__pydantic_core
extern "C" _object* PyInit__pydantic_core();
extern "C" _object* PyInit_13pydantic_core14_pydantic_core() {
    return PyInit__pydantic_core();
}

#include <functional>

namespace fuzzing {

class Defer {
 public:
  explicit Defer(std::function<void()> func) : func_(func) {}
  ~Defer() { func_(); }

 private:
  std::function<void()> func_;
};

}  // namespace fuzzing

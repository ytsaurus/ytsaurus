#include <chrono>

namespace fuzzing {

class Timer {
 public:
  Timer() : start_(std::chrono::high_resolution_clock::now()) {}

  int64_t Reset() {
    auto now = std::chrono::high_resolution_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - start_).count();
    start_ = now;
    return elapsed;
  }

 private:
  std::chrono::time_point<std::chrono::high_resolution_clock> start_;
};

}  // namespace fuzzing

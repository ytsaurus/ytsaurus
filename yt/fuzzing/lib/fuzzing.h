
#include <unistd.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string_view>
#include <thread>

namespace fuzzing {

static size_t getCurrentRSS() {
  std::ifstream statm("/proc/self/statm");
  size_t size = 0, resident = 0;

  if (statm) {
    statm >> size >> resident;
  } else {
    std::cerr << "Failed to open /proc/self/statm" << std::endl;
    return 0;
  }

  statm.close();
  long page_size = sysconf(_SC_PAGESIZE);
  return resident * page_size;  // in bytes
}

static std::string getRequestSnapshotFileName(std::string_view dir) {
  auto now = std::chrono::system_clock::now();
  std::time_t now_time = std::chrono::system_clock::to_time_t(now);

  std::stringstream day;
  day << std::put_time(std::localtime(&now_time), "%m-%d");
  std::stringstream time;
  time << std::put_time(std::localtime(&now_time), "%H-%M");

  auto thread_id = std::this_thread::get_id();
  auto day_dir = std::string(dir) + day.str();
  std::filesystem::create_directories(day_dir);
  std::stringstream s;
  s << day_dir << "/" << thread_id << "_" << time.str();
  return s.str();
}

}  // namespace fuzzing

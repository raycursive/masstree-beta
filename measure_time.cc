// #include <map>
// #include <chrono>
// #include "measure_time.h"

// namespace measure_time {


// uint64_t MeasureTime::total_ = 0;
// uint64_t MeasureTime::call_id_ = 0;

// uint64_t MeasureTime::start_time_check() {
//     call_id_ += 1;
//     id_starts_[call_id_] = std::chrono::steady_clock::now();

//     return call_id_;
// }

// void MeasureTime::end_time_check(uint64_t call_id) {
//     auto now = std::chrono::steady_clock::now(), start = id_starts_[call_id];
//     total_ += std::chrono::duration_cast<durat_t>(now - start).count();
// }

// uint64_t MeasureTime::get_cur_total() {
//     return total_;
// }
// }
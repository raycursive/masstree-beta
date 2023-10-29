#ifndef MEASURE_TIME_HH
#define MEASURE_TIME_HH
#include <chrono>
#include <string>
#include <unordered_map>
#include <iostream>

namespace measure_time {
typedef std::chrono::steady_clock::time_point time_pt;
typedef std::chrono::microseconds durat_t;
using std::string;
using std::unordered_map;


class MeasureTime {
private:
    uint64_t total_ = 0;
    uint64_t call_id_ = 0;
    unordered_map<uint64_t, time_pt> id_starts_;

public:
    uint64_t start_time_check() {
        call_id_ += 1;
        id_starts_[call_id_] = std::chrono::steady_clock::now();

        return call_id_;
    }

    void end_time_check(uint64_t call_id) {
        // if (id_starts_.find(call_id) == id_starts_.end()) {
        //     std::cout << "Received unknown call_id " << call_id;
        //     return;
        // }

        auto now = std::chrono::steady_clock::now(),
                start = id_starts_[call_id];
        total_ += std::chrono::duration_cast<durat_t>(now - start).count();
        id_starts_.erase(call_id);
    }

    uint64_t get_cur_total() const { return total_; }
};

// static MeasureTime measure_inst;
static uint64_t sa_time = 0;

}

#endif
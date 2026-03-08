#include <chrono>
#include <cmath>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

namespace {
constexpr int kRawSamplePeriodMs = 50;
constexpr int kExportedSamplePeriodMs = 250;
constexpr int kRawSamplesPerExport = kExportedSamplePeriodMs / kRawSamplePeriodMs;

struct RawSample {
    double gpu_util;
    double power_w;
    double mem_mb;
    double clock_mhz;
};

struct AggregatedSample {
    double gpu_util;
    double power_w;
    double mem_mb;
    double clock_mhz;
};

std::string now_iso8601_utc() {
    using clock = std::chrono::system_clock;
    const auto now = clock::now();
    const auto t = clock::to_time_t(now);
    std::tm tm{};
#if defined(_WIN32)
    gmtime_s(&tm, &t);
#else
    gmtime_r(&t, &tm);
#endif
    std::ostringstream os;
    os << std::put_time(&tm, "%Y-%m-%dT%H:%M:%SZ");
    return os.str();
}

AggregatedSample aggregate_raw_samples(const std::vector<RawSample>& raw_samples) {
    if (raw_samples.empty()) {
        return {0.0, 0.0, 0.0, 0.0};
    }

    AggregatedSample out{0.0, 0.0, 0.0, 0.0};
    for (const auto& sample : raw_samples) {
        out.gpu_util += sample.gpu_util;
        out.power_w += sample.power_w;
        out.mem_mb += sample.mem_mb;
        out.clock_mhz += sample.clock_mhz;
    }

    const double n = static_cast<double>(raw_samples.size());
    out.gpu_util /= n;
    out.power_w /= n;
    out.mem_mb /= n;
    out.clock_mhz /= n;
    return out;
}

bool nearly_equal(double lhs, double rhs, double eps = 1e-9) {
    return std::fabs(lhs - rhs) <= eps;
}

bool run_self_test() {
    const std::vector<RawSample> batch = {
        {50.0, 100.0, 2000.0, 1200.0},
        {60.0, 110.0, 3000.0, 1300.0},
        {70.0, 120.0, 4000.0, 1400.0},
        {80.0, 130.0, 5000.0, 1500.0},
        {90.0, 140.0, 6000.0, 1600.0},
    };

    const auto aggregated = aggregate_raw_samples(batch);
    const bool ok = nearly_equal(aggregated.gpu_util, 70.0) &&
                    nearly_equal(aggregated.power_w, 120.0) &&
                    nearly_equal(aggregated.mem_mb, 4000.0) &&
                    nearly_equal(aggregated.clock_mhz, 1400.0);

    if (!ok) {
        std::cerr << "telemetryd self-test failed: 50ms raw samples were not aggregated into expected 250ms sample" << std::endl;
        return false;
    }

    std::cout << "telemetryd self-test passed: 5x50ms raw samples aggregated to 1x250ms export sample" << std::endl;
    return true;
}
}  // namespace

int main(int argc, char** argv) {
    if (argc > 1 && std::string(argv[1]) == "--self-test") {
        return run_self_test() ? 0 : 1;
    }

    double gpu_util = 52.0;
    double power_w = 145.0;
    double mem_mb = 3072.0;
    double clock_mhz = 1350.0;

    std::vector<RawSample> batch;
    batch.reserve(kRawSamplesPerExport);

    while (true) {
        batch.push_back({gpu_util, power_w, mem_mb, clock_mhz});

        gpu_util += 3.0;
        if (gpu_util > 96.0) gpu_util = 42.0;
        power_w += 2.5;
        if (power_w > 205.0) power_w = 130.0;
        mem_mb += 64.0;
        if (mem_mb > 6144.0) mem_mb = 2048.0;
        clock_mhz += 25.0;
        if (clock_mhz > 1750.0) clock_mhz = 1200.0;

        if (static_cast<int>(batch.size()) == kRawSamplesPerExport) {
            const auto aggregated = aggregate_raw_samples(batch);
            std::cout << "{\"gpu_util\":" << aggregated.gpu_util << ","
                      << "\"power_w\":" << aggregated.power_w << ","
                      << "\"mem_mb\":" << aggregated.mem_mb << ","
                      << "\"clock_mhz\":" << aggregated.clock_mhz << ","
                      << "\"timestamp\":\"" << now_iso8601_utc() << "\","
                      << "\"raw_sample_period_ms\":" << kRawSamplePeriodMs << ","
                      << "\"exported_sample_period_ms\":" << kExportedSamplePeriodMs << "}"
                      << std::endl;
            batch.clear();
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(kRawSamplePeriodMs));
    }

    return 0;
}

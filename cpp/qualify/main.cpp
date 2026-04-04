#include <chrono>
#include <cmath>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <string>
#include <vector>

namespace {
struct QualifyResult {
    double gemm_checksum;
    double hash_throughput_mb_s;
    double tiny_conv_checksum;
    double benchmark_score;
};

std::uint64_t fnv1a64(const std::uint8_t* data, std::size_t len) {
    std::uint64_t hash = 1469598103934665603ull;
    for (std::size_t i = 0; i < len; ++i) {
        hash ^= static_cast<std::uint64_t>(data[i]);
        hash *= 1099511628211ull;
    }
    return hash;
}

double run_deterministic_gemm_checksum() {
    constexpr int n = 16;
    std::vector<double> a(n * n), b(n * n), c(n * n, 0.0);
    for (int i = 0; i < n * n; ++i) {
        a[i] = (i % 13) * 0.37;
        b[i] = (i % 7) * 0.19;
    }
    for (int i = 0; i < n; ++i) {
        for (int k = 0; k < n; ++k) {
            const double aik = a[i * n + k];
            for (int j = 0; j < n; ++j) {
                c[i * n + j] += aik * b[k * n + j];
            }
        }
    }
    double checksum = 0.0;
    for (int i = 0; i < n * n; ++i) {
        checksum += c[i] * (1.0 + (i % 5) * 0.01);
    }
    return checksum;
}

double run_random_buffer_hash_throughput_mb_s() {
    constexpr std::size_t bytes = 8 * 1024 * 1024;
    constexpr int rounds = 8;
    std::vector<std::uint8_t> buf(bytes);
    std::uint64_t seed = 88172645463325252ull;
    for (std::size_t i = 0; i < bytes; ++i) {
        seed = seed * 2862933555777941757ull + 3037000493ull;
        buf[i] = static_cast<std::uint8_t>((seed >> 33) & 0xffu);
    }

    volatile std::uint64_t sink = 0;
    const auto start = std::chrono::steady_clock::now();
    for (int r = 0; r < rounds; ++r) {
        sink ^= fnv1a64(buf.data(), buf.size());
    }
    const auto end = std::chrono::steady_clock::now();
    (void)sink;

    const std::chrono::duration<double> elapsed = end - start;
    const double total_mb = static_cast<double>(bytes) * rounds / (1024.0 * 1024.0);
    return total_mb / std::max(elapsed.count(), 1e-9);
}

double run_tiny_conv_checksum() {
    constexpr int h = 8;
    constexpr int w = 8;
    constexpr int kh = 3;
    constexpr int kw = 3;

    std::vector<double> input(h * w);
    std::vector<double> kernel = {
        0.1, 0.2, 0.1,
        0.0, 0.3, 0.0,
        -0.1, 0.2, -0.1,
    };
    for (int i = 0; i < h * w; ++i) {
        input[i] = std::sin(i * 0.11) + std::cos(i * 0.07);
    }

    const int oh = h - kh + 1;
    const int ow = w - kw + 1;
    std::vector<double> output(oh * ow, 0.0);

    for (int y = 0; y < oh; ++y) {
        for (int x = 0; x < ow; ++x) {
            double v = 0.0;
            for (int ky = 0; ky < kh; ++ky) {
                for (int kx = 0; kx < kw; ++kx) {
                    v += input[(y + ky) * w + (x + kx)] * kernel[ky * kw + kx];
                }
            }
            output[y * ow + x] = std::max(0.0, v);  // tiny inference-like relu
        }
    }

    double checksum = 0.0;
    for (int i = 0; i < static_cast<int>(output.size()); ++i) {
        checksum += output[i] * (1.0 + 0.001 * i);
    }
    return checksum;
}

QualifyResult run_qualification() {
    const double gemm_checksum = run_deterministic_gemm_checksum();
    const double hash_throughput_mb_s = run_random_buffer_hash_throughput_mb_s();
    const double tiny_conv_checksum = run_tiny_conv_checksum();

    // Minimal weighted benchmark score. Higher throughput/checksum => higher score.
    const double benchmark_score =
        0.35 * gemm_checksum + 0.45 * hash_throughput_mb_s + 0.20 * tiny_conv_checksum;

    return {
        gemm_checksum,
        hash_throughput_mb_s,
        tiny_conv_checksum,
        benchmark_score,
    };
}

bool run_self_test() {
    const auto result = run_qualification();
    const bool ok = result.gemm_checksum > 0.0 && result.hash_throughput_mb_s > 0.0 &&
                    result.tiny_conv_checksum > 0.0 && result.benchmark_score > 0.0;
    if (!ok) {
        std::cerr << "qualify self-test failed" << std::endl;
        return false;
    }
    std::cout << "qualify self-test passed" << std::endl;
    return true;
}
}  // namespace

int main(int argc, char** argv) {
    if (argc > 1 && std::string(argv[1]) == "--self-test") {
        return run_self_test() ? 0 : 1;
    }

    const auto result = run_qualification();
    std::cout << std::fixed << std::setprecision(6)
              << "{"
              << "\"source\":\"qualify\","
              << "\"gemm_checksum\":" << result.gemm_checksum << ","
              << "\"hash_throughput_mb_s\":" << result.hash_throughput_mb_s << ","
              << "\"tiny_conv_checksum\":" << result.tiny_conv_checksum << ","
              << "\"benchmark_score\":" << result.benchmark_score
              << "}" << std::endl;
    return 0;
}

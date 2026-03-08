add_executable(telemetryd main.cpp)

enable_testing()
add_test(NAME telemetryd_aggregates_50ms_to_250ms COMMAND telemetryd --self-test)

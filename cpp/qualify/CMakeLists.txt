add_executable(qualify main.cpp)

enable_testing()
add_test(NAME qualify_minimal_three_tests COMMAND qualify --self-test)

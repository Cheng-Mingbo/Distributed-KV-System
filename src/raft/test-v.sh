#!/bin/bash

# 设置测试运行次数
runs=100

# 设置并行度，默认为 CPU 核心数
parallelism=$(sysctl -n hw.ncpu)

# 设置测试用例
test="TestRPCBytes3B"

# 创建日志文件夹
mkdir -p test_logs

# 运行测试
for ((i=1; i<=$runs; i++)); do
    echo "Running test $i..."
    go test -run "$test" 2> "test_logs/test-$i.err" > "test_logs/test-$i.log" &
    pid=$!
    wait $pid
done

echo "All tests completed."

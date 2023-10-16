#!/bin/bash

# 获取主进程的PID
pid=$(ps -ef | grep python3 | grep "md.py" | awk '{print $2}')

for p in $pid
  do
    echo "Killing processes $p..."
    kill -9 "$p"
  done

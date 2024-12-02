#!/bin/bash

PORT=50051  # 종료할 포트 번호

for i in {101..111}; do
  if [ "$i" -eq 103 ]; then
    echo "Skipping 2.2.2.103"
    continue
  fi
  
  IP="2.2.2.$i"
  echo "Connecting to $IP as green user..."
  
  ssh green@$IP "
    PID=\$(lsof -i :$PORT -t)
    if [ -n \"\$PID\" ]; then
      echo \"Killing process \$PID on port $PORT at $IP\"
      kill -9 \$PID
    else
      echo \"No process found on port $PORT at $IP\"
    fi
  " &
done

# 모든 SSH 작업 완료 대기
wait
echo "Process termination completed on all servers."


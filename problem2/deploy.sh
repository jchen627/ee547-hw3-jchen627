#!/bin/bash
# HW3 Problem 2 — Part E: Simple EC2 deploy helper for api_server.py

if [ $# -ne 2 ]; then
  echo "Usage: $0 <key_file> <ec2_public_ip>"
  exit 1
fi

KEY_FILE="$1"
EC2_IP="$2"

echo "Deploying to EC2 instance: $EC2_IP"

# Copy files
scp -i "$KEY_FILE" problem2/api_server.py ec2-user@"$EC2_IP":~
scp -i "$KEY_FILE" problem2/requirements.txt ec2-user@"$EC2_IP":~

# Install and start
ssh -i "$KEY_FILE" ec2-user@"$EC2_IP" << 'EOF'
  set -e
  if command -v yum >/dev/null 2>&1; then
    sudo yum install -y python3 python3-pip
  elif command -v apt >/dev/null 2>&1; then
    sudo apt update -y
    sudo apt install -y python3 python3-pip
  fi

  pip3 install -r requirements.txt

  # Optionally export environment
  # export AWS_REGION=us-west-2
  # export ARXIV_TABLE=arxiv-papers

  pkill -f api_server.py || true
  nohup python3 api_server.py 8080 > server.log 2>&1 &
  echo "Server started. Try: curl http://localhost:8080/papers/recent?category=cs.LG&limit=5"
EOF

echo "Deployment complete"
echo "Test from local: curl \"http://$EC2_IP:8080/papers/recent?category=cs.LG&limit=5\""

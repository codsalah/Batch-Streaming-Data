#!/bin/bash
echo "Checking system resources..."
echo "--- CPU Usage ---"
top -bn1 | grep "Cpu(s)"
echo "--- Memory Usage ---"
free -h
echo "--- Disk Usage ---"
df -h /
echo "Resource check complete."

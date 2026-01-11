#!/bin/bash
# data_inspector.sh

echo "Data Inspector - Shows files and sizes"
echo "========================================="

docker exec spark-master bash -c '
echo "1. Checking directory structure..."
echo ""

echo "/opt/delta-lake/tables/"
ls -la /opt/delta-lake/tables/ 2>/dev/null || echo "   Directory not found"

echo ""
echo "/opt/delta-lake/"
ls -la /opt/delta-lake/ 2>/dev/null | head -10

echo ""
echo "2. Searching for Delta tables in ENTIRE filesystem..."
find / -name "_delta_log" -type d 2>/dev/null | head -10

echo ""
echo "3. Checking for ANY data files..."
echo "   Parquet files:"
find / -name "*.parquet" -type f 2>/dev/null | head -5

echo ""
echo "   Any data files in /opt/:"
find /opt -name "*.parquet" -o -name "*.csv" -o -name "*.json" 2>/dev/null | head -10

echo ""
echo "4. Disk usage of potential data directories..."
du -sh /opt/delta-lake/ 2>/dev/null || echo "   No /opt/delta-lake"
du -sh /tmp/ 2>/dev/null | head -1
du -sh /data/ 2>/dev/null 2>/dev/null || echo "   No /data directory"

echo ""
echo "5. Checking Spark warehouse..."
ls -la /tmp/spark-* 2>/dev/null | head -5
ls -la /user/hive/warehouse/ 2>/dev/null 2>/dev/null || echo "   No Hive warehouse"
'

echo ""
echo "========================================="
 
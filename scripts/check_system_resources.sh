#!/bin/bash

echo "Checking system resources on $(uname -s)..."
echo "==========================================="

# CPU Usage
echo "--- CPU Usage ---"
if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    # Linux
    top -bn1 | grep "Cpu(s)" | awk '{print "CPU Load: " $2 "%"}'
elif [[ "$OSTYPE" == "darwin"* ]]; then
    # macOS
    top -l 1 | grep "CPU usage"
elif [[ "$OSTYPE" == "msys" || "$OSTYPE" == "cygwin" || "$OSTYPE" == "win32" ]]; then
    # Windows (Git Bash/Cygwin)
    if command -v wmic &> /dev/null; then
        wmic cpu get loadpercentage | grep -v LoadPercentage
    else
        echo "CPU info not available (install wmic or use PowerShell)"
    fi
else
    echo "Unknown OS: $OSTYPE"
fi

# Memory Usage
echo -e "\n--- Memory Usage ---"
if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    free -h | grep -E "^(Mem|Swap):"
elif [[ "$OSTYPE" == "darwin"* ]]; then
    vm_stat | perl -ne '/page size of (\d+)/ and $size=$1; /Pages free:\s*(\d+)/ and printf("Free: %.1fG\n", $1*$size/1073741824);'
elif [[ "$OSTYPE" == "msys" || "$OSTYPE" == "cygwin" || "$OSTYPE" == "win32" ]]; then
    if command -v wmic &> /dev/null; then
        echo "Memory (Windows):"
        wmic OS get FreePhysicalMemory,TotalVisibleMemorySize /format:list | grep -v "^$"
    else
        echo "Memory info not available"
    fi
fi

# Disk Usage
echo -e "\n--- Disk Usage ---"
if [[ "$OSTYPE" == "linux-gnu"* || "$OSTYPE" == "darwin"* ]]; then
    df -h . | tail -1
elif [[ "$OSTYPE" == "msys" || "$OSTYPE" == "cygwin" || "$OSTYPE" == "win32" ]]; then
    df -h . | tail -1
    
    echo "Windows Drive Info:"
    cmd //c "wmic logicaldisk get size,freespace,caption" 2>/dev/null || echo "Use PowerShell for detailed disk info"
fi

echo -e "\n==========================================="
echo "Resource check complete."

#!/bin/bash

START_DATE="2024-11-03"

# Check if a commit message was provided
if [ $# -eq 0 ]; then
    echo "Error: No commit message provided."
    echo "Usage: $0 <commit message>"
    exit 1
fi

# Combine all arguments into a single commit message
commit_message="$*"

git add .

# Calculate days since start (in PST), adding 1 to count the start day as day 1
if date -d "2000-01-01" >/dev/null 2>&1; then
    # GNU date
    days_since_start=$(TZ='America/Los_Angeles'         expr $(date +%s) - $(date -d "$START_DATE" +%s) )
else
    # BSD date (macOS)
    days_since_start=$(TZ='America/Los_Angeles'         expr $(date +%s) - $(date -j -f "%Y-%m-%d" "$START_DATE" +%s) )
fi
days_since_start=$(( days_since_start / 86400 + 1 ))

# Function to get correct ordinal suffix
ordinal_suffix() {
    case $(($1 % 100)) in
        11|12|13) echo "th";;
        *) case $(($1 % 10)) in
            1) echo "st";;
            2) echo "nd";;
            3) echo "rd";;
            *) echo "th";;
        esac
    esac
}

# Get current date and time in PST
current_datetime=$(TZ='America/Los_Angeles' date '+%Y-%m-%d %H:%M:%S %Z')

# Commit and push
git commit -m "${days_since_start}$(ordinal_suffix $days_since_start) day: $commit_message (PST: $current_datetime)"
git push

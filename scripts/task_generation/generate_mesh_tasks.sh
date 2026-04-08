#!/bin/bash

# ----------------------------------------------------------------------
# full_factorial.sh – Generate task commands for all combinations of parameter values.
# Usage: ./full_factorial.sh <parameter_file>
# Parameter file format:
#   # lines starting with '#' are ignored
#   PARAM_NAME  min max       (space separated)
#   PARAM_NAME  min-max        (dash separated)
# Example:
#   AXI_ID_WIDTH 1 3
#   BAUD_RATE 1-2
# ----------------------------------------------------------------------

set -euo pipefail

# Check input file
if [[ $# -ne 1 || ! -f "$1" ]]; then
    echo "Usage: $0 <parameter_file>" >&2
    exit 1
fi

# Read parameters into arrays
names=()
ranges=()   # each element is "min max"
while read -r line; do
    # Skip empty lines and comments
    [[ -z "$line" || "$line" =~ ^[[:space:]]*# ]] && continue

    # Extract first token as name, the rest as range spec
    name=$(echo "$line" | awk '{print $1}')
    spec=$(echo "$line" | awk '{$1=""; print $0}' | sed 's/^[[:space:]]*//')

    # Parse spec: can be "min max" or "min-max"
    if [[ "$spec" =~ ^([0-9]+)[[:space:]]+([0-9]+)$ ]]; then
        min="${BASH_REMATCH[1]}"
        max="${BASH_REMATCH[2]}"
    elif [[ "$spec" =~ ^([0-9]+)-([0-9]+)$ ]]; then
        min="${BASH_REMATCH[1]}"
        max="${BASH_REMATCH[2]}"
    else
        echo "Warning: skipping invalid line: $line" >&2
        continue
    fi

    # Ensure min <= max
    if (( min > max )); then
        echo "Warning: min > max in line '$line', swapping" >&2
        tmp=$min; min=$max; max=$tmp
    fi

    names+=("$name")
    ranges+=("$min $max")
done < "$1"

# Check we have at least one parameter
if [[ ${#names[@]} -eq 0 ]]; then
    echo "Error: No valid parameters found." >&2
    exit 1
fi

# Build arrays of possible values for each parameter
declare -a values=()
total_combinations=1
for idx in "${!names[@]}"; do
    read -r min max <<< "${ranges[$idx]}"
    # Generate sequence using seq (works for increasing integers)
    seq_list=($(seq "$min" "$max"))
    values[$idx]="${seq_list[*]}"
    total_combinations=$((total_combinations * ${#seq_list[@]}))
done

echo "# Total combinations: $total_combinations" >&2
if [[ $total_combinations -gt 10000 ]]; then
    echo "# Warning: Large number of commands. Consider piping output to a file." >&2
fi

# ----------------------------------------------------------------------
# Recursive function to generate combinations
# Arguments:
#   depth – current parameter index (0..N-1)
#   prefix_defs – array of "NAME=VALUE" strings built so far
# ----------------------------------------------------------------------
generate() {
    local depth=$1
    shift
    local -a prefix_defs=("$@")

    if [[ $depth -eq ${#names[@]} ]]; then
        # All parameters assigned – build the command
        local arg_str="-t compile_with_defines.tcl --project cosim_top.qpf"
        for def in "${prefix_defs[@]}"; do
            arg_str+=" --define $def"
        done
        echo "task add --id $task_id --cmd \"quartus_sh\" --arg '$arg_str'"
        ((task_id++))
        return
    fi

    local name="${names[$depth]}"
    local -a this_values=(${values[$depth]})
    for val in "${this_values[@]}"; do
        # Append current assignment to the list
        generate $((depth+1)) "${prefix_defs[@]}" "$name=$val"
    done
}

# Start generation
task_id=1
generate 0

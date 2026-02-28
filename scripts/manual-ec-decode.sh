#!/usr/bin/env bash
#
# manual-ec-decode.sh — Inspect and manually decode EC strips from disk
#
# A diagnostic tool that shows how erasure-coded objects are stored
# across nodes: strip layout, sizes, hex dumps, and manual
# reconstruction via Reed-Solomon.
#
# Usage:
#   ./scripts/manual-ec-decode.sh                     # Auto-detect from running cluster
#   ./scripts/manual-ec-decode.sh --data-root /path   # Specify data directory
#   ./scripts/manual-ec-decode.sh --oid 00ab00010000   # Inspect specific OID
#   ./scripts/manual-ec-decode.sh --decode             # Reconstruct object from strips
#   ./scripts/manual-ec-decode.sh --simulate-loss 2    # Remove strip N, then reconstruct
#
set -uo pipefail

# ── Configuration ──────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "${SCRIPT_DIR}/defaults.sh"

# Defaults
OID_FILTER=""
DO_DECODE=false
SIMULATE_LOSS=""
HEXDUMP_BYTES=64
VERBOSE=false

# Parse flags
while [[ $# -gt 0 ]]; do
    case "$1" in
        --data-root)  shift; DATA_ROOT="$1" ;;
        --oid)        shift; OID_FILTER="$1" ;;
        --decode)     DO_DECODE=true ;;
        --simulate-loss) shift; SIMULATE_LOSS="$1" ;;
        --hex-bytes)  shift; HEXDUMP_BYTES="$1" ;;
        --verbose|-v) VERBOSE=true ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Inspect and decode EC strips stored on disk."
            echo ""
            echo "Options:"
            echo "  --data-root <DIR>      Data directory [default: ${DATA_ROOT}]"
            echo "  --oid <PATTERN>        Filter by OID (substring match)"
            echo "  --decode               Reconstruct full object from data strips"
            echo "  --simulate-loss <N>    Zero out strip N before reconstruct"
            echo "  --hex-bytes <N>        Bytes to hexdump per strip [default: 64]"
            echo "  --verbose, -v          Show more detail"
            echo "  --help, -h             Show this help"
            echo ""
            echo "Examples:"
            echo "  $0                                # Show all EC strips"
            echo "  $0 --oid 00db0cc6                 # Filter by VDI ID"
            echo "  $0 --decode --oid 00db0cc600000000  # Decode object 0"
            echo "  $0 --simulate-loss 2 --decode       # Lose strip 2, reconstruct"
            exit 0
            ;;
        *)  echo "Unknown flag: $1"; exit 1 ;;
    esac
    shift
done

# Resolve binary
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
if [[ -x "${REPO_ROOT}/target/release/dog" ]]; then
    DOG="${REPO_ROOT}/target/release/dog"
elif [[ -x "${REPO_ROOT}/target/debug/dog" ]]; then
    DOG="${REPO_ROOT}/target/debug/dog"
else
    DOG="dog"
fi

# ── Colors ─────────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
BOLD='\033[1m'
DIM='\033[2m'
NC='\033[0m'

info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
err()   { echo -e "${RED}[ERR]${NC}  $*" >&2; }

# ── OID Decoding ───────────────────────────────────────────────────────
decode_oid() {
    local hex="$1"
    # OID is 64-bit: 8-bit flags | 24-bit VDI ID | 32-bit object index
    # Hex: FFVVVVVVIIIIIIII (16 hex chars)
    local flags_hex="${hex:0:2}"
    local vid_hex="${hex:2:6}"
    local idx_hex="${hex:8:8}"

    local flags=$(( 16#${flags_hex} ))
    local vid=$(( 16#${vid_hex} ))
    local idx=$(( 16#${idx_hex} ))

    local type="data"
    if (( flags & 0x10 )); then type="vdi"; fi
    if (( flags & 0x20 )); then type="vmstate"; fi
    if (( flags & 0x40 )); then type="attr"; fi
    if (( flags & 0x80 )); then type="ledger"; fi

    printf "flags=0x%02x type=%-8s vid=0x%06x(%d) idx=%d" \
        "$flags" "$type" "$vid" "$vid" "$idx"
}

decode_ec_policy() {
    local policy=$1
    local d=$(( (policy >> 4) & 0x0F ))
    local p=$(( policy & 0x0F ))
    if (( d == 0 )); then
        echo "none (replication)"
    else
        echo "${d}:${p} (${d} data + ${p} parity = $(( d + p )) total)"
    fi
}

# ── Strip discovery ────────────────────────────────────────────────────
discover_strips() {
    echo -e "${BOLD}${CYAN}═══ EC Strip Discovery ═══${NC}"
    echo -e "  Data root: ${DATA_ROOT}"
    echo -e "  Nodes:     ${NUM_NODES}"
    echo ""

    # Collect all EC strip files across nodes
    local found=0
    local oid_list=""

    for i in $(seq 0 $(( NUM_NODES - 1 ))); do
        local dir="${DATA_ROOT}/node${i}/obj"
        if [[ ! -d "$dir" ]]; then
            continue
        fi

        local strips
        strips=$(ls "$dir" 2>/dev/null | grep '_[0-9a-f]') || true
        if [[ -z "$strips" ]]; then
            continue
        fi

        while IFS= read -r fname; do
            [[ -z "$fname" ]] && continue

            # Apply OID filter
            if [[ -n "$OID_FILTER" ]] && [[ "$fname" != *"$OID_FILTER"* ]]; then
                continue
            fi

            # Parse filename: {oid_hex}_{ec_index_hex}
            local oid_hex="${fname%%_*}"
            local ec_suffix="${fname##*_}"
            local ec_index=$(( 16#${ec_suffix} ))

            # Get file size
            local fpath="${dir}/${fname}"
            local sz
            sz=$(stat -f%z "$fpath" 2>/dev/null || stat -c%s "$fpath" 2>/dev/null || echo "0")

            # Collect unique OIDs
            if [[ "$oid_list" != *"$oid_hex"* ]]; then
                oid_list="${oid_list} ${oid_hex}"
            fi

            (( found++ ))
        done <<< "$strips"
    done

    if (( found == 0 )); then
        warn "No EC strips found in ${DATA_ROOT}"
        echo "  (Make sure a cluster with EC VDIs has been used)"
        return 1
    fi

    info "Found ${found} EC strip files"
    echo ""

    # Show per-object strip layout
    for oid_hex in $oid_list; do
        echo -e "${BOLD}┌─ Object: ${CYAN}${oid_hex}${NC}"
        echo -e "${BOLD}│${NC}  $(decode_oid "$oid_hex")"

        # 4 MB object size
        local obj_size=4194304

        echo -e "${BOLD}│${NC}"
        echo -e "${BOLD}│  Strip layout:${NC}"
        printf "${BOLD}│${NC}  %-6s  %-6s  %-40s  %10s  %s\n" \
            "Node" "Strip" "File" "Size" "Path"
        echo -e "${BOLD}│${NC}  ────── ────── ──────────────────────────────────────── ────────── ──────────"

        local strip_count=0
        local data_strip_size=0

        for i in $(seq 0 $(( NUM_NODES - 1 ))); do
            local dir="${DATA_ROOT}/node${i}/obj"
            [[ ! -d "$dir" ]] && continue

            local matches
            matches=$(ls "$dir" 2>/dev/null | grep "^${oid_hex}_") || true
            [[ -z "$matches" ]] && continue

            while IFS= read -r fname; do
                [[ -z "$fname" ]] && continue
                local ec_suffix="${fname##*_}"
                local ec_index=$(( 16#${ec_suffix} ))
                local fpath="${dir}/${fname}"
                local sz
                sz=$(stat -f%z "$fpath" 2>/dev/null || stat -c%s "$fpath" 2>/dev/null || echo "0")

                local label
                # Determine if data or parity based on ec_index
                # ec_index 1..D = data, D+1..D+P = parity
                # But we need to know D — infer from strip size
                if (( data_strip_size == 0 )); then
                    data_strip_size=$sz
                fi

                label="${DIM}strip${NC}"

                printf "${BOLD}│${NC}  %-6s  %-6s  %-40s  %10s  %s\n" \
                    "node${i}" "#${ec_index}" "$fname" "${sz}" "$fpath"

                (( strip_count++ ))
            done <<< "$matches"
        done

        # Infer EC parameters from strip count and size
        if (( data_strip_size > 0 )); then
            local inferred_d=$(( obj_size / data_strip_size ))
            local inferred_p=$(( strip_count - inferred_d ))
            if (( inferred_d > 0 && inferred_p >= 0 )); then
                echo -e "${BOLD}│${NC}"
                echo -e "${BOLD}│${NC}  ${MAGENTA}Inferred EC: ${inferred_d}:${inferred_p}${NC} (${strip_count} strips x ${data_strip_size} bytes)"
                echo -e "${BOLD}│${NC}  Object size: ${obj_size} = ${inferred_d} data strips x ${data_strip_size}"
            fi
        fi

        echo -e "${BOLD}└──${NC}"
        echo ""
    done

    echo "$oid_list"
}

# ── Hex dump strips ───────────────────────────────────────────────────
hexdump_strips() {
    local oid_hex="$1"
    echo -e "${BOLD}${CYAN}═══ Hex Dump: ${oid_hex} ═══${NC}"
    echo -e "  Showing first ${HEXDUMP_BYTES} bytes of each strip"
    echo ""

    for i in $(seq 0 $(( NUM_NODES - 1 ))); do
        local dir="${DATA_ROOT}/node${i}/obj"
        [[ ! -d "$dir" ]] && continue

        local matches
        matches=$(ls "$dir" 2>/dev/null | grep "^${oid_hex}_") || true
        [[ -z "$matches" ]] && continue

        while IFS= read -r fname; do
            [[ -z "$fname" ]] && continue
            local fpath="${dir}/${fname}"
            local ec_suffix="${fname##*_}"
            local ec_index=$(( 16#${ec_suffix} ))

            echo -e "${BOLD}── node${i} / strip #${ec_index} ── ${DIM}${fpath}${NC}"
            xxd -l "$HEXDUMP_BYTES" "$fpath" 2>/dev/null || hexdump -C -n "$HEXDUMP_BYTES" "$fpath" 2>/dev/null
            echo ""
        done <<< "$matches"
    done
}

# ── Manual decode (concatenate data strips) ───────────────────────────
manual_decode() {
    local oid_hex="$1"
    local outfile="/tmp/ec-decoded-${oid_hex}.bin"

    echo -e "${BOLD}${CYAN}═══ Manual Decode: ${oid_hex} ═══${NC}"

    # Collect all strips sorted by ec_index
    local -a strip_files=()
    local -a strip_indices=()
    local -a strip_nodes=()

    for i in $(seq 0 $(( NUM_NODES - 1 ))); do
        local dir="${DATA_ROOT}/node${i}/obj"
        [[ ! -d "$dir" ]] && continue

        local matches
        matches=$(ls "$dir" 2>/dev/null | grep "^${oid_hex}_") || true
        [[ -z "$matches" ]] && continue

        while IFS= read -r fname; do
            [[ -z "$fname" ]] && continue
            local ec_suffix="${fname##*_}"
            local ec_index=$(( 16#${ec_suffix} ))
            strip_files+=("${dir}/${fname}")
            strip_indices+=("$ec_index")
            strip_nodes+=("$i")
        done <<< "$matches"
    done

    local total=${#strip_files[@]}
    if (( total == 0 )); then
        err "No strips found for OID ${oid_hex}"
        return 1
    fi

    # Sort by ec_index (bubble sort for simplicity)
    for (( a=0; a < total-1; a++ )); do
        for (( b=a+1; b < total; b++ )); do
            if (( strip_indices[b] < strip_indices[a] )); then
                local tmp="${strip_files[a]}"
                strip_files[a]="${strip_files[b]}"
                strip_files[b]="$tmp"
                tmp="${strip_indices[a]}"
                strip_indices[a]="${strip_indices[b]}"
                strip_indices[b]="$tmp"
                tmp="${strip_nodes[a]}"
                strip_nodes[a]="${strip_nodes[b]}"
                strip_nodes[b]="$tmp"
            fi
        done
    done

    # Determine data vs parity
    local strip_sz
    strip_sz=$(stat -f%z "${strip_files[0]}" 2>/dev/null || stat -c%s "${strip_files[0]}" 2>/dev/null)
    local obj_size=4194304
    local data_strips=$(( obj_size / strip_sz ))
    local parity_strips=$(( total - data_strips ))

    echo -e "  EC config:  ${data_strips}:${parity_strips} (${data_strips} data + ${parity_strips} parity)"
    echo -e "  Strip size: ${strip_sz} bytes"
    echo -e "  Total:      ${total} strips"
    echo ""

    # Show strip map
    echo -e "  ${BOLD}Strip Map:${NC}"
    for (( s=0; s < total; s++ )); do
        local idx="${strip_indices[s]}"
        local node="${strip_nodes[s]}"
        local role
        if (( idx <= data_strips )); then
            role="${GREEN}DATA${NC}"
        else
            role="${YELLOW}PARITY${NC}"
        fi

        local status="OK"
        if [[ -n "$SIMULATE_LOSS" ]] && (( idx == SIMULATE_LOSS )); then
            status="${RED}LOST (simulated)${NC}"
            role="${RED}MISSING${NC}"
        fi

        printf "    strip #%-2d  node%-2d  %-20b  %b\n" "$idx" "$node" "$role" "$status"
    done
    echo ""

    if [[ -n "$SIMULATE_LOSS" ]]; then
        echo -e "  ${YELLOW}Simulating loss of strip #${SIMULATE_LOSS}${NC}"
        echo -e "  ${YELLOW}Reed-Solomon can reconstruct from any ${data_strips} of ${total} strips${NC}"
        echo ""

        if (( parity_strips > 0 )); then
            echo -e "  ${GREEN}Reconstruction possible!${NC}"
            echo -e "  With ${parity_strips} parity strip(s), can tolerate ${parity_strips} strip loss(es)"
        else
            echo -e "  ${RED}No parity strips — cannot reconstruct!${NC}"
        fi
        echo ""
    fi

    # Concatenate data strips to reconstruct object
    info "Concatenating ${data_strips} data strips -> ${outfile}"
    > "$outfile"  # truncate

    local decoded_count=0
    for (( s=0; s < total; s++ )); do
        local idx="${strip_indices[s]}"
        if (( idx > data_strips )); then
            # Skip parity strips for simple decode
            continue
        fi

        if [[ -n "$SIMULATE_LOSS" ]] && (( idx == SIMULATE_LOSS )); then
            warn "  Strip #${idx} is LOST — filling with zeros"
            dd if=/dev/zero bs="$strip_sz" count=1 >> "$outfile" 2>/dev/null
        else
            info "  Appending strip #${idx} from node${strip_nodes[s]}"
            cat "${strip_files[s]}" >> "$outfile"
        fi
        (( decoded_count++ ))
    done

    local out_sz
    out_sz=$(stat -f%z "$outfile" 2>/dev/null || stat -c%s "$outfile" 2>/dev/null)
    echo ""
    info "Decoded object: ${outfile} (${out_sz} bytes)"

    # Show hexdump of decoded
    echo ""
    echo -e "  ${BOLD}Decoded hex (first ${HEXDUMP_BYTES} bytes):${NC}"
    xxd -l "$HEXDUMP_BYTES" "$outfile" 2>/dev/null || hexdump -C -n "$HEXDUMP_BYTES" "$outfile" 2>/dev/null

    # Show pattern analysis
    echo ""
    echo -e "  ${BOLD}Pattern analysis:${NC}"
    local first_byte
    first_byte=$(xxd -l 1 -p "$outfile" 2>/dev/null)
    if [[ -n "$first_byte" ]]; then
        # Count how many of the first 4096 bytes match the first byte
        local match_count
        match_count=$(xxd -l 4096 -p "$outfile" 2>/dev/null | fold -w2 | grep -c "^${first_byte}$" 2>/dev/null) || match_count=0
        echo "    First byte: 0x${first_byte}"
        echo "    Pattern 0x${first_byte} repeats: ${match_count}/4096 in first 4K"
        if (( match_count == 4096 )); then
            echo -e "    ${GREEN}Consistent fill pattern 0x${first_byte} — data looks valid${NC}"
        elif (( match_count > 3000 )); then
            echo -e "    ${YELLOW}Mostly pattern 0x${first_byte} — may have partial overwrites${NC}"
        fi
    fi

    echo ""
    echo -e "${DIM}Tip: Compare with live read:  qemu-io -f raw -c 'read -P 0x${first_byte:-00} 0 4096' nbd://${BIND}:${NBD_PORT}/vdiname${NC}"
}

# ── Cluster info (if running) ─────────────────────────────────────────
show_cluster_ec_info() {
    local port0=$(( BASE_PORT ))
    if nc -z "$BIND" "$port0" 2>/dev/null; then
        echo -e "${BOLD}${CYAN}═══ Cluster EC VDIs ═══${NC}"
        echo ""
        local vdi_list
        vdi_list=$("$DOG" -a "$BIND" -p "$port0" vdi list 2>/dev/null) || true
        if [[ -n "$vdi_list" ]]; then
            echo "$vdi_list" | while IFS= read -r line; do
                echo "  $line"
            done
        else
            echo "  (no VDIs or cluster not running)"
        fi
        echo ""
    fi
}

# ── Main ───────────────────────────────────────────────────────────────

echo -e "${BOLD}${CYAN}"
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║         Sheepdog EC Strip Inspector / Decoder               ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo -e "${NC}"

# Show cluster info if available
show_cluster_ec_info

# Discover all strips
oid_list_output=$(discover_strips) || exit 1

# Extract just the OID list (last line of output)
oid_list=$(echo "$oid_list_output" | tail -1)
# Print the rest (strip layout)
echo "$oid_list_output" | head -n -1

echo ""

# Hexdump each object's strips
for oid_hex in $oid_list; do
    hexdump_strips "$oid_hex"
done

# Decode if requested
if $DO_DECODE; then
    if [[ -n "$OID_FILTER" ]]; then
        # Decode specific OID
        for oid_hex in $oid_list; do
            if [[ "$oid_hex" == *"$OID_FILTER"* ]]; then
                manual_decode "$oid_hex"
            fi
        done
    else
        # Decode first OID found
        local first_oid
        first_oid=$(echo "$oid_list" | awk '{print $1}')
        if [[ -n "$first_oid" ]]; then
            manual_decode "$first_oid"
        fi
    fi
fi

echo ""
echo -e "${BOLD}═══ EC Format Reference ═══${NC}"
echo ""
echo "  OID format:   FFVVVVVVIIIIIIII (16 hex chars)"
echo "                FF       = flags (00=data, 10=vdi, 20=vmstate)"
echo "                VVVVVV   = VDI ID (24 bits)"
echo "                IIIIIIII = object index (32 bits)"
echo ""
echo "  Strip file:   {oid_hex}_{ec_index:02x}"
echo "                ec_index 01..D  = data strips"
echo "                ec_index D+1..T = parity strips"
echo ""
echo "  EC policy:    (D << 4) | P  (single byte)"
echo "                0x21 = 2:1 (2 data + 1 parity, 1.5x overhead)"
echo "                0x42 = 4:2 (4 data + 2 parity, 1.5x overhead)"
echo ""
echo "  Strip size:   SD_DATA_OBJ_SIZE / D = 4MB / D"
echo "                2:1 -> 2MB per strip"
echo "                4:2 -> 1MB per strip"
echo ""
echo -e "${DIM}For full decode with Reed-Solomon reconstruction, use the sheep daemon.${NC}"
echo -e "${DIM}This script performs data-strip concatenation (no RS math).${NC}"

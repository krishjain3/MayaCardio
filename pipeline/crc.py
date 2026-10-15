"""
CRC validation utilities conforming to Section 6.2 of the MayaCardio assessment.
"CRC-32 (zlib) of the data text exactly as sent, from [ to the matching ].
A reply with a wrong value is corrupt and must not be used."
"""

import re
import zlib
from typing import Optional, Tuple


def compute_crc32(raw_slice: str) -> str:
    """Computes zlib CRC-32 of a string and returns an 8-character lowercase hex string."""
    crc = zlib.crc32(raw_slice.encode("utf-8"))
    return f"{crc:08x}"


def extract_data_slice_and_validate_crc(raw_line: str) -> Tuple[bool, Optional[str], Optional[str]]:
    """
    Extracts the exact substring of the data field array from raw JSON line
    and compares its CRC-32 against the reply's crc32 field.

    Returns:
        (is_valid, extracted_crc, expected_crc)
    """
    # Find "crc32"
    crc_match = re.search(r'"crc32"\s*:\s*"([0-9a-fA-F]{8})"', raw_line)
    if not crc_match:
        return False, None, None
    expected_crc = crc_match.group(1).lower()

    # Find "data" field opening bracket
    data_match = re.search(r'"data"\s*:\s*\[', raw_line)
    if not data_match:
        return False, None, expected_crc

    start_idx = data_match.end() - 1  # index of the first '[' in the data array

    # Bracket matching to find matching ']'
    bracket_count = 0
    in_string = False
    escape = False
    end_idx = -1

    for i in range(start_idx, len(raw_line)):
        c = raw_line[i]
        if in_string:
            if escape:
                escape = False
            elif c == "\\":
                escape = True
            elif c == '"':
                in_string = False
        else:
            if c == '"':
                in_string = True
            elif c == "[":
                bracket_count += 1
            elif c == "]":
                bracket_count -= 1
                if bracket_count == 0:
                    end_idx = i + 1
                    break

    if end_idx == -1:
        return False, None, expected_crc

    raw_data_slice = raw_line[start_idx:end_idx]
    actual_crc = compute_crc32(raw_data_slice)

    is_valid = (actual_crc.lower() == expected_crc)
    return is_valid, actual_crc, expected_crc

import pytest
from pipeline.crc import compute_crc32, extract_data_slice_and_validate_crc


def test_compute_crc32():
    data_str = "[[720, 1200, 1560], [708, 1180, 1534], [726, 1210, 1573]]"
    crc = compute_crc32(data_str)
    assert crc == "f9a5bf62"


def test_extract_and_validate_crc_valid():
    raw_line = (
        '{"session_id": "5f0c2a9e", "sensor": 1, "first_index": 120000, "count": 3, '
        '"lost": [], "session_end": false, "crc32": "f9a5bf62", '
        '"data": [[720, 1200, 1560], [708, 1180, 1534], [726, 1210, 1573]]}'
    )
    is_valid, actual, expected = extract_data_slice_and_validate_crc(raw_line)
    assert is_valid is True
    assert actual == "f9a5bf62"
    assert expected == "f9a5bf62"


def test_extract_and_validate_crc_corrupt():
    raw_line = (
        '{"session_id": "5f0c2a9e", "sensor": 1, "first_index": 120000, "count": 3, '
        '"lost": [], "session_end": false, "crc32": "deadbeef", '
        '"data": [[720, 1200, 1560], [708, 1180, 1534], [726, 1210, 1573]]}'
    )
    is_valid, actual, expected = extract_data_slice_and_validate_crc(raw_line)
    assert is_valid is False
    assert actual == "f9a5bf62"
    assert expected == "deadbeef"


def test_extract_and_validate_crc_with_nested_spaces():
    raw_line = (
        '{"session_id": "test", "sensor": 2, "first_index": 0, "count": 2, "lost": [], '
        '"crc32": "f364024c", "data": [[400000, 150000], [400005, 149998]]}'
    )
    # Check that whatever data string is in there matches its own CRC
    data_slice = "[[400000, 150000], [400005, 149998]]"
    expected_crc = compute_crc32(data_slice)
    line = raw_line.replace("f364024c", expected_crc)
    is_valid, actual, expected = extract_data_slice_and_validate_crc(line)
    assert is_valid is True
    assert actual == expected_crc

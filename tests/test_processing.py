import math
import os
import tempfile
import pytest
from pipeline.storage import Storage
from pipeline.processing import compute_window_result, detect_peaks_in_range
from mock.biosignal_mock import BiosignalGenerator


@pytest.fixture
def temp_storage():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test_proc.db")
        storage = Storage(db_path)
        yield storage


def test_window_complete_calculation(temp_storage):
    session_id = "sess_complete"
    # Populate complete window 0 (samples 0 to 399)
    # Sensor 1: 3 channels
    s1_data = [[100, 200, 300] for _ in range(400)]
    # Sensor 2: 2 channels [3, 4] -> sqrt(3^2 + 4^2) = 5.0
    s2_data = [[3, 4] for _ in range(400)]

    temp_storage.insert_samples_batch(session_id, 1, 0, s1_data)
    temp_storage.insert_samples_batch(session_id, 2, 0, s2_data)

    res = compute_window_result(temp_storage, session_id, window=0, mode="offline")
    assert res.window == 0
    assert res.missing_1 == 0
    assert res.missing_2 == 0
    assert res.complete is True
    assert res.mean["sensor_1"] == [100.0, 200.0, 300.0]
    assert res.min["sensor_1"] == [100, 200, 300]
    assert res.max["sensor_1"] == [100, 200, 300]
    assert res.mean["sensor_2"] == [3.0, 4.0]
    assert res.magnitude == 5.0
    assert res.heart_rate is None  # window 0 < 4 -> null
    assert res.mode == "offline"


def test_window_partial_missing(temp_storage):
    session_id = "sess_partial"
    # Sensor 1: only 300 samples
    s1_data = [[10, 20, 30] for _ in range(300)]
    # Sensor 2: 0 samples
    temp_storage.insert_samples_batch(session_id, 1, 0, s1_data)

    res = compute_window_result(temp_storage, session_id, window=0, mode="online")
    assert res.missing_1 == 100
    assert res.missing_2 == 400
    assert res.complete is False
    assert res.mean["sensor_1"] == [10.0, 20.0, 30.0]
    assert res.mean["sensor_2"] is None
    assert res.min["sensor_2"] is None
    assert res.max["sensor_2"] is None
    assert res.magnitude is None
    assert res.heart_rate is None
    assert res.mode == "online"


def test_heart_rate_calculation_60bpm(temp_storage):
    """
    Test 60 bpm synthetic signal over first 5 windows (0 to 4).
    Total 2000 samples (samples 0 to 1999).
    For window 4:
    Lookback 5 seconds: samples 0 to 1999.
    In 60 bpm signal, heart beat is exactly once every 400 samples (1 second at 400 Hz).
    """
    session_id = "sess_hr_60"
    gen = BiosignalGenerator(mode="signal", seed=7)

    # Generate 5 full windows (2000 samples)
    s1_samples = [gen.get_sample(1, i) for i in range(2000)]
    s2_samples = [gen.get_sample(2, i) for i in range(2000)]

    temp_storage.insert_samples_batch(session_id, 1, 0, s1_samples)
    temp_storage.insert_samples_batch(session_id, 2, 0, s2_samples)

    # Window 3: w < 4 -> heart_rate must be None
    res_w3 = compute_window_result(temp_storage, session_id, window=3, mode="offline")
    assert res_w3.heart_rate is None

    # Window 4: w == 4 -> full 5-second range available [0, 1999]
    res_w4 = compute_window_result(temp_storage, session_id, window=4, mode="offline")
    assert res_w4.complete is True
    assert res_w4.heart_rate is not None
    # In signal mode, minutes 0-2 have 60 bpm:
    assert round(res_w4.heart_rate, 1) == 60.0


def test_heart_rate_null_when_sample_missing_in_5s(temp_storage):
    session_id = "sess_hr_missing"
    gen = BiosignalGenerator(mode="signal", seed=7)

    # Generate 2000 samples but omit sample 500
    s1_samples_part1 = [gen.get_sample(1, i) for i in range(0, 500)]
    s1_samples_part2 = [gen.get_sample(1, i) for i in range(501, 2000)]
    s2_samples = [gen.get_sample(2, i) for i in range(2000)]

    temp_storage.insert_samples_batch(session_id, 1, 0, s1_samples_part1)
    temp_storage.insert_samples_batch(session_id, 1, 501, s1_samples_part2)
    temp_storage.insert_samples_batch(session_id, 2, 0, s2_samples)

    res_w4 = compute_window_result(temp_storage, session_id, window=4, mode="offline")
    # Missing sample in 5s range -> heart_rate must be null
    assert res_w4.heart_rate is None


def test_ramp_mode_formulas(temp_storage):
    session_id = "sess_ramp"
    gen = BiosignalGenerator(mode="ramp", seed=42)

    s1_samples = [gen.get_sample(1, i) for i in range(400)]
    s2_samples = [gen.get_sample(2, i) for i in range(400)]

    # Verify Section 6.1 ramp formulas
    # s1 channel c = (3i + c) mod 8388608
    for i in range(5):
        assert s1_samples[i] == [(3 * i + c) % 8388608 for c in range(3)]
        assert s2_samples[i] == [(2 * i + c) % 8388608 for c in range(2)]

    temp_storage.insert_samples_batch(session_id, 1, 0, s1_samples)
    temp_storage.insert_samples_batch(session_id, 2, 0, s2_samples)

    res = compute_window_result(temp_storage, session_id, window=0, mode="offline")
    assert res.complete is True
    assert res.missing_1 == 0
    assert res.missing_2 == 0


def test_sensor1_and_sensor2_arrive_different_times_updates_incomplete_window(temp_storage):
    """
    Validates Requirement 3:
    When Sensor 1 and Sensor 2 arrive at different times, prematurely calculated incomplete
    windows must be correctly updated to complete when the remaining samples arrive.
    """
    session_id = "sess_diff_times"

    # Step 1: Sensor 1 arrives first for Window 0 (samples 0..399)
    s1_samples = [[100, 200, 300] for _ in range(400)]
    temp_storage.insert_samples_batch(session_id, 1, 0, s1_samples)

    # Window 0 calculated prematurely (Sensor 2 not yet arrived)
    res_premature = compute_window_result(temp_storage, session_id, window=0, mode="online")
    assert res_premature.missing_1 == 0
    assert res_premature.missing_2 == 400
    assert res_premature.complete is False
    assert res_premature.mean["sensor_2"] is None
    assert res_premature.magnitude is None

    # Save premature result to storage
    temp_storage.save_window_result(res_premature)
    assert temp_storage.get_incomplete_windows(session_id, mode="online") == [0]

    # Step 2: Sensor 2 arrives later for Window 0 (samples 0..399)
    s2_samples = [[3, 4] for _ in range(400)]
    temp_storage.insert_samples_batch(session_id, 2, 0, s2_samples)

    # Window 0 recalculated upon Sensor 2 arrival
    res_updated = compute_window_result(temp_storage, session_id, window=0, mode="online")
    assert res_updated.missing_1 == 0
    assert res_updated.missing_2 == 0
    assert res_updated.complete is True
    assert res_updated.mean["sensor_1"] == [100.0, 200.0, 300.0]
    assert res_updated.mean["sensor_2"] == [3.0, 4.0]
    assert res_updated.magnitude == 5.0

    # Save updated result and verify incomplete list is now empty
    temp_storage.save_window_result(res_updated)
    assert temp_storage.get_incomplete_windows(session_id, mode="online") == []


def test_sensor2_arrives_before_sensor1_updates_cleanly(temp_storage):
    """
    Opposite order: Sensor 2 arrives before Sensor 1 for Window 0.
    """
    session_id = "sess_s2_first"

    # Sensor 2 arrives first
    s2_samples = [[6, 8] for _ in range(400)]
    temp_storage.insert_samples_batch(session_id, 2, 0, s2_samples)

    res_premature = compute_window_result(temp_storage, session_id, window=0, mode="online")
    assert res_premature.missing_1 == 400
    assert res_premature.missing_2 == 0
    assert res_premature.complete is False
    assert res_premature.mean["sensor_1"] is None
    assert res_premature.magnitude == 10.0

    temp_storage.save_window_result(res_premature)

    # Sensor 1 arrives later
    s1_samples = [[50, 150, 250] for _ in range(400)]
    temp_storage.insert_samples_batch(session_id, 1, 0, s1_samples)

    res_updated = compute_window_result(temp_storage, session_id, window=0, mode="online")
    assert res_updated.complete is True
    assert res_updated.missing_1 == 0
    assert res_updated.missing_2 == 0
    assert res_updated.mean["sensor_1"] == [50.0, 150.0, 250.0]
    assert res_updated.magnitude == 10.0

    temp_storage.save_window_result(res_updated)
    assert temp_storage.get_incomplete_windows(session_id, mode="online") == []


def test_peak_requires_800_samples_history(temp_storage):
    """
    Section 3: Sample i is a peak when:
    - the 800 samples before it, and the sample after it, are available
    - it is larger than both neighbours
    - it is larger than half of the maximum of the 800 samples before it
    - it is at least 100 samples after the previous peak
    Sample 799 has only 799 samples before it (0..798), so it can NEVER be a peak.
    Sample 800 has 800 samples before it (0..799), so it CAN be a peak.
    """
    session_id = "sess_peak_history"
    # Create 1000 baseline samples
    s1_samples = [[0, 100, 0] for _ in range(1000)]
    # Place sharp peak at sample 799
    s1_samples[799] = [0, 5000, 0]
    # Place sharp peak at sample 800
    s1_samples[800] = [0, 6000, 0]

    s1_dict = {i: s1_samples[i] for i in range(1000)}

    # Scan for peaks from 0 to 900
    peaks, _ = detect_peaks_in_range(s1_dict, 0, 900)
    # 799 cannot be a peak because 799 < 800 (not enough history)
    assert 799 not in peaks
    # 800 has 800 samples before it (0..799) and 801 after it, and 6000 > 5000
    assert 800 in peaks


def test_peak_preceding_boundary_enforces_100_sample_distance(temp_storage):
    """
    Validates boundary condition where a peak preceding hr_start (e.g. at 1180)
    suppresses a peak candidate at hr_start + 20 (sample 1220) which is < 100 samples apart.
    """
    session_id = "sess_boundary_100"
    s1_samples = [[0, 100, 0] for _ in range(3500)]
    # Peak at 1180 (within window 2)
    s1_samples[1180] = [0, 5000, 0]
    # Candidate at 1220 (within window 3, hr_start for window 7 is 1200)
    s1_samples[1220] = [0, 5000, 0]
    # Another peak at 1400 (220 samples after 1180)
    s1_samples[1400] = [0, 5000, 0]
    # Another peak at 1800
    s1_samples[1800] = [0, 5000, 0]

    s2_samples = [[3, 4] for _ in range(3500)]

    temp_storage.insert_samples_batch(session_id, 1, 0, s1_samples)
    temp_storage.insert_samples_batch(session_id, 2, 0, s2_samples)

    # For window 7: 5s range is 400*(7-4)=1200 to 400*(7+1)-1=3199 (clamped to available 2999)
    # Peak candidate 1220 is < 100 samples after 1180, so 1220 must NOT be a peak!
    # The peaks in [1200, 2999] should be 1400 and 1800 (gap = 400 samples -> 1.0s -> 60 bpm)
    res_w7 = compute_window_result(temp_storage, session_id, window=7, mode="offline")
    assert res_w7.complete is True
    # If 1220 was wrongly detected as a peak, gaps would be (1400-1220)=180 and (1800-1400)=400, giving wrong HR
    # With correct 100-sample boundary suppression, only 1400 and 1800 are peaks:
    # gap = 400 samples -> 1.0s -> 60.0 bpm
    assert res_w7.heart_rate == 60.0


def test_missing_data_at_boundaries_nulls_heart_rate(temp_storage):
    """
    Section 3: heart_rate is null if any sample in those 5 seconds is missing.
    Verify:
    1. Missing sample at exact start (hr_start = 0 for w=4)
    2. Missing sample at exact end (hr_end = 1999 for w=4)
    3. Missing sample outside the 5s window does NOT null heart rate if history is available
    """
    gen = BiosignalGenerator(mode="signal", seed=7)
    s1_samples = [gen.get_sample(1, i) for i in range(2500)]
    s2_samples = [gen.get_sample(2, i) for i in range(2500)]

    # Case A: Missing sample at index 0 (hr_start for w=4)
    st_a = Storage(os.path.join(tempfile.mkdtemp(), "a.db"))
    st_a.insert_samples_batch("sess_a", 1, 1, s1_samples[1:2000])  # omit sample 0
    st_a.insert_samples_batch("sess_a", 2, 0, s2_samples[:2000])
    res_a = compute_window_result(st_a, "sess_a", window=4, mode="offline")
    assert res_a.heart_rate is None

    # Case B: Missing sample at index 1999 (hr_end for w=4)
    st_b = Storage(os.path.join(tempfile.mkdtemp(), "b.db"))
    st_b.insert_samples_batch("sess_b", 1, 0, s1_samples[:1999])  # omit sample 1999
    st_b.insert_samples_batch("sess_b", 2, 0, s2_samples[:2000])
    res_b = compute_window_result(st_b, "sess_b", window=4, mode="offline")
    assert res_b.heart_rate is None

    # Case C: All 2000 samples present in [0, 1999] -> heart rate is 60.0 bpm
    st_c = Storage(os.path.join(tempfile.mkdtemp(), "c.db"))
    st_c.insert_samples_batch("sess_c", 1, 0, s1_samples[:2001])
    st_c.insert_samples_batch("sess_c", 2, 0, s2_samples[:2001])
    res_c = compute_window_result(st_c, "sess_c", window=4, mode="offline")
    assert res_c.heart_rate == 60.0


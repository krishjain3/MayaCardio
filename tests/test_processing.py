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

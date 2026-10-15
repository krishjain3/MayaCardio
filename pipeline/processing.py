"""
Processing logic conforming to Section 3 of the MayaCardio assessment.
Identical implementation used on device (offline mode) and cloud (online mode).
"""

import math
from typing import Dict, List, Optional, Tuple, Any
from pipeline.models import WindowResult
from pipeline.storage import Storage

WINDOW_SIZE = 400  # 400 samples per second
HR_LOOKBACK_WINDOWS = 5  # 5 seconds
HR_LOOKBACK_SAMPLES = HR_LOOKBACK_WINDOWS * WINDOW_SIZE  # 2000 samples
PEAK_HISTORY_SAMPLES = 800  # 800 samples lookback for peak detection
MIN_PEAK_DISTANCE = 100  # At least 100 samples between peaks


def detect_peaks_in_range(
    s1_samples: Dict[int, List[int]],
    start_idx: int,
    end_idx: int,
    initial_prev_peak: Optional[int] = None
) -> Tuple[List[int], Optional[int]]:
    """
    Detects R-wave peaks in Sensor 1 Channel 2 (index 1) within [start_idx, end_idx].
    Rule from Section 3:
    Sample i is a peak when all four hold:
    1. the 800 samples before it, and the sample after it, are available;
    2. it is larger than both neighbours (v[i] > v[i-1] and v[i] > v[i+1]);
    3. it is larger than half of the maximum of the 800 samples before it;
    4. it is at least 100 samples after the previous peak.

    Returns:
        (peaks_in_range, last_peak_found)
    """
    peaks = []
    prev_peak = initial_prev_peak

    # To detect peaks in [start_idx, end_idx], we scan from max(800, start_idx) to end_idx
    scan_start = max(PEAK_HISTORY_SAMPLES, start_idx)

    for i in range(scan_start, end_idx + 1):
        # 1. Check sample i and neighbours are available
        if i not in s1_samples or (i - 1) not in s1_samples or (i + 1) not in s1_samples:
            continue

        val_i = s1_samples[i][1]
        val_prev = s1_samples[i - 1][1]
        val_next = s1_samples[i + 1][1]

        # 2. Larger than both neighbours
        if not (val_i > val_prev and val_i > val_next):
            continue

        # 4. Check distance from previous peak
        if prev_peak is not None and (i - prev_peak) < MIN_PEAK_DISTANCE:
            continue

        # 1 & 3. Check 800 samples before are available and calculate max
        history_available = True
        max_history = -float("inf")
        for j in range(i - PEAK_HISTORY_SAMPLES, i):
            if j not in s1_samples:
                history_available = False
                break
            val_j = s1_samples[j][1]
            if val_j > max_history:
                max_history = val_j

        if not history_available:
            continue

        if val_i <= 0.5 * max_history:
            continue

        # All four conditions satisfied!
        peaks.append(i)
        prev_peak = i

    return peaks, prev_peak


def compute_window_result(
    storage: Storage,
    session_id: str,
    window: int,
    mode: str = "offline"
) -> WindowResult:
    """
    Calculates window result strictly conforming to Section 3:
    - window w: samples 400w to 400w + 399
    - missing_1, missing_2: unavailable samples
    - complete: missing_1 == 0 and missing_2 == 0
    - mean, min, max per channel over available samples (null if none)
    - magnitude: mean of sqrt(ch1^2 + ch2^2) over available sensor 2 samples
    - heart_rate: beats per minute from sensor 1, channel 2
    """
    start_sample = window * WINDOW_SIZE
    end_sample = start_sample + WINDOW_SIZE - 1

    s1_dict = storage.get_samples_in_range(session_id, 1, start_sample, end_sample)
    s2_dict = storage.get_samples_in_range(session_id, 2, start_sample, end_sample)

    missing_1 = WINDOW_SIZE - len(s1_dict)
    missing_2 = WINDOW_SIZE - len(s2_dict)
    complete = (missing_1 == 0 and missing_2 == 0)

    # Calculate statistics for Sensor 1 (3 channels)
    if s1_dict:
        s1_vals = list(s1_dict.values())
        mean_1 = [round(sum(v[c] for v in s1_vals) / len(s1_vals), 1) for c in range(3)]
        min_1 = [min(v[c] for v in s1_vals) for c in range(3)]
        max_1 = [max(v[c] for v in s1_vals) for c in range(3)]
    else:
        mean_1 = None
        min_1 = None
        max_1 = None

    # Calculate statistics for Sensor 2 (2 channels)
    if s2_dict:
        s2_vals = list(s2_dict.values())
        mean_2 = [round(sum(v[c] for v in s2_vals) / len(s2_vals), 1) for c in range(2)]
        min_2 = [min(v[c] for v in s2_vals) for c in range(2)]
        max_2 = [max(v[c] for v in s2_vals) for c in range(2)]

        # Magnitude: Mean of sqrt(ch1^2 + ch2^2) over available sensor 2 samples
        mag_sum = sum(math.sqrt(v[0] ** 2 + v[1] ** 2) for v in s2_vals)
        magnitude = round(mag_sum / len(s2_vals), 1)
    else:
        mean_2 = None
        min_2 = None
        max_2 = None
        magnitude = None

    mean_obj = {"sensor_1": mean_1, "sensor_2": mean_2}
    min_obj = {"sensor_1": min_1, "sensor_2": min_2}
    max_obj = {"sensor_1": max_1, "sensor_2": max_2}

    # Heart Rate Calculation
    # Rule 3: heart_rate is null if w < 4, if any sample in those 5 seconds is missing, or if fewer than 2 peaks.
    heart_rate = None
    if window >= 4:
        hr_start = 400 * (window - 4)
        hr_end = 400 * (window + 1) - 1  # 2000 samples

        # Fetch all Sensor 1 samples for the 5-second range PLUS 800 samples lookback and 1 sample lookahead
        fetch_start = max(0, hr_start - PEAK_HISTORY_SAMPLES)
        fetch_end = hr_end + 1
        s1_hr_range = storage.get_samples_in_range(session_id, 1, fetch_start, fetch_end)

        # Check if ANY sample in the 5 seconds [hr_start, hr_end] is missing
        missing_in_5s = False
        for idx in range(hr_start, hr_end + 1):
            if idx not in s1_hr_range:
                missing_in_5s = True
                break

        if not missing_in_5s:
            # We detect peaks starting from the lookback to ensure peak chaining consistency
            # Scan from max(800, fetch_start) to hr_end
            all_peaks, _ = detect_peaks_in_range(s1_hr_range, fetch_start, hr_end)

            # Filter peaks strictly in the 5-second range [hr_start, hr_end]
            peaks_in_5s = [p for p in all_peaks if hr_start <= p <= hr_end]

            if len(peaks_in_5s) >= 2:
                # heart_rate = 60 / (mean gap between consecutive peaks, in seconds)
                # gap in seconds = (p_{j+1} - p_j) / 400.0
                # mean gap = (peaks[-1] - peaks[0]) / (400.0 * (len(peaks) - 1))
                mean_gap_sec = (peaks_in_5s[-1] - peaks_in_5s[0]) / (WINDOW_SIZE * (len(peaks_in_5s) - 1))
                if mean_gap_sec > 0:
                    heart_rate = round(60.0 / mean_gap_sec, 1)

    return WindowResult(
        session_id=session_id,
        window=window,
        missing_1=missing_1,
        missing_2=missing_2,
        complete=complete,
        mean=mean_obj,
        min=min_obj,
        max=max_obj,
        magnitude=magnitude,
        heart_rate=heart_rate,
        mode=mode
    )

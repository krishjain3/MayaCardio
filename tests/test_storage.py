import os
import tempfile
import pytest
from pipeline.storage import Storage, ConflictingSampleError
from pipeline.models import Sample, LossRecord, WindowResult


@pytest.fixture
def temp_storage():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")
        storage = Storage(db_path, synchronous="FULL")
        yield storage


def test_insert_and_export_samples(temp_storage):
    session_id = "test_sess_01"
    # Insert 3 samples for sensor 1
    data_1 = [[720, 1200, 1560], [708, 1180, 1534], [726, 1210, 1573]]
    cnt = temp_storage.insert_samples_batch(session_id, sensor=1, first_index=120000, data=data_1)
    assert cnt == 3

    # Insert 2 samples for sensor 2
    data_2 = [[400000, 150000], [400005, 149998]]
    cnt2 = temp_storage.insert_samples_batch(session_id, sensor=2, first_index=120000, data=data_2)
    assert cnt2 == 2

    # Insert a loss record
    losses = [{"from_index": 4000, "to_index": 4399, "reason": "buffer_overflow"}]
    loss_cnt = temp_storage.insert_loss_records(session_id, sensor=2, losses=losses)
    assert loss_cnt == 1

    # Check export stream
    lines = list(temp_storage.export_data(session_id))
    assert len(lines) == 6  # 3 s1 samples + 2 s2 samples + 1 s2 loss = 6

    # Verify first 3 lines are sensor 1
    assert '"sensor": 1' in lines[0]
    assert '"index": 120000' in lines[0]
    assert '"values": [720, 1200, 1560]' in lines[0]

    # Verify last line is loss record
    assert '"from_index": 4000' in lines[-1]
    assert '"reason": "buffer_overflow"' in lines[-1]


def test_idempotent_duplicate_handling(temp_storage):
    session_id = "test_sess_02"
    data = [[720, 1200, 1560], [708, 1180, 1534]]
    # Insert first time
    assert temp_storage.insert_samples_batch(session_id, sensor=1, first_index=0, data=data) == 2
    # Insert duplicate identical batch
    assert temp_storage.insert_samples_batch(session_id, sensor=1, first_index=0, data=data) == 0

    saved, cloud, backlog = temp_storage.get_counts(session_id)
    assert saved == 2


def test_conflicting_duplicate_raises_error(temp_storage):
    session_id = "test_sess_03"
    data = [[720, 1200, 1560]]
    temp_storage.insert_samples_batch(session_id, sensor=1, first_index=10, data=data)

    conflicting_data = [[999, 999, 999]]
    with pytest.raises(ConflictingSampleError):
        temp_storage.insert_samples_batch(session_id, sensor=1, first_index=10, data=conflicting_data)


def test_sync_cursor_and_unuploaded_samples(temp_storage):
    session_id = "test_sess_04"
    data = [[i, i * 2, i * 3] for i in range(10)]
    temp_storage.insert_samples_batch(session_id, sensor=1, first_index=0, data=data)

    start_idx, unuploaded = temp_storage.get_unuploaded_samples(session_id, sensor=1, limit=5)
    assert start_idx == 0
    assert len(unuploaded) == 5

    temp_storage.advance_sync_cursor(session_id, sensor=1, up_to_index=4)
    start_idx2, unuploaded2 = temp_storage.get_unuploaded_samples(session_id, sensor=1, limit=5)
    assert start_idx2 == 5
    assert len(unuploaded2) == 5

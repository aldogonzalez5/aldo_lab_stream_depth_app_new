import json
import os

from constants import DATASET_PROVIDER, DATASET_NAME
from corva import StreamDepthEvent, StreamDepthRecord
from lambda_function import lambda_handler

TEST_STREAMS_PATH =  os.path.join(os.path.abspath(os.path.dirname(__file__)), 'streams')


class TestDepthStream():
    def test_app(self, app_runner, requests_mock):
        stream_file = os.path.join(TEST_STREAMS_PATH, "corva_drilling_wits_depth.json")
        with open(stream_file, encoding="utf8") as raw_stream:
            corva_drilling_wits_depth_stream = json.load(raw_stream)
        records = []
        for stream_record in corva_drilling_wits_depth_stream:
            stream_depth_record = StreamDepthRecord(
                    data=stream_record.get("data", {}), 
                    timestamp=stream_record.get("timestamp_read",1620905165),
                    measured_depth=stream_record.get("measured_depth",1620905165),
                    log_identifier=stream_record.get("log_identifier","5701c048cf9a"),
                )
            records.append(stream_depth_record)
        event = StreamDepthEvent(
            company_id=1,
            asset_id=1234,
            records=records,
        )

        requests_mock.post(f'https://data.localhost.ai/api/v1/data/{DATASET_PROVIDER}/{DATASET_NAME}/')
        outputs = app_runner(lambda_handler, event=event)
        assert outputs
        for output in outputs:
            assert "dep" in output["data"]

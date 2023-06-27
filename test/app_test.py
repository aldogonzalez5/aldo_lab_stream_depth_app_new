from corva import StreamDepthEvent, StreamTimeRecord
from lambda_function import lambda_handler


def test_app(app_runner):
    event = StreamDepthEvent(
        company_id=1,
        asset_id=1234,
        records=[
            StreamDepthRecord(
                data={'bit_depth': 4980, 'hole_depth': 5000}, measured_depth=5000, log_identifier="5701c048cf9a",
            )
        ],
    )

    app_runner(lambda_handler, event=event)

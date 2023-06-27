from corva import Api, Cache, Logger, StreamDepthEvent, stream


@stream
def lambda_handler(event: StreamDepthEvent, api: Api, cache: Cache):
    """Insert your logic here"""
    Logger.info('Hello, World!')


from constants import DATASET_NAME, DATASET_PROVIDER
from corva import Api, Cache, Logger, StreamDepthEvent, stream


@stream
def lambda_handler(event: StreamDepthEvent, api: Api, cache: Cache):
    
    # 3. Here is where you can declare your variables from the argument event: StreamDepthEvent and start using Api, Cache and Logger functionalities. You can obtain key values directly from metadata in the stream app event without making any additional API requests.

    # The stream depth app can declare the following attributes from the StreamDepthEvent: company_id: The company identifier; asset_id: The asset identifier; records: The records that include the indexes and data object with key values. 
    asset_id = event.asset_id
    company_id = event.company_id
    records = event.records

    # Records is a list
    record_count = len(records)

    # Each element of records has a measured_depth. You can declare variables for start and end measured depth. 
    start_measured_depth = records[0].measured_depth
    end_measured_depth = records[-1].measured_depth

    # Utilize the Logger functionality. The default log level is Logger.info. To use a different log level, the log level must be specified in the manifest.json file in the "settings.environment": {"LOG_LEVEL": "DEBUG"}. See the Logger documentation for more information.
    Logger.info(f"{asset_id=} {company_id=}")
    Logger.info(f"{start_measured_depth=} {end_measured_depth=} {record_count=}")

    # Utililize the Cache functionality to get a set key value. The Cache functionality is built on Redis Cache. See the Cache documentation for more information.
    # Getting last exported measured_depth from Cache 
    last_exported_measured_depth = float(cache.get(key='last_exported_measured_depth') or 0)

    # 4. Here is where you can add your app logic.
    
    # Setting state to append data to an arrray 
    outputs = []
    for record in records:

        # Making sure we are not processing duplicate data
        if record.measured_depth <= last_exported_measured_depth:
            continue

        # Each element of records has data. This is how to get specific key values from an embedded object
        avg_weight_on_bit = record.data.get("wobavg", 0)
        avg_hook_load = record.data.get("hkldav", 0)
        dep = record.data.get("hkldav", 0)
        Logger.info(f"{dep=}")
        # This is how to set up a body of a POST request to store the avg_hook_load and avg_weight_on_bit data from the StreamDepthEvent and newly calculated avg_wob_plus_hkld value 
        try:
            output = {
                "measured_depth": record.measured_depth,
                "asset_id": asset_id,
                "log_identifier": record.log_identifier,
                "company_id": company_id,
                "provider": "big-data-energy",
                "collection": "example-stream-depth-app",
                "data": {
                    "avg_hook_load": avg_hook_load,
                    "avg_weight_on_bit": avg_weight_on_bit,
                    "avg_wob_plus_hkld": avg_weight_on_bit + avg_hook_load,
                    "dep": dep
                },
                "version": 1
            }
        except Exception as ex:
            Logger.debug(f"record: {str(record)}")
            Logger.debug(f"error: {str(ex)}")
            raise ex
        # Appending the new data to the empty array
        outputs.append(output)

    # 5. Save the newly calculated data in a custom dataset

    # Set up an if statement so that if request fails, lambda will be reinvoked. So no exception handling
    if outputs:
        # Utilize Logger functionality to confirm data in log files
        Logger.debug(f"{outputs=}")

        # Utilize the Api functionality. The data=outputs needs to be an an array because Corva's data is saved as an array of objects. Objects being records. See the Api documentation for more information.
        try:
            api.post(
                f"api/v1/data/{DATASET_PROVIDER}/{DATASET_NAME}/", data=outputs,
            ).raise_for_status()
        except Exception as ex:
            Logger.debug(str(ex))
            raise ex
        # Utililize the Cache functionality to set a key value. The Cache functionality is built on Redis Cache. See the Cache documentation for more information. This example is setting the last measured_depth of the output to Cache
        cache.set(key='last_exported_measured_depth', value=outputs[-1].get("measured_depth"))

    return outputs

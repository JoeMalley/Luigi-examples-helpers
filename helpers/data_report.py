from datetime import datetime
import polars as pl

def format_date(date: str) -> pl.datetime:
    """Takes a string date and turns it into polars timestamp.

    Args:
        date (string): A date string e.g 2022-01-20

    Returns:
        pl.datetime: A polars datetime object
    """
    print(date)
    datem = datetime.strptime(date, "%Y-%m-%d")
    return pl.datetime(datem.year, datem.month, datem.day)

def get_clean_data(path) -> pl.DataFrame:
    """Get the required data from a message export and clean it.

    Only messages with 8 readings and a message type of 1 are kept.
    Messages with less than 8 readings are dropped to simplify processing,
    these are only sent in the first 24 hours so the amount of lost data is small.

    Args:
        path (str): The path to the message export

    Returns:
        pl.DataFrame: A dataframe containing the cleaned data
    """

    return (
        pl.scan_csv(path)
        .filter(
            (pl.col("readings") == 8)
            & (pl.col("msgType") == 1)
            & (pl.col("xtime") == 4)  # TODO: Support variable timeframes
        )
        .select(
            [
                pl.col("DeviceID").str.strip().alias("device_id"),
                pl.col("Timestamp")
                .str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S")
                .alias("timestamp"),
                pl.col("FLOW1").cast(pl.Int64),
                pl.col("FLOW2").cast(pl.Int64),
                pl.col("FLOW3").cast(pl.Int64),
                pl.col("FLOW4").cast(pl.Int64),
                pl.col("FLOW5").cast(pl.Int64),
                pl.col("FLOW6").cast(pl.Int64),
                pl.col("FLOW7").cast(pl.Int64),
                pl.col("FLOW8").cast(pl.Int64),
            ]
        )
    ).collect()

def get_leak_stats(df_leak) -> pl.DataFrame:
    """Perform basic statistical analysis on a dataframe of leaks.

    Args:
        df_leak (pl.DataFrame): A dataframe of leaks

    Returns:
        pl.DataFrame: A dataframe of leak statistics
    """
    return df_leak.select(
        [
            pl.count("device_id").alias("total_leaks"),
            pl.col("leak_active")
            .filter(pl.col("leak_active") == True)
            .count()
            .alias("active_leaks"),
            pl.avg("leak_amount_litres").alias("avg_leak_amount_litres"),
            pl.max("leak_amount_litres").alias("max_leak_amount_litres"),
            pl.avg("leak_length_days").alias("avg_leak_length_days"),
            pl.max("leak_length_days").alias("max_leak_length_days"),
        ]
    ).transpose(
        include_header=True,
        header_name="leakage_stat",
        column_names=["value"],
    )

def get_leak(df_consecutive_flow_state) -> pl.DataFrame:
    """Detect leaks in a flow dataframe with consecutive flow states.

    Args:
        df_consecutive_flow_state (pl.DataFrame): A dataframe of daily flows with consecutive flow states

    Returns:
        pl.DataFrame: A dataframe of leaks for each device
    """

    return (
        df_consecutive_flow_state.lazy()
        .groupby("consecutive_flow_state")
        .agg(
            [
                pl.first("device_id"),
                pl.last("date").alias("leak_start_date"),
                pl.count("device_id").alias("leak_length_days"),
                pl.min("min_flow").alias("leak_amount_litres"),
            ]
        )
        .filter((pl.col("leak_amount_litres") != 0) & (pl.col("leak_length_days") > 1))
        .sort("leak_amount_litres", reverse=True)
        .join(
            df_consecutive_flow_state.lazy()
            .groupby("device_id")
            .agg(pl.first("flowing").alias("leak_active")),
            on="device_id",
        )
        .select(pl.all().exclude(["consecutive_flow_state"]))
    ).collect()
def compute_timestamp() -> pl.Expr:
    """Compute the timestamp for a flow based on the timestamp of the message and the flow id.

    Messages (and flows) are not time aligned, so the hour a flow is most within is used.
    Message timestamp is for the end of the last flow, whereas the flow timestamp is for the start of each flow.

    Returns:
        pl.Expr: The timestamp for the flow
    """

    return (
        pl.when(pl.col("timestamp").dt.minute() < 30)
        .then(
            (
                pl.col("timestamp").cast(pl.Int64)
                - (3600000000 * (8 - pl.col("flow_id") + 1))
            ).cast(pl.Datetime)
        )
        .otherwise(
            (
                pl.col("timestamp").cast(pl.Int64)
                - (3600000000 * (8 - pl.col("flow_id")))
            ).cast(pl.Datetime)
        )
    )

def message_to_flow_df(df, flow_id) -> pl.DataFrame | pl.LazyFrame:
    """Convert a dataframe of messages to a dataframe of a single flow.

    This function is used to separate the different flows in a message into individual rows in a dataframe.

    Args:
        df (pl.DataFrame): The dataframe to filter
        flow_id (int): The flow id to filter for (1-8)
    """

    return (
        df.select(
            [
                pl.col("device_id"),
                pl.col("timestamp"),
                pl.lit(flow_id, pl.Int8).alias("flow_id"),
                pl.col(f"FLOW{flow_id}").alias("flow"),
            ]
        )
        .select(
            [
                pl.col("device_id"),
                compute_timestamp().alias("adj_timestamp"),
                pl.col("flow"),
            ]
        )
        .select(
            [
                pl.col("device_id"),
                pl.col("adj_timestamp")
                .dt.strftime("%Y-%m-%dT%H:%M:%S")
                .str.strptime(pl.Date, "%Y-%m-%dT%H:%M:%S")
                .alias("date"),
                pl.col("adj_timestamp").dt.strftime("%H").cast(pl.Int8).alias("hour"),
                pl.col("flow"),
            ]
        )
    )

def get_flow_data(df_clean_data) -> pl.DataFrame:
    """Convert a dataframe of messages to a dataframe of flows.

    Args:
        df_clean_data (pl.DataFrame): A dataframe of messages to convert

    Returns:
        pl.DataFrame: A dataframe of flows
    """

    return pl.concat(
        [message_to_flow_df(df_clean_data.lazy(), flow_id) for flow_id in range(1, 9)]
    ).collect()

def get_day_flows(df_clean_flows) -> pl.DataFrame:
    """Group flows by day and device, aggregating the total and min flow.

    The resulting dataframe contains the total and minimum flow for each device on a given day.

    Args:
        df_clean_flows (pl.DataFrame): A dataframe of flows to group

    Returns:
        pl.DataFrame: A dataframe of daily flows
    """

    return (
        df_clean_flows.lazy()
        .groupby(["device_id", "date"])
        .agg(
            [
                pl.sum("flow").alias("total_flow"),
                pl.min("flow").alias("min_flow"),
                (pl.col("flow").max() == 255).alias("clipped"),
            ]
        )
        .sort("date", reverse=True)
        .collect()
    )    

def get_consecutive_flow_state(df_day_flows) -> pl.DataFrame:
    """Generate a "consecutive flow state" for a dataframe of daily flows.

    The "consecutive flow state" is a number that increments each time the flow state changes.
    Flow state is true if the minimum flow is greater than 0.
    This is used to detect leaks as any group with the same consecutive flow state where flow
    state is true is likely to be a leak.

    Args:
        df_day_flows (pl.DataFrame): A dataframe of daily flows

    Returns:
        pl.DataFrame: A dataframe of daily flows with consecutive flow state
    """

    return (
        df_day_flows.sort(["device_id", "date"], reverse=True)
        .with_column((pl.col("min_flow") != 0).alias("flowing"))
        .with_column(pl.col("flowing").shift(1).alias("prev_flowing"))
        .with_column(pl.col("device_id").shift(1).alias("prev_device_id"))
        .with_column(
            (
                (pl.col("flowing") != pl.col("prev_flowing"))
                | (pl.col("device_id") != pl.col("prev_device_id"))
            )
            .cumsum()
            .alias("consecutive_flow_state")
        )
    )
def get_usage(df_day_flows) -> pl.DataFrame:
    """Aggregate daily total flow into daily average flow for each device.

    Args:
        df_day_flows (pl.DataFrame): A dataframe of daily flows

    Returns:
        pl.DataFrame: A dataframe of average daily flow for each device
    """
    return (
        df_day_flows.lazy()
        .groupby("device_id")
        .agg(
            [
                pl.avg("total_flow").alias("avg_daily_flow"),
                pl.max("clipped").cast(pl.Boolean),
            ]
        )
        .sort("avg_daily_flow", reverse=True)
        .filter(pl.col("avg_daily_flow") > 10)
        .collect()
    )

def get_dropped_devices(path) -> pl.DataFrame:
    """Get the devices that were dropped from a message export.

    Args:
        path (str): The path to the message export

    Returns:
        pl.DataFrame: A dataframe of dropped devices
    """

    return (
        pl.scan_csv(path)
        .groupby("DeviceID")
        .agg(
            [
                (pl.col("readings").max() == 8).alias("has_8_readings"),
                (pl.col("readings").min() == 8).alias("has_no_non_8_readings"),
                (pl.col("xtime").max() == 4).alias("has_4_xtime"),
                (pl.col("xtime").min() == 4).alias("has_no_non_4_xtime"),
            ]
        )
        .with_column(
            (~(pl.col("has_8_readings")) | ~(pl.col("has_4_xtime"))).alias("dropped")
        )
        .filter(
            ~(
                (pl.col("has_8_readings"))
                & (pl.col("has_no_non_8_readings"))
                & (pl.col("has_4_xtime"))
                & (pl.col("has_no_non_4_xtime"))
            )
        )
        .collect()
    )


def get_dropped_device_stats(
    df_dropped_devices, df_avg_usage, initial_device_count
) -> pl.DataFrame:
    """Get the number of dropped and reduced devices.

    Args:
        df_dropped_devices (pl.DataFrame): The dataframe of dropped devices

    Returns:
        pl.DataFrame: A dataframe containing stats about the dropped devices
    """
    df_post_analysis_stats = df_avg_usage.select(
        [
            pl.lit("joiner"),
            pl.lit(initial_device_count, pl.Int64).alias("initial_devices"),
            pl.count("device_id").alias("included_devices"),
            (initial_device_count - pl.count("device_id"))
            .alias("excluded_devices")
            .cast(pl.UInt32),
        ]
    )

    df_dropped_stats = df_dropped_devices.select(
        [
            pl.lit("joiner"),
            pl.col("dropped")
            .filter(pl.col("dropped"))
            .count()
            .alias("num_devices_dropped"),
            pl.col("dropped")
            .filter(~(pl.col("dropped")))
            .count()
            .alias("num_devices_reduced"),
        ]
    )

    return (
        df_post_analysis_stats.join(df_dropped_stats, on="literal")
        .select(
            [
                pl.all().exclude("literal"),
                ((pl.col("excluded_devices")) - (pl.col("num_devices_dropped"))).alias(
                    "very_low_count_excluded_devices"
                ),
            ]
        )
        .transpose(
            include_header=True,
            header_name="dropped_stat",
            column_names=["value"],
        )
    )

def get_usage_stats(df_avg_usage) -> pl.DataFrame:
    """Perform basic statistical analysis on a dataframe of average daily flow.

    Args:
        df_avg_usage (pl.DataFrame): A dataframe of average daily flow
        initial_device_count (int): The number of devices in original message data
    """
    return df_avg_usage.select(
        [
            pl.col("clipped")
            .filter(pl.col("clipped") == True)
            .count()
            .alias("clipped_devices"),
            pl.avg("avg_daily_flow"),
            pl.max("avg_daily_flow").alias("max_daily_flow"),
            pl.min("avg_daily_flow").alias("min_daily_flow"),
            pl.median("avg_daily_flow").alias("median_daily_flow"),
        ]
    ).transpose(
        include_header=True,
        header_name="usage_stat",
        column_names=["value"],
    )
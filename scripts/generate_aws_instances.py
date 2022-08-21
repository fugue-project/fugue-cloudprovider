import json

import pandas as pd


def normalize_name(name):
    return (
        name.lower()
        .replace(" ", "_")
        .replace("-", "_")
        .replace("gib", "gb")
        .replace("(", "")
        .replace(")", "")
    )


def normalize_price(price):
    if isinstance(price, (float, int)):
        return float(price)
    if pd.isna(price):
        return float("nan")
    if isinstance(price, str):
        p = price.split(" ")
        assert p[-1].lower() == "hour"
        return float(p[0])
    raise ValueError(price)


def to_list(item):
    if pd.isna(item) or item == "-" or item == "":
        return []
    return [x.strip() for x in item.split(",")]


def to_int_list(item):
    return [int(x) for x in to_list(item)]


def convert_cols(df, cols, default_value):
    return df.assign(
        **{
            x: df[x].replace("-", default_value).astype(type(default_value))
            for x in cols
        }
    )


raw_df = pd.read_csv("data/aws_ec2_instances.csv")
ncols = [normalize_name(x) for x in raw_df.columns]
raw_df.columns = ncols

raw_df["memory_gb"] = raw_df["memory_gb"].astype(float)
raw_df = convert_cols(
    raw_df,
    [
        "local_instance_storage",
        "ipv6_support",
        "auto_recovery_support",
        "dedicated_host_support",
        "on_demand_hibernation_support",
        "burstable_performance_support",
    ],
    False,
)
raw_df = convert_cols(
    raw_df, ["gpus", "storage_disk_count", "ipv6_addresses_per_interface", "fpgas"], 0
)
raw_df = convert_cols(raw_df, ["storage_gb"], 0.0)
raw_df = convert_cols(raw_df, ["storage_type"], "")
raw_df["on_demand_linux_pricing"] = raw_df["on_demand_linux_pricing"].apply(
    normalize_price
)
raw_df["on_demand_windows_pricing"] = raw_df["on_demand_windows_pricing"].apply(
    normalize_price
)

raw_df["availability_zones"] = raw_df["availability_zones"].apply(to_list)
raw_df["supported_placement_group_strategies"] = raw_df[
    "supported_placement_group_strategies"
].apply(to_list)
raw_df["valid_cores"] = raw_df["valid_cores"].apply(to_int_list)
raw_df["valid_threads_per_core"] = raw_df["valid_threads_per_core"].apply(to_int_list)


data = json.dumps(raw_df.sort_values("instance_type").to_dict("records"), indent=4)
data = (
    data.replace(": false", ": False")
    .replace(": true", ": True")
    .replace(": NaN", ': float("nan")')
)

with open("fugue_aws/_instances_data.py", "w") as w:
    w.write(
        "# flake8: noqa\nimport pandas as pd\n\n"
        f"AWS_INSTANCE_TYPES = pd.DataFrame({data})\n"
    )

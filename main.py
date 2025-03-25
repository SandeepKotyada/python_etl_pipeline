import pandas as pd
import pymysql
from sqlalchemy import create_engine
import yaml
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    filename="error_log_file/error.log",
    level=logging.ERROR,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

def load_credentials(filepath):
    """Load database credentials from YAML file."""
    try:
        with open(filepath, "r") as file:
            return yaml.safe_load(file)
    except Exception as e:
        logging.error(f"Error loading credentials: {e}")
        raise

def load_data(filepath, dtype, sep=','):
    """Load dataset with error handling."""
    try:
        return pd.read_csv(filepath, dtype=dtype, sep=sep)
    except Exception as e:
        logging.error(f"Error loading {filepath}: {e}")
        raise

def classify_category(value):
    """Classify power consumption into categories based on thresholds."""
    try:
        MIN_POWER = 3000
        MAX_POWER = 5000
        if value < MIN_POWER:
            return f"Power under {MIN_POWER} kW"
        elif MIN_POWER <= value <= MAX_POWER:
            return f"Power between {MIN_POWER} kW and {MAX_POWER} kW"
        else:
            return f"Power over {MAX_POWER} kW"
    except Exception as e:
        logging.error(f"Error classifying power category: {e}")
        return "Unknown"

def indicators_table(merged_df, has_solar):
    """Generate aggregated indicators table grouped by zipcode, year, month, and power category."""
    try:
        indicators_df = (
            merged_df[merged_df["has_solar"] == has_solar]
            .assign(
                year=lambda df: pd.to_datetime(df["date"], errors='coerce').dt.year,
                month=lambda df: pd.to_datetime(df["date"], errors='coerce').dt.month
            )
            .groupby(["zipcode", "year", "month", "power_category"], as_index=False)
            .agg(
                maxTemperature=("temperature", "max"),
                minTemperature=("temperature", "min"),
                avgRelativeHumidity=("relative_humidity", "mean"),
                n=("contract_id", "count")
            )
        )

        # Format numerical values to two decimal places
        for col in ["maxTemperature", "minTemperature", "avgRelativeHumidity"]:
            indicators_df[col] = indicators_df[col].apply(lambda x: f"{x:.2f}")

        return indicators_df
    except Exception as e:
        logging.error(f"Error generating indicators table: {e}")
        raise

def write_to_mysql(creds, df, table_name):
    """Write DataFrame to MySQL table."""
    try:
        engine = create_engine(
            f"mysql+pymysql://{creds['data_warehouse']['user']}:"
            f"{creds['data_warehouse']['password']}@"
            f"{creds['data_warehouse']['host']}/"
            f"{creds['data_warehouse']['database']}"
        )
        df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
        
        return f"{table_name} table created with {df.shape[0]} records." 
    except Exception as e:
        logging.error(f"Error writing to MySQL table {table_name}: {e}")
        raise

# Load credentials
creds = load_credentials("sql_creds/mysql_conn.yaml")

# Load datasets from csv files using load_data function.
contract_df = load_data("data/contracts_eae.csv", dtype={"ZIPCODE": str})
meteo_df = load_data("data/meteo_eae.csv", dtype={"zipcode": str}, sep=';')
zipcode_df = load_data("data/zipcode_eae_v2.csv", dtype={"ZIPCODE": str})

# Normalize column names to maintain consistency across all dataframes.
for df in [contract_df, meteo_df, zipcode_df]:
    df.columns = df.columns.str.lower()

# Identify zip codes with more than 10 contracts
zip_counts = contract_df["zipcode"].value_counts()
valid_zipcodes = set(zip_counts[zip_counts > 10].index)

# Filter datasets
contracts_filtered_df = contract_df[contract_df["zipcode"].isin(valid_zipcodes)]
meteo_filtered_df = meteo_df[meteo_df["zipcode"].isin(valid_zipcodes)]

# Apply power category classification
contracts_filtered_df = contracts_filtered_df.copy()
contracts_filtered_df["power_category"] = contracts_filtered_df["power_p1"].apply(classify_category)

# Filter contracts for client type ID 0
contracts_filtered_df = contracts_filtered_df[contracts_filtered_df["client_type_id"] == 0]

# Merge data
merged_df = meteo_filtered_df.merge(contracts_filtered_df, on="zipcode", how="left")

# Generate KPI dataframes
solar_customers_df = indicators_table(merged_df, 1)
non_solar_customers_df = indicators_table(merged_df, 0)

# Write to MySQL
rows_solar = write_to_mysql(creds, solar_customers_df, "kpi_solar_customers")
print(rows_solar)
rows_non_solar = write_to_mysql(creds, non_solar_customers_df, "kpi_non_solar_customers")
print(rows_non_solar)



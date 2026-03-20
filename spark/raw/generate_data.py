from random import uniform

from spark.spark_session import get_spark_session

COUNTRIES = ["ES", "PT", "FR", "GB", "IR", "DE"]
PRODUCTION_TYPE = ["Solar", "Wind", "Hydro", "Nuclear", "Gas", "Coal", "Oil"]

def generate_data(execution_date: str):
    
    spark = get_spark_session()
    
    data = []

    year = execution_date[:4]
    month = execution_date[5:7]
    day = execution_date[8:10]


    for hour in range(24):
        for country in COUNTRIES:
            for p_type in PRODUCTION_TYPE:
                timestamp = f"{execution_date} {hour:02d}:00:00"
                actual_load_mw = uniform(500.0, 8000.0)
                forecast_load_mw = actual_load_mw * uniform (0.95, 1.05) # ±5% deviation

                data.append({
                    "datetime":timestamp,
                    "country_code":country,
                    "production_type":p_type,
                    "actual_load_mw":actual_load_mw,
                    "forecast_load_mw":forecast_load_mw,
                    "year":year,
                    "month":month,
                    "day":day
                })

    df = spark.createDataFrame(data)
    
    return df
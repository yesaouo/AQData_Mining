import pandas as pd

def process_data(input_file):
    output_file = input_file.replace(".csv", "_cleaned.csv")
    df = pd.read_csv(input_file, encoding='utf-8-sig')
    columns_to_check = ["so2", "co", "o3", "o3_8hr", "pm10", "pm2.5", "no2", "nox", "no", "windspeed"]
    df[columns_to_check] = df[columns_to_check].apply(pd.to_numeric, errors='coerce')
    df_clean = df.dropna(subset=columns_to_check)
    df_clean.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"處理完成，已儲存至 {output_file}")

process_data("air_quality_data\\20250324_20250330.csv")
process_data("air_quality_data\\20250406_20250408.csv")
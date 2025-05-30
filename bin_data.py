import pandas as pd
import numpy as np

def process_data(input_file):
    output_file = input_file.replace("_cleaned.csv", "_binned.csv")
    df = pd.read_csv(input_file, encoding='utf-8-sig')

    # 對污染物欄位做等距三組分箱
    pollutants = ['so2', 'co', 'o3', 'o3_8hr', 'pm10', 'pm2.5', 'no2', 'nox', 'no']
    pollutant_labels = ['低', '中', '高']
    for col in pollutants:
        bins = np.linspace(df[col].min(), df[col].max(), num=4)
        df[f'{col}_level'] = pd.cut(
            df[col],
            bins=bins,
            labels=pollutant_labels,
            include_lowest=True
        )

    # 對風速欄位做等距三組分箱
    ws_col = 'windspeed'
    windspeed_labels = ['弱', '中', '強']
    bins_ws = np.linspace(df[ws_col].min(), df[ws_col].max(), num=4)
    df[f'{ws_col}_level'] = pd.cut(
        df[ws_col],
        bins=bins_ws,
        labels=windspeed_labels,
        include_lowest=True
    )

    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"處理完成，已儲存至 {output_file}")

process_data("air_quality_data\\20250324_20250330_cleaned.csv")
process_data("air_quality_data\\20250406_20250408_cleaned.csv")
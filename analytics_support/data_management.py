import logging
import pandas as pd
import numpy as np
from typing import List
from pathlib import Path


def resampling_data(df: pd.DataFrame, change_column_name: str, calculate: str = "not") -> pd.DataFrame:
    """
    데이터프레임의 Index를 설정하거나 요청에 따라 집계 및/또는 이름을 변경합니다.
    :param df: 처리할 데이터프레임
    :param change_column_name: 값 컬럼의 새로운 이름
    :param calculate: 사용할 집계 함수('sum', 'mean' 등) or 'not'일 경우 이름만 변경
    :return: 재집계 및/또는 이름이 변경된 데이터프레임
    """
    if not pd.api.types.is_datetime64_any_dtype(df.index):
        df["time"] = pd.to_datetime(df["time"])
        df.set_index("time", inplace=True)

    if calculate != "not":
        result = df.resample("H").agg({"value": calculate}).rename(columns={"value": change_column_name})
    else:
        result = df.rename(columns={"value": change_column_name})

    result.bfill(inplace=True)

    result = result.round(3)

    return result


def replace_iqr_outliers(df: pd.DataFrame) -> pd.DataFrame:
    """
    데이터프레임의 모든 수치형 컬럼에 대해 IQR 방식으로 이상치를 찾아 상하한 값으로 대체합니다.
    :param df: 이상치를 제거할 데이터프레임
    :return: 이상치가 제거 또는 대체된 데이터프레임
    """
    cleaned_df = df.copy()

    for column in cleaned_df.select_dtypes(include=["float64", "int64"]):
        Q1 = cleaned_df[column].quantile(0.25)
        Q3 = cleaned_df[column].quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR

        cleaned_df[column] = np.where(cleaned_df[column] < lower_bound, lower_bound, cleaned_df[column])
        cleaned_df[column] = np.where(cleaned_df[column] > upper_bound, upper_bound, cleaned_df[column])

    return cleaned_df


def merge_dataframes(df_list: List[pd.DataFrame]) -> pd.DataFrame:
    """
    주어진 리스트의 데이터프레임을 순차적으로 내부 조인 방식으로 병합합니다.
    :param df_list: 병합할 데이터프레임의 리스트
    :return: 'time' 컬럼을 기준으로 내부 조인된 결과 데이터프레임
    """
    merged_df = df_list[0]
    for df in df_list[1:]:
        merged_df = pd.merge(merged_df, df, on="time", how="inner", validate="1:1")

    return merged_df


def update_csv(csv_path, new_data):
    """
    기존 CSV 파일을 업데이트합니다.
    :param csv_path: 업데이트할 CSV 파일의 경로
    :param new_data: 새로 추가할 데이터 (DataFrame 형식)
    """
    # 기존 데이터 로드, `_time`을 인덱스로 설정
    if Path(csv_path).exists():
        existing_data = pd.read_csv(csv_path, index_col='time', parse_dates=True)
    else:
        existing_data = pd.DataFrame()

    # 새 데이터와 기존 데이터 병합
    updated_data = pd.concat([existing_data, new_data], axis=0)

    # 중복 인덱스 제거, 최신 데이터 유지
    updated_data = updated_data[~updated_data.index.duplicated(keep='last')]

    # 파일에 저장
    updated_data.to_csv(csv_path)


def training_data_patch(dataframes, csv_ptah):

    # 데이터 전처리(집계)
    power_socket_df = resampling_data(dataframes["PowerSocketData"], "socket_power(Wh)", "sum")
    co2_df = resampling_data(dataframes["CO2Data"], "average_co2(ppm)")
    illumination_df = resampling_data(dataframes["IlluminationData"], "average_illumination(lux)")
    logging.info("데이터 전처리 완료")

    # 이상치 제거
    power_socket_df = replace_iqr_outliers(power_socket_df)
    co2_df = replace_iqr_outliers(co2_df)
    illumination_df = replace_iqr_outliers(illumination_df)
    logging.info("데이터 이상치 제거 완료")

    # CSV 최신화
    training_set_df = merge_dataframes([power_socket_df, co2_df, illumination_df])
    update_csv(csv_ptah, training_set_df)
    logging.info("CSV 파일이 업데이트 되었습니다.")
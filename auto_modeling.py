import sys
import pytz
import logging
import pandas as pd
from pathlib import Path
from datetime import datetime
from analytics_support.database import InfluxDBManager
from analytics_support.config_loader import load_config
from analytics_support.data_management import training_data_patch
from analytics_support.modeling import modeling, generate_predictions


# 상수 정의
CONFIG_PATH = "resources/influxdb_config.yaml"
CSV_PATH = "resources/training_data/sensor_data.csv"
DEFAULT_START_DATE = "2024-04-16"
TARGET = "socket_power(Wh)"
MODEL_PATH = "resources/model/final_model"
EXOG_VARS = ['average_co2(ppm)', 'average_illumination(lux)']

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def check_to_start_date(file_path: str) -> datetime:
    """
    쿼리 조회 시작 날짜를 구하기 위해 CSV의 time컬럼을 읽어
    마지막 날짜를 UTC로 변환하여 반환합니다.

    만약 CSV가 없다면 기본 시작 날짜를 사용합니다.
    :param file_path: CSV 파일 경로
    :return: 쿼리 조회 시작 날짜
    """
    path = Path(file_path)

    # 파일 존재 여부 확인
    if not path.exists():
        # 문자열에서 datetime 객체로 변환하고, 시간대를 지정
        start_time_kst = pytz.timezone("Asia/Seoul").localize(datetime.strptime(DEFAULT_START_DATE, "%Y-%m-%d"))
        return start_time_kst.astimezone(pytz.utc)

    try:
        df = pd.read_csv(file_path)
        # 'time' 컬럼의 마지막 행의 데이터를 추출
        last_date = df['time'].iloc[-1]
        last_date_kst = pd.to_datetime(last_date).tz_localize(pytz.timezone("Asia/Seoul"))
        return last_date_kst.astimezone(pytz.utc)
    except Exception as e:
        logging.info(f"에러 발생: {e}")
        sys.exit(1)


def check_to_end_date() -> datetime:
    """
    실행 시점의 날짜를 기준으로 전날의 날짜를 계산 하여 UTC로 변환합니다.
    :param timezone: 타임존 정보를 나타내는 pytz의 타임존 객체
    :return: UTC 시간대로 변환한 datetime
    """
    today_kst = datetime.now(pytz.timezone("Asia/Seoul"))

    end_time_kst = pytz.timezone("Asia/Seoul").localize(datetime(today_kst.year, today_kst.month, today_kst.day, 0, 0, 0))

    return end_time_kst.astimezone(pytz.utc)


start_time_utc = check_to_start_date(CSV_PATH)
end_time_utc = check_to_end_date()

# 콘센트 전력(W) 조회
query_power_socket_data = f'''
import "experimental"
from(bucket: "powermetrics_data")
  |> range(start: {start_time_utc.strftime('%Y-%m-%dT%H:%M:%SZ')}, stop: {end_time_utc.strftime('%Y-%m-%dT%H:%M:%SZ')})
  |> filter(fn: (r) => r["phase"] == "total")
  |> filter(fn: (r) => r["description"] == "w")
  |> filter(fn: (r) => r["place"] == "office")
  |> filter(fn: (r) => r["location"] == "class_a_floor_heating_1" or r["location"] == "class_a_floor_heating_2")
  |> aggregateWindow(every: 1m, fn: last, createEmpty: false)
  |> map(fn: (r) => ({{r with _time: experimental.addDuration(d: 9h, to: r._time)}}))
  |> keep(columns: ["_time", "_value"])
'''

# 이산화탄소 조회 Flux 쿼리
query_co2_data = f'''
import "experimental"
from(bucket: "environmentalsensors_data")
  |> range(start: {start_time_utc.strftime('%Y-%m-%dT%H:%M:%SZ')}, stop: {end_time_utc.strftime('%Y-%m-%dT%H:%M:%SZ')})
  |> filter(fn: (r) => r["place"] == "class_a")
  |> filter(fn: (r) => r["measurement"] == "co2")
  |> aggregateWindow(every: 1h, fn: mean, createEmpty: false)
  |> map(fn: (r) => ({{r with _time: experimental.addDuration(d: 9h, to: r._time)}}))
  |> keep(columns: ["_time", "_value"])
'''

# 조도 조회 Flux 쿼리
query_illumination_data = f'''
import "experimental"
from(bucket: "environmentalsensors_data")
  |> range(start: {start_time_utc.strftime('%Y-%m-%dT%H:%M:%SZ')}, stop: {end_time_utc.strftime('%Y-%m-%dT%H:%M:%SZ')})
  |> filter(fn: (r) => r["place"] == "class_a")
  |> filter(fn: (r) => r["measurement"] == "illumination")
  |> aggregateWindow(every: 1h, fn: mean, createEmpty: false)
  |> map(fn: (r) => ({{r with _time: experimental.addDuration(d: 9h, to: r._time)}}))
  |> keep(columns: ["_time", "_value"])
'''

flux_queries = {
    "PowerSocketData": query_power_socket_data,
    "CO2Data": query_co2_data,
    "IlluminationData": query_illumination_data
}

format_dict = {
    "measurement": "power_usage",
    "field": "socket_power",
    "bucket": "ai_service_data",
    "org": "smoothing"
}


def main():
    start_time = datetime.now()
    try:
        config = load_config(CONFIG_PATH)

        logging.info("조회 날짜 확인 (UTC): %s ~ %s", start_time_utc.date(), end_time_utc.date())
        if start_time_utc.date() != end_time_utc.date():
            logging.info("CSV를 최신화 합니다.")
            db_manager = InfluxDBManager(config["smoothing_influxdb"])
            dataframes = db_manager.queries_to_dataframes(flux_queries)
            db_manager.close()

            training_data_patch(dataframes, CSV_PATH)
        else:
            logging.info("CSV가 최신버전 입니다.")

        modeling(MODEL_PATH, CSV_PATH, TARGET)

        future_data = generate_predictions(MODEL_PATH, CSV_PATH, EXOG_VARS)

        db_manager = InfluxDBManager(config["smoothing_influxdb"])
        db_manager.write_data(future_data, format_dict)
        db_manager.close()

    except Exception as e:
        logging.error(f"모델링 실패: {e}")
        sys.exit(1)
    finally:
        end_time = datetime.now()
        elapsed_time = end_time - start_time
        logging.info(f"RunTime: {elapsed_time}")


if __name__ == "__main__":
    main()

import logging
import pandas as pd
from typing import Dict
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS


class InfluxDBManager:
    """
    InfluxDB 클라이언트 인스턴스를 생성합니다.
    :param config: InfluxDB 접속 정보
    :return: InfluxDB 클라이언트
    """
    def __init__(self, config: dict):
        self.client = InfluxDBClient(
            url=config["url"],
            token=config["token"],
            org=config["org"]
        )

    def queries_to_dataframes(self, queries_dict: Dict[str, str]) -> Dict[str, pd.DataFrame]:
        """
        Flux 쿼리를 사용하여 데이터를 조회하여 데이터프레임으로 만듭니다.
        :param queries_dict: Flux 쿼리 Dict[str, str]
        :return: Dict[str, pd.DataFrame]
        """
        dataframes = {}
        for key, query in queries_dict.items():
            result = self.client.query_api().query(query=query)
            results = []

            for table in result:
                for record in table.records:
                    results.append({
                        "time": record.get_time(),
                        "value": record.get_value()
                    })

            df = pd.DataFrame(results)
            df["time"] = df["time"].astype(str).str.replace(r"\+00:00$", "", regex=True)
            dataframes[key] = df

        logging.info("데이터 조회 완료")

        return dataframes

    def write_data(self, dataframe, format_dict):
        """
        DataFrame 데이터를 InfluxDB에 씁니다.
        """
        dataframe['time'] = pd.to_datetime(dataframe['time']).dt.tz_localize(None) - pd.Timedelta(hours=9)

        write_api = self.client.write_api(write_options=SYNCHRONOUS)
        # 데이터 프레임의 각 행을 데이터 포인트로 변환
        points = [
            Point(format_dict["point"])
            .field(format_dict["field"], row[format_dict["field"]])
            .time(row['time'], WritePrecision.NS)
            for index, row in dataframe.iterrows()
        ]
        write_api.write(bucket=format_dict["bucket"], org=format_dict["org"], record=points)
        logging.info("InfluxDB에 저장 완료")

    def close(self):
        logging.info("InfluxDBClient 정상 종료")
        self.client.close()

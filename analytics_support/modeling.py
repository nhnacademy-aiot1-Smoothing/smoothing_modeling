import os
import logging
import pandas as pd
from pycaret.time_series import TSForecastingExperiment

FUTURE_DATA_PATH = "resources/prediction_data/predictive_sensor_data.csv"

os.environ["PYCARET_CUSTOM_LOGGING_LEVEL"] = "CRITICAL"


def modeling(model_path, csv_path, target):
    try:
        logging.info("학습 데이터 로드")
        training_data = pd.read_csv(csv_path)

        training_data["time"] = pd.to_datetime(training_data["time"])

        exp_exo = TSForecastingExperiment()

        logging.info("모델링 시작")
        exp_exo.setup(
            data=training_data,
            target=target,
            index="time",
            fh=24,
            session_id=42,
            verbose=False
        )
        model_exo = exp_exo.create_model(
            "arima",
            order=(0, 1, 2),
            seasonal_order=(0, 1, 1, 24),
            verbose=False
        )

        # Finalize Model : 전체 데이터를 활용한 재학습
        final_model = exp_exo.finalize_model(model_exo)
        logging.info("모델 구현 완료 - Saving...")

        exp_exo.save_model(final_model, model_path)

    except Exception as e:
        logging.info(f"모델링 실패: {e}")


def generate_predictions(model_path, csv_path, exog_vars):
    try:
        logging.info("데이터 로드")
        training_data = pd.read_csv(csv_path)

        # STEP 1: 외생변수 각각에 대한 시계열 예측 수행
        exog_exps = []
        exog_models = []

        logging.info("예측 시작")
        for exog_var in exog_vars:
            # 외생변수에 대한 예측을 도출하기 위하여 시계열 실험 생성
            exog_exp = TSForecastingExperiment()
            exog_exp.setup(
                data=training_data[["time", exog_var]],
                target=exog_var,
                index="time",
                fh=24,
                session_id=42,
                verbose=False,
            )
            best = exog_exp.create_model(
                "arima",
                order=(0, 1, 2),
                seasonal_order=(1, 1, 1, 24),
                verbose=False
            )

            final_exog_model = exog_exp.finalize_model(best)
            exog_exps.append(exog_exp)
            exog_models.append(final_exog_model)

        # STEP 2: 외생 변수에 대한 미래 예측 얻기
        future_exog = [
            exog_exp.predict_model(exog_model)
            for exog_exp, exog_model in zip(exog_exps, exog_models)
        ]

        # 예측값 concat
        future_exog = pd.concat(future_exog, axis=1)
        future_exog.columns = exog_vars

        # 미래 예측용 시계열 실험 생성
        exp_future = TSForecastingExperiment()

        # 이전에 저장한 모델 로드
        final_model = exp_future.load_model(model_path)

        # 모델의 Forecasting Horizon 과 예측으로 생성한 외생변수의 개수가 동일해야 함
        assert len(future_exog) == len(final_model.fh)

        # 예측
        future_data = exp_future.predict_model(
            final_model,  # 모델 입력
            X=future_exog,  # 외생변수 입력
        )
        logging.info("예측 완료")
        future_data.reset_index(inplace=True)
        future_data.columns = ['time', 'socket_power']
        future_data['time'] = future_data['time'].dt.to_timestamp()

        return future_data

    except Exception as e:
        logging.error(f"데이터 예측 문제 발생: {e}")
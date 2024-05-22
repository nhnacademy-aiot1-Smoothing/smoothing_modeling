import sys
import logging
import subprocess
import importlib.metadata


def install_package(package):
    try:
        # 패키지가 이미 설치되어 있는지 확인
        importlib.metadata.version(package)
        logging.info(f"{package}이(가) 이미 설치되어 있습니다.")
    except importlib.metadata.PackageNotFoundError:
        # 설치되어 있지 않다면 패키지를 설치
        try:
            logging.info(f"{package}을(를) 설치 중입니다...")
            subprocess.check_call([sys.executable, '-m', 'pip', 'install', package])
            logging.info(f"{package}이(가) 성공적으로 설치되었습니다.")
        except subprocess.CalledProcessError:
            logging.info(f"{package} 설치에 실패했습니다.")
        except Exception as e:
            logging.error(f"예기치 못한 오류가 발생했습니다: {str(e)}.")


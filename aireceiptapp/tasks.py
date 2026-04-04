import subprocess
import os
import sys
import traceback
import logging

logger = logging.getLogger(__name__)

def create_model_task(base_dir: str, **kwargs):
    """
    モデル作成の非同期処理を実行するタスク
    """
    try:
        proc_file = os.path.join(base_dir, 'aireceiptapp', 'run_preprocessing_model.py')
        args = ["python", proc_file]
        proc_model = subprocess.run(args)
    except:
        print(sys.exc_info())
        print(traceback.format_exc())


def predict_task(base_dir: str, **kwargs):
    """
    検知処理の非同期処理を実行するタスク
    """
    try:
        proc_file = os.path.join(base_dir, 'aireceiptapp', 'run_preprocessing_predict.py')
        args = ["python", proc_file]
        proc_model = subprocess.run(args)
    except:
        print(sys.exc_info())
        print(traceback.format_exc())
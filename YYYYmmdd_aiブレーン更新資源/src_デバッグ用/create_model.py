# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/20

import sys
import glob
import os
import traceback
from logging import config, getLogger
from src.common import config_reader, utils
from src.common.log_writer import output_init
from src.common.check_file_encode import check_files
from src.model.m1_receipt_processing import m1_main
from src.model.m2_receipt_processing_option import m2_main
from src.model.m3_medical_record_processing import m3_main
from src.model.m4_medical_record_processing_option import m4_main
from src.model.m5_chi_square import m5_main
from src.model.m6_feature_creation import m6_main
from src.model.m7_feature_creation_option import m7_main
from src.model.m8_model_creation import m8_main

config_base = "../config/"

try:
    print(config_base + "logging.conf")
    config.fileConfig(config_base + "logging.conf")
    logger = getLogger(__name__)
except:
    import traceback
    print(traceback.format_exc())
    print("ログファイルの作成に失敗しました。", file=sys.stderr)
    exit(1)


def create_main(args):
    logwriter = None

    # 予測の場合1を指定
    model_flag = 1

    try:
        target_base = os.path.join(os.path.dirname(__file__))
        message_base = "../config/"
        logwriter = output_init(message_base, logger)
        config_base = "../data/config/"

        l = lambda x:x[0](x[1]); l(logwriter.format_log_message("AIR001001", ["診療報酬算定漏れ検知モデル作成API"]))

        # パス読み込み
        l = lambda x: x[0](x[1]); l(logwriter.format_log_message("AIR001003", ["電子カルテデータ加工処理"]))
        if len(args) != 4:
            l = lambda x:x[0](x[1]); l(logwriter.format_log_message("AIR003001", [args]))
            raise Exception
        l = lambda x: x[0](x[1]); l(logwriter.format_log_message("AIR001005", [",".join(args[1:])]))
        receipt_path = args[1] + "/"
        if not os.path.isdir(receipt_path):
            l = lambda x:x[0](x[1]); l(logwriter.format_log_message("AIR003008", [receipt_path + " folder not found"]))  # フォルダーが存在しないエラー
            raise Exception
        # ファイルのエンコードチェック
        unread_file_list = check_files(receipt_path)
        for warn_tuple in unread_file_list:
            l = lambda x:x[0](x[1]); l(logwriter.format_log_message("AIR002003", warn_tuple))

        karte_path = args[2] + "/"
        # フォルダの有無チェック
        if not os.path.isdir(karte_path):
            l = lambda x:x[0](x[1]); l(logwriter.format_log_message("AIR003008", [karte_path + " folder not found"]))  # フォルダーが存在しないエラー
            raise Exception
        # フォルダ配下のファイルの有無チェック
        karte_file_list = glob.glob(karte_path + '/*.csv')
        if len(karte_file_list) == 0:
            l = lambda x: x[0](x[1]);
            l(logwriter.format_log_message("AIR003008", [karte_path + " file not found"]))  # ファイルが存在しないエラー
            raise Exception
        # ファイルのエンコードチェック
        unread_file_list = check_files(karte_path)
        for warn_tuple in unread_file_list:
            l = lambda x:x[0](x[1]); l(logwriter.format_log_message("AIR002003", warn_tuple))

        model_path = args[3] + "/"
        if not os.path.isdir(model_path):
            l = lambda x:x[0](x[1]); l(logwriter.format_log_message("AIR003008", [model_path+ " folder not found"]))  # フォルダーが存在しないエラー
            raise Exception

        # # remove work files
        work_path = target_base + "/../work/"
        # utils.clear_work_directory(work_path, logwriter)

        # レセプト読み込み
        m1_conf_path = config_base + "/m1_receipt_processing.json"
        if not os.path.exists(m1_conf_path):
            l = lambda x: x[0](x[1]); l(logwriter.format_log_message("AIR003008", [m1_conf_path+ " (file not found)"]))
            raise Exception
        config = config_reader.read_config(m1_conf_path)
        m1_main(config, logwriter, receipt_path, model_flag, model_path, karte_path)

        # レセプト個別処理
        m2_conf_path = config_base + "/m2_receipt_processing_option.json"
        if not os.path.exists(m2_conf_path):
            l = lambda x: x[0](x[1]); l(logwriter.format_log_message("AIR003008", [m2_conf_path+ " (file not found)"]))
            raise Exception
        config = config_reader.read_config(m2_conf_path)
        m2_main(config, logwriter, model_path)

        # 電子カルテ結合
        m3_conf_path = config_base + "/m3_medical_record_processing.json"
        if not os.path.exists(m3_conf_path):
            l = lambda x: x[0](x[1]); l(logwriter.format_log_message("AIR003008", [m3_conf_path+ " (file not found)"]))
            raise Exception
        config = config_reader.read_config(m3_conf_path)
        m3_main(config, logwriter, karte_path)

        # 前処理
        m4_conf_path = config_base + "/m4_medical_record_processing_option.json"
        if not os.path.exists(m4_conf_path):
            l = lambda x: x[0](x[1]); l(logwriter.format_log_message("AIR003008", [m4_conf_path+ " (file not found)"]))
            raise Exception
        config = config_reader.read_config(m4_conf_path)
        m4_main(config, logwriter, model_flag)

        # X二乗
        m5_conf_path = config_base + "/m5_chi_square.json"
        if not os.path.exists(m5_conf_path):
            l = lambda x: x[0](x[1]); l(logwriter.format_log_message("AIR003008", [m5_conf_path+ " (file not found)"]))
            raise Exception
        config = config_reader.read_config(m5_conf_path)
        m5_main(config, logwriter, model_path, model_flag)

        # 特徴量作成
        m6_conf_path = config_base + "/m6_feature_creation.json"
        if not os.path.exists(m6_conf_path):
            l = lambda x: x[0](x[1]); l(logwriter.format_log_message("AIR003008", [m6_conf_path+ " (file not found)"]))
            raise Exception
        config = config_reader.read_config(m6_conf_path)
        m6_main(config, logwriter, model_path, model_flag)

        # 個別対応 特徴量作成
        m7_conf_path = config_base + "/m7_feature_creation_option.json"
        if not os.path.exists(m7_conf_path):
            l = lambda x: x[0](x[1]); l(logwriter.format_log_message("AIR003008", [m7_conf_path+ " (file not found)"]))
            raise Exception
        config = config_reader.read_config(m7_conf_path)
        m7_main(config, logwriter, model_path, model_flag)
        l = lambda x:x[0](x[1]); l(logwriter.format_log_message("AIR001004", ["電子カルテデータ加工処理"]))

        l = lambda x:x[0](x[1]); l(logwriter.format_log_message("AIR001003", ["診療報酬算定漏れ検知モデル作成"]))
        # モデル作成
        m8_conf_path = config_base + "/m8_model_creation.json"
        if not os.path.exists(m8_conf_path):
            l = lambda x: x[0](x[1]); l(logwriter.format_log_message("AIR003008", [m8_conf_path+ " (file not found)"]))
            raise Exception
        config = config_reader.read_config(m8_conf_path)
        m8_main(config, logwriter, model_path)
        l = lambda x:x[0](x[1]); l(logwriter.format_log_message("AIR001004", ["診療報酬算定漏れ検知モデル作成"]))

        # remove work files
        # utils.clear_work_directory(work_path, logwriter)

        l = lambda x:x[0](x[1]); l(logwriter.format_log_message("AIR001002", ["診療報酬算定漏れ検知モデル作成API"]))


    except KeyboardInterrupt:
        l = lambda x:x[0](x[1]); l(logwriter.format_log_message("AIR001007", ["診療報酬算定漏れ検知モデル作成API"]))
        exit(1)
    except:
        logger.error(sys.exc_info())
        logger.error(traceback.format_exc())
        if logwriter is not None:
            try:
                l = lambda x:x[0](x[1]); l(logwriter.format_log_message("AIR001002", ["診療報酬算定漏れ検知モデル作成API"]))
                l = lambda x: x[0](x[1]); l(logwriter.format_log_message("AIR001006", ["1"]))
            except:
                pass
        else:
            logger.error("内部エラーが発生しました。")
        # 異常系終了
        exit(1)

    l = lambda x: x[0](x[1]); l(logwriter.format_log_message("AIR001006", ["0"]))
    # 正常系終了
    exit(0)


# 検証用
if __name__ == "__main__":
    args = sys.argv
    create_main(args)

# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/30

import sys
import traceback
import os
import glob
from logging import config, getLogger
from src.common import config_reader, utils
from src.common.check_file_encode import check_files
from src.common.log_writer import output_init
from src.predict.p1_receipt_processing import p1_main
from src.predict.r1_rule_execution import r1_main
from src.predict.p2_receipt_processing_option import p2_main
from src.model.m3_medical_record_processing import m3_main
from src.model.m4_medical_record_processing_option import m4_main
from src.predict.p5_feature_creation_preprocessing import p5_main
from src.model.m6_feature_creation import m6_main
from src.model.m7_feature_creation_option import m7_main
from src.predict.p8_predictive_execution import p8_main
from src.predict.p9_result_output import p9_main


config_base = "../config/"

try:
    config.fileConfig(config_base + "logging.conf")
    logger = getLogger(__name__)
except:
    print("ログファイルの作成に失敗しました。", file=sys.stderr)
    exit(1)


def create_main(args):
    logwriter = None

    try:
        # 予測の場合0を指定
        predict_flag = 0

        message_base = "../config/"
        logwriter = output_init(message_base, logger)
        config_base = "../data/config/"

        l = lambda x: x[0](x[1]);
        l(logwriter.format_log_message("AIR001001", ["診療報酬算定漏れ検知API"]))

        # パス読み込み
        l = lambda x: x[0](x[1]);
        l(logwriter.format_log_message("AIR001003", ["電子カルテデータ加工処理"]))
        if len(args) != 6:
            l = lambda x: x[0](x[1]);
            l(logwriter.format_log_message("AIR003001", [args]))
            raise Exception
        receipt_path = args[1] + "/"
        if not os.path.isdir(receipt_path):
            l = lambda x: x[0](x[1]);
            l(logwriter.format_log_message("AIR003008", [receipt_path + " (directory not found)"]))  # フォルダーが存在しないエラー
            raise Exception
        # ファイルのエンコードチェック
        unread_file_list = check_files(receipt_path)
        for warn_tuple in unread_file_list:
            l = lambda x: x[0](x[1]);
            l(logwriter.format_log_message("AIR002003", warn_tuple))

        karte_path = args[2] + "/"
        # フォルダの有無チェック
        if not os.path.isdir(karte_path):
            l = lambda x: x[0](x[1]);
            l(logwriter.format_log_message("AIR003008", [karte_path + " (directory not found)"]))  # フォルダーが存在しないエラー
            raise Exception
        # フォルダ配下のファイルの有無チェック
        karte_file_list = glob.glob(karte_path + '/*.csv')
        if len(karte_file_list) == 0:
            l = lambda x: x[0](x[1]);
            l(logwriter.format_log_message("AIR003008", [karte_path + " (file not found)"]))  # ファイルが存在しないエラー
            raise Exception
        # ファイルのエンコードチェック
        unread_file_list = check_files(karte_path)
        for warn_tuple in unread_file_list:
            l = lambda x: x[0](x[1]);
            l(logwriter.format_log_message("AIR002003", warn_tuple))

        model_path = args[3] + "/"
        if not os.path.isdir(model_path):
            l = lambda x: x[0](x[1]);
            l(logwriter.format_log_message("AIR003008", [model_path + " (directory not found)"]))  # フォルダーが存在しないエラー
            raise Exception

        out_path = args[4] + "/"
        if not os.path.isdir(out_path):
            l = lambda x: x[0](x[1]);
            l(logwriter.format_log_message("AIR003009", [out_path + " (directory not found)"]))  # フォルダーが存在しないエラー
            raise Exception

        _in_targets = args[5]
        if not type(_in_targets) is str:
            l = lambda x: x[0](x[1]);
            l(logwriter.format_log_message("AIR003001", [_in_targets]))  # 型が str でないエラー
            raise Exception

        # 読み込んだ_in_targetsの文字列をlistに変換
        in_targets = _in_targets.replace(" ", "").split(",")

        # 文字列の長さチェック
        check_list = []
        for in_target in in_targets:
            if len(in_target) > 256:
                check_list.append(in_target)
        if len(check_list) > 0:
            for erre_code in check_list:
                l = lambda x: x[0](x[1]);
                l(logwriter.format_log_message("AIR003001", [erre_code]))  # 文字列の長さが256文字より大きい（引数エラー）
            raise Exception

        l = lambda x: x[0](x[1]); l(logwriter.format_log_message("AIR001005", [",".join(args[1:])]))

        # r1_json_targets のtargetsがあれば、 in_targetsからいったん削除
        r1_conf_path = config_base + "/r1_rule_execution.json"
        if not os.path.exists(r1_conf_path):
            l = lambda x: x[0](x[1]); l(logwriter.format_log_message("AIR003008", [r1_conf_path + " (file not found)"]))
            raise Exception
        r_config = config_reader.read_config(r1_conf_path)
        r_targets = []
        for key, params in sorted(r_config.items()):
            if "target" in params:
                r_targets.append(params["target"])
            if "targets" in params:
                r_targets.extend(params["targets"])
        r_in_targets = list(set(in_targets) & set(r_targets))  # ルールの算定項目
        drop_r_in_targets = list(set(in_targets) - set(r_in_targets))  # ルール以外の算定項目

        # ルール以外の算定項目のモデル有無チェック
        check_list = []
        for in_target in drop_r_in_targets:
            if not os.path.isfile(model_path + '/model/' + in_target + ".pickle"):
                check_list.append(in_target)
        if len(check_list) > 0:
            for erre_code in check_list:
                l = lambda x: x[0](x[1]);
                l(logwriter.format_log_message("AIR003008", [erre_code+ " (modelfile not found)"]))  # 引数の算定コードのモデルが存在しない場合はエラー
            raise Exception

        # drop_r_in_targetsのチェックが完了したら、r_in_targetsと結合
        in_targets = drop_r_in_targets + r_in_targets

        # remove work files
        work_path = "../work/"
        utils.clear_work_directory(work_path, logwriter)

        # レセプト読み込み
        p1_conf_path = config_base + "/p1_receipt_processing.json"
        if not os.path.exists(p1_conf_path):
            l = lambda x: x[0](x[1]); l(logwriter.format_log_message("AIR003008", [p1_conf_path+ " (file not found)"]))
            raise Exception
        config = config_reader.read_config(p1_conf_path)
        p1_main(config, logwriter, receipt_path, predict_flag)
        
        # ルール
        r1_main(r_config, logwriter, karte_path, model_path, out_path, in_targets)

        # drop_r_in_targets がある場合処理続行、ない場合はルールの処理終了後に正常終了
        if len(drop_r_in_targets) != 0:

            # レセプト個別処理
            p2_conf_path = config_base + "/p2_receipt_processing_option.json"
            if not os.path.exists(p2_conf_path):
                l = lambda x: x[0](x[1]); l(logwriter.format_log_message("AIR003008", [p2_conf_path+ " (file not found)"]))
                raise Exception
            config = config_reader.read_config(p2_conf_path)
            p2_main(config, logwriter, model_path, in_targets)
            
            # 電子カルテ結合
            p3_conf_path = config_base + "/p3_medical_record_processing.json"
            if not os.path.exists(p3_conf_path):
                l = lambda x: x[0](x[1]); l(logwriter.format_log_message("AIR003008", [p3_conf_path + " (file not found)"]))
                raise Exception
            config = config_reader.read_config(p3_conf_path)
            m3_main(config, logwriter, karte_path)
            
            # 前処理
            p4_conf_path = config_base + "/p4_medical_record_processing_option.json"
            if not os.path.exists(p4_conf_path):
                l = lambda x: x[0](x[1]); l(logwriter.format_log_message("AIR003008", [p4_conf_path + " (file not found)"]))
                raise Exception
            config = config_reader.read_config(p4_conf_path)
            m4_main(config, logwriter, predict_flag)
            
            # 算定項目調整
            p5_conf_path = config_base + "/p5_feature_creation_preprocessing.json"
            if not os.path.exists(p5_conf_path):
                l = lambda x: x[0](x[1]); l(logwriter.format_log_message("AIR003008", [p5_conf_path + " (file not found)"]))
                raise Exception
            config = config_reader.read_config(p5_conf_path)
            p5_main(config, logwriter, predict_flag, in_targets)

            # 特徴量作成
            p6_conf_path = config_base + "/p6_feature_creation.json"
            if not os.path.exists(p6_conf_path):
                l = lambda x: x[0](x[1]); l(logwriter.format_log_message("AIR003008", [p6_conf_path + " (file not found)"]))
                raise Exception
            config = config_reader.read_config(p6_conf_path)
            m6_main(config, logwriter, model_path, predict_flag, in_targets)

            # 個別対応 特徴量作成
            p7_conf_path = config_base + "/p7_feature_creation_option.json"
            if not os.path.exists(p7_conf_path):
                l = lambda x: x[0](x[1]); l(logwriter.format_log_message("AIR003008", [p7_conf_path + " (file not found)"]))
                raise Exception
            config = config_reader.read_config(p7_conf_path)
            m7_main(config, logwriter, model_path, predict_flag, in_targets)
            l = lambda x: x[0](x[1]); l(logwriter.format_log_message("AIR001004", ["電子カルテデータ加工処理"]))

            # 予測実施
            l = lambda x: x[0](x[1]); l(logwriter.format_log_message("AIR001003", ["診療報酬算定漏れ検知処理"]))
            p8_conf_path = config_base + "/p8_predictive_execution.json"
            if not os.path.exists(p8_conf_path):
                l = lambda x: x[0](x[1]); l(logwriter.format_log_message("AIR003008", [p8_conf_path + " (file not found)"]))
                raise Exception
            config = config_reader.read_config(p8_conf_path)
            p8_main(config, logwriter, model_path, in_targets)

            # 結果出力
            p9_conf_path = config_base + "/p9_result_output.json"
            if not os.path.exists(p9_conf_path):
                l = lambda x: x[0](x[1]); l(logwriter.format_log_message("AIR003008", [p9_conf_path + " (file not found)"]))
                raise Exception
            config = config_reader.read_config(p9_conf_path)
            p9_main(config, logwriter, model_path, out_path, in_targets)

            l = lambda x: x[0](x[1]); l(logwriter.format_log_message("AIR001004", ["診療報酬算定漏れ検知処理"]))

        # remove work files
        #work_path = "../work/"
        #utils.clear_work_directory(work_path, logwriter)

        l = lambda x: x[0](x[1]);
        l(logwriter.format_log_message("AIR001002", ["診療報酬算定漏れ検知API"]))

    except KeyboardInterrupt:
        l = lambda x:x[0](x[1]); l(logwriter.format_log_message("AIR001007", ["診療報酬算定漏れ検知API"]))
        exit(1)
    except:
        logger.error(sys.exc_info())
        logger.error(traceback.format_exc())
        if logwriter is not None:
            try:
                l = lambda x: x[0](x[1]);
                l(logwriter.format_log_message("AIR001002", ["診療報酬算定漏れ検知API"]))
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


if __name__ == "__main__":
    args = sys.argv
    create_main(args)

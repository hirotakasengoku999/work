# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/30

from src.model.f0_data_conversion.s1_table_processing import call as m1  # 1
from src.model.f0_data_conversion.s2_header import call as m2  # 2
from src.model.f0_data_conversion.s3_header_dpc import call as m3  # 3
from src.model.f0_data_conversion.s3_medical_record_number_check import call as check_num  # 4

from src.model.f0_data_conversion.s4_cd_extraction import call as m4  # 5
from src.model.f0_data_conversion.s5_rese import call as m5  # 6
from src.model.f0_data_conversion.s6_sy_rese import call as m6  # 7
from src.model.f0_data_conversion.s7_last_month_sy import call as m7  # 8
from src.model.f0_data_conversion.s7_date_conversion import call as m8  # 9
from src.model.f5_individual_correspondence.s1_hospitalization_category import call as m9  # 10
from src.model.f5_individual_correspondence.f_date_of_hospitalization.s1_DPC_processing import call as m10  # 11
from src.model.f5_individual_correspondence.f_date_of_hospitalization.s1_IKA_processing import call as m11  # 12
from src.model.f5_individual_correspondence.f_date_of_hospitalization.s2_merging_DPC_and_IKA import call as m12  # 13
from src.model.f0_data_conversion.s8_merging_sy import call as m13  # 14
from src.model.f5_individual_correspondence.r_rule_info import call as m14  # 15


def m1_main(config, logger, input_path, model_flag, model_path, karte_path):
    # 処理は昇順に実行する
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001001", ["m1_receipt_processing"]))

    for key, params in config.items():
        try:
            if key == "1":
                m1(params, input_path, logger)
            elif key == "2":
                m2(params, logger)
            elif key == "3":
                m3(params, logger)
            elif key == "4":
                check_num(params, logger)
            elif key == "5":
                m8(params, logger)
            elif key == "6":
                m4(params, logger)
            elif key == "7":
                m5(params, logger)
            elif key == "8":
                m6(params, logger)
            elif key == "9":
                m7(params, logger, model_flag)
            elif key == "10":
                m9(params, logger)
            elif key == "11":
                m10(params, logger)
            elif key == "12":
                m11(params, logger)
            elif key == "13":
                m12(params, logger)
            elif key == "14":
                m13(params, logger)
            elif key == "15":
                m14(params, logger, model_path, karte_path)
            elif key == "_comment":
                pass
            else:
                # 固有処理の呼び出し
                exec(f"import {params['package']}; {params['package']}.{params['function']}({','.join(params['params'])})")
        except TypeError as te:
            l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003001", te.args))
            raise
        except OSError as ose:
            import traceback, sys
            l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003002", [ose.filename, traceback.format_exception(*sys.exc_info())[-1]]))
            raise
        except KeyError as ke:
            l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR003010", ke.args))
            raise
        except:
            l = lambda x:x[0](x[1]); l(logger.log_message("AIR003006"))
            raise
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001002", ["m1_receipt_processing"]))

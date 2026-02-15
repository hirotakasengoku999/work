# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/30

import traceback, sys
from src.model.f5_individual_correspondence.s_patient_age import call as age  # 1
from src.model.f5_individual_correspondence.s_public_expense_num import call as ko  # 3
from src.model.f5_individual_correspondence.s_malignant_tumor import call as malignant_tumor  # 4
from src.model.f5_individual_correspondence.s2_hospitalization_category import call as hospitalization_category  # 5
from src.model.f5_individual_correspondence.s_calculations_count import call as calculations_count  # 6


def m2_main(config, logger, heap_dir):
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001001", ["m2_receipt_processing_option"]))
    # 処理は昇順に実行する
    for key, params in sorted(config.items()):
        try:
            
            if key == "1":
                age(params, logger)
            elif key == "3":
                ko(params, logger)
            elif key == "4":
                malignant_tumor(params, logger)
            elif key == "5":
                hospitalization_category(params, logger)
            elif key == "6":
                calculations_count(params, logger, heap_dir)
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
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001002", ["m2_receipt_processing_option"]))

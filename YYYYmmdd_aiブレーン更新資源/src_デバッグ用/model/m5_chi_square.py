# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/30

import traceback, sys
from src.model.f5_individual_correspondence.f_emergency_medical_addition.s_by_category import call as t1  # 1
from src.model.f5_individual_correspondence.f_general_anesthesia_4.s_by_category import call as t2  # 2
from src.model.f4_chi_square.s1_chi_square_si import call as chi1  # 3
from src.model.f4_chi_square.s2_chi_square_not_si import call as chi2  # 4
from src.model.f4_chi_square.s3_chi_square_doc import call as chi3  # 5
from src.model.f4_chi_square.s4_chi_square_all_doc import call as chi4  # 6
from src.model.f4_chi_square.s5_chi_square_designated_doc import call as chi5  # 7
from src.model.f4_chi_square.s6_after_chi_squared_doc import call as chi6  # 8
from src.model.f4_chi_square.s7_after_chi_squared_add_del import call as add_del  # 9


def m5_main(config, logger, model_path, model_flagh):
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001001", ["m5_chi_square"]))
    for key, params in config.items():
        try:
            if key == "1":
                t1(params, logger, model_flagh)
            elif key == "2":
                t2(params, logger, model_flagh)
            elif key == "3":
                chi1(params, logger, model_path)
            elif key == "4":
                chi2(params, logger, model_path)
            elif key == "5":
                chi3(params, logger)
            elif key == "6":
                chi4(params, logger, model_path)
            elif key == "7":
                chi5(params, logger)
            elif key == "8":
                chi6(params, logger, model_path)
            elif key == "9":
                add_del(params, logger, model_path)
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
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001002", ["m5_chi_square"]))


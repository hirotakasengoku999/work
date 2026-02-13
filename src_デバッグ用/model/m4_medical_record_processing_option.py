# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/30

from src.model.f1_medical_record_processing.s5_orphological_analysis_processing import call as morphologic
from src.model.f1_medical_record_processing.s3_medical_record_processing import call as ecr
from src.model.f1_medical_record_processing.s4_title_processing import call as title
from src.model.f1_medical_record_processing.s6_surgical_anesthesia_processing import call as anesthesia

from src.model.f5_individual_correspondence.k_triage_level import call as triage
from src.model.f5_individual_correspondence.k_inspection_item import call as nipma
from src.model.f5_individual_correspondence.k_care_support_doc import call as guidancefee
from src.model.f5_individual_correspondence.s_JCS_level import call as jcscr


def m4_main(config, logger, flag):
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001001", ["m4_medical_record_processing_option"]))
    # 処理は昇順に実行する
    for key, params in sorted(config.items()):
        try:
            if key == "1":
                morphologic(params, logger, flag)
            elif key == "2":
                ecr(params, logger)
            elif key == "3":
                title(params, logger)
            elif key == "4":
                anesthesia(params, logger)
            elif key == "5":
                triage(params, logger)
            elif key == "6":
                nipma(params, logger)
            elif key == "7":
                guidancefee(params, logger)
            elif key == "8":
                jcscr(params, logger)
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
    l = lambda x:x[0](x[1]); l(logger.format_log_message("AIR001002", ["m4_medical_record_processing_option"]))

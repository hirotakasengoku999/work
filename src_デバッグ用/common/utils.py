# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/20
import os
import shutil
import datetime
import errno

def clear_work_directory(dir_path, logwriter):
    """
    workディレクトリ配可のディレクトリを削除する
    Parameters
    ----------
    dir_path: str
        workディレクトリのパス
    Returns
    -------

    """
    if os.path.isdir(dir_path):
        try:
            # shutil.rmtree時にタイムラグのために出力フォルダ作成でエラーが発生するため、
            # 一度renameする
            time = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
            backup_dir = dir_path[:-1] + time
            if os.path.isdir(backup_dir):
                time = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
                backup_dir = dir_path[:-1] + time
            os.rename(dir_path, backup_dir)
            os.makedirs(dir_path)
            shutil.rmtree(backup_dir)
        except Exception as ex:
            l = lambda x:x[0](x[1]); l(logwriter.format_log_message("AIR003006"))
            raise
    else:
        l = lambda x: x[0](x[1]);l(logwriter.format_log_message("AIR003008", [dir_path + " (directory not found)"]))
        raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), dir_path)
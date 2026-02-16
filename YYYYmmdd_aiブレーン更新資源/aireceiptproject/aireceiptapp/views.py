from django import forms
from django.shortcuts import render, redirect, get_object_or_404
from django.contrib.auth.decorators import login_required
from django.contrib.auth import authenticate, login, logout
from django.contrib.auth.models import User
from django.db import IntegrityError
from django.db.models import Max
from .models import HANYO, KENCHI, KARTE_FILE, KARTE_FILE_COLUMN, RESULT_COLUMN
from .forms import HanyoForm, ResultForm, KenchiForm, SystemLogSelectForm, UserListSelectForm, KarteFileForm, KarteFileFormColumn, ResultColumnForm, TestMulti, ChartSelect, AddItemAllForm
from . import folder, update_time, get_model_list, get_update_item, write_db_csv, proc_flg_update, check_validation, get_hanyo, paginator, move_dir, receipt_pre_chk, karte_pre_chk, get_uke_billing_yearmonth, get_kenchi, get_kenchikekka, karte_conversion, check_user, read_kartefile, regist_backup_result, get_request_amount, get_chart, get_inspectiondb, read_log, update_chart_app, update_results, remove_old_data
import sys
import os
import glob
import traceback
import subprocess
import pathlib
import datetime
import time
import platform
from logging import getLogger # ログ出力
import logging
from django.utils import timezone
from aireceiptapp.models import USERLOG
from dateutil.parser import parse
from django.http import HttpResponse
import shutil
import pandas as pd
import re
import json
from dateutil import relativedelta
from pathlib import Path

logger = logging.getLogger(__name__)
try:
  base = folder.base()
except:
  base = os.getcwd()
login_user = ""


# ログイン
def loginfunc(request):
  params = {
    'error':''
  }

  # ログイン画面の写真と病院名を汎用テーブルから取得する

  # 入力された利用者IDとパスワードをチェックする
  if request.method == 'POST':
    input_username = request.POST['userid']
    input_password = request.POST['password']
    user = authenticate(request, username=input_username, password=input_password)
    if user is not None:
      if check_user.get_active_user(input_username):
        login(request, user)
        client_addr = request.META.get('REMOTE_ADDR')
        logger.info(' ' + str(request.user) + 'がログインしました。 ' + 'ip=' + str(client_addr))
        return redirect(to='/indexview')
      else:
        params['error'] = 'この利用者は期限が切れています。'
    else:
      params['error'] = 'この利用者は登録されていません。'
  return render(request, 'login.html', params)

@login_required
def indexview(request):
  warning_message_list = []
  warning_message = get_hanyo.get_message()
  if warning_message:
    warning_message_list = [warning_message,'お手数ですが、担当者にご連絡ください']
    get_hanyo.delete_message()
  params = {
    'warning_message_list':warning_message_list,
    "login_user":request.user.first_name,
    "user_authority":get_hanyo.get_user_authority(request.user),
    "menu_info":get_hanyo.get_menu_info(),
  }
  return render(request, 'index.html',params)

# ログアウト
def logoutfunc(request):
  logger.info(' ' + str(request.user) + 'がログアウトしました。')
  logout(request)
  return redirect('login')

# モデル作成画面表示
def createmodel(request):
  os.chdir(base)
  proc_name = proc_flg_update.get_proc_name('model')

  # 処理中なら排他画面を返す
  active_proc_list, wait_proc_list = proc_flg_update.proc_status_now()
  if len(active_proc_list) > 0 or len(wait_proc_list) > 0:
    exclusion_messe = ""
    if len(active_proc_list) > 0:
      for proc in active_proc_list:
        exclusion_messe += proc + "、"
      exclusion_messe = exclusion_messe[:-1]
    if len(wait_proc_list) > 0:
      for proc in wait_proc_list:
        exclusion_messe += proc + "、"
      exclusion_messe = exclusion_messe[:-1]

    # 進捗度合いを取得
    progress_proba = 0
    process_name = 'createmodel'
    predict_name = proc_flg_update.get_proc_name('predict')
    if predict_name in exclusion_messe:
      process_name = 'predict'
      proba_dict = {'run_receipt_pp':1, 'run_karte_pp':10, 'run_predict':20, 'backup_result':90, 'write_db_csv':91, 'write_tensu':93, 'write_ward':95, 'comp':100}
      # 実行中のプロセスを取得
      request_sql = "SELECT CONTROLTEXT2 FROM aireceiptapp_hanyo WHERE CODE1 = 'proc' AND CODE2 = 'predict' LIMIT 1"
      rows = get_hanyo.get_user_list(request_sql)
      run_process_name = ''
      for row in rows:
        run_process_name = row['CONTROLTEXT2']
      if run_process_name == 'run_predict':
        # ログから進捗を取得する
        log_dir = os.path.join(folder.base(), 'aireceipt', 'logs')
        progress_proba = read_log.read_log(process_name, log_dir)
      else:
        progress_proba = proba_dict[run_process_name]
    else:
      proba_dict = {'run_receipt_pp':1, 'run_karte_pp':10, 'run_model':20, 'comp':100}
      # 実行中のプロセスを取得
      request_sql = "SELECT CONTROLTEXT2 FROM aireceiptapp_hanyo WHERE CODE1 = 'proc' AND CODE2 = 'model' LIMIT 1"
      rows = get_hanyo.get_user_list(request_sql)
      run_process_name = ''
      for row in rows:
        run_process_name = row['CONTROLTEXT2']
      if run_process_name == 'run_model':
        # ログから進捗を取得する
        log_dir = os.path.join(folder.base(), 'aireceipt', 'logs')
        progress_proba = read_log.read_log(process_name, log_dir)
      else:
        progress_proba = proba_dict[run_process_name]
    obj_dict = {}
    obj_dict['progress_proba'] = progress_proba

    params = {
      "proc_name":proc_name,
      'obj_json': json.dumps(obj_dict),
      "exclusion_messe":exclusion_messe,
      "login_user":request.user.first_name,
      "user_authority":get_hanyo.get_user_authority(request.user),
      "menu_info":get_hanyo.get_menu_info(),
    }
    return render(request, 'exclusion.html', params)

  user_receipt_pre_folder = folder.user_receipt_folder("createmodel")
  user_karte_pre_folder = folder.user_karte_folder("createmodel")
  karte_folder = folder.karte("createmodel")
  karte_pre_folder = folder.karte_pre('createmodel')
  predict_karte_folder = folder.karte("predict")

  # データチェック
  logger.info(f"カルテデータのチェックを開始します")
  # predict_in/karteフォルダにファイルがある事を確認する
  predict_karte_list = list(pathlib.Path(predict_karte_folder).glob('**/*.csv'))
  predict_karte_bool = True # predictフォルダにカルテファイルがあるかのフラグ★★★★★
  if len(predict_karte_list) == 0:
    predict_karte_bool = False
  template_name = 'createModel.html'
  karte_pre_object_list = None
  karte_period = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
  if predict_karte_bool:
    karte_file_ng_flag = 0
    template_name = 'createModelReceipt.html'
    # createmodel_in/karteフォルダにpredict_karte_listを移動する
    for predict_karte in predict_karte_list:
      if 'カルテ_全結合' in predict_karte.name:
        logger.info(f"{predict_karte.name}を{karte_folder}に移動します")
        after_createmodel_karte = pathlib.Path(f"{karte_folder}{predict_karte.name}")
        shutil.copy(predict_karte, after_createmodel_karte)
        logger.info(f"{predict_karte.name}を{karte_folder}に移動しました")
    # モデル作成の期間が何か月かを汎用テーブルから取得する
    try:
      period = HANYO.objects.values('CONTROLINT1').filter(CODE1='hos_info', CODE2='model').first()['CONTROLINT1']
    except:
      period = 12
    # createmodel_in/karteフォルダにperiod個以上のファイルがあれば、一番過去のファイルを削除する
    createmodel_in_karte_files = list(pathlib.Path(karte_folder).glob('**/カルテ_全結合*.csv'))
    if period < len(createmodel_in_karte_files):
      # karteフォルダ内にファイルが12個以上あったら削除する
      class FileInfo:
        def __init__(self):
          self.FILENAME = ''
          self.UPDATETIME = None

      file_info = []
      for create_in_karte_file in createmodel_in_karte_files:
        fileinfo = FileInfo()
        fileinfo.FILENAME = create_in_karte_file
        fileinfo.UPDATETIME = create_in_karte_file.stat().st_mtime
        file_info.append(fileinfo)
      updatetime_list = []
      for i in file_info:
        updatetime_list.append(i.UPDATETIME)
      min_updatetime = min(updatetime_list)
      for i in file_info:
        if i.UPDATETIME == min_updatetime:
          os.remove(i.FILENAME)
          logger.info(f'{i.FILENAME} を削除しました')
    # karte_pre_folderのファイルは不要なので削除する
    unlink_files = list(pathlib.Path(karte_pre_folder).glob('**/*.csv'))
    for unlink_file in unlink_files:
      unlink_file.unlink()
      logger.info(f"{unlink_file}を削除しました")
  else:

    # カルテファイルが格納されているかを確認する
    karte_files = os.listdir(user_karte_pre_folder)
    karte_csvfiles = [f for f in karte_files if os.path.isfile(os.path.join(user_karte_pre_folder,f))and '.csv' in f]

    # カルテファイルが確認されている場合、列の変換を行う
    file_syuzyutukiroku = ''
    if karte_csvfiles:
      coversion_colmns_dict = karte_conversion.get_column_conversion_dict('createmodel')
      if coversion_colmns_dict:
        karte_conversion.conversion_columns(coversion_colmns_dict, user_karte_pre_folder)
    karte_pre_object_list = karte_pre_chk.GetKarteFolderList(user_karte_pre_folder, 'createmodel')
    # 前処理前のカルテデータが正常に格納されているかをチェックする
    karte_file_ng_flag = file_ng_check(karte_pre_object_list)

    # カルテ診療日チェック
    karte_start_period_list, karte_end_period_list, karte_period_ng_flag = karte_pre_chk.karte_period(karte_pre_object_list)

    # カルテデータの最も過去と最も最近の請求年月
    if karte_start_period_list and karte_end_period_list:
      karte_start_period = min(karte_start_period_list)
      karte_end_period = max(karte_end_period_list)
      karte_period = f"{karte_start_period.strftime('%Y%m')}_{karte_end_period.strftime('%Y%m')}"

  # レセプトデータチェック 1っカ月分のみでいい
  t = datetime.date.today()
  rece_chk_files = get_hanyo.get_hanyo(f"SELECT CONTROLTEXT1, CONTROLTEXT2 FROM aireceiptapp_hanyo WHERE CODE1 = 'receipt_file_chk' AND STARTDATE <= '{t}' AND ENDDATE >= '{t}'")
  rece_pre_object_list = receipt_pre_chk.receipt_chk_one_month(user_receipt_pre_folder, rece_chk_files)
  rece_file_none_messe = ''

  # 前処理前のレセプトデータが正常に格納されているかをチェックする
  rece_file_ng_flag = 1 if 1 in [rece_pre_obj['flg'] for rece_pre_obj in rece_pre_object_list] else 0
  if rece_file_ng_flag != 0:
    rece_file_none_messe = 'レセプトファイルを格納してください。'

  file_ng_flag = 0

  if rece_file_ng_flag == 1 or karte_file_ng_flag ==1:
    file_ng_flag = 1

  # レセプトの診療月
  rece_month_unique_list = list(set([rpo['month'] for rpo in rece_pre_object_list]))
  rece_month = None
  rece_month_ng_flag = False
  if len(rece_month_unique_list) > 1:
    rece_month_str = 'レセプトIRレコードの請求月をご確認ください'
    rece_month_ng_flag = True
  else:
    rece_month_str = rece_month_unique_list[0]
    try:
      rece_month = datetime.datetime.strptime(rece_month_str, '%Y-%m')
    except:
      pass

  period_messe = ''
  karte_period_messe = ''
  period_str = ''
  period_ng_flag = 0 # レセプト、カルテの期間は同じかのフラグ

  if (not predict_karte_bool) and rece_month and karte_start_period_list and karte_end_period_list:
    if karte_start_period <= rece_month <= karte_end_period:
      period_ng_flag = 0
    else:
      period_ng_flag = 1

  if period_ng_flag == 1:
    period_messe = "レセプト、カルテデータの期間が合っているかをご確認ください。"

  # 期間を文字列型にする
  if period_messe == "":
      period_str = rece_month_str

  ng_flag = 0
  if file_ng_flag == 1 or period_ng_flag == 1 or rece_file_none_messe or rece_month_ng_flag:
    ng_flag = 1

  params = {
    "rece_pre_object_list":rece_pre_object_list,
    "karte_pre_object_list":karte_pre_object_list,
    "receipt_pre_folder":folder.display_user_folder("createmodel", "receipt"),
    "karte_pre_folder":folder.display_user_folder("createmodel", "karte"),
    "period_messe":period_messe,
    "karte_period_messe":karte_period_messe,
    'karte_period': karte_period,
    "ng_flag":ng_flag,
    "period_str":period_str,
    "rece_file_none_messe":rece_file_none_messe,
    "login_user":request.user.first_name,
    "user_authority":get_hanyo.get_user_authority(request.user),
    "menu_info":get_hanyo.get_menu_info(),
  }

  return render(request, template_name, params)


# 検知画面を表示する
def predict(request):
  os.chdir(base)
  proc_name = proc_flg_update.get_proc_name('predict')
  # 処理中なら排他画面を返す
  active_proc_list, wait_proc_list = proc_flg_update.proc_status_now()
  if len(active_proc_list) > 0 or len(wait_proc_list):
    exclusion_messe = ""
    if len(active_proc_list) > 0:
      for proc in active_proc_list:
        exclusion_messe += proc + "、"
      exclusion_messe = exclusion_messe[:-1]
    if len(wait_proc_list) > 0:
      for proc in wait_proc_list:
        exclusion_messe += proc + '、'
      exclusion_messe = exclusion_messe[:-1]

    # 進捗度合いを取得
    progress_proba = 0
    process_name = 'createmodel'
    predict_name = proc_flg_update.get_proc_name('predict')
    if predict_name in exclusion_messe:
      process_name = 'predict'
      proba_dict = {'run_receipt_pp':1, 'run_karte_pp':10, 'run_predict':20, 'backup_result':90, 'write_db_csv':91, 'write_tensu':93, 'write_ward':95, 'comp':100}
      # 実行中のプロセスを取得
      request_sql = "SELECT CONTROLTEXT2 FROM aireceiptapp_hanyo WHERE CODE1 = 'proc' AND CODE2 = 'predict' LIMIT 1"
      rows = get_hanyo.get_user_list(request_sql)
      run_process_name = ''
      for row in rows:
        run_process_name = row['CONTROLTEXT2']
      if run_process_name == 'run_predict':
        # ログから進捗を取得する
        log_dir = os.path.join(folder.base(), 'aireceipt', 'logs')
        progress_proba = read_log.read_log(process_name, log_dir)
      else:
        progress_proba = proba_dict[run_process_name]
    else:
      proba_dict = {'run_receipt_pp':1, 'run_karte_pp':10, 'run_model':20, 'comp':100}
      # 実行中のプロセスを取得
      request_sql = "SELECT CONTROLTEXT2 FROM aireceiptapp_hanyo WHERE CODE1 = 'proc' AND CODE2 = 'model' LIMIT 1"
      rows = get_hanyo.get_user_list(request_sql)
      run_process_name = ''
      for row in rows:
        run_process_name = row['CONTROLTEXT2']
      if run_process_name == 'run_model':
        # ログから進捗を取得する
        log_dir = os.path.join(folder.base(), 'aireceipt', 'logs')
        progress_proba = read_log.read_log(process_name, log_dir)
      else:
        progress_proba = proba_dict[run_process_name]
    obj_dict = {}
    obj_dict['progress_proba'] = progress_proba

    params = {
      "proc_name":proc_name,
      'obj_json': json.dumps(obj_dict),
      "exclusion_messe":exclusion_messe,
      "login_user":request.user.first_name,
      "user_authority":get_hanyo.get_user_authority(request.user),
      "menu_info":get_hanyo.get_menu_info(),
    }
    return render(request, 'exclusion.html', params)

  # 検知用データへのパス
  user_receipt_pre_folder = folder.user_receipt_folder("predict")
  user_karte_pre_folder = folder.user_karte_folder("predict")

  # カルテファイルが格納されているかを確認する
  karte_files = os.listdir(user_karte_pre_folder)
  karte_csvfiles = [f for f in karte_files if os.path.isfile(os.path.join(user_karte_pre_folder,f))and '.csv' in f]

  # カルテファイルが確認されている場合、列の変換を行う
  file_syuzyutukiroku = ''
  if karte_csvfiles:
    coversion_colmns_dict = karte_conversion.get_column_conversion_dict('predict')
    if coversion_colmns_dict:
      karte_conversion.conversion_columns(coversion_colmns_dict, user_karte_pre_folder)

  karte_pre_object_list = karte_pre_chk.GetKarteFolderList(user_karte_pre_folder, 'predict')
  # 前処理前のカルテデータが正常に格納されているかをチェックする
  karte_file_ng_flag = file_ng_check(karte_pre_object_list)

  # カルテデータの診療日に関連するを格納する変数
  karte_start_period = None
  karte_end_period = None

  # カルテ診療日チェック
  karte_start_period_list, karte_end_period_list, karte_period_ng_flag = karte_pre_chk.karte_period(
    karte_pre_object_list)
  karte_period = datetime.datetime.now().strftime('%Y%m%d%H%M%S')

  # カルテデータの最も過去と最も最近の請求年月
  if karte_start_period_list and karte_end_period_list:
    karte_start_period = min(karte_start_period_list)
    karte_end_period = max(karte_end_period_list)
    karte_period = f"{karte_start_period.strftime('%Y%m')}_{karte_end_period.strftime('%Y%m')}"

  # レセプトチェック　1っか月分
  t = datetime.date.today()
  rece_chk_files = get_hanyo.get_hanyo(f"SELECT CONTROLTEXT1, CONTROLTEXT2 FROM aireceiptapp_hanyo WHERE CODE1 = 'receipt_file_chk' AND STARTDATE <= '{t}' AND ENDDATE >= '{t}'")
  rece_chk_files_display = '、'.join([rcf['CONTROLTEXT2'] for rcf in rece_chk_files])
  rece_pre_object_list = receipt_pre_chk.receipt_chk_one_month(user_receipt_pre_folder, rece_chk_files)

  rece_file_none_messe = ''

  # 前処理前のレセプトデータが正常に格納されているかをチェックする
  rece_file_ng_flag = 1 if 1 in [rece_pre_obj['flg'] for rece_pre_obj in rece_pre_object_list] else 0
  if rece_file_ng_flag != 0:
    rece_file_none_messe = 'レセプトファイルを格納してください。'

  # レセプトの診療月
  rece_month_unique_list = list(set([rpo['month'] for rpo in rece_pre_object_list]))
  rece_month = None
  rece_month_ng_flag = False
  if len(rece_month_unique_list) > 1:
    rece_month_str = 'レセプトIRレコードの請求月をご確認ください'
    rece_month_ng_flag = True
  else:
    rece_month_str = rece_month_unique_list[0]
    try:
      rece_month = datetime.datetime.strptime(rece_month_str, '%Y-%m')
    except:
      pass

  file_ng_flag = 0

  if rece_file_ng_flag == 1 or karte_file_ng_flag ==1:
    file_ng_flag = 1

  period_messe = ''
  period_str = ''
  karte_period_messe = ''
  period_ng_flag = 0 # レセプト、カルテの期間は同じかのフラグ

  if rece_month and karte_start_period_list and karte_end_period_list:
    if karte_start_period <= rece_month <= karte_end_period:
      period_ng_flag = 0
    else:
      period_ng_flag = 1

  if period_ng_flag == 1:
    period_messe = "レセプト、カルテデータの期間が合っているかをご確認ください。"

  ng_flag = 0
  if file_ng_flag == 1 or period_ng_flag == 1 or rece_file_none_messe or rece_month_ng_flag:
    ng_flag = 1

  if period_messe == "":
      period_str = rece_month_str

  params = {
    "rece_chk_files_display":rece_chk_files_display,
    "rece_pre_object_list":rece_pre_object_list,
    "karte_pre_object_list":karte_pre_object_list,
    "receipt_pre_folder":folder.display_user_folder("predict", "receipt"),
    "karte_pre_folder":folder.display_user_folder("predict", "karte"),
    "karte_period_messe":karte_period_messe,
    'karte_period': karte_period,
    "period_messe":period_messe,
    'period_str':period_str,
    "ng_flag":ng_flag,
    "rece_file_none_messe":rece_file_none_messe,
    "login_user":request.user.first_name,
    "user_authority":get_hanyo.get_user_authority(request.user),
    "menu_info":get_hanyo.get_menu_info(),
  }
  return render(request, 'predict.html', params)

# 指定されたファイルが入っているかのフラグを設定する
def file_ng_check(l):
  ng = 0
  for i in l:
    if i.NGFLAG == 1:
      ng = 1
  return(ng)

# システム管理画面を表示する
def MaintenanceIndex(request):
  os.chdir(base)
  params = {
    "login_user":request.user.first_name,
    "user_authority":get_hanyo.get_user_authority(request.user),
    "menu_info":get_hanyo.get_menu_info(),
  }
  return render(request, 'maintenanceIndex.html', params)

# ログ照会画面
def userlog(request):
  os.chdir(base)
  obj = USERLOG.objects.all().order_by('-id')
  params = {
    "obj":obj,
    "page_title": "ログ照会（ユーザー用）",
    "login_user":request.user.first_name,
    "user_authority":get_hanyo.get_user_authority(request.user),
    "menu_info":get_hanyo.get_menu_info(),
  }
  return render(request, 'userLog.html', params)

# 管理者用ログ
def SystemLog(request):
  os.chdir(base)
  select_form = SystemLogSelectForm()
  select = request.POST.get('log_date')
  p = pathlib.Path(base + "/aireceipt/logs/aireceipt.log")
  newfile = datetime.datetime.fromtimestamp(p.stat().st_mtime)
  newfile = newfile.strftime("%Y-%m-%d")

  select_form.fields['log_date'].choices = GetMonthlist(newfile)
  if request.method == 'POST':
    select = request.POST.get('log_date')
    if not select:
      select = newfile
    object_list = GetSystemLog(select,newfile)
    select_form.fields['log_date'].initial = select
  else:
    select = newfile

  object_list = GetSystemLog(select,newfile)

  params = {
    "object_list":object_list,
    "select_form":select_form,
    "select":select,
    "page_title":"ログ照会（管理者用）",
    "login_user":request.user.first_name,
    "user_authority":get_hanyo.get_user_authority(request.user),
    "menu_info":get_hanyo.get_menu_info(),
  }
  return render(request, "systemLog.html", params)

def GetMonthlist(newfile):
  # logフォルダ内のファイル名を取得する
  logdir = base + "/aireceipt/logs/"
  file_list = os.listdir(logdir)
  select_list = []
  for i in file_list:
    if i == 'aireceipt.log':
        i = newfile # 「aireceipt.log」というファイル名は本日のログなので本日の日付に変更する
    else:
        i = i.replace('aireceipt.log.','')
    select_list.append(i)
  select_list = sorted(select_list)
  # ファイルレコードの加工
  selectMonth = []
  selectMonth.append(('','日付を選択してください'))
  for month in select_list:
    try:
      selectMonth.append((month, month))
    except:
      pass
  return selectMonth

# システムログレコード
class SysLogRecord:
  def __init__(self):
    self.ASCTIME = ""
    self.STATUS = ""
    self.LOGIC = ""
    self.OUTPUT = ""
  def get_queryset(self):
    return super().get_queryset().order_by('-ASCTIME')

def GetSystemLog(select,newfile):
  if select is None:
    select = "aireceipt.log"
  else:
    if select == newfile:
      select = 'aireceipt.log'
    else:
      select = 'aireceipt.log.' + select

  # ファイルの一括読み込み
  logName = base + "/aireceipt/logs/" + select
  with open(logName) as logFile:
    allLines = logFile.readlines()

  # ファイルレコードの加工
  sysLogs_work = []
  for lineData in allLines:
    work = lineData.rstrip('\n')
    # 行データの分割
    words = work.split(" ")
    try:
      logRecord = SysLogRecord()
      timestamp = parse(words[0] + " " + words[1])
      logRecord.ASCTIME = timestamp.strftime('%Y/%m/%d %H:%M:%S')
      w = words[2]
      w = '情報' if '[INFO]' in w else 'ワーニング' if '[WARNING]' in w else 'エラー' if '[ERROR]' in w else w
      logRecord.STATUS = w
      logRecord.LOGIC = words[3]
      logRecord.OUTPUT = ' '.join(words[4:])
      sysLogs_work.append( logRecord )
    except:
      # 変形できなかった場合は、前のレコードに結合。（前が無ければ破棄）
      if 0 < len(sysLogs):
        sysLogs_work[-1].OUTPUT += ('\n' + work)
    sysLogs = []
    for i in reversed(sysLogs_work):
        sysLogs.append(i)

  return sysLogs

# モデル作成を実行する
def run_create_model(request):
  os.chdir(base)
  proc_name = proc_flg_update.get_proc_name('model')
  from . import folder
  user_receipt_pre_folder = folder.user_receipt_folder("createmodel")
  user_karte_pre_folder = folder.user_karte_folder("createmodel")
  receipt_pre_folder = folder.receipt_pre("createmodel")
  karte_pre_folder = folder.karte_pre("createmodel")
  receipt_pre_BK_folder = folder.receipt_pre_BK_folder('createmodel')
  karte_pre_BK_folder = folder.karte_pre_BK_folder('createmodel')
  admin_karte_backup_folder = folder.admin_karte_backup()
  admin_rece_backup_folder = folder.admin_rece_backup()

  # 利用者用レセプトフォルダー内のファイルリストを生成
  in_files = [Path(user_receipt_pre_folder).glob('**/*UKE')]

  # システム用レセプトデータフォルダーがなければ作成する
  if not os.path.isdir(receipt_pre_folder):
    os.makedirs(receipt_pre_folder)

  # システム用レセプトフォルダリストを生成
  out_files_all = os.listdir(receipt_pre_folder)
  out_files = [f for f in out_files_all if os.path.isdir(os.path.join(receipt_pre_folder, f))]

  # システム用レセプトプレフォルダをバックアップに移動した後、利用者フォルダにあるレセプトデータをシステム用レセプトフォルダに移動する
  if in_files:
    if out_files:
      remove_old_data.rm_old(Path(receipt_pre_BK_folder), 0)
      move_dir.move_dir(receipt_pre_folder, receipt_pre_BK_folder, out_files)
    t = datetime.date.today()
    remove_old_data.rm_old(Path(admin_rece_backup_folder), 0)
    uke_master_t = get_hanyo.get_hanyo(f"SELECT CONTROLTEXT2 FROM aireceiptapp_hanyo WHERE CODE1 = 'receipt_file_chk' AND STARTDATE <= '{t}' AND ENDDATE >= '{t}'")
    receipt_pre_chk.uke_conversion(user_receipt_pre_folder, receipt_pre_folder, admin_rece_backup_folder, uke_master_t)

  # 利用者用カルテフォルダー内のファイルリストを生成
  in_files_all = os.listdir(user_karte_pre_folder)
  in_files = [f for f in in_files_all if f[-3:] == 'csv']

  # プロジェクト用カルテフォルダがなければ作成する
  if not os.path.isdir(karte_pre_folder):
    os.makedirs(karte_pre_folder)

  # システム用カルテフォルダ内のファイルリストを生成
  out_files_all = os.listdir(karte_pre_folder)
  out_files = [f for f in out_files_all if f[-3:] == 'csv']

  # システム用カルテフォルダをバックアップに移動した後、利用者フォルダにあるカルテデータをプロジェクト用レセプトフォルダに移動する
  if in_files:
    if out_files:
      remove_old_data.rm_old(Path(karte_pre_BK_folder), 0)
      new_bk_dir = move_dir.make_BKfolder(karte_pre_BK_folder)
      move_dir.move_dir(karte_pre_folder, new_bk_dir, out_files)
    karte_pre_file_list = read_kartefile.get_karte_pre_file_list()
    move_dir.copy_karte_file(user_karte_pre_folder, admin_karte_backup_folder, in_files, karte_pre_file_list)
    move_dir.move_karte_file(user_karte_pre_folder, karte_pre_folder, in_files, karte_pre_file_list)

  # カルテフォルダーに残ったファイルがあれば削除する
  files = os.listdir(user_karte_pre_folder)
  if files:
    for file in files:
      delete_file = os.path.join(user_karte_pre_folder, file)
      try:
        os.remove(delete_file)
      except:
        pass

  # モデル作成をタスクスケジューラに登録
  cmd_file = "RegistTask.bat"
  regist_task_dir = os.path.join(base,"regist_task")
  date_today = datetime.date.today()
  date_today_str = date_today.strftime('%Y-%m-%d')
  date_time = datetime.datetime.now()
  date_time_2min = date_time + datetime.timedelta(minutes=2)
  date_time_2min_str = date_time_2min.strftime('%H:%M')
  codes = ''
  command = cmd_file + " " + "preprocessing_model" + " " + date_today_str + " " + date_time_2min_str + " " + codes
  os.chdir(regist_task_dir)
  os.system(command)
  os.chdir(base)

  # ステータスを「wait」にする
  get_hanyo.update_proc_flag('model',2)

  logger.info(' 利用者ID' + str(request.user) + 'がモデル作成を開始しました。')
  # 処理中のプロセス名にレセプト前処理を登録する
  request_sql = "UPDATE aireceiptapp_hanyo SET CONTROLTEXT2 = 'run_receipt_pp' WHERE CODE1 = 'proc' AND CODE2 = 'model'"
  get_hanyo.update_aireceiptdb(request_sql)

  params = {
    "proc_name":proc_name,
    "login_user":request.user.first_name,
    "user_authority":get_hanyo.get_user_authority(request.user),
    "menu_info":get_hanyo.get_menu_info(),
  }
  return render(request, "createmodelStart.html", params)

# 検知を実行する
def run_predict(request):
  os.chdir(base)
  proc_name = proc_flg_update.get_proc_name('predict')
  from . import folder
  # 検知用データへのパス
  user_receipt_pre_folder = folder.user_receipt_folder("predict")
  user_karte_pre_folder = folder.user_karte_folder("predict")
  receipt_pre_folder = folder.receipt_pre("predict")
  karte_pre_folder = folder.karte_pre("predict")
  receipt_pre_BK_folder = folder.receipt_pre_BK_folder('predict')
  karte_pre_BK_folder = folder.karte_pre_BK_folder('predict')
  admin_karte_backup_folder = folder.admin_karte_backup()
  admin_rece_backup_folder = folder.admin_rece_backup()

  # カルテバックアップフォルダを空にする
  remove_old_data.rm_old(Path(admin_karte_backup_folder),0)
  # 入院情報をDBに保存する
  logger.info('入院情報をDBに保存します')
  try:
    logger.info('write_movementを開始します')
    args = ["python", os.path.join(folder.base(), 'aireceiptapp', 'write_movement.py')]
    proc_model = subprocess.Popen(args)
    while True:
      time.sleep(1)
      if proc_model.poll() is not None:
        break
    rcode = proc_model.returncode
    logger.info('write_movement rcode={0}'.format(rcode))
    if rcode == 0:
      logger.info('入院情報を更新しました')
  except:
    logger.warning(sys.exc_info())
    logger.warning(traceback.format_exc())

  # 利用者用レセプトフォルダー内のファイルリストを生成
  in_files_all = os.listdir(user_receipt_pre_folder)
  in_files = [f for f in in_files_all if os.path.isdir(os.path.join(user_receipt_pre_folder, f))]

  # システム用レセプトプレフォルダーが存在しなければ作成
  if not os.path.isdir(receipt_pre_folder):
    os.makedirs(receipt_pre_folder)

  # システム用レセプトプレフォルダーのファイルリスト作成
  out_files_all = os.listdir(receipt_pre_folder)
  out_files = [f for f in out_files_all if os.path.isdir(os.path.join(receipt_pre_folder, f))]
  if in_files_all:
    if out_files:
      # システム用レセプトプレフォルダーの中身をバックアップフォルダに移動する
      remove_old_data.rm_old(Path(receipt_pre_BK_folder), 0)
      move_dir.move_dir(receipt_pre_folder, receipt_pre_BK_folder, out_files)
    # 利用者用レセプトプレフォルダー -> システム用レセプトプレフォルダーに移動、利用者用バックアップフォルダー
    t = datetime.date.today()
    uke_master_t = get_hanyo.get_hanyo(f"SELECT CONTROLTEXT2 FROM aireceiptapp_hanyo WHERE CODE1 = 'receipt_file_chk' AND STARTDATE <= '{t}' AND ENDDATE >= '{t}'")
    remove_old_data.rm_old(Path(admin_rece_backup_folder), 0)
    receipt_pre_chk.uke_conversion(user_receipt_pre_folder, receipt_pre_folder, admin_rece_backup_folder, uke_master_t)

  # 利用者用カルテフォルダー内のファイルリストを生成
  in_files = list(Path(user_karte_pre_folder).glob('**/*.csv'))

  # システム用カルテプレフォルダーがなければ作成
  if not os.path.isdir(karte_pre_folder):
    os.makedirs(karte_pre_folder)

  out_files = list(Path(karte_pre_folder).glob('**/*.csv'))

  if len(in_files) > 0:
    if len(out_files) > 0:
      remove_old_data.rm_old(Path(karte_pre_BK_folder), 0)
      new_bk_dir = Path(move_dir.make_BKfolder(karte_pre_BK_folder))
      # システム用カルテフォルダー -> システム用バックアップフォルダー
      for outfile in out_files:
        shutil.move(outfile, new_bk_dir/outfile.name)
    Path_admin_karte_backup_folder = Path(admin_karte_backup_folder)
    karte_pre_folder = Path(karte_pre_folder)
    karte_pre_file_list = read_kartefile.get_karte_pre_file_list()
    for in_file in in_files:
      shutil.copy(in_file, Path_admin_karte_backup_folder/in_file.name)
      shutil.move(in_file, karte_pre_folder/in_file.name)

  # カルテフォルダーに残ったファイルがあれば削除する
  karte_folder_in = os.listdir(user_karte_pre_folder)
  files = [f for f in karte_folder_in if '.csv' in f]
  folders = [f for f in karte_folder_in if os.path.isdir(os.path.join(user_karte_pre_folder))]
  if files:
    logger.info('カルテフォルダ内を空にします')
    for file in files:
      delete_file = os.path.join(user_karte_pre_folder, file)
      os.remove(delete_file)
    logger.info('カルテフォルダ内を空にしました')
  if folders:
    for folder in folders:
      delete_folder = os.path.join(user_karte_pre_folder, folder)
      try:
        shutil.rmtree(delete_folder)
      except:
        pass

  # 検知をタスクスケジューラに登録
  cmd_file = "RegistTask.bat"
  regist_task_dir = os.path.join(base,"regist_task")
  date_today = datetime.date.today()
  date_today_str = date_today.strftime('%Y-%m-%d')
  date_time = datetime.datetime.now()
  date_time_2min = date_time + datetime.timedelta(minutes=2)
  date_time_2min_str = date_time_2min.strftime('%H:%M')
  codes = ''
  command = cmd_file + " " + "preprocessing_predict" + " " + date_today_str + " " + date_time_2min_str + " " + codes
  os.chdir(regist_task_dir)
  logger.info(f'「{command}」の内容でタスクに登録します')
  os.system(command)
  os.chdir(base)

  # ステータスを「wait」にする
  get_hanyo.update_proc_flag('predict',2)

  logger.info(' 利用者ID' + str(request.user) + 'が検知を開始しました。')

  # 処理中のプロセス名にレセプト前処理を登録する
  request_sql = "UPDATE aireceiptapp_hanyo SET CONTROLTEXT2 = 'run_receipt_pp' WHERE CODE1 = 'proc' AND CODE2 = 'predict'"
  get_hanyo.update_aireceiptdb(request_sql)

  params = {
    "proc_name":proc_name,
    "login_user":request.user.first_name,
    "user_authority":get_hanyo.get_user_authority(request.user),
    "menu_info":get_hanyo.get_menu_info(),
  }
  return render(request, "predictStart.html", params)

# 利用者一覧
def user_list(request):
  try:
    login_user_name = request.user.first_name
  except:
    return redirect(to='/logout')
  form = UserListSelectForm()
  form.fields['target_date'].initial = datetime.date.today()
  params = {
    "form":form,
    "title":'利用者',
    "user_list":None,
    "login_user":request.user.first_name,
    "user_authority":get_hanyo.get_user_authority(request.user),
    "menu_info":get_hanyo.get_menu_info(),
  }
  request_sql = "SELECT id, CODE2, NAME, STARTDATE, ENDDATE FROM aireceiptapp_hanyo WHERE CODE1 = 'user'"
  if get_hanyo.get_user_authority(request.user) != "9":
    request_sql += " AND AUTHORITY <> 9"
  date_today = datetime.date.today()
  criteria_date = date_today.strftime('%Y-%m-%d')
  if request.method == 'POST':
    l = request.POST
    params["form"] = UserListSelectForm(request.POST)
    if l['userid']:
      request_sql += " AND CODE2 LIKE '%" + l['userid'] + "%'"
    if l['username']:
      request_sql += " AND NAME LIKE '%" + l['username'] + "%'"
    if l['target_date']:
      criteria_date = l['target_date']
  request_sql += " AND STARTDATE <= '" + criteria_date + "' AND ENDDATE >= '" + criteria_date + "'"
  params['user_list'] = get_hanyo.get_user_list(request_sql)
  return render(request, 'userList.html', params)

# 利用者詳細
def user_detail(request, num):
  obj = get_object_or_404(HANYO, id=num)
  params = {
    "title":'利用者詳細',
    "user_authority":get_hanyo.get_user_authority(request.user),
    "menu_info":get_hanyo.get_menu_info(),
    "login_user":request.user.first_name,
    "obj":obj,
  }
  return render(request, 'userDetail.html', params)

# 利用者編集
def user_edit(request, num):
  obj = HANYO.objects.get(id=num)
  initial_dict = dict(
    code1=obj.CODE1,
    code2=obj.CODE2,
    cname=obj.CNAME,
    name=obj.NAME,
    startdate=obj.STARTDATE,
    enddate=obj.ENDDATE,
    controltext1=obj.CONTROLTEXT1,
    controltext2=obj.CONTROLTEXT2,
    controlint1=obj.CONTROLINT1,
    controlint2=obj.CONTROLINT2,
    authority=obj.AUTHORITY,
    createuser=obj.CREATEUSER,
    updateuser=request.user,
    createdate=obj.CREATEDATE,
    )
  form = HanyoForm(request.GET or None, initial=initial_dict)
  form.fields['authority'].choices = get_hanyo.get_authority_edit()
  form.fields['controltext1'].widget=forms.TextInput(attrs={'class':'form-control','type':'password'})
  params = {
    'title':'利用者編集',
    'id':num,
    'form':form,
    'end_start':None,
    'error_item':"",
    'error_str':"",
    'duplicate':"",
    'duplicate_message':None,
    "login_user":request.user.first_name,
    "user_authority":get_hanyo.get_user_authority(request.user),
    "menu_info":get_hanyo.get_menu_info(),
  }
  if request.method == 'POST':
    l=request.POST
    start_end = check_validation.start_end_date(l['startdate'], l['enddate'])
    if start_end:
      params['start_end'] = start_end
      form = HanyoForm(request.POST)
      form.fields['authority'].choices = get_hanyo.get_authority(request.user)
      params['form'] = form
      return render(request, 'userEdit.html', params)
    u = User.objects.get(username=obj.CODE2)
    u.username = l['code2']
    u.first_name = l['name']
    u.set_password(l['controltext1'])
    u.save()
    t = HANYO(
      id=num,
      CODE1='user',
      CODE2=l['code2'],
      CNAME='',
      NAME=l['name'],
      STARTDATE=l['startdate'],
      ENDDATE=l['enddate'],
      CONTROLTEXT1=l['controltext1'],
      CONTROLTEXT2='',
      CONTROLINT1=0,
      CONTROLINT2=0,
      AUTHORITY=l['authority'],
      UPDATEUSER=l['updateuser'],
      CREATEDATE=obj.CREATEDATE,
      )
    t.save()
    return redirect(to='/user_list/')
  return render(request, 'userEdit.html', params)

# 利用者追加
def user_add(request):
  form = HanyoForm()
  date_today = datetime.date.today()
  date_today_str = date_today.strftime('%Y-%m-%d')
  form.fields['code1'].initial = 'user'
  form.fields['startdate'].initial = date_today_str
  form.fields['enddate'].initial = '2099-12-31'
  form.fields['createuser'].initial = request.user
  form.fields['updateuser'].initial = request.user
  form.fields['authority'].choices = get_hanyo.get_authority(request.user)
  form.fields['controltext1'].widget=forms.TextInput(attrs={'class':'form-control','type':'password'})
  params = {
    "title":'利用者追加',
    "duplicate":"",
    "start_end":None,
    "user_authority":get_hanyo.get_user_authority(request.user),
    "menu_info":get_hanyo.get_menu_info(),
    "login_user":request.user.first_name,
    "form":form,
  }
  if request.method == 'POST':
    l = request.POST
    start_end = check_validation.start_end_date(l['startdate'], l['enddate'])
    if start_end:
      params['start_end'] = start_end
      form = HanyoForm(request.POST)
      form.fields['authority'].choices = get_hanyo.get_authority(request.user)
      params['form'] = form
      return render(request, 'userAdd.html', params)
    try:
      user = User.objects.create_user(l['code2'],'',l['controltext1'])
      user.first_name = l['name']
      user.save()
      t = HANYO(
        CODE1='user',
        CODE2=l['code2'],
        CNAME='',
        NAME=l['name'],
        STARTDATE=l['startdate'],
        ENDDATE=l['enddate'],
        CONTROLTEXT1=l['controltext1'],
        CONTROLTEXT2='',
        CONTROLINT1=0,
        CONTROLINT2=0,
        AUTHORITY=l['authority'],
        CREATEUSER=l['createuser'],
        UPDATEUSER=l['updateuser'],
        )
      t.save()
    except IntegrityError:
      params["duplicate"] = "この利用者IDは既に登録されています"
      form= HanyoForm(request.POST)
      form.fields['authority'].choices = get_hanyo.get_authority(request.user)
      params["form"] = form
      return render(request, 'userAdd.html', params)

    return redirect(to='/user_list/')
  return render(request, 'userAdd.html', params)

# 利用者削除
def user_delete(request, num):
  obj = HANYO.objects.get(id=num)
  message = '※以下の利用者を削除します。'
  if request.method == 'POST':
    obj.delete()
    if obj.CODE1 == 'user':
      try:
        User.objects.filter(username=obj.CODE2).delete()
      except:
        pass
    return redirect(to='/user_list/')
  params = {
    'title':'利用者削除',
    'id':num,
    'obj':obj,
    "login_user":request.user.first_name,
    "user_authority":get_hanyo.get_user_authority(request.user),
    "menu_info":get_hanyo.get_menu_info(),
    "message":message,
  }
  return render(request, 'userDelete.html', params)


# 汎用トップ画面
def hanyoview(request):
  params = {
    "title":'設定',
    "headers":get_hanyo.get_active_header(request.user),
    "select_header_objects":None,
    "login_user":request.user.first_name,
    "user_authority":get_hanyo.get_user_authority(request.user),
    "menu_info":get_hanyo.get_menu_info(),
  }
  return render(request, 'hanyo.html', params)

# 汎用ヘッダーの項目画面
def hanyo_leaf(request, header_code):
  if header_code == 'header':
    header_name = 'ヘッダー項目'
    header_id = '0'
  else:
    header_id,header_name, = get_hanyo.get_active_header_name_id(header_code)
  try:
    login_user_name = request.user.first_name
  except:
    return redirect(to='/logout')
  params = {
    "title":'設定',
    "headers":get_hanyo.get_active_header(request.user),
    "select_header_objects":get_hanyo.get_active_detail(header_code, request.user),
    "login_user":login_user_name,
    "user_authority":get_hanyo.get_user_authority(request.user),
    "menu_info":get_hanyo.get_menu_info(),
    "header_name":header_name,
    "header_code":header_code,
    "header_id":header_id,
  }
  return render(request, 'hanyo.html', params)

# 汎用項目詳細
def detail_hanyo(request,num):
  obj = get_object_or_404(HANYO, id=num)
  authority_list = list(obj.AUTHORITY)
  authority_info = get_hanyo.get_authority(request.user)
  display_authority = ""
  for i in authority_info:
    if i[0] in authority_list:
      display_authority += i[1] + "、"
  display_authority = display_authority[:-1]
  params = {
    "title":'設定項目詳細',
    "user_authority":get_hanyo.get_user_authority(request.user),
    "menu_info":get_hanyo.get_menu_info(),
    "login_user":request.user.first_name,
    "obj":obj,
    "display_authority":display_authority,
  }
  return render(request, 'detailHanyo.html', params)

# 汎用ヘッダー項目削除
def delete_hanyo(request, num):
  obj = HANYO.objects.get(id=num)
  message = '※以下のレコードを削除します。'
  if obj.CODE1 == 'header':
    message = '※以下のレコードと、コード１が「' + obj.CODE2 + '」のレコードを全て削除します。'
  if request.method == 'POST':
    header_code = obj.CODE2
    obj.delete()
    if obj.CODE1 == 'header':
      HANYO.objects.filter(CODE1=header_code).delete()
    if obj.CODE1 == 'user':
      try:
        User.objects.filter(username=obj.CODE2).delete()
      except:
        pass
    return redirect(to='/hanyo_leaf/'+obj.CODE1)
  params = {
    'title':'汎用ヘッダー項目削除',
    'id':num,
    'obj':obj,
    "login_user":request.user.first_name,
    "user_authority":get_hanyo.get_user_authority(request.user),
    "menu_info":get_hanyo.get_menu_info(),
    "message":message,
  }
  return render(request, 'deleteHanyo.html', params)

# 汎用項目詳細新規追加
def add_hanyo(request, header_code):
  form = HanyoForm()
  form.fields['code1'].initial = header_code
  form.fields['createuser'].initial = request.user
  form.fields['updateuser'].initial = request.user
  form.fields['authority'].choices = get_hanyo.get_authority(request.user)
  params = {
    "title":'項目追加',
    "user_authority":get_hanyo.get_user_authority(request.user),
    "menu_info":get_hanyo.get_menu_info(),
    "login_user":request.user.first_name,
    "form":form,
  }
  if request.method == 'POST':
    l = request.POST
    if header_code == 'user':
      user = User.objects.create_user(l['code2'],'',l['controltext1'])
      user.first_name = l['name']
      user.save()
    update_control_int1 = 0
    update_control_int2 = 0
    if l['controlint1']:
      update_control_int1 = l['controlint1']
    if l['controlint2']:
      update_control_int2 = l['controlint2']
    t = HANYO(
      CODE1=l['code1'],
      CODE2=l['code2'],
      CNAME=l['cname'],
      NAME=l['name'],
      STARTDATE=l['startdate'],
      ENDDATE=l['enddate'],
      CONTROLTEXT1=l['controltext1'],
      CONTROLTEXT2=l['controltext2'],
      CONTROLINT1=update_control_int1,
      CONTROLINT2=update_control_int2,
      AUTHORITY=l['authority'],
      CREATEUSER=l['createuser'],
      UPDATEUSER=l['updateuser'],
      )
    print(t)
    t.save()
    return redirect(to='/hanyo_leaf/'+l['code1'])
  return render(request, 'addHanyo.html', params)

# 汎用リーフ項目編集
def edit_hanyo(request, num):
  obj = HANYO.objects.get(id=num)
  initial_dict = dict(
    code1=obj.CODE1,
    code2=obj.CODE2,
    cname=obj.CNAME,
    name=obj.NAME,
    startdate=obj.STARTDATE,
    enddate=obj.ENDDATE,
    controltext1=obj.CONTROLTEXT1,
    controltext2=obj.CONTROLTEXT2,
    controlint1=obj.CONTROLINT1,
    controlint2=obj.CONTROLINT2,
    authority=obj.AUTHORITY,
    createuser=obj.CREATEUSER,
    updateuser=request.user,
    createdate=obj.CREATEDATE,
    )
  form = HanyoForm(request.GET or None, initial=initial_dict)
  form.fields['authority'].choices = get_hanyo.get_authority_edit()
  params = {
    'title':'設定項目編集',
    'id':num,
    'form':form,
    'end_start':None,
    'error_item':"",
    'error_str':"",
    'duplicate':"",
    'duplicate_message':None,
    "login_user":request.user.first_name,
    "user_authority":get_hanyo.get_user_authority(request.user),
    "menu_info":get_hanyo.get_menu_info(),
  }
  if request.method == 'POST':
    l=request.POST
    if l['code1'] == 'user':
      u = User.objects.get(username=obj.CODE2)
      u.username = l['code2']
      u.first_name = l['name']
      u.set_password(l['controltext1'])
      u.save()
    t = HANYO(
      id=num,
      CODE1=l['code1'],
      CODE2=l['code2'],
      CNAME=l['cname'],
      NAME=l['name'],
      STARTDATE=l['startdate'],
      ENDDATE=l['enddate'],
      CONTROLTEXT1=l['controltext1'],
      CONTROLTEXT2=l['controltext2'],
      CONTROLINT1=l['controlint1'],
      CONTROLINT2=l['controlint2'],
      AUTHORITY=l['authority'],
      UPDATEUSER=l['updateuser'],
      CREATEDATE=obj.CREATEDATE,
      )
    t.save()
    return redirect(to='/hanyo_leaf/'+l['code1'])
  return render(request, 'editHanyo.html', params)


def test_paginator(request):
  column_name_obj = RESULT_COLUMN.objects.all()
  column_name_dict = {}
  column_name_list = []
  for col in column_name_obj:
    column_name_dict[col.ORIGINAL_NAME] = col.COLUMN_NAME
    column_name_list.append(col.COLUMN_NAME)
  seikyunengetsu_dict = get_kenchikekka.get_unique_seikyunengetsu()
  seikyunengetsu_list = []
  seikyunengetsu_datetime_list = []
  max_seikyunengetsu = ''
  display_predict_proba_dict = get_kenchi.get_kenchi_predict_proba()
  if seikyunengetsu_dict:
    for seikyunengetsu in seikyunengetsu_dict:
      seikyunengetsu_list.append(seikyunengetsu['SEIKYUNENGETSU'])
    if seikyunengetsu_list:
      for seikyunengetsu in seikyunengetsu_list:
        seikyunengetsu_datetime_list.append(datetime.datetime.strptime(seikyunengetsu, '%Y-%m'))
      if seikyunengetsu_datetime_list:
        max_seikyunengetsu = max(seikyunengetsu_datetime_list).strftime('%Y-%m')

  if not max_seikyunengetsu:
    max_seikyunengetsu = datetime.date.today().strftime('%Y-%m')

  select_form = ResultForm()
  select_form.fields['seikyunengetsu'].initial = max_seikyunengetsu
  dept_tupple, dept_list = get_hanyo.get_dept()
  select_form.fields['dept'].choices = dept_tupple
  select_form.fields['dept'].initial = dept_list
  ward_tupple, ward_list = get_hanyo.get_ward()
  select_form.fields['ward'].choices = ward_tupple
  select_form.fields['ward'].initial = ward_list
  codename_tupple, codename_list = get_kenchi.get_kenchi()
  select_form.fields['codename'].choices = codename_tupple
  select_form.fields['codename'].initial = codename_list
  request_sql = "SELECT id, USER_CHECK, PATIENT_ID, PATIENT_NAMEJ, MEDICAL_CARE_DATE, DEPT, WARD, IN_OUT, CODE, CODENAME, CODE2, CODE2NAME, POINTS, PREDICT_PROBA, ZISSEKI, KENCHI01, KENCHI03, KENCHI05, KENCHI07, KENCHI09, KENCHI11, KENCHI12, KENCHI13, KENCHI14, KENCHI15, KENCHI16, KENCHI17, KENCHI18, KENCHI19, KENCHI20, KENCHI21, KENCHI22, KENCHI50 FROM no_calculation_results WHERE"
  table_name = 'no_calculation_results'
  if max_seikyunengetsu:
    add_seikyunengetsu_sql = " (SEIKYUNENGETSU = '" + max_seikyunengetsu +"')"
    request_sql += add_seikyunengetsu_sql

  max_outputdate = get_kenchikekka.get_unique_outputdate(max_seikyunengetsu, 'no_calculation_results')
  if max_outputdate:
    add_outputdate_sql = " AND (OUTPUTDATE LIKE '" + max_outputdate + "%%')"
    request_sql += add_outputdate_sql

  if request.method == "POST":
    l = request.POST
    santeikoumoku_list = request.POST.getlist('codename')
    select_dept_list = request.POST.getlist('dept')
    select_ward_list = request.POST.getlist('ward')
    table_name = 'no_calculation_results' if l['zisseki'] == '0' else 'calculation_exists_results'
    if l['seikyunengetsu']:
      select_form.fields['seikyunengetsu'].initial = l['seikyunengetsu']
      if l['seikyunengetsu'] in seikyunengetsu_list:
        request_sql = request_sql.replace('no_calculation_results', table_name)
      else:
        table_name = f"{table_name}_bk"
        # 選択された請求年月のデータ更新年月のユニークリストを取得
        max_outputdate = get_kenchikekka.get_unique_outputdate(l['seikyunengetsu'], 'no_calculation_results_bk')
        request_sql = f"SELECT id, USER_CHECK, PATIENT_ID, PATIENT_NAMEJ, MEDICAL_CARE_DATE, DEPT, WARD, IN_OUT, CODE, CODENAME, CODE2, CODE2NAME, POINTS, PREDICT_PROBA, ZISSEKI, KENCHI01, KENCHI03, KENCHI05, KENCHI07, KENCHI09, KENCHI11, KENCHI12, KENCHI13, KENCHI14, KENCHI15, KENCHI16, KENCHI17, KENCHI18, KENCHI19, KENCHI20, KENCHI21, KENCHI22, KENCHI50 FROM {table_name} WHERE"
        add_seikyunengetsu_sql = " (SEIKYUNENGETSU = '" + l['seikyunengetsu'] +"')"
        request_sql += add_seikyunengetsu_sql
        if max_outputdate:
          add_outputdate_sql = " AND (OUTPUTDATE LIKE '" + max_outputdate + "%%')"
          request_sql += add_outputdate_sql       
    if l['check']:
      request_sql += " AND (USER_CHECK = '" + l['check'] + "')"
      select_form.fields['check'].initial = l['check']
    if select_dept_list and not len(dept_list) == len(select_dept_list):
      add_dept = " AND ("
      for d in select_dept_list:
        add_dept += "(DEPT = '" + d + "') OR "
      add_dept = add_dept[:-4]
      add_dept += ")"
      request_sql += add_dept
      select_form.fields['dept'].initial = select_dept_list
    if select_ward_list and not len(ward_list) == len(select_ward_list):
      add_ward = " AND ("
      for w in select_ward_list:
        add_ward += "(WARD = '" + w + "') OR "
      add_ward = add_ward[:-4]
      add_ward += ")"
      request_sql += add_ward
      select_form.fields['ward'].initial = select_ward_list
    if santeikoumoku_list and not len(codename_list) == len(santeikoumoku_list):
      add_codename = " AND ("
      for santeikoumoku in santeikoumoku_list:
        add_codename += "(CODENAME = '" + santeikoumoku + "') OR "
      add_codename = add_codename[:-4]
      add_codename += ")"
      request_sql += add_codename
      select_form.fields['codename'].initial = santeikoumoku_list
    if l['zisseki']:
      request_sql += "AND (ZISSEKI = '" + l['zisseki'] + "')"
      select_form.fields['zisseki'].initial = l['zisseki']
  else:
    request_sql += "AND (ZISSEKI = '0')"


  add_proba_sql = ""
  if display_predict_proba_dict:
    add_proba_sql += " AND ("
    for k, v in display_predict_proba_dict.items():
      if k == '111000110':
        where_sql = f" (CODE = '{k}' AND PREDICT_PROBA >= '{str(v)}' AND IN_OUT = '入院') OR"
      elif k == '113006910':
        where_sql = f" (CODE = '{k}' AND PREDICT_PROBA >= '{str(v)}' AND IN_OUT = '入院' AND KENCHI16 = '0.0') OR"
      elif k == '190101770':
        where_sql = f" (CODE = '{k}' AND PREDICT_PROBA >= '{str(v)}' AND IN_OUT = '入院' AND(KENCHI01 <> '' OR KENCHI03 <> '' OR KENCHI05 <> '' OR KENCHI07 <> '' OR KENCHI09 <> '')) OR"
      elif k == '140033770':
        where_sql = f" (CODE = '{k}' AND PREDICT_PROBA >= '{str(v)}' AND IN_OUT = '入院' AND(KENCHI01 <> '' OR KENCHI03 <> '' OR KENCHI05 <> '' OR KENCHI07 <> '' OR KENCHI09 <> '')) OR"
      elif k == '140053670':
        where_sql = f" (CODE = '{k}' AND PREDICT_PROBA >= '{str(v)}' AND IN_OUT = '入院' AND(KENCHI01 <> '' OR KENCHI03 <> '' OR KENCHI05 <> '' OR KENCHI07 <> '' OR KENCHI09 <> '')) OR"
      elif k == '820100017':
        where_sql = f" (CODE = '{k}' AND PREDICT_PROBA >= '{str(v)}' AND IN_OUT = '入院' AND(KENCHI14 <> '1.0' AND KENCHI14 <> '0.0' AND KENCHI14 <> 'nan')) OR"
      elif k == '190128110':
        where_sql = f" (CODE = '{k}' AND PREDICT_PROBA >= '{str(v)}' AND IN_OUT = '入院' AND CAST(KENCHI16 AS DECIMAL(10,2)) < 20.0) OR"
      else:
        where_sql = " (CODE = '" + k + "' AND PREDICT_PROBA >= " + str(v) + ") OR"
      add_proba_sql += where_sql
    if add_proba_sql:
      add_proba_sql = add_proba_sql[:-3]
      add_proba_sql += ")"

  if add_proba_sql and 'no_calculation_results' in table_name:
    request_sql += add_proba_sql
  request_sql += " ORDER BY CODE"
  objects = paginator.get_page(request_sql)

  context = {
    'objects' : objects,
    'count_row':len(objects),
    'column_name_list': column_name_list,
    'column_name_json': json.dumps(column_name_dict),
    "login_user":request.user.first_name,
    "user_authority":get_hanyo.get_user_authority(request.user),
    "menu_info":get_hanyo.get_menu_info(),
    "form":select_form,
    "select_menu": "paginator",
  }
  return render(request, 'paginator.html', context)

def update_result(request):
  if request.method == 'GET':

    id = request.GET['id']
    user_check = request.GET['user_check']
    field_name = request.GET['field_name']
    update_results.update_results(id, [field_name, user_check])
    # 検知結果のチェックフラグ集計を1に更新
    update_sql = "UPDATE aireceiptapp_hanyo SET CONTROLINT1 = 1 WHERE CODE1 = 'hos_info' AND CODE2 = 'chart_update_flag'"
    get_hanyo.update_hanyo(update_sql)
    return HttpResponse('success')
  else:
    return HttpResponse("unsuccesful")

# 算定項目マスタ画面
def maintenance_item(request):
  items = KENCHI.objects.all().order_by('CODE')
  add_item_all_form = AddItemAllForm()
  add_item_all_form['startdate'].initial = KENCHI.objects.aggregate(Max('STARTDATE'))['STARTDATE__max'] + relativedelta.relativedelta(years=2)
  params = {
    "items":items,
    "login_user":request.user.first_name,
    "user_authority":get_hanyo.get_user_authority(request.user),
    "menu_info":get_hanyo.get_menu_info(),
    "add_item_all_form":add_item_all_form
  }
  return render(request, 'maintenanceItem.html', params)

# 全算定項目追加
def add_allitem(request):
  if request.method == 'POST':
    try:
      startdate = datetime.datetime.strptime(request.POST['startdate'], '%Y-%m-%d')
      enddate = startdate - datetime.timedelta(minutes=1)
      max_startdate = KENCHI.objects.aggregate(Max('STARTDATE'))['STARTDATE__max']
      KENCHI.objects.values().filter(STARTDATE=max_startdate).update(ENDDATE=enddate)
      old = KENCHI.objects.filter(STARTDATE=max_startdate).values('CODE', 'CODE2', 'NAME', 'USE_FLAG', 'PREDICT_PROBA', 'POINTS', 'RULE_FLAG')
      item_objects = []
      for i in old:
        item_objects.append(
          KENCHI(
            CODE=i['CODE'],
            CODE2=i['CODE2'],
            NAME=i['NAME'],
            USE_FLAG=i['USE_FLAG'],
            PREDICT_PROBA=i['PREDICT_PROBA'],
            POINTS=i['POINTS'],
            RULE_FLAG=i['RULE_FLAG'],
            STARTDATE=startdate,
            ENDDATE=datetime.date(2099,12,31),
            CREATEUSER=request.user,
            UPDATEUSER=request.user
            )
          )
      KENCHI.objects.bulk_create(item_objects)
      KENCHI.objects.values().filter(STARTDATE=max_startdate).update(USE_FLAG='0')
      newitems = KENCHI.objects.filter(STARTDATE=startdate).values('CODE', 'CODE2', 'NAME', 'USE_FLAG', 'PREDICT_PROBA', 'POINTS', 'RULE_FLAG', 'STARTDATE', 'ENDDATE')
      logger.info(f'全算定項目を開始日{startdate}で追加しました')
    except Exception as e:
      logger.warning(f'全算定項目の更新に失敗しました')
      logger.warning(e)
    finally:
      pass

  params = {
    "login_user":request.user.first_name,
    "user_authority":get_hanyo.get_user_authority(request.user),
    "menu_info":get_hanyo.get_menu_info(),
    "title": "追加項目完了確認",
    "newitems": newitems
  }
  return render(request, 'addAllItem.html', params)

# 算定項目追加
def create_item(request):
  form = KenchiForm()
  form.fields['createuser'].initial = request.user
  form.fields['updateuser'].initial = request.user
  params = {
    "title":'算定項目追加',
    "user_authority":get_hanyo.get_user_authority(request.user),
    "menu_info":get_hanyo.get_menu_info(),
    "login_user":request.user.first_name,
    "form":form,
  }
  if request.method == 'POST':
    l = request.POST
    t = KENCHI(
      CODE=l['code1'],
      CODE2=l['code2'],
      NAME=l['name'],
      USE_FLAG=l['use_flag'],
      PREDICT_PROBA=l['predict_proba'],
      POINTS=l['points'],
      RULE_FLAG=l['rule_flag'],
      STARTDATE=l['startdate'],
      ENDDATE=l['enddate'],
      NOTE=l['note'],
      SPARE='',
      CREATEUSER=l['createuser'],
      UPDATEUSER=l['updateuser'],
      )
    t.save()
    return redirect(to='/maintenance_item')
  return render(request, 'createItem.html', params)


# 算定項目詳細画面
def detail_item(request, num):
  obj = KENCHI.objects.get(id=num)
  params = {
    "obj":obj,
    "login_user":request.user.first_name,
    "user_authority":get_hanyo.get_user_authority(request.user),
    "menu_info":get_hanyo.get_menu_info(),
  }
  return render(request, 'detailItem.html', params)

# 算定項目編集
def edit_item(request, num):
  obj = KENCHI.objects.get(id=num)
  initial_dict = dict(
    id=num,
    code1=obj.CODE,
    code2=obj.CODE2,
    name=obj.NAME,
    use_flag=obj.USE_FLAG,
    predict_proba=obj.PREDICT_PROBA,
    points=obj.POINTS,
    rule_flag=obj.RULE_FLAG,
    startdate=obj.STARTDATE,
    enddate=obj.ENDDATE,
    note=obj.NOTE,
    createuser=obj.CREATEUSER,
    updateuser=request.user,
    createdate=obj.CREATEDATE,
    )
  form = KenchiForm(request.GET or None, initial=initial_dict)
  params = {
    'title':'算定項目編集',
    'id':num,
    'form':form,
    'end_start':None,
    'error_item':"",
    'error_str':"",
    'duplicate':"",
    'duplicate_message':None,
    "login_user":request.user.first_name,
    "user_authority":get_hanyo.get_user_authority(request.user),
    "menu_info":get_hanyo.get_menu_info(),
  }
  if request.method == 'POST':
    forms = KenchiForm(request.POST)
    l = request.POST
    t = KENCHI(
      id=num,
      CODE=l['code1'],
      CODE2=l['code2'],
      NAME=l['name'],
      USE_FLAG=l['use_flag'],
      PREDICT_PROBA=l['predict_proba'],
      POINTS=l['points'],
      STARTDATE=l['startdate'],
      ENDDATE=l['enddate'],
      NOTE=l['note'],
      RULE_FLAG=l['rule_flag'],
      CREATEUSER=l['createuser'],
      CREATEDATE=obj.CREATEDATE,
      UPDATEUSER=l['updateuser'],
      )
    t.save()
    return redirect(to='/maintenance_item')
  return render(request, 'editItem.html', params)

# 算定項目削除
def delete_item(request, num):
  obj = KENCHI.objects.get(id=num)
  message = '※以下のレコードを削除します。'
  if request.method == 'POST':
    obj.delete()
    return redirect(to='/maintenance_item')
  params = {
    'title':'算定項目削除',
    'id':num,
    'obj':obj,
    "login_user":request.user.first_name,
    "user_authority":get_hanyo.get_user_authority(request.user),
    "menu_info":get_hanyo.get_menu_info(),
    "message":message,
  }
  return render(request, 'deleteItem.html', params)

# カルテファイル設定
def karte_file(request, table_name):
    params = {
        "karte_file_list": KARTE_FILE.objects.all(),
        "karte_column_list": None,
        "title": 'カルテファイル設定',
        "display_name": None,
        "file_code": None,
        "headers": get_hanyo.get_active_header(request.user),
        "login_user": request.user.first_name,
        "user_authority": get_hanyo.get_user_authority(request.user),
        "menu_info": get_hanyo.get_menu_info(),
    }
    if table_name == 'karte_file':
        params['display_name'] = 'カルテファイル設定'
    elif 'kp' in table_name:
        display_name_ = KARTE_FILE.objects.filter(FILE_CODE=table_name)
        for i in display_name_:
            params['display_name'] = i.FILE_NAME
            params['file_code'] = i.FILE_CODE
        params['karte_column_list'] = KARTE_FILE_COLUMN.objects.filter(
            FILE_CODE=table_name)
    elif 'mv' in table_name:
        display_name_ = KARTE_FILE.objects.filter(FILE_CODE=table_name)
        for i in display_name_:
            params['display_name'] = i.FILE_NAME
            params['file_code'] = i.FILE_CODE
        params['karte_column_list'] = KARTE_FILE_COLUMN.objects.filter(
            FILE_CODE=table_name)
    return render(request, 'karte.html', params)

# カルテファイル追加
def add_karte_file(request):
    form = KarteFileForm()
    date_today = datetime.date.today()
    date_today_str = date_today.strftime('%Y-%m-%d')
    form.fields['startdate'].initial = date_today_str
    form.fields['enddate'].initial = '2099-12-31'
    form.fields['createuser'].initial = request.user
    form.fields['updateuser'].initial = request.user
    params = {
        "title": 'カルテファイル追加',
        "duplicate": "",
        "user_authority": get_hanyo.get_user_authority(request.user),
        "menu_info": get_hanyo.get_menu_info(),
        "login_user": request.user.first_name,
        "form": form,
    }
    if request.method == 'POST':
        l = request.POST
        start_end = check_validation.start_end_date(l['startdate'],
                                                    l['enddate'])
        if start_end:
            params['start_end'] = start_end
            params['form'] = KarteFileForm(request.POST)
            print('params["form"] = {}'.format(params['form']))
            return render(request, 'addKarteFile.html', params)
        try:
            t = KARTE_FILE(
                FILE_CODE=l['file_code'],
                FILE_NAME=l['file_name'],
                CONVERSION_FLAG=l['conversion_flag'],
                STARTDATE=l['startdate'],
                ENDDATE=l['enddate'],
                CREATEUSER=l['createuser'],
                UPDATEUSER=l['updateuser'],
            )
            t.save()
            return redirect(to='/karte_file/karte_file')
        except IntegrityError:
            params["duplicate"] = "このファイルコードは既に登録されています"
            params['form'] = KarteFileForm(request.POST)
    return render(request, 'addKarteFile.html', params)

# カルテファイル詳細
def detail_karte_file(request, file_code):
  obj = get_object_or_404(KARTE_FILE, FILE_CODE=file_code)
  params = {
    "title":'カルテファイル詳細',
    "user_authority":get_hanyo.get_user_authority(request.user),
    "menu_info":get_hanyo.get_menu_info(),
    "login_user":request.user.first_name,
    "obj":obj,
  }
  return render(request, 'detailKartefile.html', params)

# カルテファイル編集
def edit_karte_file(request, file_code):
    obj = get_object_or_404(KARTE_FILE, FILE_CODE=file_code)
    initial_dict = dict(
        file_code=obj.FILE_CODE,
        file_name=obj.FILE_NAME,
        conversion_flag=obj.CONVERSION_FLAG,
        startdate=obj.STARTDATE,
        enddate=obj.ENDDATE,
        createuser=obj.CREATEUSER,
        updateuser=request.user,
        createdate=obj.CREATEDATE,
    )
    form = KarteFileForm(request.GET or None, initial=initial_dict)
    params = {
        'title': 'カルテファイル編集',
        'file_code': obj.FILE_CODE,
        'form': form,
        'end_start': None,
        'error_item': "",
        'error_str': "",
        'duplicate': "",
        'duplicate_message': None,
        "login_user": request.user.first_name,
        "user_authority": get_hanyo.get_user_authority(request.user),
        "menu_info": get_hanyo.get_menu_info(),
    }
    if request.method == 'POST':
        l = request.POST
        start_end = check_validation.start_end_date(l['startdate'],
                                                    l['enddate'])
        if start_end:
            params['start_end'] = start_end
            params['form'] = KarteFileForm(request.POST)
            return render(request, 'editKarteFile.html', params)
        try:
            t = KARTE_FILE(
                FILE_CODE=l['file_code'],
                FILE_NAME=l['file_name'],
                CONVERSION_FLAG=l['conversion_flag'],
                STARTDATE=l['startdate'],
                ENDDATE=l['enddate'],
                UPDATEUSER=l['updateuser'],
                CREATEDATE=obj.CREATEDATE,
            )
            t.save()
            return redirect(to='/karte_file/karte_file')
        except IntegrityError:
            params["duplicate"] = "ファイルコードは既に登録されています"
            params['form'] = KarteFileForm(request.POST)
    return render(request, 'editKarteFile.html', params)

# カルテファイル削除
def delete_karte_file(request, file_code):
    obj = KARTE_FILE.objects.get(FILE_CODE=file_code)
    message = '※以下のレコードを削除します。'
    if request.method == 'POST':
        obj.delete()
        return redirect(to='/karte_file/karte_file')
    params = {
        'title': 'カルテファイル削除',
        'file_code': file_code,
        'obj': obj,
        'message': message,
        "login_user": request.user.first_name,
        "user_authority": get_hanyo.get_user_authority(request.user),
        "menu_info": get_hanyo.get_menu_info(),
    }
    return render(request, 'deleteKartefile.html', params)

    # カルテファイル列追加


def add_karte_file_column(request, file_code):
    form = KarteFileFormColumn()
    date_today = datetime.date.today()
    date_today_str = date_today.strftime('%Y-%m-%d')
    form.fields['startdate'].initial = date_today_str
    form.fields['enddate'].initial = '2099-12-31'
    form.fields['createuser'].initial = request.user
    form.fields['updateuser'].initial = request.user
    form.fields['file_code'].initial = file_code
    o = KARTE_FILE.objects.filter(FILE_CODE=file_code)
    title = ''
    for i in o:
        title = i.FILE_NAME
    params = {
        "title": '「' + title + '」列追加',
        "duplicate": "",
        "file_code": file_code,
        "user_authority": get_hanyo.get_user_authority(request.user),
        "menu_info": get_hanyo.get_menu_info(),
        "login_user": request.user.first_name,
        "form": form,
    }
    if request.method == 'POST':
        l = request.POST
        start_end = check_validation.start_end_date(l['startdate'],
                                                    l['enddate'])
        if start_end:
            params['start_end'] = start_end
            params['form'] = KarteFileFormColumn(request.POST)
            return render(request, 'addKarteFileColumn.html', params)
        try:
            t = KARTE_FILE_COLUMN(
                FILE_CODE=l['file_code'],
                COLUMN_NAME=l['column_name'],
                BEFORE_COLUMN_NAME=l['before_column_name'],
                DATA_TYPE=l['data_type'],
                ADD_COLUMN_FLAG=l['add_column_flag'],
                ADD_COLUMN_TEXT=l['add_column_text'],
                STARTDATE=l['startdate'],
                ENDDATE=l['enddate'],
                CREATEUSER=l['createuser'],
                UPDATEUSER=l['updateuser'],
            )
            t.save()
            return redirect(to='/karte_file/' + file_code)
        except IntegrityError:
            params['form'] = KarteFileFormColumn(request.POST)
    return render(request, 'addKarteFileColumn.html', params)


# カルテファイル列詳細
def detail_karte_file_column(request, num):
    obj = get_object_or_404(KARTE_FILE_COLUMN, id=num)
    file_name = get_object_or_404(KARTE_FILE, FILE_CODE=obj.FILE_CODE).FILE_NAME
    params = {
        "title": '「' + file_name + '」列詳細',
        "user_authority": get_hanyo.get_user_authority(request.user),
        "menu_info": get_hanyo.get_menu_info(),
        "login_user": request.user.first_name,
        "obj": obj,
    }
    return render(request, 'detailKartefileColumn.html', params)


# カルテファイル列編集
def edit_karte_file_column(request, num):
    obj = get_object_or_404(KARTE_FILE_COLUMN, id=num)
    initial_dict = dict(
        file_code=obj.FILE_CODE,
        column_name=obj.COLUMN_NAME,
        before_column_name=obj.BEFORE_COLUMN_NAME,
        data_type=obj.DATA_TYPE,
        add_column_flag=obj.ADD_COLUMN_FLAG,
        add_column_text=obj.ADD_COLUMN_TEXT,
        startdate=obj.STARTDATE,
        enddate=obj.ENDDATE,
        createuser=obj.CREATEUSER,
        updateuser=request.user,
        createdate=obj.CREATEDATE,
    )
    form = KarteFileFormColumn(request.GET or None, initial=initial_dict)
    params = {
        'title': '「' + get_object_or_404(KARTE_FILE,
                                         FILE_CODE=obj.FILE_CODE).FILE_NAME + '」列編集',
        'id': num,
        'form': form,
        'end_start': None,
        'error_item': "",
        'error_str': "",
        'duplicate': "",
        'duplicate_message': None,
        "login_user": request.user.first_name,
        "user_authority": get_hanyo.get_user_authority(request.user),
        "menu_info": get_hanyo.get_menu_info(),
    }
    if request.method == 'POST':
        l = request.POST
        start_end = check_validation.start_end_date(l['startdate'],
                                                    l['enddate'])
        if start_end:
            params['start_end'] = start_end
            params['form'] = KarteFileFormColumn(request.POST)
            return render(request, 'editKarteFileColumn.html', params)
        try:
            t = KARTE_FILE_COLUMN(
                id=num,
                FILE_CODE=l['file_code'],
                COLUMN_NAME=l['column_name'],
                BEFORE_COLUMN_NAME=l['before_column_name'],
                DATA_TYPE=l['data_type'],
                ADD_COLUMN_FLAG=l['add_column_flag'],
                ADD_COLUMN_TEXT=l['add_column_text'],
                STARTDATE=l['startdate'],
                ENDDATE=l['enddate'],
                CREATEUSER=l['createuser'],
                UPDATEUSER=l['updateuser'],
                CREATEDATE=obj.CREATEDATE,
            )
            t.save()
            return redirect(to='/karte_file/' + l['file_code'])
        except IntegrityError:
            params["duplicate"] = "ファイルコードは既に登録されています"
            params['form'] = KarteFileFormColumn(request.POST)
    return render(request, 'editKarteFileColumn.html', params)


# カルテファイル削除
def delete_karte_file_column(request, num):
    obj = KARTE_FILE_COLUMN.objects.get(id=num)
    message = '※以下のレコードを削除します。'
    if request.method == 'POST':
        obj.delete()
        return redirect(to='/karte_file/' + obj.FILE_CODE)
    params = {
        'title': 'カルテファイル削除',
        'id': num,
        'obj': obj,
        'message': message,
        "login_user": request.user.first_name,
        "user_authority": get_hanyo.get_user_authority(request.user),
        "menu_info": get_hanyo.get_menu_info(),
    }
    return render(request, 'deleteKartefileColumn.html', params)

# カルテファイル設定
def result_column_list(request):
  params = {
    "obj": RESULT_COLUMN.objects.all(),
    "title": '検知結果列名リスト',
    "headers": get_hanyo.get_active_header(request.user),
    "login_user": request.user.first_name,
    "user_authority": get_hanyo.get_user_authority(request.user),
    "menu_info": get_hanyo.get_menu_info(),
  }
  return render(request, 'resultColumn.html', params)

# 検知結果列名称編集
def edit_result_column(request, num):
  obj = RESULT_COLUMN.objects.get(id=num)
  initial_dict = dict(
    COLUMN_NAME=obj.COLUMN_NAME,
    ORIGINAL_NAME=obj.ORIGINAL_NAME,
    CREATEUSER=obj.CREATEUSER,
    UPDATEUSER=request.user,
  )
  form = ResultColumnForm(request.GET or None, initial=initial_dict)
  if request.method == 'POST':
    r = ResultColumnForm(request.POST, instance=obj)
    r.save()
    return redirect(to='/result_column_list')
  params = {
    'title':'列名称編集',
    'id':num,
    'form':form,
    "login_user":request.user.first_name,
    "user_authority":get_hanyo.get_user_authority(request.user),
    "menu_info":get_hanyo.get_menu_info(),
  }
  return render(request, 'editResultColumn.html', params)

def chart(request, chart_category):
  # 検知結果のチェックが集計テーブルに反映されているか確認
  request_sql = "SELECT CONTROLINT1 FROM aireceiptapp_hanyo WHERE CODE1 = 'hos_info' AND CODE2 = 'chart_update_flag' LIMIT 1"
  rows = get_hanyo.get_hanyo(request_sql)
  update_flag = False
  for row in rows:
    if row['CONTROLINT1'] == '1' or row['CONTROLINT1'] == 1:
      update_flag = True
      break

  # 検知結果のチェックが集計テーブルに反映されていなければ、反映させる
  if update_flag:
    max_seikyunengetsu = update_chart_app.get_max_seikyunengetsu()
    table_name = 'no_calculation_results'
    chart_table = 'indicate'
    update_chart_app.write_add_calc(table_name, max_seikyunengetsu, chart_table)

    # チェック反映フラグを０に更新する
    request_sql = "UPDATE aireceiptapp_hanyo SET CONTROLINT1 = 0 WHERE CODE1 = 'hos_info' AND CODE2 = 'chart_update_flag'"
    get_hanyo.update_hanyo(request_sql)
  form = ChartSelect()
  # 請求年月
  seikyunengetsu_dict = get_kenchikekka.get_unique_seikyunengetsu()
  seikyunengetsu_list = []
  seikyunengetsu_datetime_list = []
  max_seikyunengetsu = ''
  if seikyunengetsu_dict:
    for seikyunengetsu in seikyunengetsu_dict:
      seikyunengetsu_list.append(seikyunengetsu['SEIKYUNENGETSU'])
    if seikyunengetsu_list:
      for seikyunengetsu in seikyunengetsu_list:
        seikyunengetsu_datetime_list.append(datetime.datetime.strptime(seikyunengetsu, '%Y-%m'))
      if seikyunengetsu_datetime_list:
        max_seikyunengetsu = max(seikyunengetsu_datetime_list).strftime('%Y-%m')

  if not max_seikyunengetsu:
    max_seikyunengetsu = datetime.date.today().strftime('%Y-%m')

  form.fields['seikyunengetsu'].initial = max_seikyunengetsu

  table_name = 'no_calculation_results'
  
  if request.method == 'POST':
    seikyunengetsu = request.POST.get('seikyunengetsu')
    if seikyunengetsu:
      max_seikyunengetsu = seikyunengetsu

    form = ChartSelect(request.POST)
  # 最新の更新日時
  request_sql = "SELECT * FROM add_calc WHERE SEIKYUNENGETSU = '" + max_seikyunengetsu + "' AND attribute = '" + chart_category + "'"
  df = get_chart.get_chart(request_sql)
  ai_df, ai_dict = get_chart.df_dict_conversion(df)
  request_sql = "SELECT * FROM indicate WHERE SEIKYUNENGETSU = '" + max_seikyunengetsu + "' AND attribute = '" + chart_category + "'"
  df = get_chart.get_chart(request_sql)
  real_df, real_dict = get_chart.df_dict_conversion(df)

  count_category = '算定項目'

  if chart_category == 'CODENAME':
    master_item_tupple, master_item_list = get_kenchi.get_kenchi()
    column_list = ['項目名','件数','内訳1','件数1','内訳2','件数2','内訳3','件数3','内訳4','件数4','点数']
    count_category = '診療科'
  elif chart_category == 'DEPT':
    master_item_tupple, master_item_list = get_hanyo.get_dept()
    column_list = ['診療科','件数','内訳1','件数1','内訳2','件数2','内訳3','件数3','内訳4','件数4','点数']
  elif chart_category == 'WARD':
    master_item_tupple, master_item_list = get_hanyo.get_ward()
    column_list = ['病棟','件数','内訳1','件数1','内訳2','件数2','内訳3','件数3','内訳4','件数4','点数']

  info_dict = {}
  info_dict['count_category'] = count_category
  info_dict['column_list'] = column_list

  from decimal import Decimal

  def decimal_default_proc(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError
  for col in ai_df.columns:
    if ai_df[col].dtype == 'float64':
      ai_df[col] = ai_df[col].astype('int')
  params = {
    'form':form,
    'ai_table_json':ai_df.to_json(orient='records'),
    'real_table_json':real_df.to_json(orient='records'),
    'chart_category':chart_category,
    'add_item_list':None,
    'ai_chart_json': json.dumps(ai_dict, default=decimal_default_proc),
    'real_chart_json': json.dumps(real_dict, default=decimal_default_proc),
    'info_json': json.dumps(info_dict),
    'column_list':column_list,
    'title':'サマリー',
    "login_user":request.user.first_name,
    "user_authority":get_hanyo.get_user_authority(request.user),
    "menu_info":get_hanyo.get_menu_info(),
  }
  return render(request, 'chart.html', params)

def line_chart(request, chart_category):
  request_sql = "SELECT seikyunengetsu, SUM(total_count) AS SUMCOUNT FROM add_calc WHERE attribute = 'CODENAME' GROUP BY seikyunengetsu ORDER BY seikyunengetsu"
  ai_df = get_chart.get_chart(request_sql)
  ai_dict = {}
  for col in ai_df.columns:
    if ai_df[col].dtype == 'float64':
      ai_df[col] = ai_df[col].astype('int')
    ai_dict[col] = ai_df[col].tolist()
  request_sql = "SELECT seikyunengetsu, SUM(total_count) AS SUMCOUNT FROM indicate WHERE attribute = 'CODENAME' GROUP BY seikyunengetsu ORDER BY seikyunengetsu"
  real_df = get_chart.get_chart(request_sql)
  real_dict = {}
  for col in real_df.columns:
    if real_df[col].dtype == 'float64':
      real_df[col] = real_df[col].astype('int')
    real_dict[col] = real_df[col].tolist()
  params = {
    'form': ChartSelect(),
    'ai_json': json.dumps(ai_dict),
    'real_json': json.dumps(real_dict),
    'title':'サマリー',
    "login_user":request.user.first_name,
    "user_authority":get_hanyo.get_user_authority(request.user),
    "menu_info":get_hanyo.get_menu_info(),
  }
  return render(request, 'lineChart.html', params)

class RequestAmount:
  def __init__(self):
    self.SEIKYUGETSU = ""
    self.COUNTS = ""
    self.DIFFERENCE = ""
    self.RATIO = ""
    self.DIFFERENCE_MONTH = ""
    self.RATIO_MONTH = ""

def request_amount(request):
  request_sql = "SELECT seikyunengetsu, ROUND(SUM(points)*10) AS sp FROM summary GROUP BY seikyunengetsu ORDER BY seikyunengetsu"
  obj = get_request_amount.get_request_amount(request_sql)
  obj_dict = {}
  active_s_list = []
  before_s_list = []
  before_month_s_list = []
  active_c_list = []
  before_c_list = []
  before_month_c_list = []
  table_obj = []
  if obj:
    seikyunengetsu_list = []
    total_list = []
    count_dict = {}
    for o in obj:
      seikyunengetsu_list.append(o['seikyunengetsu'])
      total_list.append(o['sp'])
      count_dict[o['seikyunengetsu']] = o['sp']
    obj_dict['seikyunengetsu_list'] = seikyunengetsu_list
    obj_dict['total_list'] = total_list
    seikyunengetsu_date_list = []
    for s in seikyunengetsu_list:
      seikyunengetsu_date_list.append(datetime.datetime.strptime(s, '%Y-%m'))
    max_s = max(seikyunengetsu_date_list)
    one_year_ago = max_s - relativedelta.relativedelta(years=1)

    for o in obj:
      obj_s_dt = datetime.datetime.strptime(o['seikyunengetsu'], '%Y-%m')
      if one_year_ago < obj_s_dt:
        active_s_list.append(o['seikyunengetsu'])
        active_c_list.append(o['sp'])

    for a in active_s_list:
      o_y_a_d = datetime.datetime.strptime(a, '%Y-%m') - relativedelta.relativedelta(years=1)
      o_m_a_d = datetime.datetime.strptime(a, '%Y-%m') - relativedelta.relativedelta(months=1)
      o_y_a_s = o_y_a_d.strftime('%Y-%m')
      o_m_a_s = o_m_a_d.strftime('%Y-%m')
      before_s_list.append(o_y_a_s)
      before_month_s_list.append(o_m_a_s)

      before_count = 0
      before_month_count = 0
      try:
        before_count = count_dict[o_y_a_s]
      except:
        pass
      try:
        before_month_count = count_dict[o_m_a_s]
      except:
        pass
      before_c_list.append(before_count)
      before_month_c_list.append(before_month_count)
  obj_dict['active_s_list'] = active_s_list
  obj_dict['before_s_list'] = before_s_list
  obj_dict['active_c_list'] = active_c_list
  obj_dict['before_c_list'] = before_c_list
  for aS, bS, aC, bC, bmc in zip(active_s_list, before_s_list, active_c_list, before_c_list, before_month_c_list):
    seikyugetsu = aS[0:4] + '年' + str(int(aS[5:7])) + '月'
    counts = "{:,}".format(int(aC))
    if bC != 0:
      difference = "{:,}".format(int(aC - bC))
      if not '-' in difference and difference != '0':
        difference = '+' + difference
    else:
      difference = ''

    try:
      ratio = (aC - bC) / bC * 100
    except:
      ratio = 0
    if ratio == 0:
      ratio_s = ''
    elif ratio == 1:
      ratio_s = '0'
    elif ratio < 1:
      ratio_s = '-' + str(round(ratio)) + '%'
    else:
      ratio_s = '+' + str(round(ratio)) + '%'
    difference_month = "{:,}".format(int(aC-bmc))
    if not '-' in difference_month and difference_month != '0':
      difference_month = '+' + difference_month
    try:
      ratio_month_work = (aC - bmc) / bmc * 100
      print(f'ratio_month_work = {ratio_month_work}')
    except:
      ratio_month_work = 0
    
    if ratio_month_work > 0:
      ratio_month_s = '+' + str(round(ratio_month_work)) + '%'
    else:
      ratio_month_s = str(round(ratio_month_work)) + '%'

    # 出力をセット
    table_row = RequestAmount()
    table_row.SEIKYUGETSU = seikyugetsu
    table_row.COUNTS = counts
    table_row.DIFFERENCE = difference
    table_row.RATIO = ratio_s
    table_row.DIFFERENCE_MONTH = difference_month
    table_row.RATIO_MONTH = ratio_month_s

    # 出力リストに追加
    table_obj.append( table_row )

  params = {
    'table_obj': table_obj,
    'obj_json': json.dumps(obj_dict),
    'title':'サマリー',
    "login_user": request.user.first_name,
    "user_authority": get_hanyo.get_user_authority(request.user),
    "menu_info": get_hanyo.get_menu_info(),
  }
  return render(request, 'requestAmount.html', params)

def inspection(request):
  pid = ''
  if request.method == 'POST':
    l = request.POST
    pid = l['pid'].replace(' ', '')
  request_sql = "SELECT patient_id, medical_care_date, doctype, care_detail, taii, first_dept, uketukesaisyosinkubun, kaigosien, surgery_start_time, kensakoumoku, dept, karte_text FROM karte_all WHERE patient_id = '" + pid + "'"
  karte_df = get_inspectiondb.get_inspection(request_sql)
  doctype_list= karte_df['doctype'].unique().tolist()
  request_sql = "SELECT file_type, receipt_num, receipt_text FROM receipt_detail WHERE patient_id = '" + pid + "'"
  rece_obj = get_inspectiondb.get_inspection_dict(request_sql)
  profile, rece_view, table_key_dict = get_inspectiondb.create_receiptview(rece_obj)
  rece_df = get_inspectiondb.get_inspection(request_sql)
  receipt_row = False
  if len(rece_df) > 0:
    receipt_row = True
    doctype_list.append('UKE')
  print(doctype_list)
  obj_dict = {}
  obj_dict['doctype_list'] = doctype_list
  obj_dict['receipt_row'] = receipt_row
  logger.info(rece_obj)
  params = {
    'doctype_list': doctype_list,
    'obj_json': json.dumps(obj_dict),
    'profile': profile,
    'rece_view': rece_view,
    'rece_view_json': json.dumps(rece_view),
    'table_key_json': json.dumps(table_key_dict),
    'karte_df': karte_df,
    'rece_obj': json.dumps(rece_obj),
    'rece_df': rece_df,
    'receipt_row': receipt_row,
    'pid':pid,
    'title':'カルテテキスト',
    "login_user": request.user.first_name,
    "user_authority": get_hanyo.get_user_authority(request.user),
    "menu_info": get_hanyo.get_menu_info(),
  }
  return render(request, 'inspection.html', params)


def inspection_index(request):
  os.chdir(base)
  params = {
    "login_user":request.user.first_name,
    "user_authority":get_hanyo.get_user_authority(request.user),
    "title":'カルテ情報検索',
    "menu_info":get_hanyo.get_menu_info(),
  }
  return render(request, 'inspectionIndex.html', params)


def word_search(request):
  word = ''
  if request.method == 'POST':
    l = request.POST
    word = l['word'].replace(' ', '')
  request_sql = f"SELECT patient_id, doctype, taii, karte_text, medical_care_date FROM karte_all WHERE taii LIKE '%%{word}%%' OR karte_text LIKE '%%{word}%%';"
  karte_df = get_inspectiondb.get_inspection(request_sql)
  params = {
    'karte_df': karte_df,
    'word':word,
    'title':'カルテワード検索',
    "login_user": request.user.first_name,
    "user_authority": get_hanyo.get_user_authority(request.user),
  }
  return render(request, 'wordsearch.html', params)

import os
from datetime import datetime as dt
from dateutil.relativedelta import relativedelta
import datetime
import pandas as pd
import shutil
from pathlib import Path

# レセプト処理  
# フォルダ一覧
class ReceFolderInfo:
  def __init__(self):
    self.FOLDER = ""
    self.STATUS = ""
    self.NGFLAG = 0
    self.SEIKYUGETSU = dt.today()
    
def GetReceiptFolderList(receipt_folder, check_files):
  # フォルダ下一覧取得
  files = os.listdir( receipt_folder )
  # フォルダのみを抽出
  folders = [fn for fn in files if os.path.isdir(os.path.join(receipt_folder, fn))]

  # 全フォルダに対し、年月フォルダとチェック対象ファイルの存在チェック
  results = []
  for fol in folders:
    uke_dict = {}
    for check_file in check_files:
      uke_dict[check_file] = 1
    status = ""
    # 年月フォルダまでのフルパスを作成
    folPath = os.path.join(receipt_folder, fol)
    # 年月フォルダ直下のファイルリストを取得
    folPath_list = os.listdir(folPath)

    # 年月フォルダ直下にUKEファイルが存在するか
    ukedirs = [f for f in folPath_list if os.path.isdir(os.path.join(folPath, f))]
    seikyugetsu_list = []
    if not ukedirs:
      pass # 直下にUKEがある場合の処理
    else:
      # 年月フォルダのフォルダリスト
      folPath_dirs = [f for f in folPath_list if os.path.isdir(os.path.join(folPath, f))]
      # 年月フォルダのフォルダリストの中身
      for folPath_dir in folPath_dirs:
        target_dir = os.path.join(folPath, folPath_dir)
        target_files = os.listdir(target_dir)
        target_ukefile_list = [f for f in target_files if os.path.isfile(os.path.join(target_dir, f)) and '.UKE' in f]
        for target_ukefile in target_ukefile_list:
          target_ukefile_path = os.path.join(target_dir, target_ukefile)
          # UKEファイルのIR列を開き、「社保」か「国保」かを判断する
          with open(target_ukefile_path, encoding='cp932') as f:
            l = f.readline()
          ir_list = l.split(',')
          if ir_list[1] == "1":
            add_name = "社保"
          else:
            add_name = "国保"
          check_file_name = target_ukefile[:-4] + add_name + target_ukefile[-4:]
          for k,v in uke_dict.items():
            if check_file_name in k:
              uke_dict[k] = 0
          # IR行の7列目、請求月
          seikyugetsu_list.append(ir_list[7])

    # サブフォルダ下のファイル存在チェック

    for k, v in uke_dict.items():
      # チェック対象ファイルが存在するか？
      if v == 1:
        # 存在しない場合、状態に書き来いm
        if status == "":
          status = "「" + k + "、"
        else:
          status = status + k + "、"

    # 状態が未更新（＝全ファイル検出）の場合、OKとする。
    if status == "":
      status = "指定されたファイルが入っています。"
      ngflag = 0
    else:
      status = status[:-1] + "」のファイルが入っていません。 "
      ngflag = 1

    # フォルダ内UKEファイルの期間が合致しているか
    seikyugetsu = None
    seikyugetsu_date = None
    if seikyugetsu_list:
      if all(x==seikyugetsu_list[0] for x in seikyugetsu_list):
      	seikyugetsu = seikyugetsu_list[0]
      	# 西暦の場合
      	if len(seikyugetsu) == 6:
      		seikyugetsu_date = dt.strptime(seikyugetsu, '%Y%m')
      	# 和暦の場合
      	elif len(seikyugetsu) == 5:
      		seikyugetsu_seireki = years_conversion(seikyugetsu)
      		seikyugetsu_date = dt.strptime(seikyugetsu_seireki, '%Y%m')
      	elif ngflag == 1:
      		status += "UKEファイルの請求年月を確認してください"
      	else:
      		status = "UKEファイルの請求年月を確認してください"
      		ngflag = 1

      elif ngflag == 1:
        status += "フォルダ内UKEファイルの請求年月が一致していません。"
      else:
        status = "フォルダ内UKEファイルの請求年月が一致していません。"
        ngflag = 1

    # 請求年月ひく１
    if seikyugetsu_date:
      seikyugetsu_date = seikyugetsu_date + relativedelta(months=-1)


    # 出力をセット
    result = ReceFolderInfo()
    result.FOLDER = fol # フォルダ名を格納
    result.STATUS = status # 状態を格納
    result.NGFLAG = ngflag
    result.SEIKYUGETSU = seikyugetsu_date

    # 出力リストに追加
    results.append( result )

  return results




# ここから期間のチェック
# 請求年月が５桁だった場合、和暦から西暦に変換する
def years_conversion(year):
    # 和暦とコードの対応表df作成
    master_df = pd.DataFrame({
        'コード': ["1","2","3","4","5"],
        '年': ["1867","1911","1925","1988","2018"],
        '内容': ["明治","大正","昭和","平成","令和"]
    })
    target_year = master_df["年"][master_df["コード"]==year[0]].values[0]
    seireki = int(target_year)  + int(year[1:3])
    seireki_gappi = str(seireki) + year[3:]
    return(seireki_gappi)

# UKEファイルのIE列に記載されている請求年月を取得する
def get_seikyunengetsu(file):
    fileobj = open(file, "r", encoding="cp932")
    year_month = ""
    while True:
        line = fileobj.readline()
        line_list = line.split(',')
        if line_list[0] == "IR":
            year_month = line_list[7]
        else:
            break
    return(year_month)

# 請求年月の前の月を取得する
def date_str_conversion(year_month):
    print('year_month = {}'.format(year_month))
    result_date = dt.strptime(year_month,'%Y%m') # 文字列を日付型に変換
    result_date = result_date + relativedelta(months=-1) # 1カ月前にする
    return(result_date)
    
# フォルダ内のファイルリストを取得する
def getUKEfiles(path):
    files = os.listdir(path)
    UKE_file = [f for f in files if f[-3:] == 'UKE']
    if len(UKE_file) < 4:
      UKE_file = []
      dirs = [f for f in files if os.path.isdir(os.path.join(path, f))]
      for d in dirs:
        target_dir = os.path.join(path,d)
        files = os.listdir(target_dir)
        ukefiles = [f for f in files if f == 'RECEIPTC.UKE' or f == 'RECEIPTD.UKE']
        for ukefile in ukefiles:
          target_path = os.path.join(target_dir, ukefile)
          UKE_file.append(target_path)
    return(UKE_file)

# フォルダ内の全てのUKEファイルの請求年月が合致しているかどうかをチェックする
def chk_seikyunengetsu(path):
  try:
    UKE_files = getUKEfiles(path)
    UKE_seikyunengetsu_list = []
    if UKE_files:
      for file_path in UKE_files:
        year_month = get_seikyunengetsu(file_path)
        # 取得した請求年月が和暦だった場合は西暦に変換する
        if len(year_month) == 5:
          year_month = years_conversion(year_month)
        year_month = date_str_conversion(year_month)
        UKE_seikyunengetsu_list.append(year_month)
      ngflg = all(x==UKE_seikyunengetsu_list[0] for x in UKE_seikyunengetsu_list)
      seikyunengetsu = UKE_seikyunengetsu_list[0]
    else:
      ngflg = False
      seikyunengetsu = ''
    return(ngflg, seikyunengetsu) # ngflg:フォルダ内のUKEファイルが全て一致しているか 
  except:
    pass 
    

# UKEファイルチェック用
class ReceiptPreFolderInfo:
  def __init__(self):
    self.FOLDER = ""
    self.FLG = ""
    self.STATUS = "" 

# UKEファイルのIR行の請求年月を取得
def get_uke_seikyunengetsu(target_folder):
  receipt_dir = os.listdir(target_folder)
  # 年月フォルダリストを作成
  receipt_folders = [f for f in receipt_dir if os.path.isdir(os.path.join(target_folder,f))]
  results = []
  for rece_fol in receipt_folders:
    path = os.path.join(target_folder,rece_fol)
    flg, seikyunengetsu = chk_seikyunengetsu(path)

    # 出力をセット
    result = ReceiptPreFolderInfo()
    result.FOLDER = rece_fol # フォルダ名
    result.FLG = flg # フォルダ内の請求年月が一致していたか
    result.STATUS = seikyunengetsu # 請求年月を格納
    results.append(result)

  return results

def min_max_seikyunengetsu(receipt_folder):
  receipt_pre_object_list = get_uke_seikyunengetsu(receipt_folder)
  seikyunengetsu_list = []
  ng_seikyunengetsu_list = []
  seikyunengetsu_ng_flag = 0
  for i in receipt_pre_object_list:
    if i.FLG:
      seikyunengetsu_list.append(i.STATUS)
    else:
      ng_seikyunengetsu_list.append(i.FOLDER)
      seikyunengetsu_ng_flag = 1
  return(seikyunengetsu_list, ng_seikyunengetsu_list, seikyunengetsu_ng_flag)

# UKEファイルを決められた構成にする
def uke_conversion(input_path, output_path, bk_path, uke_master_t):
  uke_master = [i['CONTROLTEXT2'] for i in uke_master_t]
  input_path = Path(input_path)
  ukefiles = input_path.glob('**/*.UKE')
  out_dir = Path(output_path)
  for ukefile in ukefiles:
    target_uke_file = False
    for um in uke_master:
      if um in ukefile.name:
        target_uke_file = True

    if target_uke_file:

      # 国保or社保判定
      insurance_type = ''
      check_path = str(ukefile.relative_to(input_path))
      if "国保" in check_path:
        insurance_type = "国保"
      elif "社保" in check_path:
        insurance_type = "社保"

      with open(ukefile, encoding='cp932') as f:
        l = f.readlines()
      for line in l:
        work_list = line.split(',')
        if work_list[0] == 'IR':
          siekyugetsu = dt.strptime(work_list[7], '%Y%m')  # 文字列を日付型に変換
          medical_care_month = siekyugetsu + relativedelta(months=-1)
          medical_care_month_str = medical_care_month.strftime('%Y%m')
        elif work_list[0] == 'RE':
          if len(work_list) == 38:
            filename = 'RECEIPTC'
          elif len(work_list) == 30:
            filename = 'RECEIPTD'

      out_month_dir = out_dir / medical_care_month_str
      if not out_month_dir.exists():
        out_month_dir.mkdir(parents=True)

      out_file_dir = out_month_dir / f"{filename}{insurance_type}.UKE"
      shutil.copy(ukefile, out_file_dir)

  # 変換前のレセプトフォルダをバックアップ
  if Path(bk_path).exists():
    shutil.rmtree(Path(bk_path))
  shutil.move(input_path, bk_path)
  if not Path(input_path).exists():
    Path(input_path).mkdir(parents=True)

# receipt_folder内のUKEの請求月を取得する（検知結果を保存する時に使用）
def get_seikyugetsu(target_dir):
  target_dir_in = os.listdir(target_dir)
  month_dirs = [f for f in target_dir_in if os.path.isdir(os.path.join(target_dir, f))]
  seikyugetsu_list = []
  for d in month_dirs:
    target_month_dir = os.path.join(target_dir, d)
    target_month_dir_in = os.listdir(target_month_dir)
    uke_files = [f for f in target_month_dir_in if os.path.isfile(os.path.join(target_month_dir, f)) and '.UKE' in f]
    for uke_file in uke_files:
      target_file = os.path.join(target_month_dir, uke_file)
      with open(target_file, encoding='cp932') as f:
        l = f.readline()
      ir_list = l.split(',')
      seikyugetsu_list.append(dt.strptime(ir_list[7], '%Y%m'))
  seikyugetsu = max(seikyugetsu_list).strftime('%Y-%m')
  return(seikyugetsu_list ,seikyugetsu)  

def receipt_chk_one_month(user_receipt_pre_folder, rece_chk_files):
  results = []
  uke_file_dict = {}
  # 利用者用レセプトデータ格納フォルダに入っているUKEファイルを取得
  receipt_path = Path(user_receipt_pre_folder)
  rece_chk_file_list = list(dict.fromkeys(d["CONTROLTEXT2"] for d in rece_chk_files))
  for file in list(receipt_path.glob('**/*.UKE')):
    if file.name in rece_chk_file_list:
      # 国保or社保判定
      insurance_type = ''
      check_path = str(file.relative_to(receipt_path))
      if "国保" in check_path:
        insurance_type = "国保"
      elif "社保" in check_path:
        insurance_type = "社保"
      with open(file, encoding='cp932') as f:
        rows = f.readlines()

      for row in rows:
        row_list = row.split(',')
        # 社保か国保か判定
        if row_list[0] == 'IR':
          medical_month = f"{(dt.strptime(row_list[7], '%Y%m') + relativedelta(months=-1)).strftime('%Y-%m')}"

        # 医科かDPCかを判定
        if row_list[0] == 'RE':
          receipt_type = ''
          if len(row_list) == 30:
            receipt_type = 'RECEIPTD'
          elif len(row_list) == 38:
            receipt_type = 'RECEIPTC'
      uke_file_dict[f"{receipt_type}{insurance_type}.UKE"] = medical_month
  # チェック対象ファイルの有無
  for rece_chk_file in rece_chk_files:
    status = 'ファイルを格納してください'
    flg = 1
    month = ''
    if rece_chk_file['CONTROLTEXT1'] in uke_file_dict:
      status = f'診療月「{uke_file_dict[rece_chk_file["CONTROLTEXT1"]].replace("-","/")}」のファイルが格納されています'
      flg = 0
      month = uke_file_dict[rece_chk_file["CONTROLTEXT1"]]
    results.append({'file':rece_chk_file['NAME'], 'status':status, 'flg':flg, 'month':month, 'before_filename':rece_chk_file['CONTROLTEXT2']})
  return results

"""
if __name__ == '__main__':
  import folder
  import get_hanyo
  t = datetime.date.today()
  uke_master_t = get_hanyo.get_hanyo(f"SELECT CONTROLTEXT1, CONTROLTEXT2 FROM aireceiptapp_hanyo WHERE CODE1 = 'receipt_file_chk' AND STARTDATE <= '{t}' AND ENDDATE >= '{t}'")
  user_receipt_pre_folder = folder.user_receipt_folder("createmodel")
  receipt_pre_folder = folder.receipt_pre("createmodel")
  receipt_pre_BK_folder = folder.receipt_pre_BK_folder('createmodel')
  admin_rece_backup_folder = folder.admin_rece_backup()
  result = uke_conversion(user_receipt_pre_folder, receipt_pre_folder, admin_rece_backup_folder, uke_master_t)
  print(result)
"""

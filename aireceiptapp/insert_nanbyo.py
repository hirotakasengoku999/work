# -*- coding: utf-8 -*-
import pandas as pd
from pathlib import Path
import MySQLdb
from sqlalchemy import create_engine
import datetime
import sys
from pathlib import Path

# DB情報
db_passwd = 'administrator'
db_name = 'aireceiptdb'

def write_result(input_path, seikyunengetsu):
	# master
	import folder
	from pathlib import Path
	master_df = []
	base = Path(folder.base())
	try:
		file = base / 'aireceipt/data/receipt_code/add_sy_190101770.csv'
		master_df = pd.read_csv(file, engine='python', encoding='cp932', dtype='object', index_col='コード')
	except:
		pass
	# filtered_df = master_df[master_df['コード'] == '3580006']
	print(f"★★★{master_df.loc[3580006]['名称']}")

	# result.csvファイルリストを作る
	files = Path(input_path).glob('**/*.csv')

	import get_kenchi
	code_name_dict = get_kenchi.get_code_name_dict()

	# csvファイルの結合
	list_file = []
	for file in files:
		# ファイル名からレセ電コードを取得
		item_code = file.name[:-4]
		# 難病患者等入院診療加算 190101770 なら該当病名の有無データに書き換える
		if item_code == '190101770':
			usecols = ["カルテ番号等","氏名","_date","入外","診療科名","算定項目名称","算定漏れ確率","算定実績","karute_all_多発性硬化症","karute_all_スモン","karute_all_重症筋無力症","karute_all_筋萎縮性側索硬化症","karute_all_脊髄小脳変性症","karute_all_ハンチントン病","karute_all_進行性核上性麻痺","karute_all_大脳皮質基底核変性症","karute_all_パーキンソン病","karute_all_多系統萎縮症","karute_all_線条体黒質変性症","karute_all_オリーブ橋小脳萎縮症","karute_all_シャイ・ドレーガー症候群","karute_all_プリオン病","karute_all_亜急性硬化性全脳炎","karute_all_ライソゾーム病","karute_all_副腎白質ジストロフィー","karute_all_脊髄性筋萎縮症","karute_all_球脊髄性筋萎縮症","karute_all_慢性炎症性脱髄性多発神経炎","karute_all_後天性免疫不全症候群","karute_all_多剤耐性結核","3409005","3410003","3580006","8835762","3352007","8835986","8843948","3318005","8841403","3320002","8846156","8843952","8843953","8843954","8843934","8848413","8839589","8846211","8846214","8839695","8835990","8832469","8841670","8841400","2793007","8847112"]
			d = pd.read_csv(file, engine='python', usecols=usecols, encoding='cp932')
			# 値が全てゼロの列を除去
			for col in d.columns:
				if not (col in ["カルテ番号等", "氏名", "_date", "入外", "診療科名", "算定項目名称", "算定漏れ確率", "算定実績"]):
					value_unique = list(d[col].unique())
					if value_unique == [0.0]:
						d = d.drop(col, axis=1)
			# 各列を処理して1.0を列名称に変換
			for col in d.columns:
				if not (col in ["カルテ番号等", "氏名", "_date", "入外", "診療科名", "算定項目名称","算定漏れ確率", "算定実績"]):
					d[col] = d[col].replace(1.0, col)
			# 各列の0.0を除去して左詰め
			outdf_datas = []
			for index, value in d.iterrows():
				row_list = list(value)
				target_words = row_list[8:]
				target_words2_tmp = [item for item in target_words if item != 0.0]
				target_words2 = []
				# コードを名称に変換
				for tw2 in target_words2_tmp:
					try:
						target_words2.append(master_df.loc[int(tw2)]['名称'])
					except:
						target_words2.append(tw2)
				if len(target_words2) < 5:
					while len(target_words2) < 5:
						target_words2.append("")
				new_row_list = row_list[:8] + target_words2
				outdf_datas.append(new_row_list)
			columns = ["カルテ番号等","氏名","_date","入外","診療科名","算定項目名称","算定漏れ確率","算定実績","根拠1","根拠2","根拠3","根拠4","根拠5"]
			outdf = pd.DataFrame(outdf_datas, columns=columns)
			outdf['算定項目名称'] = code_name_dict[item_code]
			outdf['CODE'] = item_code
			# データベースの列名にリネーム
			rename_dict = {
				"カルテ番号等":"PATIENT_ID",
				"氏名":"PATIENT_NAMEJ",
				"_date":"MEDICAL_CARE_DATE",
				"入外":"IN_OUT",
				"診療科名":"DEPT",
				"算定項目名称":"CODENAME",
				"算定漏れ確率":"PREDICT_PROBA",
				"算定実績":"ZISSEKI",
				"根拠1":"KENCHI01",
				"根拠2":"KENCHI03",
				"根拠3":"KENCHI05",
				"根拠4":"KENCHI07",
				"根拠5":"KENCHI09"
			}
			outdf = outdf.rename(columns=rename_dict)
			# 足りない列を補完
			for i in ['02', '04', '06', '08', '10']:
				outdf[f"KENCHI{i}"] = 0.0
			for i in range(10,51):
				outdf[f"KENCHI{str(i).zfill(2)}"] = ""
			outdf['PATIENT_ID'] = outdf['PATIENT_ID'].astype('str').str.zfill(8)
			outdf['SEIKYUNENGETSU'] = seikyunengetsu
			list_file.append(outdf)
	# pandasのdataframeに変換
	if len(list_file) > 0:
		df_all = pd.concat(list_file)
		import get_hanyo
		df_no_calculation_results = df_all[df_all['ZISSEKI'] == 0.0]
		result = (get_hanyo.get_hanyo("SELECT DISTINCT OUTPUTDATE FROM no_calculation_results"))
		outputdate = result[0]['OUTPUTDATE'] if len(result) > 0 else datetime.datetime.now()
		print(f"★★★★{outputdate}")
		df_no_calculation_results['OUTPUTDATE'] = outputdate
		df_calculation_exists_results = df_all[df_all['ZISSEKI'] == 1.0]
		result = (get_hanyo.get_hanyo("SELECT DISTINCT OUTPUTDATE FROM calculation_exists_results"))
		outputdate = result[0]['OUTPUTDATE'] if len(result) > 0 else datetime.datetime.now()
		df_calculation_exists_results['OUTPUTDATE'] = outputdate

		con = create_engine('mysql://root:administrator@127.0.0.1/aireceiptdb?charset=utf8')
		df_no_calculation_results.to_sql('no_calculation_results', con=con, if_exists='append', index=None)
		df_calculation_exists_results.to_sql('calculation_exists_results', con=con, if_exists='append', index=None)

if __name__ == '__main__':
# 	import folder
# 	from pathlib import Path
# 	base = Path(folder.base())
# 	file = base/'aireceipt/data/receipt_code/add_sy_190101770.csv'
# 	df = pd.read_csv(file, engine='python', encoding='cp932', dtype='object')
# 	print(df[df['コード'] == '3409005']['名称'][0])

    args = sys.argv
    input_path = args[1]
    seikyunengetsu = args[2]
    output_datetime = ''
    write_result(input_path, seikyunengetsu)

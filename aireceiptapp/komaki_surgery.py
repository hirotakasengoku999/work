import pandas as pd
import os
import glob

def split_by_itemkey(input_path, in_filename, out_filename):
	target_file = os.path.join(input_path, in_filename)
	df = pd.read_csv(target_file, engine='python', encoding='cp932', usecols=['患者ID','手術実施日時表示名称','手術開始時間','テンプレート項目キー','テキスト型データ'], dtype={'患者ID':'object'})
	df = df[df['テンプレート項目キー'] == 4102846]
	df = df.drop('テンプレート項目キー', axis=1)
	output_file = os.path.join(input_path, out_filename)
	df.to_csv(output_file, index=False, encoding='cp932')
	return('ok')

# if __name__ == '__main__':

# 	filename = '【a.iブレーン】 手術記録・手術歴（実施・レポート・レポート明細）.csv'
# 	input_path = os.getcwd()
# 	result = split_by_itemkey(input_path, filename)

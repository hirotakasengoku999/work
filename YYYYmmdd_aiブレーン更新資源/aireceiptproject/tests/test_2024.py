import pandas as pd

def get_pid_list(file_path):
    df = pd.read_csv(file_path, engine='python', encoding='cp932', dtype='object')
    # 算定漏れ確率が0.0を除外
    df = df[df['算定漏れ確率'] != "0"]
    df = df[df['算定漏れ確率'] != "0.0"]
    df = df[df['算定漏れ確率'] != "0.00"]
    df = df[df['算定漏れ確率'] != "0.000"]

    # 算定実績=1を除外
    df = df[df['算定実績'] != "1"]
    df = df[df['算定実績'] != "1.0"]
    df = df[df['算定実績'] != "1.00"]
    df = df[df['算定実績'] != "1.000"]

    # 算定漏れのカルテ番号等を取得
    pid_list = set(df['カルテ番号等'].tolist())
    return pid_list


def compare_pid_lists(expected_pid_list, corrected_pid_list):
    # 修正前に存在していないカルテ番号等が修正後に存在するか確認
    new_detections = corrected_pid_list - expected_pid_list
    if new_detections:
        print("修正後に新たに検知されたカルテ番号等:")
        for pid in new_detections:
            print(pid)
        print("テスト結果: NG")
    else:
        print("修正後に新たに検知されたカルテ番号等はありません。テスト結果: OK")


if __name__ == '__main__':
    # 修正前の初診料検知結果
    expected_results = input("修正前の初診料検知結果 111000110.csv のパスを入力してください: ")
    # 修正後の初診料検知結果
    corrected_results = input("修正後の初診料検知結果 111000110.csv のパスを入力してください: ")

    # 修正前の初診料算定漏れリスト
    expected_pid_list = get_pid_list(expected_results)

    # 修正後の初診料算定漏れリスト
    corrected_pid_list = get_pid_list(corrected_results)




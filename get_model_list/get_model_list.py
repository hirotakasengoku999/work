from pathlib import Path
import json

def get_model_list(in_dir: Path) -> list:

    result = []
    sav_list = [file.name.replace('_result.sav', '') for file in in_dir.glob('*.sav')]
    pickle_list = [file.name.replace('.pickle', '') for file in in_dir.glob('*.pickle')]
    csv_list = [file.name.replace('_importance.csv', '') for file in in_dir.glob('*.csv')]

    for sav in sav_list:
        if sav in pickle_list and sav in csv_list:
            result.append(sav)

    return result

def get_rule_list(json_file: Path) -> list:
    # json_fileをdict型に変換
    with open(json_file, 'r', encoding='utf8') as f:
        json_dict = json.load(f)

    result = []

    for key, value in json_dict.items():
        for sub_key, sub_value in value.items():
            if sub_key == 'target':
                result.append(sub_value)

    return result

def main():
    base = Path('C:/aibrain_main/aireceiptproject')
    in_dir = base/'data/model/model'
    models = get_model_list(in_dir)
    in_dir = base/'aireceipt/data/config/r1_rule_execution.json'
    rule_list = get_rule_list(in_dir)
    codes = models + rule_list
    print(','.join(codes))

if __name__ == '__main__':
    main()
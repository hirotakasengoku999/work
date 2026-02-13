from pathlib import Path
import shutil
import datetime

def get_files() -> list:
    # 対象ファイルを読み込み、リストに変換
    # cp932で読み込んでみて失敗したらutf8
    try:
        with open('updated_files.txt', encoding='cp932') as f:
            rows = f.readlines()
    except UnicodeDecodeError:
        with open('updated_files.txt', encoding='utf8') as f:
            rows = f.readlines()

    files = [row.strip() for row in rows]
    return files

def copy_file(in_dir: Path, out_dir: Path, files: list) -> str:
    # 対象ファイルのみout_dirにまとめる
    try:
        for file in files:
            in_file = in_dir/file
            if not in_file.name in ['.gitkeep', 'README.md', '.gitignore']:
                if not in_file.exists():
                    print(f"{in_file} は存在しません")
                    continue
                out_file = out_dir/file
                if not out_file.parent.exists():
                    out_file.parent.mkdir(parents=True)
                shutil.copy2(in_file, out_file)
                print(f"{file}をコピーしました")
        return f"対象ファイルを{out_dir}にコピーしました"
    except Exception as e:
        return f"エラー: {e}"

def main():
    in_dir = Path("C:/aibrain_main/aireceiptproject")
    # システム日付をYYYYmmddの形式で取得
    out_dir = Path(f"C:/aiブレーン/作業用/BK/modules/{datetime.datetime.now():%Y%m%d}/aireceiptproject")
    files = get_files()
    print(copy_file(in_dir, out_dir, files))

    in_dir = Path.cwd() / 'aireceiptproject'
    out_dir = Path("C:/aibrain_main/aireceiptproject")
    print(copy_file(in_dir, out_dir, files))

    input("終了するにはエンターキーを押してください...")


if __name__ == '__main__':
    main()
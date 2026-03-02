from pathlib import Path
import shutil

# ファイル一覧を取得
def rm_old(in_dir, num):
    if in_dir.exists():
        while True:
            dir_g = [x for x in in_dir.iterdir()]
            if len(dir_g) > num:
                dir_d = {}
                for x in dir_g:
                    dir_d[x.stat().st_mtime] = x
                delete_data = dir_d[min(dir_d)]
                # ディレクトリかファイルか
                if delete_data.is_dir():
                    shutil.rmtree(delete_data)
                elif delete_data.is_file():
                    delete_data.unlink()
            else:
                break
    else:
        pass

if __name__ == '__main__':
    in_dir = Path.cwd()/'data'
    num = 4
    rm_old(in_dir, num)

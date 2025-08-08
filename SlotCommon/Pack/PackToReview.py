import os
import hashlib
import shutil
import time
import stat
import json


def calculate_sha1(file_path):
    """計算指定文件的 SHA-1"""
    sha1 = hashlib.sha1()
    try:
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                sha1.update(chunk)
        return sha1.hexdigest()
    except Exception as e:
        print(f"無法計算 {file_path} 的 SHA-1：{e}")
        return None


def calculate_sha1_for_folder(folder_path):
    """計算資料夾中所有文件的 SHA-1"""
    result = {}
    for root, dirs, files in os.walk(folder_path):
        skip_folder = [".git", "Docker"]
        is_skip = False
        for i in skip_folder:
            if i not in root:
                continue
            is_skip = True

        if is_skip:
            continue

        for file in files:
            file_path = os.path.join(root, file)
            sha1_value = calculate_sha1(file_path)
            if sha1_value:
                result[file_path] = sha1_value
    return result


def handle_remove_readonly(func, path, exc_info):
    os.chmod(path, stat.S_IWRITE)  # 設置文件為可寫
    func(path)  # 重試刪除


def create_tar_gz(folder_path, output_filename):
    """將資料夾打包成 tar.gz"""
    print(f"打包 {folder_path} 為 {output_filename}.tar.gz")
    tar_gz_path = shutil.make_archive(output_filename, 'gztar', folder_path)
    time.sleep(0.5)
    return tar_gz_path

def read_json_settings(file_path):
    """讀取 JSON 設定檔"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"無法讀取 {file_path}：{e}")
        return None

def main():
    # 檢查是否存在設定檔
    script_dir = os.path.dirname(os.path.abspath(__file__))
    setting_path = os.path.join(script_dir, "setting.json")
    if not os.path.exists(setting_path):
        print("找不到設定檔 setting.json")
        return
    # 讀取設定檔
    setting = read_json_settings("./setting.json")
    pack_ver = setting.get("pack_ver", "1.0.0")
    game_name = setting.get("game_name", "CashKing")
    game_git_tag = setting.get("game_git_tag", "1.0.0")
    slot_common_git_tag = setting.get("slot_common_git_tag", "1.0.3")
    slot_server_git_tag = setting.get("slot_server_git_tag", "1.0.3")
    folder_path = os.path.join(script_dir, f"GameSend_{pack_ver}/")

    # 檢查資料夾是否存在
    if os.path.exists(folder_path):
        # 移除資料夾
        shutil.rmtree(folder_path, onerror=handle_remove_readonly)

    git_clone_cmds = [
        f'git clone --branch {slot_common_git_tag} --depth 1 "https://github.com/IGS-ARCADE-DIVISION-RD4-SLOT/Slot_Server_SlotCommon" "{folder_path}SlotCommon"',
        f'git clone --branch {game_git_tag} --depth 1 "https://github.com/IGS-ARCADE-DIVISION-RD4-SLOT/Slot_Server_{game_name}" "{folder_path}{game_name}"',
        f'git clone --branch {slot_server_git_tag} --depth 1 "https://github.com/IGS-ARCADE-DIVISION-RD4-SLOT/Slot_Server_SlotServer" "{folder_path}SlotServer"',
    ]

    # clone the repositories
    for cmd in git_clone_cmds:
        print(f"執行指令：{cmd}")
        os.system(cmd)

    time.sleep(1)

    # delete the .git folders
    for root, dirs, files in os.walk(folder_path):
        if "Docker" in root:
            shutil.rmtree(os.path.join(root), onerror=handle_remove_readonly)
        if ".git" in dirs:
            shutil.rmtree(os.path.join(root, ".git"), onerror=handle_remove_readonly)

    sha1_results = calculate_sha1_for_folder(folder_path)
    output_file = os.path.join(folder_path, "SHA1.csv")

    # sort the dictionary by key
    sha1_results = dict(sorted(sha1_results.items()))

    with open(output_file, "w") as f:
        for file_path, sha1 in sha1_results.items():
            f.write(f"{file_path},{sha1}\n")

    print(f"SHA1 計算完成，輸出檔案：{output_file}")

    # Pack the 3 cloned git repositories into tar.gz
    pack_result = [
        create_tar_gz(f"{folder_path}SlotCommon", f"{folder_path}SlotCommon"),
        create_tar_gz(f"{folder_path}CashKing", f"{folder_path}CashKing"),
        create_tar_gz(f"{folder_path}SlotServer", f"{folder_path}SlotServer")
    ]
    for i in pack_result:
        print(f"打包完成：{i}")

    time.sleep(2)

    # remove the folders
    shutil.rmtree(f"{folder_path}SlotCommon", onerror=handle_remove_readonly)
    time.sleep(2)
    shutil.rmtree(f"{folder_path}CashKing", onerror=handle_remove_readonly)
    time.sleep(2)
    shutil.rmtree(f"{folder_path}SlotServer", onerror=handle_remove_readonly)

    print("按下 Enter 鍵結束程式...")
    input()


if __name__ == "__main__":
    main()

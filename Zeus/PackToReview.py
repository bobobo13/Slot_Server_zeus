import os
import hashlib
import shutil
import time
import stat


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
    tar_gz_path = shutil.make_archive(output_filename, 'gztar', folder_path)
    time.sleep(0.5)
    return tar_gz_path


def main():
    # ask the user for the tag name
    tag_name = input("Please enter the tag name: ")
    folder_path = f"./GameSend_{tag_name}/"

    git_clone_cmds = [
        f'git clone --branch 1.0.3 --depth 1 "https://github.com/IGS-ARCADE-DIVISION-RD4/AW_SlotCommon" "{folder_path}SlotCommon"',
        f'git clone --branch {tag_name} --depth 1 "https://github.com/IGS-ARCADE-DIVISION-RD4/AW_Slot_Zeus" "{folder_path}Zeus"',
        f'git clone --branch 1.0.3 --depth 1 "https://github.com/IGS-ARCADE-DIVISION-RD4/AW_SlotServer" "{folder_path}SlotServer"',
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
        create_tar_gz(f"{folder_path}SlotCommon", f"./{folder_path}SlotCommon"),
        create_tar_gz(f"{folder_path}Zeus", f"./{folder_path}Zeus"),
        create_tar_gz(f"{folder_path}SlotServer", f"./{folder_path}SlotServer")
    ]
    for i in pack_result:
        print(f"打包完成：{i}")

    time.sleep(2)

    # remove the folders
    shutil.rmtree(f"{folder_path}SlotCommon", onerror=handle_remove_readonly)
    time.sleep(2)
    shutil.rmtree(f"{folder_path}Zeus", onerror=handle_remove_readonly)
    time.sleep(2)
    shutil.rmtree(f"{folder_path}SlotServer", onerror=handle_remove_readonly)

    print("按下 Enter 鍵結束程式...")
    input()


if __name__ == "__main__":
    main()

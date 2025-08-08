FROM python312_slot_base

ENV TZ="Asia/Taipei"


# step1
# 將 Dockerfile 所在目錄下的所有檔案複製到 Image 的工作目錄 /code 底下
ARG pkg
ARG docker_file_path
ARG app_path=/service
ARG app=SlotService

COPY ${pkg} .
RUN mkdir -p ${app_path}/${app}/
RUN tar -zxvf ./${pkg} -C ${app_path}/${app}/ --strip-components=1

COPY /Config/SlotService /etc/igs/SlotService/Game/config
RUN mkdir -p /etc/igs/
COPY ${docker_file_path}/requirements.txt ${app_path}/${app}/


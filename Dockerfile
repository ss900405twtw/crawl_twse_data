# 使用 Ubuntu 20.04 LTS 基礎鏡像
FROM ubuntu:20.04

# 避免在安裝過程中出現提示
ENV DEBIAN_FRONTEND=noninteractive

# 更新軟件包列表並安裝 Python 3.8
RUN apt-get update && \
    apt-get install -y python3.8 python3.8-dev python3-pip && \
    apt-get install -y libmysqlclient-dev pkg-config && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*



# 設置 Python 3.8 為預設的 Python 版本
RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.8 1

# 確保pip是最新的
RUN python3 -m pip install --upgrade pip

# 設置工作目錄
WORKDIR /app

# 將你的 Python 程式碼複製到容器中
COPY requirements.txt  /app

# 安裝你的 Python 程式所需的依賴
RUN pip3 install -r requirements.txt


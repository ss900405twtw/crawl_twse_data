docker build -t my_ubuntu20 .
docker run --add-host=host.docker.internal:host-gateway -v <src_path>:/app -it --rm my_ubuntu20 python3 main.py -t tw_stock_price_day_twse
docker run -v /home/ss900405tw/Desktop/crawl_twse_data:/app -it --rm my_ubuntu20 python3 main.py -t tw_stock_price_day_twse

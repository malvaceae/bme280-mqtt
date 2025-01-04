import argparse
import json
import time

from awscrt import mqtt
from awsiot import mqtt_connection_builder
from bme280 import BME280

if __name__ == "__main__":
    # コマンドライン引数
    parser = argparse.ArgumentParser()
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--port", default=8883, type=int)
    parser.add_argument("--cert", required=True)
    parser.add_argument("--key", required=True)
    parser.add_argument("--ca_file", required=True)
    parser.add_argument("--client_id", required=True)
    parser.add_argument("--topic", required=True)
    args = parser.parse_args()

    # BME280
    bme280 = BME280()
    bme280.open(0, 0)

    try:
        # 各種設定
        bme280.setup(
            osrs_t=0b010, # 気温 オーバーサンプリング x 2
            osrs_p=0b101, # 気圧 オーバーサンプリング x 16
            osrs_h=0b001, # 湿度 オーバーサンプリング x 1
            mode=0b11,    # ノーマルモード
            t_sb=0b000,   # 測定待機時間 0.5ms
            filter=0b100, # IIRフィルタ係数 16
            spi3w_en=0b0, # 4線式SPI
        )

        # MQTTコネクション
        mqtt_connection = mqtt_connection_builder.mtls_from_path(
            endpoint=args.endpoint,
            port=args.port,
            cert_filepath=args.cert,
            pri_key_filepath=args.key,
            ca_filepath=args.ca_file,
            client_id=args.client_id,
            clean_session=False,
            keep_alive_secs=30,
        )

        # 接続
        print("接続中…")
        connect_future = mqtt_connection.connect()
        connect_future.result()
        print("接続完了")

        try:
            while True:
                # 現在時刻のミリ秒が0になるまで待機
                time.sleep(1 - (time.time() % 1))

                # タイムスタンプ
                timestamp = int(time.time())

                # 1分ごと
                if timestamp % 60 == 0:
                    # 気温・気圧・湿度
                    temperature, pressure, humidity = bme280.measure()

                    print(f"タイムスタンプ: {timestamp}")
                    print(f"気温: {temperature:7.2f} ℃")
                    print(f"気圧: {pressure:7.2f} hPa")
                    print(f"湿度: {humidity:7.2f} ％")
                    print()

                    # 送信
                    mqtt_connection.publish(
                        topic=args.topic,
                        payload=json.dumps({
                            "timestamp": timestamp,
                            "temperature": temperature,
                            "pressure": pressure,
                            "humidity": humidity,
                        }),
                        qos=mqtt.QoS.AT_LEAST_ONCE,
                    )
        finally:
            # 切断
            print("切断中…")
            disconnect_future = mqtt_connection.disconnect()
            disconnect_future.result()
            print("切断完了")
    finally:
        bme280.close()

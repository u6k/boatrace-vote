import io
import os
import sys
from datetime import datetime, timedelta

import pandas as pd
import utils

L = utils.get_logger("vote_tickets")


def get_target_race(current_datetime, s3_vote_folder):
    """投票対象レースを見つける
    """

    # レース一覧データを取得する
    s3 = utils.S3Storage()
    obj = s3.get_object(f"{s3_vote_folder}/df_racelist.pkl.gz")

    with io.BytesIO(obj) as b:
        df_racelist = pd.read_pickle(b, compression="gzip")

    L.debug("レース一覧データ")
    L.debug(df_racelist)

    L.debug("現在時刻の1時間前後のレース")
    start_t = current_datetime - timedelta(hours=1)
    end_t = current_datetime + timedelta(hours=1)
    df_tmp = df_racelist[(start_t <= df_racelist["start_datetime"]) & (df_racelist["start_datetime"] < end_t)]
    L.debug(df_tmp)

    # 未投票、現在時刻+10分より前、最新、のレースを特定する
    df_target_race = df_racelist[df_racelist["vote_timestamp"].isnull()]

    t = current_datetime + timedelta(minutes=10)
    df_target_race = df_target_race[df_target_race["start_datetime"] < t]

    if len(df_target_race) > 0:
        df_target_race = df_target_race.tail(1)

        L.debug("対象レース")
        L.debug(df_target_race)
    else:
        L.debug("対象レースがない")

        df_target_race = None

    return df_target_race, df_racelist


def output_parameter_files(df_arg_race):
    """クロールを実行するためのパラメーターファイルを出力する。
    """

    # 値を抽出する
    race_id = df_arg_race["race_id"].values[0]
    race_round = df_arg_race["race_round"].values[0]
    place_id = df_arg_race["place_id"].values[0]
    start_datetime = df_arg_race["start_datetime"].dt.strftime("%Y%m%d").values[0]

    # パラメーターを生成する
    s3_feed_url = f"s3://boatrace/feed/race_{race_id}.json"
    L.debug(f"S3フィードURL: {s3_feed_url}")

    crawl_url = f"https://www.boatrace.jp/owpc/pc/race/racelist?rno={race_round}&jcd={place_id}&hd={start_datetime}"
    L.debug(f"クロールURL: {crawl_url}")

    # ファイルに出力する
    with open("/var/output/s3_feed_url.txt", mode="w") as f:
        f.write(s3_feed_url)

    with open("/var/output/crawl_url.txt", mode="w") as f:
        f.write(crawl_url)


def main():
    """投票対象レースを発見するメイン処理
    """

    # 設定を取得する
    current_datetime = datetime.strptime(os.environ["CURRENT_DATETIME"], "%Y-%m-%d %H:%M:%S")
    L.info(f"現在時刻: {current_datetime}")

    s3_vote_folder = os.environ["AWS_S3_VOTE_FOLDER"]
    L.info(f"S3投票データフォルダ: {s3_vote_folder}")

    # 日レース一覧データを読み込み、投票対象レースを特定する
    L.info("# 日レース一覧データを読み込み、投票対象レースを特定する")

    df_target_race, df_racelist = get_target_race(current_datetime, s3_vote_folder)

    L.info("対象レース")
    L.info(df_target_race)

    if df_target_race is None:
        L.info("対象レースが存在しないため、処理を終了する")
        sys.exit(1)

    # クロール用パラメーターファイルを出力する
    L.info("# クロール用パラメーターファイルを出力する")

    output_parameter_files(df_target_race)


if __name__ == "__main__":
    main()

import io
import os
from datetime import datetime, timedelta

import pandas as pd
import utils

L = utils.get_logger("find_payoff_race")


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

    # 投票済み、未清算、現在時刻-15分より前、最新、のレースを特定する
    df_target_race = df_racelist.dropna(subset="vote_timestamp")
    df_target_race = df_target_race[df_target_race["result_timestamp"].isnull()]

    t = current_datetime - timedelta(minutes=15)
    df_target_race = df_target_race[df_target_race["start_datetime"] < t]

    if len(df_target_race) > 0:
        df_target_race = df_target_race.tail(1)

        L.debug("対象レース")
        L.debug(df_target_race)
    else:
        L.debug("対象レースがない")

        df_target_race = None

    return df_target_race, df_racelist


def output_parameter_files(df_arg_race, df_racelist):
    """クロールを実行するためのパラメーターファイルを出力する。
    """

    if df_arg_race is not None:
        # 投票対象レースが存在する場合、ファイル出力する
        df_arg_race.to_csv("/var/output/df_payoff_race.csv")
        df_arg_race.to_pickle("/var/output/df_payoff_race.pkl.gz")
    else:
        # 存在しない場合、ファイルを削除する
        if os.path.isfile("/var/output/df_payoff_race.csv"):
            os.remove("/var/output/df_payoff_race.csv")
        if os.path.isfile("/var/output/df_payoff_race.pkl.gz"):
            os.remove("/var/output/df_payoff_race.pkl.gz")

    df_racelist.to_csv("/var/output/df_racelist.csv")
    df_racelist.to_pickle("/var/output/df_racelist.pkl.gz")


def main():
    """清算対象レースを発見するメイン処理
    """

    # 設定を取得する
    current_datetime = datetime.strptime(os.environ["CURRENT_DATETIME"], "%Y-%m-%d %H:%M:%S")
    L.info(f"現在時刻: {current_datetime}")

    s3_vote_folder = os.environ["AWS_S3_VOTE_FOLDER"]
    L.info(f"S3投票データフォルダ: {s3_vote_folder}")

    # 日レース一覧データを読み込み、清算対象レースを特定する
    L.info("# 日レース一覧データを読み込み、清算対象レースを特定する")

    df_target_race, df_racelist = get_target_race(current_datetime, s3_vote_folder)

    L.info("対象レース")
    L.info(df_target_race)

    # クロール用パラメーターファイルを出力する
    L.info("# クロール用パラメーターファイルを出力する")

    output_parameter_files(df_target_race, df_racelist)


if __name__ == "__main__":
    main()

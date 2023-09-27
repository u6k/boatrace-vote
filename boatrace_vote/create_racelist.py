import io
import os
from datetime import datetime, timedelta

import pandas as pd
import utils

L = utils.get_logger("create_racelist")


def create_racelist(target_date, s3_pred_folder, s3_vote_folder):
    s3 = utils.S3Storage()

    # 予測データを取得する
    obj = s3.get_object(f"{s3_pred_folder}/df_pred.pkl.gz")

    with io.BytesIO(obj) as b:
        df_pred = pd.read_pickle(b, compression="gzip")

    L.debug("予測データ")
    L.debug(df_pred)

    # レース一覧データを構築する
    start_date = target_date
    end_date = start_date + timedelta(days=1)

    start_date = start_date.strftime("%Y-%m-%d") + " 00:00:00"
    end_date = end_date.strftime("%Y-%m-%d") + " 00:00:00"

    df_racelist = df_pred[[
        "race_id",
        "start_datetime",
        "place_id",
        "race_round",
    ]] \
        .query(f"'{start_date}'<=start_datetime<'{end_date}'") \
        .drop_duplicates() \
        .sort_values(["start_datetime"]) \
        .reset_index(drop=True)

    df_racelist["vote_timestamp"] = None
    df_racelist["vote_amount"] = None
    df_racelist["payoff_timestamp"] = None
    df_racelist["payoff_amount"] = None

    L.debug("レース一覧データ")
    L.debug(df_racelist)

    # アップロードする
    with io.BytesIO() as b:
        df_racelist.to_pickle(b, compression="gzip")
        key = f"{s3_vote_folder}/df_racelist.pkl.gz"

        s3.put_object(key, b.getvalue())

        L.debug(f"アップロード: {key}")


if __name__ == "__main__":
    target_date = datetime.strptime(os.environ["VOTE_TARGET_DATE"], "%Y-%m-%d")
    L.info(f"対象日: {target_date}")

    s3_pred_folder = os.environ["AWS_S3_PRED_FOLDER"]
    L.info(f"S3予測データフォルダ: {s3_pred_folder}")

    s3_vote_folder = os.environ["AWS_S3_VOTE_FOLDER"]
    L.info(f"S3投票データフォルダ: {s3_vote_folder}")

    create_racelist(target_date, s3_pred_folder, s3_vote_folder)

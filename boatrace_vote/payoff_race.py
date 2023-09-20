import io
import json
import os
import sys
from datetime import datetime, timedelta

import pandas as pd
import utils

L = utils.get_logger("payoff_race")


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



def get_vote_race(df_arg_race, s3_vote_folder):
    """投票データを取得する。
    """

    race_id = df_arg_race["race_id"].values[0]

    s3 = utils.S3Storage()
    obj = s3.get_object(f"{s3_vote_folder}/df_vote_{race_id}.pkl.gz")

    with io.BytesIO(obj) as b:
        df_vote = pd.read_pickle(b, compression="gzip")

    return df_vote









def get_feed_data(df_arg_race):
    """フィードjsonデータを取得する。
    """

    # フィードjsonを読み込む
    race_id = df_arg_race["race_id"].values[0]

    s3 = utils.S3Storage()
    obj = s3.get_object(f"feed/race_{race_id}_after.json")

    with io.BytesIO(obj) as b:
        json_data = json.loads(b.getvalue())

    # パースする
    return utils.parse_feed_json(json_data)








def payoff_race(current_datetime, df_arg_racelist, df_arg_race, df_arg_odds, df_arg_payoff, df_arg_vote):
    """対象レースを清算(仮)する。
    """

    race_id = df_arg_race["race_id"].values[0]

    if df_arg_race["vote_timestamp"].values[0] is None:
        # 未投票の場合、処理をしない
        return None, df_arg_racelist

    if df_arg_odds is None:
        # 中止になった場合、0で清算する
        df_vote["odds_fix"] = 0.0
        df_vote["payoff_amount"] = 0.0

        idx = df_arg_racelist[df_arg_racelist["race_id"] == race_id].index[0]

        df_arg_racelist.at[idx, "result_timestamp"] = current_datetime
        df_arg_racelist.at[idx, "return_amount"] = 0

        return df_vote, df_arg_racelist

    # TODO: まだ結果が出ていない場合
    # TODO: payoff/100.0する

    if len(df_arg_vote) == 0:
        # 舟券に投票しなかった場合、0で清算する
        idx = df_arg_racelist[df_arg_racelist["race_id"] == race_id].index[0]

        df_arg_racelist.at[idx, "result_timestamp"] = current_datetime
        df_arg_racelist.at[idx, "return_amount"] = 0

        return None, df_arg_racelist

    # 普通に投票した場合、
    # オッズを結合する
    df_odds = df_arg_odds.query("bet_type==1")[[
        "race_id",
        "bracket_number_1",
        "odds_1",
    ]].rename(columns={
        "bracket_number_1": "bracket_number",
        "odds_1": "odds_fix",
    })

    df_vote = pd.merge(
        df_arg_vote, df_odds,
        on=["race_id", "bracket_number"], how="left",
    )

    # 払戻を結合する
    df_payoff = df_arg_payoff.query("bet_type==1")[[
        "race_id",
        "bracket_number_1",
        "payoff",
    ]].rename(columns={
        "bracket_number_1": "bracket_number",
    })

    df_vote = pd.merge(
        df_vote, df_payoff,
        on=["race_id", "bracket_number"], how="left",
    )

    df_vote["payoff"] = df_vote["payoff"].fillna(0.0)

    # 清算する
    df_vote["payoff_amount"] = df_vote["vote_amount"] * df_vote["payoff"]

    # レース一覧に記録する
    idx = df_arg_racelist[df_arg_racelist["race_id"] == race_id].index[0]

    df_arg_racelist.at[idx, "result_timestamp"] = current_datetime
    df_arg_racelist.at[idx, "return_amount"] = df_vote["payoff_amount"].sum()

    return df_vote, df_arg_racelist


def upload_vote(df_arg_race, df_arg_vote, df_arg_racelist, s3_vote_folder):
    """投票データをアップロードする。
    """

    race_id = df_arg_race["race_id"].values[0]

    s3 = utils.S3Storage()

    if df_arg_vote is not None:
        with io.BytesIO() as b:
            df_arg_vote.to_pickle(b, compression="gzip")
            key = f"{s3_vote_folder}/df_vote_{race_id}.pkl.gz"

            s3.put_object(key, b.getvalue())

            L.debug(f"投票データをアップロード: {key}")
    else:
        L.debug("投票データがNoneのためアップロードしない")

    with io.BytesIO() as b:
        df_arg_racelist.to_pickle(b, compression="gzip")
        key = f"{s3_vote_folder}/df_racelist.pkl.gz"

        s3.put_object(key, b.getvalue())

        L.debug(f"レース一覧データをアップロード: {key}")


def main():
    """清算アクションのメイン処理。
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

    if df_target_race is None:
        L.info("対象レースが存在しないため、処理を終了する")
        return

    race_id = df_target_race["race_id"].values[0]



    # 投票データを取得する
    L.info("# 投票データを取得する")

    df_vote = get_vote_race(df_target_race, s3_vote_folder)

    L.info("df_vote")
    L.info(df_vote)



    # フィードjsonからレースデータを取得する
    L.info("# フィードjsonからレースデータを取得する")

    df_race_bracket, df_race_info, df_race_result, df_race_payoff, df_race_odds = get_feed_data(df_target_race)

    L.debug("df_race_bracket")
    L.debug(df_race_bracket)

    L.debug("df_race_info")
    L.debug(df_race_info)

    L.debug("df_race_result")
    L.debug(df_race_result)

    L.debug("df_race_payoff")
    L.debug(df_race_payoff)

    L.debug("df_race_odds")
    L.debug(df_race_odds)





    # 清算する
    L.info("# 清算する")

    df_vote, df_racelist = payoff_race(current_datetime, df_racelist, df_target_race, df_race_odds, df_race_payoff, df_vote)

    L.debug("df_vote")
    L.debug(df_vote)

    L.debug("df_racelist")
    L.debug(df_racelist)
    L.debug(df_racelist[df_racelist["race_id"] == race_id])

    # 投票データをアップロードする
    L.info("# 投票データをアップロードする")

    upload_vote(df_target_race, df_vote, df_racelist, s3_vote_folder)


if __name__ == "__main__":
    main()

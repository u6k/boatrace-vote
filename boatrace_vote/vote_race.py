import io
import json
import os
import sys
from datetime import datetime, timedelta

import pandas as pd
import utils

L = utils.get_logger("vote_race")


def get_target_race(current_datetime, s3_vote_folder):
    """投票対象レースを発見する。
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


def get_pred_tickets(s3_pred_folder, df_arg_race):
    """舟券予測データを取得する。
    """

    s3 = utils.S3Storage()

    race_id = df_arg_race["race_id"].values[0]

    def _read_ticket(key):
        obj = s3.get_object(key)

        with io.BytesIO(obj) as b:
            df_tmp = pd.read_pickle(b, compression="gzip")

        df_tmp = df_tmp[df_tmp["race_id"] == race_id]

        return df_tmp

    df_ticket_t = _read_ticket(f"{s3_pred_folder}/df_ticket_t.pkl.gz")
    df_ticket_f = _read_ticket(f"{s3_pred_folder}/df_ticket_f.pkl.gz")
    df_ticket_k = _read_ticket(f"{s3_pred_folder}/df_ticket_k.pkl.gz")
    df_ticket_2t = _read_ticket(f"{s3_pred_folder}/df_ticket_2t.pkl.gz")
    df_ticket_2f = _read_ticket(f"{s3_pred_folder}/df_ticket_2f.pkl.gz")
    df_ticket_3t = _read_ticket(f"{s3_pred_folder}/df_ticket_3t.pkl.gz")
    df_ticket_3f = _read_ticket(f"{s3_pred_folder}/df_ticket_3f.pkl.gz")

    return df_ticket_t, df_ticket_f, df_ticket_k, df_ticket_2t, df_ticket_2f, df_ticket_3t, df_ticket_3f


def get_feed_data(df_arg_race):
    """フィードjsonデータを取得する。
    """

    # フィードjsonを読み込む
    race_id = df_arg_race["race_id"].values[0]

    s3 = utils.S3Storage()
    obj = s3.get_object(f"feed/race_{race_id}_before.json")

    with io.BytesIO(obj) as b:
        json_data = json.loads(b.getvalue())

    # パースする
    return utils.parse_feed_json(json_data)


def get_funds(df_arg_racelist):
    """残高を取得する。
    """

    funds = df_arg_racelist["return_amount"].sum() - df_arg_racelist["vote_amount"].sum()

    return funds


def vote_race(current_datetime, df_arg_racelist, df_arg_race, df_arg_odds, df_arg_ticket, arg_pred_threshold):
    """対象レースに投票(仮)する。
    """

    race_id = df_arg_race["race_id"].values[0]

    if df_arg_odds is None:
        # レースが中止となった場合、投票しない
        df_vote = None

        idx = df_arg_racelist[df_arg_racelist["race_id"] == race_id].index[0]

        df_arg_racelist.at[idx, "vote_timestamp"] = current_datetime
        df_arg_racelist.at[idx, "vote_amount"] = 0

        return df_vote, df_arg_racelist

    # 投票対象の舟券を抽出する
    df_vote = df_arg_ticket[[
        "race_id",
        "bracket_number",
        "pred_prob",
    ]]

    df_vote = df_vote[df_vote["pred_prob"] >= arg_pred_threshold]

    # オッズを結合する
    df_odds = df_arg_odds.query("bet_type==1")[[
        "race_id",
        "bracket_number_1",
        "odds_1",
    ]].rename(columns={
        "bracket_number_1": "bracket_number",
        "odds_1": "odds",
    })

    df_vote = pd.merge(
        df_vote, df_odds,
        on=["race_id", "bracket_number"], how="left",
    )

    # 期待値を算出して、投票量を決定する
    df_vote["expected_return"] = df_vote["odds"] * df_vote["pred_prob"]

    df_vote["vote_amount"] = df_vote["expected_return"].map(lambda r: 1 if r >= 1.50 else 0)
    df_vote["vote_amount"] = df_vote["vote_amount"] * (100.0 / df_vote["odds"])
    df_vote["vote_amount"] = df_vote["vote_amount"].map(lambda v: int(v))

    # レース一覧に記録する
    idx = df_arg_racelist[df_arg_racelist["race_id"] == race_id].index[0]

    df_arg_racelist.at[idx, "vote_timestamp"] = current_datetime
    df_arg_racelist.at[idx, "vote_amount"] = df_vote["vote_amount"].sum()

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
    """投票アクションのメイン処理。
    """

    # 設定を取得する
    current_datetime = datetime.strptime(os.environ["CURRENT_DATETIME"], "%Y-%m-%d %H:%M:%S")
    L.info(f"現在時刻: {current_datetime}")

    s3_pred_folder = os.environ["AWS_S3_PRED_FOLDER"]
    L.info(f"S3予測データフォルダ: {s3_pred_folder}")

    s3_vote_folder = os.environ["AWS_S3_VOTE_FOLDER"]
    L.info(f"S3投票データフォルダ: {s3_vote_folder}")

    pred_threshold = float(os.environ["PRED_THRESHOLD"])
    L.info(f"予測閾値: {pred_threshold}")

    # 日レース一覧データを読み込み、投票対象レースを特定する
    L.info("# 日レース一覧データを読み込み、投票対象レースを特定する")

    df_target_race, df_racelist = get_target_race(current_datetime, s3_vote_folder)

    L.info("対象レース")
    L.info(df_target_race)

    if df_target_race is None:
        L.info("対象レースが存在しないため、処理を終了する")
        return

    race_id = df_target_race["race_id"].values[0]

    L.info("# 舟券予測データを取得する")
    df_ticket_t, df_ticket_f, df_ticket_k, df_ticket_2t, df_ticket_2f, df_ticket_3t, df_ticket_3f = get_pred_tickets(s3_pred_folder, df_target_race)

    L.debug("df_ticket_t")
    L.debug(df_ticket_t)

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

    # 残高を確認する
    L.info("# 残高を確認する")

    funds = get_funds(df_racelist)

    L.info(f"残高: {funds}")

    # 投票する
    L.info("# 投票する")

    df_vote, df_racelist = vote_race(current_datetime, df_racelist, df_target_race, df_race_odds, df_ticket_t, pred_threshold)

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

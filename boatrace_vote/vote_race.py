import io
import os
from datetime import datetime

import pandas as pd
import utils

L = utils.get_logger("vote_race")


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


def merge_ticket_odds(df_arg_t, df_arg_f, df_arg_k, df_arg_2t, df_arg_2f, df_arg_3t, df_arg_3f, df_arg_odds):
    # 単勝
    df_tmp_t = df_arg_t.drop([
        "odds_1",
        "odds",
        "payoff",
    ], axis=1)

    df_odds_t = df_arg_odds.query("bet_type==1")[[
        "race_id",
        "bracket_number_1",
        "odds_1",
    ]].rename(columns={
        "bracket_number_1": "bracket_number",
    })

    df_odds_t["odds"] = df_odds_t["odds_1"]

    df_result_t = pd.merge(
        df_tmp_t, df_odds_t,
        on=["race_id", "bracket_number"], how="left",
    )

    # 複勝
    df_tmp_f = df_arg_f.drop([
        "odds_1",
        "odds_2",
        "odds",
        "payoff",
    ], axis=1)

    df_odds_f = df_arg_odds.query("bet_type==2")[[
        "race_id",
        "bracket_number_1",
        "odds_1",
        "odds_2",
    ]].rename(columns={
        "bracket_number_1": "bracket_number",
    })

    df_odds_f["odds"] = df_odds_f["odds_1"]

    df_result_f = pd.merge(
        df_tmp_f, df_odds_f,
        on=["race_id", "bracket_number"], how="left",
    )

    # 拡張複
    df_tmp_k = df_arg_k.drop([
        "odds_1",
        "odds_2",
        "odds",
        "payoff",
    ], axis=1)

    df_odds_k = df_arg_odds.query("bet_type==3")[[
        "race_id",
        "bracket_number_1",
        "bracket_number_2",
        "odds_1",
        "odds_2",
    ]]

    df_odds_k["odds"] = df_odds_k["odds_1"]

    df_result_k = pd.merge(
        df_tmp_k, df_odds_k,
        on=["race_id", "bracket_number_1", "bracket_number_2"], how="left",
    )

    # 2連単
    df_tmp_2t = df_arg_2t.drop([
        "odds_1",
        "odds",
        "payoff",
    ], axis=1)

    df_odds_2t = df_arg_odds.query("bet_type==4")[[
        "race_id",
        "bracket_number_1",
        "bracket_number_2",
        "odds_1",
    ]]

    df_odds_2t["odds"] = df_odds_2t["odds_1"]

    df_result_2t = pd.merge(
        df_tmp_2t, df_odds_2t,
        on=["race_id", "bracket_number_1", "bracket_number_2"], how="left",
    )

    # 2連複
    df_tmp_2f = df_arg_2f.drop([
        "odds_1",
        "odds",
        "payoff",
    ], axis=1)

    df_odds_2f = df_arg_odds.query("bet_type==5")[[
        "race_id",
        "bracket_number_1",
        "bracket_number_2",
        "odds_1",
    ]]

    df_odds_2f["odds"] = df_odds_2f["odds_1"]

    df_result_2f = pd.merge(
        df_tmp_2f, df_odds_2f,
        on=["race_id", "bracket_number_1", "bracket_number_2"], how="left",
    )

    # 3連単
    df_tmp_3t = df_arg_3t.drop([
        "odds_1",
        "odds",
        "payoff",
    ], axis=1)

    df_odds_3t = df_arg_odds.query("bet_type==6")[[
        "race_id",
        "bracket_number_1",
        "bracket_number_2",
        "bracket_number_3",
        "odds_1",
    ]]

    df_odds_3t["odds"] = df_odds_3t["odds_1"]

    df_result_3t = pd.merge(
        df_tmp_3t, df_odds_3t,
        on=["race_id", "bracket_number_1", "bracket_number_2", "bracket_number_3"], how="left",
    )

    # 3連複
    df_tmp_3f = df_arg_3f.drop([
        "odds_1",
        "odds",
        "payoff",
    ], axis=1)

    df_odds_3f = df_arg_odds.query("bet_type==7")[[
        "race_id",
        "bracket_number_1",
        "bracket_number_2",
        "bracket_number_3",
        "odds_1",
    ]]

    df_odds_3f["odds"] = df_odds_3f["odds_1"]

    df_result_3f = pd.merge(
        df_tmp_3f, df_odds_3f,
        on=["race_id", "bracket_number_1", "bracket_number_2", "bracket_number_3"], how="left",
    )

    return df_result_t, df_result_f, df_result_k, df_result_2t, df_result_2f, df_result_3t, df_result_3f


def get_funds(df_arg_racelist):
    """残高を取得する。
    """

    funds = df_arg_racelist["payoff_amount"].sum() - df_arg_racelist["vote_amount"].sum()

    return funds


def vote_expected_return_equal_payoff(df_arg_racelist, df_arg_race, df_arg_ticket):
    """対象レースに期待値投票(均等払い戻し)(仮)する。
    """

    pred_threshold = float(os.environ["PRED_THRESHOLD"])
    expected_return_threshold = float(os.environ["EXPECTED_RETURN_THRESHOLD"])
    expected_payoff = float(os.environ["EXPECTED_PAYOFF"])

    # 投票対象の舟券を抽出する
    df_vote = df_arg_ticket[df_arg_ticket["pred_prob"] >= pred_threshold]

    # 期待値を算出して、投票量を決定する
    df_vote["expected_return"] = df_vote["odds"] * df_vote["pred_prob"]

    df_vote["vote_amount"] = df_vote["expected_return"].map(lambda r: 1 if r >= expected_return_threshold else 0)
    df_vote["vote_amount"] = df_vote["vote_amount"] * (expected_payoff / df_vote["odds"])
    df_vote["vote_amount"] = df_vote["vote_amount"].map(lambda v: int(v))

    # レース一覧に記録する
    df_arg_racelist.at[df_arg_race.index[0], "vote_timestamp"] = datetime.now()
    df_arg_racelist.at[df_arg_race.index[0], "vote_amount"] = df_vote["vote_amount"].sum()

    return df_vote, df_arg_racelist


def main(s3_feed_folder, s3_pred_folder, s3_vote_folder):
    """投票アクションのメイン処理。
    """

    #
    # レース一覧データを取得する
    #
    L.info("# レース一覧データを取得する")

    df_racelist = utils.get_racelist(s3_vote_folder)

    L.debug(df_racelist)

    #
    # 投票対象レースを抽出する
    #
    L.info("# 投票対象レースを抽出する")

    df_racelist_target = utils.get_unprocessed_racelist(df_racelist, s3_feed_folder, "vote_timestamp", "_before")

    L.debug("df_racelist_target")
    L.debug(df_racelist_target)

    if len(df_racelist_target) == 0:
        L.debug("投票対象レースがない")
        return

    df_race = df_racelist_target.tail(1)
    race_id = df_race["race_id"].values[0]

    L.debug("df_race")
    L.debug(df_race)

    L.debug("race_id")
    L.debug(race_id)

    #
    # 舟券予測データを取得する
    #
    L.info("# 舟券予測データを取得する")

    df_ticket_t, df_ticket_f, df_ticket_k, df_ticket_2t, df_ticket_2f, df_ticket_3t, df_ticket_3f = get_pred_tickets(s3_pred_folder, df_race)

    L.debug("df_ticket_t")
    L.debug(df_ticket_t)

    L.debug("df_ticket_f")
    L.debug(df_ticket_f)

    L.debug("df_ticket_k")
    L.debug(df_ticket_k)

    L.debug("df_ticket_2t")
    L.debug(df_ticket_2t)

    L.debug("df_ticket_2f")
    L.debug(df_ticket_2f)

    L.debug("df_ticket_3t")
    L.debug(df_ticket_3t)

    L.debug("df_ticket_3f")
    L.debug(df_ticket_3f)

    #
    # フィードjsonからレースデータを取得する
    #
    L.info("# フィードjsonからレースデータを取得する")

    df_race_bracket, df_race_info, df_race_result, df_race_payoff, df_race_odds = utils.get_feed_data(df_race, s3_feed_folder, "_before")

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

    #
    # 舟券予測データとオッズデータを結合する
    #
    L.info("# 舟券予測データとオッズデータを結合する")

    if df_race_odds is not None:
        df_ticket_t, df_ticket_f, df_ticket_k, df_ticket_2t, df_ticket_2f, df_ticket_3t, df_ticket_3f = merge_ticket_odds(df_ticket_t, df_ticket_f, df_ticket_k, df_ticket_2t, df_ticket_2f, df_ticket_3t, df_ticket_3f, df_race_odds)
    else:
        L.debug("レースが中止となったため、マージしない")

    L.debug("df_ticket_t")
    L.debug(df_ticket_t)

    L.debug("df_ticket_f")
    L.debug(df_ticket_f)

    L.debug("df_ticket_k")
    L.debug(df_ticket_k)

    L.debug("df_ticket_2t")
    L.debug(df_ticket_2t)

    L.debug("df_ticket_2f")
    L.debug(df_ticket_2f)

    L.debug("df_ticket_3t")
    L.debug(df_ticket_3t)

    L.debug("df_ticket_3f")
    L.debug(df_ticket_3f)

    #
    # 残高を確認する
    #
    L.info("# 残高を確認する")

    funds = get_funds(df_racelist)

    L.info(f"残高: {funds}")

    #
    # 投票する
    #
    L.info("# 投票する")

    if df_race_odds is not None:
        df_vote, df_racelist = vote_expected_return_equal_payoff(df_racelist, df_race, df_ticket_t)
    else:
        L.debug("レースが中止となったため、投票しない")

        df_vote = df_ticket_t.copy()
        df_vote["vote_amount"] = None
        df_vote = df_vote.dropna()

        df_racelist.at[df_race.index[0], "vote_timestamp"] = datetime.now()
        df_racelist.at[df_race.index[0], "vote_amount"] = 0

    L.debug("df_vote")
    L.debug(df_vote)

    L.debug("df_racelist")
    L.debug(df_racelist)
    L.debug(df_racelist.loc[df_race.index[0]])

    #
    # 投票データをアップロードする
    #
    L.info("# 投票データをアップロードする")

    utils.put_racelist(df_racelist, s3_vote_folder)
    utils.put_vote(df_vote, race_id, s3_vote_folder)


if __name__ == "__main__":
    #
    # 設定を取得する
    #
    s3_feed_folder = os.environ["AWS_S3_FEED_FOLDER"]
    L.info(f"S3フィードデータフォルダ: {s3_feed_folder}")

    s3_pred_folder = os.environ["AWS_S3_PRED_FOLDER"]
    L.info(f"S3予測データフォルダ: {s3_pred_folder}")

    s3_vote_folder = os.environ["AWS_S3_VOTE_FOLDER"]
    L.info(f"S3投票データフォルダ: {s3_vote_folder}")

    #
    # 投票アクションのメイン処理
    #
    main(s3_feed_folder, s3_pred_folder, s3_vote_folder)

import os
import time
from datetime import datetime

import utils

L = utils.get_logger("crawl_loop")


def crawl_race(start_date, place_id, race_round, file_suffix):
    """レースデータをクロールする。

    :param start_date: レース日(yyyymmdd)
    :param place_id: 場所ID
    :param race_round: ラウンド数
    :param file_suffix: クロール結果ファイルの接尾辞 `race_{race_id}{file_suffix}.json`
    :returns: 終了コード, 標準出力＋標準エラー出力
    """
    L.info(f"#crawl_race: start: start_date={start_date}, place_id={place_id}, race_round={race_round}, file_suffix={file_suffix}")

    # パラメーターを構築する
    race_id = f"{start_date}_{place_id}_{race_round}"

    s3_feed_url = f"s3://{os.environ['AWS_S3_BUCKET']}/{os.environ['AWS_S3_FEED_FOLDER']}/race_{race_id}{file_suffix}.json"
    L.debug(f"S3フィードURL: {s3_feed_url}")
    crawl_url = f"https://www.boatrace.jp/owpc/pc/race/racelist?rno={race_round}&jcd={place_id}&hd={start_date}"
    L.debug(f"クロールURL: {crawl_url}")

    # クロールする
    return_code, return_str = utils.subprocess_crawl(crawl_url, s3_feed_url)

    return return_code, return_str


def crawl_race_before(s3_vote_folder, current_datetime):
    L.info(f"#crawl_race_before: start: s3_vote_folder={s3_vote_folder}, current_datetime={current_datetime}")

    #
    # レース一覧を取得する
    #
    L.debug("# レース一覧を取得する")

    df_racelist = utils.get_racelist(s3_vote_folder)

    L.debug(df_racelist)

    #
    # 直前レースを取得する
    #
    L.debug("# 直前レースを取得する")

    df_race = utils.find_vote_race(df_racelist, current_datetime)

    if df_race is None:
        L.debug("対象レースがない")
        return

    L.debug(df_race)

    #
    # レースをクロールする
    #
    L.debug("# レースをクロールする")

    race_id = df_race["race_id"].values[0]
    start_date = df_race["start_datetime"].dt.strftime("%Y%m%d").values[0]
    place_id = df_race["place_id"].values[0]
    race_round = df_race["race_round"].values[0]
    file_suffix = "_before"

    L.debug(f"race_id={race_id}")

    return_code, return_str = crawl_race(start_date, place_id, race_round, file_suffix)

    L.debug(f"return_code={return_code}")
    L.debug(f"return_str={return_str}")

    #
    # レース一覧を更新する
    #
    L.debug("# レース一覧を更新する")

    df_racelist.at[df_race.index[0], "vote_timestamp"] = current_datetime

    L.debug(f"updated: {df_racelist.loc[df_race.index[0]]}")

    #
    # レース一覧をアップロード
    #
    L.debug("# レース一覧をアップロード")

    utils.put_racelist(df_racelist, s3_vote_folder)


def crawl_race_after(s3_vote_folder, current_datetime):
    L.info(f"#crawl_race_after: start: s3_vote_folder={s3_vote_folder}, current_datetime={current_datetime}")

    #
    # レース一覧を取得する
    #
    L.debug("# レース一覧を取得する")

    df_racelist = utils.get_racelist(s3_vote_folder)

    L.debug(df_racelist)

    #
    # 直後レースを取得
    #
    L.debug("# 直後レースを取得")

    df_race = utils.find_payoff_race(df_racelist, current_datetime)

    if df_race is None:
        L.debug("対象データがない")
        return

    L.debug(df_race)

    #
    # レースをクロール
    #
    L.debug("# レースをクロール")

    race_id = df_race["race_id"].values[0]
    start_date = df_race["start_datetime"].dt.strftime("%Y%m%d").values[0]
    place_id = df_race["place_id"].values[0]
    race_round = df_race["race_round"].values[0]
    file_suffix = "_after"

    L.debug(f"race_id={race_id}")

    return_code, return_str = crawl_race(start_date, place_id, race_round, file_suffix)

    L.debug(f"return_code={return_code}")
    L.debug(f"return_str={return_str}")

    #
    # レース一覧を更新する
    #
    L.debug("# レース一覧を更新する")

    df_racelist.at[df_race.index[0], "payoff_timestamp"] = current_datetime

    L.debug(f"updated: {df_racelist.loc[df_race.index[0]]}")

    #
    # レース一覧をアップロード
    #
    L.debug("# レース一覧をアップロード")

    utils.put_racelist(df_racelist, s3_vote_folder)


def remaining_racelist(s3_vote_folder, current_datetime):
    L.info(f"#remaining_racelist: start: s3_vote_folder={s3_vote_folder}, current_datetime={current_datetime}")

    #
    # レース一覧を取得する
    #
    L.debug("# レース一覧を取得する")

    df_racelist = utils.get_racelist(s3_vote_folder)

    #
    # 残りのレースを抽出する
    #
    L.debug("# 残りのレースを抽出する")

    df_racelist = df_racelist.query("vote_timestamp.isnull() or payoff_timestamp.isnull()")

    return df_racelist

    pass


def crawl_loop(s3_vote_folder):
    prev_datetime = datetime.now()
    while True:
        current_datetime = datetime.now()

        # レース一覧を取得して、全てのレースがクロール済みならbreakする
        df_racelist = remaining_racelist(s3_vote_folder, current_datetime)
        if len(df_racelist) == 0:
            L.info("全てのレースを処理した")
            break

        # 前回から60秒経過していなければ、スリープしてからcontinue
        if (datetime.now() - prev_datetime).total_seconds() < 60:
            L.debug("スキップ")
            time.sleep(10)
            continue

        # クロールする
        crawl_race_before(s3_vote_folder, current_datetime)
        crawl_race_after(s3_vote_folder, current_datetime)

        prev_datetime = datetime.now()


if __name__ == "__main__":
    s3_vote_folder = os.environ["AWS_S3_VOTE_FOLDER"]
    print(f"s3_vote_folder={s3_vote_folder}")

    crawl_loop(s3_vote_folder)

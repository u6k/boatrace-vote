import os
import subprocess
import time
from datetime import datetime, timedelta

import pandas as pd

AWS_ENDPOINT_URL = os.environ["AWS_ENDPOINT_URL"]
AWS_ACCESS_KEY_ID = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]
AWS_S3_BUCKET = os.environ["AWS_S3_BUCKET"]
AWS_S3_PRED_FOLDER = os.environ["AWS_S3_PRED_FOLDER"]
AWS_S3_VOTE_FOLDER = os.environ["AWS_S3_VOTE_FOLDER"]
AWS_S3_CACHE_BUCKET = os.environ["AWS_S3_CACHE_BUCKET"]
AWS_S3_CACHE_FOLDER = os.environ["AWS_S3_CACHE_FOLDER"]
VOTE_TARGET_DATE = os.environ["VOTE_TARGET_DATE"]
PRED_THRESHOLD = float(os.environ["PRED_THRESHOLD"])
USER_AGENT = os.environ["USER_AGENT"]
DOCKER_IMAGE_VOTE = os.environ["DOCKER_IMAGE_VOTE"]
DOCKER_IMAGE_CRAWLER = os.environ["DOCKER_IMAGE_CRAWLER"]


def main_vote():
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"現在時刻: {now_str}")

    # 投票対象レースを探す
    print("# 投票対象レースを探す")

    proc = subprocess.run([
        "docker", "run", "--rm",
        "-e", "TZ=Asia/Tokyo",
        "-e", f"AWS_ENDPOINT_URL={AWS_ENDPOINT_URL}",
        "-e", f"AWS_ACCESS_KEY_ID={AWS_ACCESS_KEY_ID}",
        "-e", f"AWS_SECRET_ACCESS_KEY={AWS_SECRET_ACCESS_KEY}",
        "-e", f"AWS_S3_BUCKET={AWS_S3_BUCKET}",
        "-e", f"AWS_S3_PRED_FOLDER={AWS_S3_PRED_FOLDER}",
        "-e", f"AWS_S3_VOTE_FOLDER={AWS_S3_VOTE_FOLDER}",
        "-e", f"VOTE_TARGET_DATE={VOTE_TARGET_DATE}",
        "-e", f"CURRENT_DATETIME={now_str}",
        "-e", f"PRED_THRESHOLD={PRED_THRESHOLD}",
        "-v", "./output:/var/output",
        "-v", ".:/var/myapp",
        DOCKER_IMAGE_VOTE, "poe", "find_vote_race",
    ], capture_output=True, text=True)

    print("stdout")
    print(proc.stdout)
    print("stderr")
    print(proc.stderr)

    if not os.path.isfile("./output/df_vote_race.pkl.gz"):
        # 投票対象レースがない場合、処理を戻す
        print("投票対象レースが存在しない")
        return

    else:
        df_race = pd.read_pickle("./output/df_vote_race.pkl.gz")

    # 投票対象レースの最新データをクロールする
    print("# 投票対象レースの最新データをクロールする")

    race_id = df_race["race_id"].values[0]
    race_round = df_race["race_round"].values[0]
    place_id = df_race["place_id"].values[0]
    start_datetime = df_race["start_datetime"].dt.strftime("%Y%m%d").values[0]

    s3_feed_url = f"s3://{AWS_S3_BUCKET}/feed/race_{race_id}_before.json"
    print(f"S3フィードURL: {s3_feed_url}")
    crawl_url = f"https://www.boatrace.jp/owpc/pc/race/racelist?rno={race_round}&jcd={place_id}&hd={start_datetime}"
    print(f"クロールURL: {crawl_url}")

    proc = subprocess.run([
        "docker", "run", "--rm",
        "-e", "TZ=Asia/Tokyo",
        "-e", f"AWS_ENDPOINT_URL={AWS_ENDPOINT_URL}",
        "-e", f"AWS_ACCESS_KEY_ID={AWS_ACCESS_KEY_ID}",
        "-e", f"AWS_SECRET_ACCESS_KEY={AWS_SECRET_ACCESS_KEY}",
        "-e", f"AWS_S3_CACHE_BUCKET={AWS_S3_CACHE_BUCKET}",
        "-e", f"AWS_S3_CACHE_FOLDER={AWS_S3_CACHE_FOLDER}",
        "-e", f"AWS_S3_FEED_URL={s3_feed_url}",
        "-e", f"USER_AGENT={USER_AGENT}",
        "-e", "RECACHE_RACE=True",
        "-e", "RECACHE_DATA=False",
        DOCKER_IMAGE_CRAWLER,
        "scrapy", "crawl", "boatrace_spider", "-a", f"start_url={crawl_url}",
    ], capture_output=True, text=True)

    print("stdout")
    print(proc.stdout)
    print("stderr")
    print(proc.stderr)

    # 投票する
    print("# 投票する")

    proc = subprocess.run([
        "docker", "run", "--rm",
        "-e", "TZ=Asia/Tokyo",
        "-e", f"AWS_ENDPOINT_URL={AWS_ENDPOINT_URL}",
        "-e", f"AWS_ACCESS_KEY_ID={AWS_ACCESS_KEY_ID}",
        "-e", f"AWS_SECRET_ACCESS_KEY={AWS_SECRET_ACCESS_KEY}",
        "-e", f"AWS_S3_BUCKET={AWS_S3_BUCKET}",
        "-e", f"AWS_S3_PRED_FOLDER={AWS_S3_PRED_FOLDER}",
        "-e", f"AWS_S3_VOTE_FOLDER={AWS_S3_VOTE_FOLDER}",
        "-e", f"VOTE_TARGET_DATE={VOTE_TARGET_DATE}",
        "-e", f"CURRENT_DATETIME={now_str}",
        "-e", f"PRED_THRESHOLD={PRED_THRESHOLD}",
        "-v", "./output:/var/output",
        "-v", ".:/var/myapp",
        DOCKER_IMAGE_VOTE, "poe", "vote_race",
    ], capture_output=True, text=True)

    print("stdout")
    print(proc.stdout)
    print("stderr")
    print(proc.stderr)


def loop():
    prev_time = datetime.now() - timedelta(hours=1)

    while (True):
        if (datetime.now() - prev_time).total_seconds() < 60:
            print(f"スキップ: {datetime.now()}")
            time.sleep(10)
            continue

        prev_time = datetime.now()

        main_vote()

        df_racelist = pd.read_pickle("./output/df_racelist.pkl.gz").query("result_timestamp.isnull()")

        if len(df_racelist) == 0:
            print("全てのレースを処理した")
            break


if __name__ == "__main__":
    loop()

"""video fetching to database"""
import logging
import json
from db.connection import Neo4jConnection
from models.Video import fromJson, video_query
logging.basicConfig(filename='logs//video_gather.log', level=logging.INFO)


def main():
    conn = Neo4jConnection()

    # with open('..//data//analyzed.json', 'r', encoding='utf-8') as file:
    #     basic_data = json.load(file)
    #     rows = list(map(fromJson, basic_data["items"]))
    #     conn.bulk_insert_data(
    #         video_query, rows)
    #     conn.close()
    # for video_id in video_ids:
    #     jsons, quota_exceeded = query_by_video_id(video_id)
    #     rows = list(map(fromJson_dc, process(jsons)))
    #     conn.bulk_insert_data(
    #         comment_query, [comment for comment_thread in rows for comment in comment_thread])
    #     if quota_exceeded:
    #         break
    # conn.close()
    # if quota_exceeded:
    #     logging.info("Fetching comments partialy failled %s",
    #                  datetime.datetime.now())
    # else:
    #     logging.info("Fetching comments succeeded %s", datetime.datetime.now())


if __name__ == "__main__":
    main()

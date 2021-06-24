import datetime
import logging
from db.connection import Neo4jConnection
from acquire.video_api import get_videos_by_id
from models.Video import fromJson, video_query
from process.video_process import process

in_db_query = '''
match (video:Video)
where video.videoId in $videos
return video.videoId
'''


def main(video_ids: 'list[str]' = [], games: 'list[str]' = [], languages: 'list[str]' = []):
    logging.basicConfig(filename='logs//video_gather.log', level=logging.INFO,force=True)
    if len(video_ids) == 0 or len(games) == 0 or len(languages) == 0:
        return
    logging.info("Fetching videos started %s", datetime.datetime.now())
    conn = Neo4jConnection()
    videos = {video_id for video_id in video_ids}
    if len(videos) > 0:
        videos_in_db = {v[0] for v in conn.query(
            in_db_query, {"videos": list(videos)})}
        videos = videos.difference(videos_in_db)

    logging.info("Fetching %d videos", len(videos))
    quota_exceeded = False
    if len(videos) > 0:
        jsons, quota_exceeded = get_videos_by_id(videos)
        rows = list(map(fromJson, process(jsons, languages, games, set())))
        conn.bulk_insert_data(video_query, rows)
        conn.close()
    if quota_exceeded:
        logging.info("Fetching videos partialy failled %s",
                     datetime.datetime.now())
    else:
        logging.info("Fetching videos succeeded %s", datetime.datetime.now())


if __name__ == "__main__":
    main()

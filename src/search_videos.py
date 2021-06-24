"""video fetching to database"""
import logging
import datetime
from acquire.video_search_api import query_by_keyword, query_by_relative_id
from db.connection import Neo4jConnection
from models.Video import fromJson, video_query
from process.video_process import process

by_genre = '''
match (ch:channel)--(v:Video)--(g:Game) 
where g.title in $games
return v.videoId
'''


def main(keywords: 'list[tuple[str,list[str],list[str],int]]' = []):
    logging.basicConfig(filename='logs//video_gather.log',
                        level=logging.INFO, force=True)
    conn = Neo4jConnection()
    quota_exceeded = False
    for keyword in keywords:
        MISS_SEARCH_PARAM = 2

        videos = {v for v in conn.query(by_genre, {"games": keyword[1]})}
        if len(videos) == 0:
            query_limit = keyword[3]*MISS_SEARCH_PARAM
            initialVideos = list(map(fromJson, process(query_by_keyword(keyword[0], query_limit),
                                                       keyword[1], keyword[2], videos)))

            conn.bulk_insert_data(video_query, initialVideos)
        while len(videos) < keyword[3] and not quota_exceeded:
            query_limit = (keyword[3]-len(videos))*MISS_SEARCH_PARAM
            for videoId in videos:
                foundVideos, quota_exceeded = query_by_relative_id(
                    videoId[0], query_limit)
                newVideos = list(map(fromJson, process(foundVideos,
                                                       keyword[1], keyword[2], videos)))
                conn.bulk_insert_data(video_query, newVideos)
                if len(videos) >= keyword[3] or quota_exceeded:
                    break
        if quota_exceeded:
            logging.info("%s partialy succeded", keyword[0])
            break
        logging.info("%s succeded", keyword[0])

    conn.close()
    if quota_exceeded:
        logging.info("Fetching videos partialy failled %s",
                     datetime.datetime.now())
    else:
        logging.info("Fetching videos succeeded %s", datetime.datetime.now())


if __name__ == "__main__":
    main()

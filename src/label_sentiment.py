
from textblob import TextBlob
from typing import cast
import datetime
import logging
from time import time
import numpy as np
from operator import itemgetter
from db.connection import Neo4jConnection
from models.Comment_dt import Video_dc, comment_query, fromJson_child_dc
from process.comment_process import process_command
from acquire.comment_api import query_by_comment_id


dream_children_query = '''
match (:Channel{channelId:"UCTkXRDQl0luXxVQrRQvWS6w"})<--(v:Video)<--(c:Comment)
return c.commentId,c.textOriginal
'''
all_query = '''
MATCH (c:Comment) 
WHERE NOT EXISTS(c.polarity) OR NOT EXISTS(c.subjectivity) 
RETURN c.commentId,c.textOriginal
'''
channel_query = '''
MATCH (ch:Channel)--(v:Video)--(c:Comment) 
WHERE ch.channelId in $channels AND NOT EXISTS(c.polarity) OR NOT EXISTS(c.subjectivity) 
RETURN c.commentId,c.textOriginal
'''
video_query = '''
MATCH (v:Video)--(c:Comment) 
WHERE video.videoId in $videos AND NOT EXISTS(c.polarity) OR NOT EXISTS(c.subjectivity) 
RETURN c.commentId,c.textOriginal
'''

update_query = """
UNWIND $rows AS row
MATCH (c:Comment{commentId: row.commentId})
SET c.polarity = row.polarity
SET c.subjectivity = row.subjectivity
"""


def parse(comment_id, test_sentiment):
    return {"commentId": comment_id, "polarity": test_sentiment.polarity,
            "subjectivity": test_sentiment.subjectivity}


def main(video_ids: 'list[str]' = [], channel_ids: 'list[str]' = [], all: bool = False):
    logging.basicConfig(
        filename='logs//comments_gather.log', level=logging.INFO, force=True)
    logging.info("Labeling comments started %s", datetime.datetime.now())
    conn = Neo4jConnection()
    comments = {}
    start = time()
    if all:
        logging.warning(
            "Labeling may fail because memory limit")
        comments = {c[0]: c for c in conn.query(all_query)}
    else:
        if len(video_ids) > 0:
            comments_by_videos_ids = {c[0]: c for c in conn.query(
                video_query, {"videos": video_ids})}
            comments.update(comments_by_videos_ids)
        if len(channel_ids) > 0:
            comments_by_channel_ids = {c[0]: c for c in conn.query(
                channel_query, {"channels": channel_ids})}
            comments.update(comments_by_channel_ids)
    comments = list(comments.values())
    logging.info("[FETCHING] %d items in duration: %d",
                 len(comments), int(time()-start))
    start = time()
    rows = [parse(comments[i][0], TextBlob(comments[i][1]).sentiment)
            for i in range(len(comments))]
    logging.info("[LABELING] duration: %d",  int(time()-start))
    conn.bulk_insert_data(
        update_query, rows)

    logging.info("Labeling comments succeeded %s", datetime.datetime.now())


if __name__ == "__main__":
    main()

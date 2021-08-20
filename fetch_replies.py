from inspect import Parameter
from typing import cast
import datetime
import logging
from operator import itemgetter
from db.connection import Neo4jConnection
from models.Comment_dt import Video_dc, comment_query, fromJson_child_dc
from process.comment_process import process_command
from acquire.comment_api import query_by_comment_id

dream_children_query = '''
match (:Channel{channelId:"UCTkXRDQl0luXxVQrRQvWS6w"})--(video:Video)--(parent:Comment)--(child:Comment)
with parent,video,count(child) as childrenCount,collect(child.commentId) as children
where parent.totalReplyCount > childrenCount and (parent.totalReplyCount-childrenCount)>$limit
return parent.commentId,parent.totalReplyCount,video.videoId,children
'''
replies_by_channel = '''
match (ch:Channel)--(video:Video)--(parent:Comment)--(child:Comment)
where ch.channelId in $channels
with parent,video,count(child) as childrenCount,collect(child.commentId) as children
where parent.totalReplyCount > childrenCount and (parent.totalReplyCount-childrenCount)>$limit
return parent.commentId,parent.totalReplyCount,video.videoId,children
'''
replies_by_video = '''
match (video:Video)--(parent:Comment)--(child:Comment)
where video.videoId in $videos
with parent,video,count(child) as childrenCount,collect(child.commentId) as children
where parent.totalReplyCount > childrenCount and (parent.totalReplyCount-childrenCount)>$limit
return parent.commentId,parent.totalReplyCount,video.videoId,children
'''
replies_all = '''
match (video:Video)--(parent:Comment)--(child:Comment)
with parent,video,count(child) as childrenCount,collect(child.commentId) as children
where parent.totalReplyCount > childrenCount and (parent.totalReplyCount-childrenCount)>$limit
return parent.commentId,parent.totalReplyCount,video.videoId,children
'''


def add_video_id_field(json, videoId):

    json["snippet"]["videoId"] = videoId
    return json


def main(video_ids: 'list[str]' = [], channel_ids: 'list[str]' = [], all: bool = False, min_replies: int = 100):
    logging.basicConfig(
        filename='logs//replies_gather.log', level=logging.INFO, force=True)
    logging.info("Fetching replies started %s", datetime.datetime.now())
    if min_replies < 3:
        logging.warning(
            "There are comments with reply count minimaly higher than true, where low min-replies will just waste quaota to not gain anything.")
    conn = Neo4jConnection()
    parents = {}
    if all:
        parents = {p[0]: p for p in conn.query(
            replies_all, {"limit": min_replies})}
    else:
        if len(video_ids) > 0:
            parents_from_videos = {p[0]: p for p in conn.query(
                replies_by_video, {"videos": video_ids, "limit": min_replies})}
            parents.update(parents_from_videos)
        if len(channel_ids) > 0:
            parents_from_channels = {p[0]: p for p in conn.query(
                replies_by_channel, {"channels": channel_ids, "limit": min_replies})}
            parents.update(parents_from_channels)
    parents = parents.values()
    logging.info("Fetching %d parents", len(parents))
    quota_exceeded = False
    for i, parent in enumerate(parents):
        logging.info("[FETCHING] %d/%d parents", i, len(parents))
        jsons, quota_exceeded = query_by_comment_id(parent[0])
        jsons = [add_video_id_field(json, parent[2])
                 for json in jsons if json["id"] not in parent[3]]
        rows = list(map(fromJson_child_dc, process_command(jsons)))
        conn.bulk_insert_data(
            comment_query, rows)
        if quota_exceeded:
            break
    conn.close()
    if quota_exceeded:
        logging.info("Fetching replies partialy failled %s",
                     datetime.datetime.now())
    else:
        logging.info("Fetching replies succeeded %s", datetime.datetime.now())


if __name__ == "__main__":
    main()

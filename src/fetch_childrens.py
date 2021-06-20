from typing import cast
import datetime
import logging
from operator import itemgetter
from db.connection import Neo4jConnection
from models.Comment_dt import Video_dc, comment_query, fromJson_child_dc
from process.comment_process import process_command
from acquire.comment_api import query_by_comment_id
logging.basicConfig(filename='logs//comments_gather.log', level=logging.INFO)

dream_children_query = '''
match (:Channel{channelId:"UCTkXRDQl0luXxVQrRQvWS6w"})<--(video:Video)<--(parent:Comment)<--(child:Comment)
with parent,video,count(child) as childrenCount,collect(child.commentId) as children
where parent.totalReplyCount > childrenCount
return parent.commentId,parent.totalReplyCount,video.videoId,children
'''


def add_video_id_field(json, parentToVideo):

    json["snippet"]["videoId"] = parentToVideo.get(json["snippet"]["parentId"])
    return json


def main():
    logging.info("Fetching comments started %s", datetime.datetime.now())
    conn = Neo4jConnection()
    children = conn.query(dream_children_query)
    group_by_data = []
    subgroup = []
    suma = 0
    for child in children:
        if suma == 0:
            subgroup.append(child)
            suma += itemgetter(1)(child)
        elif suma//100 == (suma+itemgetter(1)(child))//100:
            subgroup.append(child)
            suma += itemgetter(1)(child)
        else:
            group_by_data.append(subgroup)
            suma = 0
            subgroup = []
    if len(subgroup) > 0:
        group_by_data.append(subgroup)
        subgroup = []

    quota_exceeded = False
    for subgroup in group_by_data:
        parentToVideo = {item[0]: item[2] for item in subgroup}
        bannedId = set().union(*[item[3] for item in subgroup])
        jsons, quota_exceeded = query_by_comment_id(
            [itemgetter(0)(parent) for parent in subgroup])
        jsons = [add_video_id_field(json, parentToVideo)
                 for json in jsons if json["id"] not in bannedId]
        rows = list(map(fromJson_child_dc, process_command(jsons)))
        conn.bulk_insert_data(
            comment_query, rows)
        if quota_exceeded:
            break
    conn.close()
    if quota_exceeded:
        logging.info("Fetching comments partialy failled %s",
                     datetime.datetime.now())
    else:
        logging.info("Fetching comments succeeded %s", datetime.datetime.now())


if __name__ == "__main__":
    main()

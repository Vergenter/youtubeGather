from datetime import datetime
from utils.types import CommentId, VideoId


def to_update_videos(update: datetime):
    def channel_id_to_update(video_id: VideoId):
        return (video_id, update)
    return channel_id_to_update


videos_to_update_query = 'select video_id,max(update) from comments.videos_comments where video_id not in (select video_id from comments.videos_comments where update > $1) group by video_id limit $2;'
updated_insert_query = 'INSERT INTO comments.videos_comments VALUES ($1,$2)'
update_insert_new_query = 'INSERT INTO comments.videos_comments VALUES ($1,$2) ON CONFLICT DO NOTHING'

all_comment_query = '''
UNWIND $rows AS row
with row
MERGE (comment:Comment{commentId: row.comment_id})
with row,comment
FOREACH(ignoreMe IN CASE WHEN row.authorChannelId IS NOT NULL THEN [1] ELSE [] END |
    MERGE (channel:Channel{channelId: row.authorChannelId})
    MERGE (comment)-[:OWNED_BY]->(channel)
)
with row,comment
MERGE (video:Video{videoId: row.video_id})
MERGE (comment)-[:TO]->(video)
with row, comment
SET comment.publishedAt= row.publishedAt,
    comment.authorDisplayName= row.authorDisplayName
with row,comment
CREATE (commentStatistics:CommentStatistics{
    from: row.update,
    textOriginal: row.textOriginal,
    updatedAt: row.updatedAt,
    likeCount: row.likeCount,
    totalReplyCount: row.totalReplyCount,
    isPublic: row.isPublic
    })
with commentStatistics,row,comment
CREATE (commentStatistics)-[:OF]->(comment)
'''

comment_static_insert_query = """
UNWIND $rows AS row
with row
MERGE (comment:Comment{commentId: row.comment_id})
with row,comment
FOREACH(ignoreMe IN CASE WHEN row.authorChannelId IS NOT NULL THEN [1] ELSE [] END |
    MERGE (channel:Channel{channelId: row.authorChannelId})
    MERGE (comment)-[:OWNED_BY]->(channel)
)
with row,comment
MERGE (video:Video{videoId: row.video_id})
MERGE (comment)-[:TO]->(video)
with row, comment
SET comment.publishedAt= row.publishedAt,
    comment.authorDisplayName= row.authorDisplayName
"""

comment_dynamic_insert_query = """
UNWIND $rows AS row
CREATE (commentStatistics:CommentStatistics{
    from: row.update,
    textOriginal: row.textOriginal,
    updatedAt: row.updatedAt,
    likeCount: row.likeCount,
    totalReplyCount: row.totalReplyCount,
    isPublic: row.isPublic
    })
MERGE (comment:Comment{commentId: row.comment_id})
CREATE (commentStatistics)-[:OF]->(comment)
"""

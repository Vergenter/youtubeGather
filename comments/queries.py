videos_update_query = 'select distinct video_id from comments.videos_comments where video_id not in (select video_id from comments.videos_comments where update > $1;'
new_videos_query = 'SELECT id FROM unnest($1::text[]) as V(id) EXCEPT SELECT video_id FROM comments.videos_comments;'
update_insert_query = 'INSERT INTO comments.videos_comments VALUES ($1,$2)'

all_comment_query = '''
UNWIND $rows AS row
with row
MERGE (comment:Comment{commentId: row.comment_id})
MERGE (channel:Channel{channelId: row.authorChannelId})
MERGE (comment)-[:OWNED_BY]->(channel)
with row,comment
MERGE (video:Video{videoId: row.video_id})
MERGE (comment)-[:TO]->(video)
with row, comment
SET comment.publishedAt= row.publishedAt,
    comment.authorDisplayName= row.authorDisplayName
with row,comment
CREATE (commentStatistics:CommentStatistics{
    textOriginal: row.textOriginal,
    updatedAt: row.updatedAt,
    likeCount: row.likeCount,
    totalReplyCount: row.totalReplyCount,
    isPublic: row.isPublic
    })
with commentStatistics,row,comment
CREATE (commentStatistics)-[:OF{at: row.update}]->(comment)
'''

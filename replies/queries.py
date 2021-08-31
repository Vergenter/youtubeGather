to_update_query = 'select distinct toUpdate.parent_id from replies.parent_comments toUpdate where toUpdate.parent_id not in (select updated.parent_id from replies.parent_comments updated where updated.update > $1);'
updated_insert_query = "INSERT INTO replies.parent_comments VALUES ($1,$2) ON CONFLICT DO NOTHING"
all_comment_query = '''
UNWIND $rows AS row
with row
MERGE (comment:Comment{commentId: row.reply_id})
MERGE (channel:Channel{channelId: row.authorChannelId})
MERGE (comment)-[:OWNED_BY]->(channel)
with row,comment
MERGE (parent:Comment{commentId: row.parentId})
MERGE (comment)-[:To]->(parent)
with row, comment
SET comment.publishedAt= row.publishedAt,
    comment.authorDisplayName= row.authorDisplayName
with row,comment
CREATE (commentStatistics:CommentStatistics{
    textOriginal: row.textOriginal,
    updatedAt: row.updatedAt,
    likeCount: row.likeCount,
    })
with commentStatistics,row,comment
CREATE (commentStatistics)-[:OF{at: row.update}]->(comment)
'''

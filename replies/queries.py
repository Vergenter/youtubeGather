
from datetime import datetime
from typing import Tuple

import asyncpg
from utils.types import CommentId


def to_update(update: datetime):
    def channel_id_to_update(comment_id: CommentId):
        return (comment_id, update)
    return channel_id_to_update


async def upsert(records: list[Tuple], pool: asyncpg.Pool):
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute('CREATE TEMP TABLE IF NOT EXISTS tmptable (LIKE "replies"."insuficient_parent_comments")')
            await conn.execute('TRUNCATE tmptable')
            await conn.copy_records_to_table('tmptable', records=records)
            return await conn.fetch('INSERT INTO "replies"."insuficient_parent_comments" SELECT * FROM tmptable ON CONFLICT DO NOTHING RETURNING *')


parents_to_update_query = 'select parent_id,max(update) from replies.parent_comments where parent_id not in (select parent_id from replies.parent_comments where update > $1) group by parent_id limit $2;'
upsert_full_parent_query = "INSERT INTO replies.insuficient_parent_comments VALUES ($1,$2) ON CONFLICT DO NOTHING RETURNING parent_id, update"
update_insert_new_query = "INSERT INTO replies.parent_comments VALUES ($1,$2) ON CONFLICT DO NOTHING"
updated_insert_query = 'INSERT INTO replies.parent_comments VALUES ($1,$2)'

all_comment_query = '''
UNWIND $rows AS row
with row
MERGE (comment:Comment{commentId: row.reply_id})
with row,comment
FOREACH(ignoreMe IN CASE WHEN row.authorChannelId IS NOT NULL THEN [1] ELSE [] END |
    MERGE (channel:Channel{channelId: row.authorChannelId})
    MERGE (comment)-[:OWNED_BY]->(channel)
)
with row,comment
MERGE (parent:Comment{commentId: row.parentId})
MERGE (comment)-[:To]->(parent)
with row, comment
SET comment.publishedAt= row.publishedAt,
    comment.authorDisplayName= row.authorDisplayName
with row,comment
CREATE (commentStatistics:CommentStatistics{
    from: row.update,
    textOriginal: row.textOriginal,
    updatedAt: row.updatedAt,
    likeCount: row.likeCount
    })
with commentStatistics,row,comment
CREATE (commentStatistics)-[:OF]->(comment)
'''

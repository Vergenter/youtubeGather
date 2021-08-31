from datetime import datetime
from utils.types import ChannelId, VideoId
from channel_model import Channel


def channel_to_update(channel: Channel):
    return (channel.channel_id, channel.update)


def to_update(update: datetime):
    def channel_id_to_update(channel_id: ChannelId):
        return (channel_id, update)
    return channel_id_to_update


channel_update_query = 'select distinct channel_id from channels.channels where channel_id not in (select channel_id from channels.channels where update > $1);'
new_channel_query = 'SELECT id FROM unnest($1::text[]) as V(id) EXCEPT SELECT channel_id FROM channels.channels;'
update_insert_query = 'INSERT INTO channels.channels VALUES ($1,$2)'

static_channel_query = """
UNWIND $rows AS row
with row
    MERGE (channel:Channel{channelId: row.channel_id})
with channel,row
SET channel.publishedAt = row.publishedAt,
    channel.title = row.title,
    channel.description = row.description,
    channel.uploadsPlaylist = row.uploadsPlaylist,
    channel.isLinked= row.isLinked,
    channel.trackingAnalyticsAccountId= row.trackingAnalyticsAccountId,
    channel.unsubscribedTrailer= row.unsubscribedTrailer,
    channel.showRelatedChannels= row.showRelatedChannels

with channel,row
FOREACH(ignoreMe IN CASE WHEN row.country IS NOT NULL THEN [1] ELSE [] END |
    MERGE (country:Country{country: row.country})
        CREATE (channel)-[:FROM]->(country)
)
with channel,row
    FOREACH (tag in row.tags | 
    MERGE (t:Tag{tagName: tag})
    CREATE (channel)-[:TAGGED]->(t)
    )
with channel,row
    FOREACH (topic in row.topicCategories | 
    MERGE (t:Topic{url: topic})
    CREATE (channel)-[:OF_TOPIC]->(t)
    )
"""
dynamic_channel_query = """
UNWIND $rows AS row
with row
    MERGE (channel:Channel{channelId: row.channel_id})
with row,channel
CREATE (channelStatistics:ChannelStatistics{
    viewCount: row.viewCount,
    subscriberCount: row.subscriberCount,
    videoCount: row.videoCount,
    madeForKids: row.madeForKids,
    public: row.public,
    customUrl: row.customUrl,
    moderateComments: row.moderateComments
    })
with channelStatistics,row,channel
CREATE (channelStatistics)-[:OF{at: row.update}]->(channel)
"""

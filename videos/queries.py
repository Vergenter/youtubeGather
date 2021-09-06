
from datetime import datetime
from utils.types import VideoId
from video_model import Video


def video_to_update(video: Video):
    return (video.video_id, video.update)


def to_update(update: datetime):
    def channel_id_to_update(video_id: VideoId):
        return (video_id, update)
    return channel_id_to_update


videos_to_update_query = 'select video_id,max(update) from videos.videos where video_id not in (select video_id from videos.videos where update > $1) group by video_id limit $2;'
update_insert_query = 'INSERT INTO videos.videos VALUES ($1,$2)'
update_insert_new_query = 'INSERT INTO videos.videos VALUES ($1,$2) ON CONFLICT DO NOTHING'

static_video_query = '''
UNWIND $rows AS row
with row
    MERGE (video:Video{videoId: row.video_id})
with video,row
SET video.publishedAt= row.publishedAt,
    video.description= row.description,
    video.liveBroadcastContent= row.liveBroadcastContent,
    video.duration= row.duration,
    video.is3D= row.is3D,
    video.ishd= row.ishd,
    video.licensedContent= row.licensedContent,
    video.is360degree= row.is360degree,
    video.cclicense= row.cclicense,
    video.madeForKids= row.madeForKids

with video,row
FOREACH(ignoreMe IN CASE WHEN row.defaultLanguage IS NOT NULL THEN [1] ELSE [] END |
    MERGE (defaultLang:Language{language: row.defaultLanguage})
        CREATE (video)-[:DEFAULT_LANGUAGE]->(defaultLang)
)

with video,row
FOREACH(ignoreMe IN CASE WHEN row.defaultAudioLanguage IS NOT NULL THEN [1] ELSE [] END |
    MERGE (defaultAudioLang:Language{language: row.defaultAudioLanguage})
        CREATE (video)-[:DEFAULT_AUDIO_LANGUAGE]->(defaultAudioLang)
)   

with video,row
    MERGE (channel:Channel{channelId: row.channel_id})
    MERGE (video)-[:OWNED_BY]->(channel)


with video,row
    MERGE (category:Category{categoryId: row.category_id})
    CREATE (video)-[:CATEGORIZED_BY]->(category)


with video,row
    FOREACH (language in row.localizations | 
    MERGE (lang:Language{language: language})
    CREATE (video)-[:LOCALIZED]->(lang)
    )
with video,row
    FOREACH (tag in row.tags | 
    MERGE (t:Tag{tagName: tag})
    CREATE (video)-[:TAGGED]->(t)
    )
with video,row
    FOREACH (topic in row.topicCategories | 
    MERGE (t:Topic{url: topic})
    CREATE (video)-[:OF_TOPIC]->(t)
    )
with video,row
    FOREACH (region in row.regionRestrictionAllowed | 
    MERGE (r:Region{regionCode: region})
    CREATE (video)-[:ALLOWED_ONLY]->(r)
    )
with video,row
    FOREACH (region in row.regionRestrictionBlocked | 
    MERGE (r:Topic{regionCode: region})
    CREATE (video)-[:BLOCKED]->(r)
    )
'''

dynamic_video_query = '''
UNWIND $rows AS row
with row
MERGE (video:Video{videoId: row.video_id})
with row, video
CREATE (videoStatistics:VideoStatistics{
    from: row.update,
    title: row.title,
    hasCaption: row.hasCaption,
    status: row.status,
    public: row.public,
    embeddable: row.embeddable,
    publicStatsViewable: row.publicStatsViewable,
    viewCount: row.viewCount,
    likeCount: row.likeCount,
    dislikeCount: row.dislikeCount,
    commentCount: row.commentCount
    })
with videoStatistics,row,video
CREATE (videoStatistics)-[:OF]->(video)
'''

empty_video_insert_query = """
UNWIND $rows AS row
with row
    MERGE (video:Video{videoId: row.video_id})
with row, video
CREATE (videoStatistics:VideoStatistics{
    from: row.update,
    public: row.public
    })
with videoStatistics,row,video
CREATE (videoStatistics)-[:OF]->(video)
"""

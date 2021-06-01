CREATE CONSTRAINT video_id_unique IF NOT EXISTS
ON (v:Video)
ASSERT v.videoId IS UNIQUE;

CREATE CONSTRAINT channel_id_unique IF NOT EXISTS
ON (c:Channel)
ASSERT c.channelId IS UNIQUE;

CREATE CONSTRAINT comment_id_unique IF NOT EXISTS
ON (c:Comment)
ASSERT c.commentId IS UNIQUE;

CREATE CONSTRAINT topic_id_unique IF NOT EXISTS
ON (t:Topic)
ASSERT t.topicId IS UNIQUE;

CREATE CONSTRAINT game_title_unique IF NOT EXISTS
ON (g:Game)
ASSERT g.title IS UNIQUE;
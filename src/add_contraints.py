from db.connection import Neo4jConnection


def main():
    conn = Neo4jConnection()
    conn.query("""
        CREATE CONSTRAINT video_id_unique IF NOT EXISTS
        ON (v:Video)
        ASSERT v.videoId IS UNIQUE""")

    conn.query("""
        CREATE CONSTRAINT channel_id_unique IF NOT EXISTS
        ON (c:Channel)
        ASSERT c.channelId IS UNIQUE""")

    conn.query("""
        CREATE CONSTRAINT comment_id_unique IF NOT EXISTS
        ON (c:Comment)
        ASSERT c.commentId IS UNIQUE""")

    conn.query("""
        CREATE CONSTRAINT game_title_unique IF NOT EXISTS
        ON (g:Game)
        ASSERT g.title IS UNIQUE""")


if __name__ == "__main__":
    main()

from utils.connection import Neo4jConnection


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

    conn.query("""
        CREATE CONSTRAINT category_id_unique IF NOT EXISTS
        ON (c:Category)
        ASSERT c.categoryId IS UNIQUE""")

    conn.query("""
        CREATE CONSTRAINT tag_unique IF NOT EXISTS
        ON (t:Tag)
        ASSERT t.tagName IS UNIQUE""")

    conn.query("""
        CREATE CONSTRAINT topic_unique IF NOT EXISTS
        ON (t:Topic)
        ASSERT t.url IS UNIQUE""")

    conn.query("""
        CREATE CONSTRAINT region_unique IF NOT EXISTS
        ON (r:Region)
        ASSERT r.regionCode IS UNIQUE""")

    conn.query("""
        CREATE CONSTRAINT language_unique IF NOT EXISTS
        ON (l:Language)
        ASSERT l.language IS UNIQUE""")

    conn.query("""
        CREATE CONSTRAINT country_unique IF NOT EXISTS
        ON (c:Country)
        ASSERT c.country IS UNIQUE""")

    conn.close()
    print("SUCCESS")


if __name__ == "__main__":
    main()

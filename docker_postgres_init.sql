\set POSTGRES_ADMIN `echo "$POSTGRES_USER"`

\connect channeldb;
CREATE SCHEMA channels
	CREATE TABLE channels (
		channel_id text NOT NULL,
		update TIMESTAMP NOT NULL,
		CONSTRAINT channel_id_update PRIMARY KEY(channel_id,update)
	);

CREATE DATABASE videodb
    WITH 
    OWNER = :POSTGRES_ADMIN
    ENCODING = 'UTF8'
	LC_COLLATE = 'en_US.utf8'
    LC_CTYPE = 'en_US.utf8'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1;

\connect videodb;
CREATE SCHEMA videos
	CREATE TABLE videos (
		video_id text NOT NULL,
		update TIMESTAMP NOT NULL,
		CONSTRAINT video_id_update PRIMARY KEY(video_id,update)
	)

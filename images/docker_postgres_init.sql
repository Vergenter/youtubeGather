\set POSTGRES_ADMIN `echo "$POSTGRES_USER"`

\connect searchdb;
CREATE SCHEMA searches
	CREATE TABLE videos (
		video_id char(11) NOT NULL,
		update date NOT NULL,
		CONSTRAINT video_id_update PRIMARY KEY(video_id,update)
	);
\connect
CREATE DATABASE channeldb
    WITH 
    OWNER = :POSTGRES_ADMIN
    ENCODING = 'UTF8'
    LC_COLLATE = 'en_US.utf8'
    LC_CTYPE = 'en_US.utf8'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1;

\connect channeldb;
CREATE SCHEMA channels
	CREATE TABLE channels (
		channel_id char(22) NOT NULL,
		update date NOT NULL,
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
		video_id char(11) NOT NULL,
		update date NOT NULL,
		CONSTRAINT video_id_update PRIMARY KEY(video_id,update)
	)
	CREATE TABLE channels (
		channel_id char(22) NOT NULL,
		update date NOT NULL,
		CONSTRAINT channel_id_update PRIMARY KEY(channel_id,update)
	);

CREATE DATABASE commentdb
    WITH 
    OWNER = :POSTGRES_ADMIN
    ENCODING = 'UTF8'
    LC_COLLATE = 'en_US.utf8'
    LC_CTYPE = 'en_US.utf8'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1;

\connect commentdb;
CREATE SCHEMA comments;
	CREATE TABLE comments (
		comment_id varchar(49) NOT NULL,
		update date NOT NULL,
		CONSTRAINT comment_id_update PRIMARY KEY(comment_id,update)
	)
	CREATE TABLE videos (
		video_id char(11) NOT NULL,
		update date NOT NULL,
		CONSTRAINT video_id_update PRIMARY KEY(video_id,update)
	);

CREATE DATABASE replydb
    WITH 
    OWNER = :POSTGRES_ADMIN
    ENCODING = 'UTF8'
    LC_COLLATE = 'en_US.utf8'
    LC_CTYPE = 'en_US.utf8'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1;

\connect replydb;
CREATE SCHEMA replies;
	CREATE TABLE replies (
		comment_id varchar(49) NOT NULL,
		update date NOT NULL,
		CONSTRAINT comment_id_update PRIMARY KEY(comment_id,update)
	);
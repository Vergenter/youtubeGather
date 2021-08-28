\set POSTGRES_ADMIN `echo "$POSTGRES_USER"`

\connect channeldb;
CREATE SCHEMA channels
	CREATE TABLE channels (
		channel_id text NOT NULL,
		update TIMESTAMP,
		CONSTRAINT channel_id_update PRIMARY KEY(channel_id,update)
	);

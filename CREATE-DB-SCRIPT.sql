# Creating the database for YoutubeCommenter
DROP TABLE searcher_comments;
DROP TABLE searcher_videos;
DROP TABLE searcher_users;

# Creating the searcher_users table
CREATE TABLE searcher_users(
    user_channel_id       	MEDIUMINT NOT NULL AUTO_INCREMENT,
    user_channel_youtube_id	VARCHAR(100) UNIQUE NOT NULL,
    user_channel_title    	VARCHAR(100),

    PRIMARY KEY (user_channel_id)
);         

# Creating the searcher_videos table
CREATE TABLE searcher_videos(
    video_id		MEDIUMINT NOT NULL AUTO_INCREMENT,
    video_youtube_id	VARCHAR(100) UNIQUE NOT NULL,
    video_name 		VARCHAR(500),
    video_view_count	BIGINT,
    video_comment_count	BIGINT,
    video_embeddable	BOOLEAN,
    video_url		VARCHAR(150),
    video_channel_id_id	MEDIUMINT NOT NULL,
    
    PRIMARY KEY (video_id),
    FOREIGN KEY (video_channel_id_id) 
	REFERENCES searcher_users(user_channel_id) 
        ON DELETE CASCADE
);

# Creating the searcher_comments tabe     
CREATE TABLE searcher_comments(
    comment_id			MEDIUMINT NOT NULL AUTO_INCREMENT,
    comment_youtube_id		VARCHAR(100) UNIQUE NOT NULL,
    comment_text		TEXT,
    like_count			BIGINT,
    comment_channel_id_id	MEDIUMINT NOT NULL REFERENCES Users(user_channel_id),
    video_id_id			MEDIUMINT NOT NULL REFERENCES Videos(video_id),
	
    PRIMARY KEY (comment_id),
    FOREIGN KEY (video_id_id) 
	REFERENCES searcher_videos(video_id)
        ON DELETE CASCADE,
    FOREIGN KEY (comment_channel_id_id) 
	REFERENCES searcher_users(user_channel_id)
        ON DELETE CASCADE
);

# Creating indexes
CREATE INDEX user_channel_youtube_id_index
ON searcher_users(user_channel_youtube_id);

CREATE INDEX video_youtube_id_index
ON searcher_videos(video_youtube_id);

CREATE INDEX comment_youtube_id_index
ON searcher_comments(comment_youtube_id);

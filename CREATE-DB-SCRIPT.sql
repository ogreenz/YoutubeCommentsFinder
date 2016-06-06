# Creating the database for YoutubeCommenter

# Creating the searcher_users table
CREATE TABLE searcher_users(
	user_channel_id       VARCHAR(500),
    user_channel_title    VARCHAR(500),

    PRIMARY KEY (user_channel_id)
);         

# Creating the searcher_videos table
CREATE TABLE searcher_videos(
	video_id			VARCHAR(500),
	video_name 			VARCHAR(500),
    video_view_count	BIGINT,
    video_comment_count	BIGINT,
    video_embeddable	BOOLEAN,
    video_url			VARCHAR(500),
    video_channel_id_id	VARCHAR(500),
    
	PRIMARY KEY (video_id),
    FOREIGN KEY (video_channel_id_id) 
		REFERENCES searcher_users(user_channel_id) 
        ON DELETE CASCADE
);

# Creating the searcher_comments tabe     
CREATE TABLE searcher_comments(
	comment_id					VARCHAR(500),
    comment_text				TEXT,
    like_count					BIGINT,
    comment_channel_id_id		VARCHAR(500) REFERENCES Users(user_channel_id),
    video_id_id					VARCHAR(500) REFERENCES Videos(video_id),
	
    PRIMARY KEY (comment_id),
    FOREIGN KEY (video_id_id) 
		REFERENCES searcher_videos(video_id)
        ON DELETE CASCADE,
    FOREIGN KEY (comment_channel_id_id) 
		REFERENCES searcher_users(user_channel_id)
        ON DELETE CASCADE
);

# Creating indexes

CREATE INDEX  user_channel_title_index 
ON searcher_users(user_channel_title);

CREATE INDEX  video_name_index 
ON searcher_videos(video_name);
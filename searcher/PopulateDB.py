from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from oauth2client.tools import argparser
import MySQLdb
#from django.db import connection

# constants
DEVELOPER_KEY = "AIzaSyBs2Te9khQZNLYB2N0chEq3bRx3qtikNSI"
YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"
DB_SERVER = 'mysqlsrv.cs.tau.ac.il'
DB_NAME = 'DbMysql09'
DB_USER = DB_NAME
DB_PASSWORD = DB_NAME
VIDEO_LIST_PART = 'id, snippet, statistics, status, player'
VIDEO_LIST_FIELDS = 'items(id ,snippet(channelId, title, thumbnails), statistics(viewCount, commentCount), status(embeddable), player(embedHtml))'

class PopulateDB(object):

    def __init__(self):
        self.youtube_service = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey = DEVELOPER_KEY)
        self.db_connection = MySQLdb.connect(DB_SERVER, DB_USER, DB_USER, DB_PASSWORD)
        self.db_cursor = self.db_connection.cursor()
        #self.db_cursor = connection.cursor()
        
    def cleanup(self):
        self.db_connection.close()
    
    def addUser(self, channel_id, user_channel_title = None):
        '''
        Adds the user with the given channel id to the db.
        If the channel's title is given, it should be given as retrieved from the api response (not encoded).
        If the channel's title is not given, the function uses the youtube service to retrieve it.
        Return True for success, False for failure.
        '''
        
        if user_channel_title is None:
            # Retrieving the channel's title
            channel_response = self.youtube_service.channels().list(
                id = channel_id,
                part = "snippet",
                fields = "items(snippet(title))"
            ).execute()
            
            if "items" not in channel_response:
                print("Invalid channel response for channelId = %s: 'items' doens't exist." % (channel_id))
                return False
            if len(channel_response["items"]) != 1:
                print("Invalid channel response for channelId = %s: 'items' has length %d" % (channel_id, len(channel_response["items"])))
                return False
            user_channel = channel_response["items"][0]
            user_channel_title = user_channel["snippet"]["title"]
        user_channel_title_encoded = user_channel_title.encode('utf-8')
        
        # Adding to the db
        try:
            self.db_cursor.execute(
                 "INSERT INTO searcher_users (user_channel_id, user_channel_title) " \
                 "Values (%s, %s) " \
                 "ON DUPLICATE KEY UPDATE user_channel_id = user_channel_id",
                (channel_id, user_channel_title_encoded)
            )
            self.db_connection.commit()
        except:
            # TODO: Handle
            self.db_connection.rollback()
            raise
            
        return True
        
    @staticmethod
    def getVideoUrl(videoId, isEmbeddable):
        '''
        Returns the url of the given video. If it's embeddable, returns its embed url.
        '''
        video_url = "//www.youtube.com/"
        if isEmbeddable:
            video_url += "embed/"
        else:
            video_url += "watch?v="
        video_url += videoId
        return video_url

        
    def addSingleComment(self, comment_id, video_id, author_channel_id, author_display_name, text_display, like_count):
        '''
        Adds the given comment to the db, after adding its author.
        The given author_display_name and text_display should be as retrieved from the api response (not encoded).
        '''
        self.addUser(author_channel_id, author_display_name)
        text_display_encoded = text_display.encode('utf-8')
        author_display_name_encoded = author_display_name.encode('utf-8')
        try:
            self.db_cursor.execute(
                 "INSERT INTO searcher_comments (comment_id, video_id_id, comment_channel_id_id, comment_author_display_name, comment_text, like_count)" \
                 "Values (%s, %s, %s, %s, %s, %s)" \
                 "ON DUPLICATE KEY UPDATE comment_id = comment_id",
                (comment_id, video_id, author_channel_id, author_display_name_encoded, text_display_encoded, like_count)
            )
            self.db_connection.commit()
        except:
            # TODO: Handle
            self.db_connection.rollback()
            raise
            
    def addCommentPage(self, commentThreads, videoId):
        '''
        Adds the given comment threads to the db.
        commentThreads is an array of CommentThread resources.
        '''
        print("Adding comment page")
        for comment_thread in commentThreads:
            comment = comment_thread["snippet"]["topLevelComment"]
            comment_snippet = comment["snippet"]
            self.addSingleComment(
                comment["id"],
                videoId,
                comment_snippet["authorChannelId"]["value"],
                comment_snippet["authorDisplayName"],
                comment_snippet["textDisplay"],
                comment_snippet["likeCount"]
            )
            
    def addVideoComments(self, videoId):
        '''
        Adds comments related to the given video id to the db, using the youtube service.
        '''
        requested_fields = "nextPageToken, pageInfo, items(snippet(topLevelComment(id, snippet(authorDisplayName, authorChannelId, textDisplay, likeCount))))"
        
        # Retrieving the first comments page
        comment_threads = self.youtube_service.commentThreads().list(
            part = "snippet", 
            fields = requested_fields, 
            videoId = videoId, 
            maxResults = 100,
            textFormat = 'plainText'
        ).execute()
        comments_num = len(comment_threads.get('items', []))
        self.addCommentPage(comment_threads.get('items', []), videoId)

        # Retrieving the remaining comments pages
        while 'nextPageToken' in comment_threads:

            next_page_token = comment_threads['nextPageToken']
            
            comment_threads = self.youtube_service.commentThreads().list(
                part = "snippet", 
                fields = requested_fields, 
                videoId = videoId, 
                maxResults = 100,
                textFormat = 'plainText',
                pageToken = next_page_token
            ).execute()
            
            comments_num += len(comment_threads.get('items', []))
            self.addCommentPage(comment_threads.get('items', []), videoId)
            
    
    def addVideo(self, video_resource):
        '''
        Adds the given video to the db. 
        video_resource is the youtube video resource.
        '''
        # Retrieving the video information from the resource
        video_id = video_resource["id"]
        video_name = video_resource["snippet"]["title"]
        video_name_encoded = video_name.encode('utf-8')
        video_channel_id = video_resource["snippet"]["channelId"]
        video_view_count = int(video_resource["statistics"]["viewCount"])
        video_comment_count = int(video_resource["statistics"]["commentCount"])
        video_embeddable = video_resource["status"]["embeddable"]
        video_url = PopulateDB.getVideoUrl(video_id, video_embeddable)
        
        # Adding the video to the db
        try:
            self.db_cursor.execute(
                 "INSERT INTO searcher_videos (video_id, video_name, video_channel_id_id, video_view_count, video_comment_count, video_embedable, video_url)" \
                 "Values (%s, %s, %s, %s, %s, %s, %s)" \
                 "ON DUPLICATE KEY UPDATE video_id = video_id",
                (video_id, video_name_encoded, video_channel_id, video_view_count, video_comment_count, video_embeddable, video_url)
            )
            self.db_connection.commit()
        except:
            # TODO: Handle
            self.db_connection.rollback()
            raise
        
    
    def addVideoAndUploaderAndCommentsByVideoResource(self, video_resource):
        '''
        Adds the given video to the db, as well as its uplodaer and its comments.
        video_resource is the youtube video resource.
        '''
        # Adding the user first, then the video and at last the comments (it must be in this order, due to foreign keys constrains)
        if not self.addUser(video_resource["snippet"]["channelId"]):
            return False
        self.addVideo(video_resource)
        return self.addVideoComments(video_resource["id"])
        
        
    def addVideoAndUploaderAndCommentsByVideoId(self, videoId):
        '''
        Adds the specified video to the db, as well as its uplodaer and its comments.
        The video is specified by its id.
        '''
        # Retrieving the video resource by its id
        video_list_response = self.youtube_service.videos().list(
            id = videoId, 
            part = VIDEO_LIST_PART, 
            fields  = VIDEO_LIST_FIELDS
        ).execute()
        
        video_resource = video_list_response.get("items", [])[0]
        return self.addVideoAndUploaderAndCommentsByVideoResource(video_resource)
        
    def addPopularVideosAndUploadersAndComments(self):
        '''
        Finds the popular videos available in Israel, and adds them as well as their uploaders and comments to the db.
        '''
        # Retrieving the videos
        video_list_response = self.youtube_service.videos().list(
            chart = 'mostPopular', 
            regionCode = 'IL',
            part = VIDEO_LIST_PART, 
            fields  = VIDEO_LIST_FIELDS
        ).execute()
        
        for video_resource in video_list_response.get("items", []):
            self.addVideoAndUploaderAndCommentsByVideoResource(video_resource)
            
 
        
if __name__ == "__main__":
    db = PopulateDB()
    db.addPopularVideosAndUploadersAndComments()
    db.cleanup()
        

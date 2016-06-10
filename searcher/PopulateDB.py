from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from oauth2client.tools import argparser
import MySQLdb
import httplib
import httplib2
import random
import sys
import time

# constants
DEVELOPER_KEY = "AIzaSyBs2Te9khQZNLYB2N0chEq3bRx3qtikNSI"
YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"
DB_SERVER = 'mysqlsrv.cs.tau.ac.il'
DB_NAME = 'DbMysql09'
DB_USER = DB_NAME
DB_PASSWORD = DB_NAME
VIDEO_LIST_PART = 'id, snippet, statistics, status'
VIDEO_LIST_FIELDS = 'nextPageToken, items(id ,snippet(channelId, title), statistics(viewCount, commentCount), status(embeddable))'
DB_RECORDS_NUM_MIN = 150000
DEFAULT_COMMENTS_PER_VIDEO_MAX = 2000
USAGE = "USAGE: The script arguments can be one of the following:\n" \
        "command can be:\n" \
        "populate [comments_per_video_max] - you should use this when the db is empty, and it will be populated with at least %d records.\n" \
        "update [comments_per_video_max] - updates the db by updating the details of the existing videos and re-adding their comments.\n" \
        "add_by_keyword <search_string> <max_videos> [comments_per_video_max] - adds at most max_videos videos that match search_string. " \
        "max_videos should be between 1 to 50.\n" \
        "The default for comments_per_video_max is %d" % (DB_RECORDS_NUM_MIN, DEFAULT_COMMENTS_PER_VIDEO_MAX)

# Maximum number of times to retry before giving up.
MAX_RETRIES = 2

# Always retry when these exceptions are raised.
RETRIABLE_EXCEPTIONS = (httplib2.HttpLib2Error, IOError, httplib.NotConnected,
  httplib.IncompleteRead, httplib.ImproperConnectionState,
  httplib.CannotSendRequest, httplib.CannotSendHeader,
  httplib.ResponseNotReady, httplib.BadStatusLine)

# Always retry when an apiclient.errors.HttpError with one of these status
# codes is raised.
RETRIABLE_STATUS_CODES = [400, 500, 502, 503, 504]

class PopulateDB(object):

    def __init__(self, comments_per_video_max = DEFAULT_COMMENTS_PER_VIDEO_MAX, db_connection = None):
        '''
        Creates the youtube service and the db connection if it's not given.
        '''
        self.youtube_service = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey = DEVELOPER_KEY)
        if db_connection is not None:
            self.db_connection = db_connection
            self.should_close_db_connection = False
        else:
            self.db_connection = MySQLdb.connect(DB_SERVER, DB_USER, DB_USER, DB_PASSWORD)
            self.should_close_db_connection = True
        self.db_cursor = self.db_connection.cursor()
        self.db_records_num = 0
        self.comments_per_video_max = comments_per_video_max
        
    def cleanup(self):
        if self.should_close_db_connection:
            self.db_connection.close()
        
    @staticmethod
    def callMethodWithRetries(method, method_arg1, method_arg2 = None):
        '''
        Tries to call the given method with the given arguments.
        Some exceptions cause another retry (if the retries number hasn't reached the limit) and other cause a failure.
        This function returns the given method return value, 
        or None if it raised an non-retriable exception or the retries number has reached the limit.
        '''
        response = None
        error = None
        retry = 0
        while response is None:
            try:
                return method(method_arg1, method_arg2)
            except HttpError, e:
                if e.resp.status in RETRIABLE_STATUS_CODES:
                    error = "A retriable HTTP error %d occurred:\n%s" % (e.resp.status, e.content)
                else:
                    return None
            except RETRIABLE_EXCEPTIONS, e:
                error = "A retriable error occurred: %s" % e

            if error is not None:
                print error
                retry += 1
                if retry > MAX_RETRIES:
                    print("No longer attempting to retry.")
                    return None
                max_sleep = 2 ** retry
                sleep_seconds = random.random() * max_sleep
                print("Sleeping %f seconds and then retrying..." % sleep_seconds)
                time.sleep(sleep_seconds)
    
    def getChannelListResponse(self, channel_id, dummy_arg = None):
        '''
        Calls the youtube api list method of the resource channels.
        This should be wrapped with callMethodWithRetries.
        '''
        channel_list_response = self.youtube_service.channels().list(
            id = channel_id,
            part = "snippet",
            fields = "items(snippet(title))"
        ).execute()
        return channel_list_response
    
    @staticmethod
    def validateChannelListResponse(channel_list_response, channel_id):
        '''
        Checks if the given channel list response is valid, and print an error message if it isn't.
        The response must include a single item.
        '''
        if channel_list_response is None:
            print("Failed retrieving the channel list resource")
            return False

        if len(channel_list_response.get('items', [])) != 1:
            print("Invalid channel response for channel_id = %s: 'items' has length %d" % (channel_id, len(channel_list_response.get("items", []))))
            return False
            
        return True
        
    @staticmethod
    def validateChannelResource(channel_resource, channel_id):
        '''
        Checks if the given channel resource is valid, and prints an error message if it isn't.
        '''
        if 'snippet' not in channel_resource:
            print("Invalid channel resource for channel_id = %s: 'snippet' field is missing." % (channel_id,))
            return False
            
        if 'title' not in channel_resource['snippet']:
            print("Invalid channel resource for channel_id = %s: 'snippet.title' field is missing." % (channel_id,))
            return False
            
        return True
    
    def addUser(self, channel_youtube_id, user_channel_title = None):
        '''
        Adds the user with the given channel id to the db.
        If the channel's title is given, it should be given as retrieved from the api response (not encoded).
        If the channel's title is not given, the function uses the youtube service to retrieve it.
        Return the user id (the db primary key) or None if an error occured.
        '''
        if user_channel_title is None:
            # Retrieving the channel's title
            channel_list_response = PopulateDB.callMethodWithRetries(self.getChannelListResponse, channel_youtube_id)
            if not PopulateDB.validateChannelListResponse(channel_list_response, channel_youtube_id):
                return None
            user_channel_resource = channel_list_response["items"][0]
            if not PopulateDB.validateChannelResource(user_channel_resource, channel_youtube_id):
                return None
            user_channel_title = user_channel_resource["snippet"]["title"]
        user_channel_title_encoded = user_channel_title.encode('utf-8')
        
        # Adding to the db
        try:
            # Making sure to follow the unique constraint of user_channel_youtube_id
            self.db_cursor.execute(
                "SELECT user_channel_id " \
                "FROM searcher_users " \
                "WHERE user_channel_youtube_id = %s",
                (channel_youtube_id,))
            user_rows = self.db_cursor.fetchall()
            if len(user_rows) == 0:
                self.db_cursor.execute(
                     "INSERT INTO searcher_users (user_channel_youtube_id, user_channel_title) " \
                     "Values (%s, %s) ",
                    (channel_youtube_id, user_channel_title_encoded,)
                )
                user_id = self.db_cursor.lastrowid
                self.db_connection.commit()
                self.db_records_num += 1
            else:
                user_id = user_rows[0][0]
            return user_id
        except Exception as e:
            self.db_connection.rollback()
            print("Failed inserting the user with channel_youtube_id = %s to the db: %s" % (channel_youtube_id, str(e),))
            return None
        except:
            # Raising the exception again so keyboard interrupt wouldn't be ignored
            self.db_connection.rollback()
            print("Failed inserting the user with channel_youtube_id = %s to the db." % (channel_youtube_id,))
            raise
        
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

        
    def addSingleComment(self, comment_youtube_id, video_id, author_channel_youtube_id, author_display_name, text_display, like_count):
        '''
        Adds the given comment to the db, after adding its author.
        The given author_display_name and text_display should be as retrieved from the api response (not encoded).
        '''
        # First adding the user who commented this comment
        user_id = self.addUser(author_channel_youtube_id, author_display_name)
        if user_id is None:
            return
            
        text_display_encoded = text_display.encode('utf-8')
        try:
            # Making sure to follow the unique constraint of comment_youtube_id
            self.db_cursor.execute(
                "SELECT comment_id " \
                "FROM searcher_comments " \
                "WHERE comment_youtube_id = %s",
                (comment_youtube_id,))
            comment_rows = self.db_cursor.fetchall()
            if len(comment_rows) == 0:
                self.db_cursor.execute(
                     "INSERT INTO searcher_comments (comment_youtube_id, video_id_id, comment_channel_id_id, comment_text, like_count)" \
                     "Values (%s, %s, %s, %s, %s)",
                    (comment_youtube_id, video_id, user_id, text_display_encoded, like_count,)
                )
                self.db_connection.commit()
                self.db_records_num += 1
        except Exception as e:
            self.db_connection.rollback()
            print("Failed inserting comment with youtube_id = %s to the db: %s" % (comment_youtube_id, str(e)))
            return
        except:
            # Raising the exception again so keyboard interrupt wouldn't be ignored
            self.db_connection.rollback()
            print("Failed inserting comment with youtube_id = %s to the db: %s" % (comment_youtube_id,))
            raise

    @staticmethod
    def validateCommentThreadResource(comment_thread_resource):
        '''
        Checks if the given comment thread resource is valid, and prints an error message if it isn't.
        '''
        if 'id' not in comment_thread_resource:
            print("Invalid comment thread resource: 'id' field is missing")
            return False
            
        if 'snippet' not in comment_thread_resource:
            print("comment thread with id = %s is invalid: 'snippet' field is missing." % (comment_thread_resource['id']))
            return False
            
        if 'topLevelComment' not in comment_thread_resource['snippet']:
            print("comment thread with id = %s is invalid: 'snippet.topLevelComment' field is missing." % (comment_thread_resource['id']))
            return False
            
        comment_resource = comment_thread_resource['snippet']['topLevelComment']
        if 'id' not in comment_resource:
            print("comment thread with id = %s is invalid: 'snippet.topLevelComment.id' field is missing." % (comment_thread_resource['id']))
            return False
            
        if 'snippet' not in comment_resource:
            print("comment with id = %s is invalid: 'snippet' field is missing." % (comment_resource['id']))
            return False
        
        if 'authorDisplayName' not in comment_resource['snippet']:
            print("comment with id = %s is invalid: 'snippet.authorDisplayName' field is missing." % (comment_resource['id']))
            return False

        if 'authorChannelId' not in comment_resource['snippet']:
            print("comment with id = %s is invalid: 'snippet.authorChannelId' field is missing." % (comment_resource['id']))
            return False
            
        if 'value' not in comment_resource['snippet']['authorChannelId']:
            print("comment with id = %s is invalid: 'snippet.authorChannelId.value' field is missing." % (comment_resource['id']))
            return False            
            
        if 'textDisplay' not in comment_resource['snippet']:
            print("comment with id = %s is invalid: 'snippet.textDisplay' field is missing." % (comment_resource['id']))
            return False
            
        if 'likeCount' not in comment_resource['snippet']:
            print("comment with id = %s is invalid: 'snippet.likeCount' field is missing." % (comment_resource['id']))
            return False                       

        return True
                       
            
    def addCommentPage(self, commentThreads, videoId):
        '''
        Adds the given comment threads to the db.
        commentThreads is an array of CommentThread resources.
        '''
        print("Adding comment page")
        for comment_thread_resource in commentThreads:
            if PopulateDB.validateCommentThreadResource(comment_thread_resource):
                comment_resource = comment_thread_resource["snippet"]["topLevelComment"]
                comment_snippet = comment_resource["snippet"]
                self.addSingleComment(
                    comment_resource["id"],
                    videoId,
                    comment_snippet["authorChannelId"]["value"],
                    comment_snippet["authorDisplayName"],
                    comment_snippet["textDisplay"],
                    comment_snippet["likeCount"]
                )
            
    def getCommentThreadsListReponse(self, video_youtube_id, page_token):
        '''
        Calls the youtube api list method of the resource CommentsThreads.
        This should be wrapped with callMethodWithRetries.
        '''
        requested_fields = "nextPageToken, items(id, snippet(topLevelComment(id, snippet(authorDisplayName, authorChannelId, textDisplay, likeCount))))"
        if page_token is None:
            comment_threads_list_response = self.youtube_service.commentThreads().list(
                part = "snippet", 
                fields = requested_fields, 
                videoId = video_youtube_id, 
                maxResults = 100,
                textFormat = 'plainText'
            ).execute()   
        else:
            comment_threads_list_response = self.youtube_service.commentThreads().list(
                part = "snippet", 
                fields = requested_fields, 
                videoId = video_youtube_id, 
                maxResults = 100,
                textFormat = 'plainText',
                pageToken = page_token
            ).execute()          
        return comment_threads_list_response
            
    def addVideoComments(self, video_id, video_youtube_id):
        '''
        Adds comments related to the given video youtube id to the db, using the youtube service.
        Returns True for success or False for failure.
        '''
        # Retrieving the first comments page
        comment_threads_list_response = PopulateDB.callMethodWithRetries(self.getCommentThreadsListReponse, video_youtube_id, None)
        if comment_threads_list_response is None:
            print("Failed retrieving the first comments page")
            return
        comments_num = len(comment_threads_list_response.get('items', []))
        self.addCommentPage(comment_threads_list_response.get('items', []), video_id)

        # Retrieving the remaining comments pages
        while ('nextPageToken' in comment_threads_list_response) and (comments_num < self.comments_per_video_max):
            page_token = comment_threads_list_response['nextPageToken']
            comment_threads_list_response = PopulateDB.callMethodWithRetries(self.getCommentThreadsListReponse, video_youtube_id, page_token)
            if comment_threads_list_response is None:
                print("Failed retrieving the comments page with the page_token = %s" % (page_token))
                return
            comments_num += len(comment_threads_list_response.get('items', []))
            self.addCommentPage(comment_threads_list_response.get('items', []), video_id)
            
    
    def addVideo(self, user_id, video_resource):
        '''
        Adds the given video to the db. 
        video_resource is the youtube video resource.
        Returns the video id (the primary key of the db) or None if an error occured.
        '''
        # Retrieving the video information from the resource
        video_youtube_id = video_resource["id"]
        video_name = video_resource["snippet"]["title"]
        video_name_encoded = video_name.encode('utf-8')
        video_view_count = int(video_resource["statistics"]["viewCount"])
        video_comment_count = int(video_resource["statistics"]["commentCount"])
        video_embeddable = video_resource["status"]["embeddable"]
        video_url = PopulateDB.getVideoUrl(video_youtube_id, video_embeddable)
        
        # Adding the video to the db
        try:
            # Making sure to follow the unique constraint of video_youtube_id
            self.db_cursor.execute(
                "SELECT video_id " \
                "FROM searcher_videos " \
                "WHERE video_youtube_id = %s",
                (video_youtube_id,))
            video_rows = self.db_cursor.fetchall()
            if len(video_rows) == 0:
                self.db_cursor.execute(
                     "INSERT INTO searcher_videos (video_youtube_id, video_name, video_channel_id_id, video_view_count, video_comment_count, video_embeddable, video_url)" \
                     "Values (%s, %s, %s, %s, %s, %s, %s)",
                    (video_youtube_id, video_name_encoded, user_id, video_view_count, video_comment_count, video_embeddable, video_url,)
                )
                video_id = self.db_cursor.lastrowid
                self.db_connection.commit()
                self.db_records_num += 1
            else:
                video_id = video_rows[0][0]
            return video_id
        except Exception as e:
            self.db_connection.rollback()
            print("Failed inserting the video with youtube_id = %s to the db: %s" % (video_youtube_id, str(e),))
            return None
        except:
            # Raising the exception again so keyboard interrupt wouldn't be ignored
            self.db_connection.rollback()
            print("Failed inserting the video with youtube_id = %s to the db." % (video_youtube_id,))
            raise
        
    @staticmethod
    def validateVideoResource(video_resource):
        '''
        Validates that the given video resource contains all of the requested parts and fields, 
        and prints an error message in case it doesn't.
        '''
        if 'id' not in video_resource:
            print("Invalid video resource, 'id' is missing.")
            return False
            
        if 'snippet' not in video_resource:
            print("Invalid video resource, 'snippet' is missing.")
            return False
            
        if 'channelId' not in video_resource['snippet']:
            print("Invalid video resource, 'snippet.channelId' is missing.")
            return False
            
        if 'title' not in video_resource['snippet']:
            print("Invalid video resource, 'snippet.title' is missing.")
            return False
            
        if 'statistics' not in video_resource:
            print("Invalid video resource, 'statistics' is missing.")
            return False

        if 'viewCount' not in video_resource['statistics']:
            print("Invalid video resource, 'statistics.viewCount' is missing.")
            return False       

        if 'commentCount' not in video_resource['statistics']:
            print("Invalid video resource, 'statistics.commentCount' is missing.")
            return False               
 
        if 'status' not in video_resource:
            print("Invalid video resource, 'status' is missing.")
            return False
            
        if 'embeddable' not in video_resource['status']:
            print("Invalid video resource, 'status.embeddable' is missing.")
            return False

        return True
    
    def addVideoAndUploaderAndCommentsByVideoResource(self, video_resource):
        '''
        Adds the given video to the db, as well as its uplodaer and its comments.
        video_resource is the youtube video resource.
        '''
        if not PopulateDB.validateVideoResource(video_resource):
            return 
        
        # Adding the user first, then the video and at last the comments (it must be in this order, due to foreign keys constrains).
        # Each step has to succeed in order for the next one to succeed.
        user_id = self.addUser(video_resource["snippet"]["channelId"])
        if user_id is not None:
            video_id = self.addVideo(user_id, video_resource)
            if video_id is not None:
                self.addVideoComments(video_id, video_resource["id"])
        
        
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
        self.addVideoAndUploaderAndCommentsByVideoResource(video_resource)
        
    def getPopularVideoListResponse(self, page_token, dummy_arg = None):
        '''
        Calls the youtube api list method of the resource Video, in order to find the popular videos.
        This should be wrapped with callMethodWithRetries.
        '''
        if page_token is not None:
            video_list_response = self.youtube_service.videos().list(
                part = VIDEO_LIST_PART, 
                fields  = VIDEO_LIST_FIELDS,
                chart = 'mostPopular', 
                maxResults = 50,
                pageToken = page_token
            ).execute()
        else:
            video_list_response = self.youtube_service.videos().list(
                part = VIDEO_LIST_PART, 
                fields  = VIDEO_LIST_FIELDS,
                chart = 'mostPopular', 
                maxResults = 50
            ).execute()
        return video_list_response
        
    def addVideoAndUploaderAndCommentsForVideosList(self, video_list_response, video_index):
        '''
        For each video in the given video-list response, tries to add it as well as its uploader and comments to the db.
        '''
        for video_resource in video_list_response.get("items", []):
            if 'id' in video_resource:                    
                print("Adding video #%d, youtube_id = %s" % (video_index, video_resource['id'],))
            else:
                print("Adding video #%d" % (video_index,))
            self.addVideoAndUploaderAndCommentsByVideoResource(video_resource)
            video_index += 1 
                
        return video_index
        
        
    def addPopularVideosAndUploadersAndComments(self):
        '''
        Finds the popular videos, and adds them as well as their uploaders and comments to the db.
        '''
        video_index = 0
        
        # Retrieving the first videos page
        video_list_response = PopulateDB.callMethodWithRetries(self.getPopularVideoListResponse, None)
        if video_list_response is None:
            print("Failed retrieving the first page of popular videos.")
            return
        video_index = self.addVideoAndUploaderAndCommentsForVideosList(video_list_response, video_index)
                
        # Retrieving the remaining videos pages
        while ('nextPageToken' in video_list_response) and (self.db_records_num < DB_RECORDS_NUM_MIN):
            page_token = video_list_response['nextPageToken']
            video_list_response = PopulateDB.callMethodWithRetries(self.getPopularVideoListResponse, page_token)  
            if video_list_response is None:
                print("Failed retrieving the popular videos page with page_token = %s" % (page_token,))
                return
            video_index = self.addVideoAndUploaderAndCommentsForVideosList(video_list_response, video_index)

    def getSpecificVideoListResponse(self, video_ids, dummy_arg = None):
        '''
        Calls the youtube api list method of the resource Videos, in order to retrieve infomration regarding
        the given videos (their ids are given).
        This should be wrapped with callMethodWithRetries.
        '''
        video_list_response = self.youtube_service.videos().list(
            part = VIDEO_LIST_PART, 
            fields  = VIDEO_LIST_FIELDS,
            id = video_ids, 
            maxResults = 50
        ).execute()
        return video_list_response
        
    def removeVideo(self, video_resource):
        '''
        Removes the given video from the db, if possible.
        '''
        if 'id' not in video_resource:
            return
        video_youtube_id = video_resource["id"]
        
        try:
            self.db_cursor.execute(
                "DELETE FROM searcher_videos " \
                "WHERE video_youtube_id = '%s'" 
                (video_youtube_id,)
            )
            self.db_connection.commit()
            self.db_records_num -= 1
            return True
        except Exception as e:
            self.db_connection.rollback()
            print("Failed removing the video with youtube_id = %s: %s" % (video_youtube_id, str(e),))
            return False
        except:
            # Raising the exception again so keyboard interrupt wouldn't be ignored
            self.db_connection.rollback()
            print("Failed removing the video with id = %s." % (video_youtube_id,))
            raise
        
    def updateVideo(self, video_resource):
        '''
        Updates the tuple of the given video in search_videos.
        Returns the video id (the primary key of the db) of the updated video, or None if an error occured.
        '''
        # Retrieving the video information from the resource
        video_youtube_id = video_resource["id"]
        video_name = video_resource["snippet"]["title"]
        video_name_encoded = video_name.encode('utf-8')
        video_view_count = int(video_resource["statistics"]["viewCount"])
        video_comment_count = int(video_resource["statistics"]["commentCount"])
        video_embeddable = video_resource["status"]["embeddable"]
        video_url = PopulateDB.getVideoUrl(video_youtube_id, video_embeddable)
        
        # Updating the video's row in the db
        try:
            self.db_cursor.execute(
                "SELECT video_id " \
                "FROM searcher_videos " \
                "WHERE video_youtube_id = %s",
                (video_youtube_id,))
            video_rows = self.db_cursor.fetchall()
            if len(video_rows) != 1:
                print("Error: There is no video with youtube_video_id = %s in the db." % (video_youtube_id,))
                return None
            video_id = video_rows[0][0]
            self.db_cursor.execute(
                "UPDATE  searcher_videos " \
                "SET     video_name = %s, video_view_count = %s, video_comment_count = %s, video_embeddable = %s, video_url = %s " \
                "WHERE   video_id = %s",
                (video_name_encoded, video_view_count, video_comment_count, video_embeddable, video_url, video_id,)
            )
            self.db_connection.commit()
            self.db_records_num += 1
            return video_id
        except Exception as e:
            self.db_connection.rollback()
            print("Failed updating the video with youtube_id = %s: %s" % (video_youtube_id, str(e),))
            return None
        except:
            # Raising the exception again so keyboard interrupt wouldn't be ignored
            self.db_connection.rollback()
            print("Failed updating the video with youtube_id = %s." % (video_youtube_id,))
            raise

        
    def updateVideoAndAddCommentsByVideoResource(self, video_resource):
        '''
        Updates the given video and adds its comments.
        '''
        if not PopulateDB.validateVideoResource(video_resource):
            self.removeVideo(video_resource)
            return 
        
        video_id = self.updateVideo(video_resource)
        if video_id is not None:
            self.addVideoComments(video_id, video_resource["id"])
   
    def updateVideoAndAddCommentsForVideosList(self, video_list_response, video_index):
        '''
        For each video in the given video-list response, tries to update it and add its comments to the db.
        '''
        for video_resource in video_list_response.get("items", []):
            if 'id' in video_resource:                    
                print("Updating video #%d, id = %s" % (video_index, video_resource['id'],))
            else:
                print("Updating video #%d" % (video_index,))
            self.updateVideoAndAddCommentsByVideoResource(video_resource)
            video_index += 1 
                
        return video_index
            
    def updateExistingVideosAndAddComments(self):
        '''
        Updates the information of the existing videos and add their comments.
        '''
        # Retrieving the ids of the videos
        video_youtube_ids = []
        try:
            query_exec_result = self.db_cursor.execute('SELECT video_youtube_id FROM searcher_videos')
            video_rows = self.db_cursor.fetchall()
        except Exception as e:
            print("Failed retrieving the youtube ids of the videos from the db: %s" % (str(e),))
            return
        except:
            # Raising the exception again so keyboard interrupt wouldn't be ignored
            print("Failed retrieving the youtube ids of the videos from the db")
            raise
            
        for video_row in video_rows:
            video_youtube_ids.append(video_row[0])
        
        # Retrieving the information on each group of videos, each group has at most 50 videos (because of the api limitation)
        video_index = 0
        video_youtube_id_groups = [video_youtube_ids[i : i + 50] for i in range(0, len(video_youtube_ids), 50)]
        for video_youtube_id_group in video_youtube_id_groups:
            video_list_response = PopulateDB.callMethodWithRetries(self.getSpecificVideoListResponse, ",".join(video_youtube_id_group))
            if video_list_response is None:
                print("Failed retrieving information regarding the following video group: %s" % (str(video_youtube_id_group),))
                return
            video_index = self.updateVideoAndAddCommentsForVideosList(video_list_response, video_index)
            
    def updateDB(self):
        '''
        Updates the db by deleting the existing comments and their authors (except for those who are also uploaders),
        updating the existing videos and at last adding the new comments (and their authors).
        '''
        try:
            print("Deleting the all of the existing comments and their authors")
            # Deleting all comments-authors (except for authors who are also uploaders), 
            # which will cause also the cascading deletion of their comments.
            self.db_cursor.execute("DELETE FROM searcher_users "                \
                                   "WHERE user_channel_id NOT IN "              \
                                   "    (SELECT DISTINCT video_channel_id_id "  \
                                   "     FROM searcher_videos)"
            )
            # Deleting the uploader's comments that weren't cascadingly deleted
            # TODO: Is it possible, mysql-wise, to just delete all of the table without a 'where' clause?
            self.db_cursor.execute("DELETE FROM searcher_comments "             \
                                   "WHERE comment_channel_id_id IN "            \
                                   "    (SELECT DISTINCT video_channel_id_id "  \
                                   "     FROM searcher_videos)"
            )
            self.db_connection.commit()
        except Exception as e:
            self.db_connection.rollback()
            print("Failed deleting the comments or the authors from the db: %s" % (str(e)))
            return
        except:
            # Raising the exception again so keyboard interrupt wouldn't be ignored
            self.db_connection.rollback()
            print("Failed deleting comments-authors (and their comments) from the db.")
            raise
            
        self.updateExistingVideosAndAddComments()
        
    def getSearchListResponse(self, search_string, max_results):
        '''
        Calls the youtube api list method of the resource Search, in order to find the videos that match
        the give search string.
        The max_results parameter should be between 1 and 50.
        This should be wrapped with callMethodWithRetries.
        '''
        search_list_response = self.youtube_service.search().list(
            part = 'id',
            fields = 'items(id(kind, videoId))', 
            q = search_string, 
            type = 'video',
            maxResults = max_results
        ).execute()
        return search_list_response
        
    @staticmethod
    def validateSearchResource(search_resource):
        '''
        Checks if the given search resource is valid, and prints an error message if it isn't.
        Returns True if it's valid, False otherwise.
        '''
        if 'id' not in search_resource:
            print("Invalid search resource: 'id' part is missing.")
            return False

        if 'kind' not in search_resource['id']:
            print("Invalid search resource: 'id.kind' field is missing.")
            return False

        if search_resource['id']['kind'] != 'youtube#video':
            print("Invalid search resource: 'id.kind' field is not 'youtube#video'")
            return False
            
        if 'videoId' not in search_resource['id']:
            print("Invalid search resouece: 'id.videoId' field is missing.")
            return False

        return True
            
    def addVideosAndUploadersAndCommentsBySearchString(self, search_string, max_videos):
        '''
        Finds videos that match the given search string, 
        and adds them as well as their uploaders and comments to the db. 
        The max_results parameter should be between 1 and 50. 
        '''
        # Validating the max_results parameter
        if max_videos < 1:
            print("The max_videos parameter for the matching videos should be between 1 to 50. Using max_videos = 1.")
            max_videos = 1
        elif max_videos > 50:
            print("The max_videos parameter for the matching videos should be between 1 to 50. Using max_videos = 50.")
            max_videos = 50
        
        # Retrieving the video ids
        video_ids = []
        search_list_response = PopulateDB.callMethodWithRetries(self.getSearchListResponse, search_string, max_videos)
        if search_list_response is None:
            print("Failed retrieving the matching videos.")
            return
        for search_resource in search_list_response.get('items',[]):
            if PopulateDB.validateSearchResource(search_resource):
                video_ids.append(search_resource['id']['videoId'])
               
        # Retrieving the information (and adding to the db) for each group of videos, each group has at most 50 videos 
        # (because of the api limitation)
        video_index = 0
        video_id_groups = [video_ids[i : i + 50] for i in range(0, len(video_ids), 50)]
        for video_id_group in video_id_groups:
            video_list_response = PopulateDB.callMethodWithRetries(self.getSpecificVideoListResponse, ",".join(video_id_group))
            if video_list_response is None:
                print("Failed retrieving information regarding the following video group: %s" % (str(video_id_group),))
                return
            video_index = self.addVideoAndUploaderAndCommentsForVideosList(video_list_response, video_index)
        

def areArgsValid(args):
    '''
    Checks if the given command-line arguments are valid.
    '''
    if len(args) < 2:
        return False
    
    if args[1] in ['populate', 'update']:
        if len(args) == 3:
            return args[2].isdigit()
        return len(args) == 2
        
    elif args[1] == 'add_by_keyword':
        if len(args) < 4:
            return False
        if not args[3].isdigit():
            return False
        if len(args) > 5:
            return False
        if len(args) == 5:
            return args[4].isdigit()
        return True
        
    else:
        return False
        
def isSpecifiedCommentsPerVideoMax(args):
    return ((len(args) == 5) or (len(args) == 3))
        
if __name__ == "__main__":

    if not areArgsValid(sys.argv):
        print(USAGE)
        exit()
        
    if isSpecifiedCommentsPerVideoMax(sys.argv):
        # This parameter is always last since it's optional
        db = PopulateDB(comments_per_video_max = int(sys.argv[-1]))
    else:
        db = PopulateDB()
        
    try:        
        if (len(sys.argv) == 1) or (sys.argv[1] == 'populate'):
            db.addPopularVideosAndUploadersAndComments()
            print("Populated the db with %d records." % (db.db_records_num,))
        elif sys.argv[1] == 'update':
            db.updateDB()
            print("After update: db has %d records." % (db.db_records_num,))
        elif sys.argv[1] == 'add_by_keyword':
            db.addVideosAndUploadersAndCommentsBySearchString(sys.argv[2], int(sys.argv[3]))
            print("Added %d new records." % (db.db_records_num,))        
        db.cleanup()
    except:
        db.cleanup()
        raise

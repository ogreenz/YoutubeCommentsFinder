from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from oauth2client.tools import argparser
import MySQLdb
import httplib
import httplib2
import os
import random
import sys
import time
import traceback
#from django.db import connection

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
COMMENTS_PER_VIDEO_MAX = 2000

# Maximum number of times to retry before giving up.
MAX_RETRIES = 3

# Always retry when these exceptions are raised.
RETRIABLE_EXCEPTIONS = (httplib2.HttpLib2Error, IOError, httplib.NotConnected,
  httplib.IncompleteRead, httplib.ImproperConnectionState,
  httplib.CannotSendRequest, httplib.CannotSendHeader,
  httplib.ResponseNotReady, httplib.BadStatusLine)

# Always retry when an apiclient.errors.HttpError with one of these status
# codes is raised.
RETRIABLE_STATUS_CODES = [400, 500, 502, 503, 504]

class PopulateDB(object):

    def __init__(self):
        self.youtube_service = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey = DEVELOPER_KEY)
        self.db_connection = MySQLdb.connect(DB_SERVER, DB_USER, DB_USER, DB_PASSWORD)
        self.db_cursor = self.db_connection.cursor()
        self.db_records_num = 0
        
    def cleanup(self):
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
    
    def addUser(self, channel_id, user_channel_title = None):
        '''
        Adds the user with the given channel id to the db.
        If the channel's title is given, it should be given as retrieved from the api response (not encoded).
        If the channel's title is not given, the function uses the youtube service to retrieve it.
        Return True for success, False for failure.
        '''
        if user_channel_title is None:
            # Retrieving the channel's title
            channel_list_response = PopulateDB.callMethodWithRetries(self.getChannelListResponse, channel_id)
            if not PopulateDB.validateChannelListResponse(channel_list_response, channel_id):
                return False
            user_channel_resource = channel_list_response["items"][0]
            if not PopulateDB.validateChannelResource(user_channel_resource, channel_id):
                return False
            user_channel_title = user_channel_resource["snippet"]["title"]
        user_channel_title_encoded = user_channel_title.encode('utf-8')
        
        # Adding to the db
        try:
            affected_rows_num = self.db_cursor.execute(
                 "INSERT INTO searcher_users (user_channel_id, user_channel_title) " \
                 "Values (%s, %s) " \
                 "ON DUPLICATE KEY UPDATE user_channel_id = user_channel_id",
                (channel_id, user_channel_title_encoded)
            )
            self.db_connection.commit()
            if affected_rows_num == 1:
                self.db_records_num += 1
            return True
        except Exception as e:
            self.db_connection.rollback()
            print("Failed inserting the user with channelId = %s to the db." %(channel_id))
            return False
        except:
            # Raising the exception again so keyboard interrupt wouldn't be ignored
            self.db_connection.rollback()
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

        
    def addSingleComment(self, comment_id, video_id, author_channel_id, author_display_name, text_display, like_count):
        '''
        Adds the given comment to the db, after adding its author.
        The given author_display_name and text_display should be as retrieved from the api response (not encoded).
        '''
        # First adding the user who commented this comment
        if not self.addUser(author_channel_id, author_display_name):
            return
            
        text_display_encoded = text_display.encode('utf-8')
        try:
            self.db_cursor.execute(
                 "INSERT INTO searcher_comments (comment_id, video_id_id, comment_channel_id_id, comment_text, like_count)" \
                 "Values (%s, %s, %s, %s, %s)" \
                 "ON DUPLICATE KEY UPDATE comment_id = comment_id",
                (comment_id, video_id, author_channel_id, text_display_encoded, like_count)
            )
            self.db_connection.commit()
            self.db_records_num += 1
        except Exception as e:
            self.db_connection.rollback()
            print("Failed inserting comment with id = %s to the db" % (comment_id,))
            return
        except:
            # Raising the exception again so keyboard interrupt wouldn't be ignored
            self.db_connection.rollback()
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
            
    def getCommentThreadsListReponse(self, video_id, page_token):
        '''
        '''
        requested_fields = "nextPageToken, items(id, snippet(topLevelComment(id, snippet(authorDisplayName, authorChannelId, textDisplay, likeCount))))"
        if page_token is None:
            comment_threads_list_response = self.youtube_service.commentThreads().list(
                part = "snippet", 
                fields = requested_fields, 
                videoId = video_id, 
                maxResults = 100,
                textFormat = 'plainText'
            ).execute()   
        else:
            comment_threads_list_response = self.youtube_service.commentThreads().list(
                part = "snippet", 
                fields = requested_fields, 
                videoId = video_id, 
                maxResults = 100,
                textFormat = 'plainText',
                pageToken = page_token
            ).execute()          
        return comment_threads_list_response
            
    def addVideoComments(self, video_id):
        '''
        Adds comments related to the given video id to the db, using the youtube service.
        Returns True for success or False for failure.
        '''
        # Retrieving the first comments page
        comment_threads_list_response = PopulateDB.callMethodWithRetries(self.getCommentThreadsListReponse, video_id, None)
        if comment_threads_list_response is None:
            print("Failed retrieving the comments page")
            return
        comments_num = len(comment_threads_list_response.get('items', []))
        self.addCommentPage(comment_threads_list_response.get('items', []), video_id)

        # Retrieving the remaining comments pages
        while ('nextPageToken' in comment_threads_list_response) and (comments_num < COMMENTS_PER_VIDEO_MAX):
            page_token = comment_threads_list_response['nextPageToken']
            comment_threads_list_response = PopulateDB.callMethodWithRetries(self.getCommentThreadsListReponse, video_id, page_token)
            if comment_threads_list_response is None:
                print("Failed retrieving the comments page")
                return
            comments_num += len(comment_threads_list_response.get('items', []))
            self.addCommentPage(comment_threads_list_response.get('items', []), video_id)
            
    
    def addVideo(self, video_resource):
        '''
        Adds the given video to the db. 
        video_resource is the youtube video resource.
        Returns True for success of False for failure.
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
                 "INSERT INTO searcher_videos (video_id, video_name, video_channel_id_id, video_view_count, video_comment_count, video_embeddable, video_url)" \
                 "Values (%s, %s, %s, %s, %s, %s, %s)" \
                 "ON DUPLICATE KEY UPDATE video_id = video_id",
                (video_id, video_name_encoded, video_channel_id, video_view_count, video_comment_count, video_embeddable, video_url)
            )
            self.db_connection.commit()
            self.db_records_num += 1
            return True
        except Exception as e:
            self.db_connection.rollback()
            print("Failed inserting the video with id = %s to the db." % (video_id,))
            return False
        except:
            # Raising the exception again so keyboard interrupt wouldn't be ignored
            self.db_connection.rollback()
            print("Failed inserting the video with id = %s to the db." % (video_id,))
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
        if self.addUser(video_resource["snippet"]["channelId"]):
            if self.addVideo(video_resource):
                self.addVideoComments(video_resource["id"])
        
        
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
        
    def getVideoListResponse(self, page_token, dummy_arg = None):
        '''
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
                print("Adding video #%d, id = %s" % (video_index, video_resource['id'],))
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
        video_list_response = PopulateDB.callMethodWithRetries(self.getVideoListResponse, None)
        if video_list_response is None:
            print("Failed retrieving the popular videos page")
            return
        video_index = self.addVideoAndUploaderAndCommentsForVideosList(video_list_response, video_index)
                
        # Retrieving the remaining videos pages
        while ('nextPageToken' in video_list_response) and (self.db_records_num < DB_RECORDS_NUM_MIN):
        
            video_list_response = PopulateDB.callMethodWithRetries(self.getVideoListResponse, video_list_response['nextPageToken'])  
            if video_list_response is None:
                print("Failed retrieving the popular videos page")
                return
            video_index = self.addVideoAndUploaderAndCommentsForVideosList(video_list_response, video_index)
 
 
if __name__ == "__main__":
    db = PopulateDB()
    db.addPopularVideosAndUploadersAndComments()
    print("Populated the db with %d records." % (db.db_records_num,))
    if db.db_records_num < DB_RECORDS_NUM_MIN:
        print("The required minimal number of records is %d. " \
              "Consider changing the boundry of number of comments per video." % (DB_RECORDS_NUM_MIN,))
    db.cleanup()
        

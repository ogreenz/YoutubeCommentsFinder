from django.db import connection
from .SearchResult import CommentResult, SearchResult
from .PopulateDB import PopulateDB
import pdb
import searcher
import MySQLdb

DB_SERVER = 'mysqlsrv.cs.tau.ac.il'
DB_NAME = 'DbMysql09'
DB_USER = DB_NAME
DB_PASSWORD = DB_NAME
COMMENTS_PER_NEW_VIDEO_MAX = 100
NEW_VIDEOS_PER_UNSUCCESSFUL_SEARCH = 1

# The following dictionaries are based on the order of the attributes in each table in the database
USER_ATTRS = { 
                'user_channel_id':      0, 
                'user_channel_title':   1 
             }
VIDEO_ATTRS = { 
                'video_id':             0,
                'video_name':           1,
                'video_view_count':     2,
                'video_comment_count':  3,
                'video_embeddable':     4,
                'video_url':            5,
                'video_channel_id_id':  6 
              }

COMMENT_ATTR = {
                'comment_id':           0,
                'comment_text':         1,
                'like_count':           2,
                'comment_channel_id_id':3,
                'video_id_id':          4 
               }

class DB(object):

    def __init__(self):
        self.db_connection = MySQLdb.connect(DB_SERVER, DB_USER, DB_USER, DB_PASSWORD)
        self.db_populater = PopulateDB(comments_per_video_max = COMMENTS_PER_NEW_VIDEO_MAX, db_connection = self.db_connection) 
		
    def cleanup(self):
        self.db_populater.cleanup()
        self.db_connection.close()

    def getVideosAndComments(self, videoName, videoUploader, videoCommenter, commentText):
        return self.fetchFromDB(True, videoName, videoUploader, videoCommenter, commentText)

    @staticmethod
    def getUtf8EncodedString(strToEncode):
        if strToEncode is not None:
            strToEncode = strToEncode.encode('utf-8')
        return strToEncode
        
    def fetchFromDB(self, isFirstTry, videoName, videoUploader, videoCommenter, commentText):

        if commentText is '':
            # retrn with nothing... we must have commentText to search
            pass
            
        # Encoding in utf-8
        videoName = DB.getUtf8EncodedString(videoName)
        videoUploader = DB.getUtf8EncodedString(videoUploader)
        videoCommenter = DB.getUtf8EncodedString(videoCommenter)
        commentText = DB.getUtf8EncodedString(commentText)

        #cursor = connection.cursor()
        cursor = self.db_connection.cursor()


        searchRes = []
        video_ids = []
        video_rows = []
        comment_rows = []

        is_sub_qurey_has_results = True

        """
        First we try to get video Id's that answer on the videoName and videoUploader category
        """
        query = "SELECT	* From searcher_videos as videos, searcher_users as users " \
                "Where videos.video_channel_id_id = users.user_channel_id "

        if videoName != '':
            query += "and videos.video_name Like '%"+videoName+"%' "
        if videoUploader != '':
            query += "and users.user_channel_title LIKE '%"+videoUploader+"%' "

        query += "Order By videos.video_id"
        if videoName != '' or videoUploader != '':
            exec_result = cursor.execute(query)
            video_rows = cursor.fetchall()
            if len(video_rows) > 0:
                for row in video_rows:
                    video_ids.append("'" + row[VIDEO_ATTRS['video_id']] + "'")
            else:
                is_sub_qurey_has_results = False
                # lets try to find some video with these params
                if isFirstTry:
                    # call the function that adds new videos to our db
                    if videoName is not None:
                        self.db_populater.addVideosAndUploadersAndCommentsBySearchString(videoName, NEW_VIDEOS_PER_UNSUCCESSFUL_SEARCH)
                        # retry the search
                        return self.fetchFromDB(False,videoName, videoUploader, videoCommenter, commentText)
                    else:
                        # let the user know there is no data for this search
                        # return with nothing
                        pass
                else:
                    # let the user know there is no data for this search
                    # return with nothing
                    pass

        """
        We got results for the videoUploader or videoName, lets fint some data with the right comment text and comment author
        Or no query regarding the video data was preformed
        """
        if is_sub_qurey_has_results:
            query = "SELECT	* From searcher_comments as comments, searcher_users as users " \
                    "Where  comments.comment_channel_id_id = users.user_channel_id "
            if len(video_rows) > 0:
                query += "and comments.video_id_id in ("+ ",".join(video_ids) + ") "


            if videoCommenter != '':
                query += "and users.user_channel_title Like '%" + videoCommenter + "%' "

            if commentText != '':
                query += "and comments.comment_text Like '%" + commentText + "%' "

            query += "Order By comments.video_id_id"
            if videoCommenter != '' or commentText != '':
                exec_result = cursor.execute(query)
                comment_rows = cursor.fetchall()

                if len(comment_rows) <= 0:
                    # we found nothing
                    if isFirstTry:
                        # call the function that adds new videos to our db
                        if videoName is not None:
                            self.db_populater.addVideosAndUploadersAndCommentsBySearchString(videoName, NEW_VIDEOS_PER_UNSUCCESSFUL_SEARCH)
                            # retry the search
                            return self.fetchFromDB(False,videoName, videoUploader, videoCommenter, commentText)
                        # else
                            # let the user know there is no data for this search
                            # return with nothing
                            pass
                    else:
                        # let the user know there is no data for this search
                        # return with nothing
                        pass

            """
            In case no query regarding the video data was preformed-
            now we have the comments data, so we know which video id's we need and we can get the relevant videos data
            """
            if (len(video_ids) <= 0) and (len(comment_rows) > 0):
                #we need to get the video data (because no search related to video was preformed)
                for row in comment_rows:
                    video_ids.append("'" + row[COMMENT_ATTR['video_id_id']] + "'")
                    
                # now we have video id's, and we can preform the query
                query = "SELECT	* From searcher_videos as videos, searcher_users as users " \
                        "Where videos.video_channel_id_id = users.user_channel_id " \
                        "and videos.video_id in ("+ ",".join(video_ids) + ") "
                print(query)
                exec_result = cursor.execute(query)
                video_rows = cursor.fetchall()

        """
        now we have videos+comments data, we can bulid the results for the web app

        if len(video_rows)<=0 we wiil return the empty searchRes and web will know there are no results
        """
        if len(video_rows) > 0:
            for video in video_rows:
                comment_list = []
                for comment in comment_rows:
                    if video[VIDEO_ATTRS['video_id']] == comment[COMMENT_ATTR['video_id_id']]:
                        cmt = CommentResult(comment[len(COMMENT_ATTR) + USER_ATTRS['user_channel_title']].decode('utf-8'), 
                                            comment[COMMENT_ATTR['comment_text']].decode('utf-8'), 
                                            str(comment[COMMENT_ATTR['like_count']]))
                        comment_list.append(cmt)
                if len(comment_list) != 0:
                    vid = SearchResult(video[VIDEO_ATTRS['video_name']], 
                                       video[len(VIDEO_ATTRS) + USER_ATTRS['user_channel_title']], 
                                       comment_list, 
                                       video[VIDEO_ATTRS['video_url']], 
                                       video[VIDEO_ATTRS['video_embeddable']])
                    searchRes.append(vid)

        return searchRes

    def addVideoToInternalDB(self, videoName):
        #For Tamar And Alon
        raise NotImplemented()

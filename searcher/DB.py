from django.db import connection
from .SearchResult import CommentResult, SearchResult
import pdb
import searcher
import MySQLdb

DB_SERVER = 'mysqlsrv.cs.tau.ac.il'
DB_NAME = 'DbMysql09'
DB_USER = DB_NAME
DB_PASSWORD = DB_NAME

class DB(object):

    def __init__(self):
		self.db_connection = MySQLdb.connect(DB_SERVER, DB_USER, DB_USER, DB_PASSWORD)
		
    def cleanup(self):
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
                    video_ids.append("'" + row[0] + "'")
            else:
                is_sub_qurey_has_results = False
                # lets try to find some video with these params
                if isFirstTry:
                    # call the function that adds new videos to our db
                    # if videoName is not None:
                    # self.addVideoToInternalDB(videoName)
                    # retry the search
                    # searchRes = DB.fetchFromDB(False,videoName, videoUploader, videoCommenter, commentText)
                    # return video_Objs
                    # else
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
                query += "and comments.comment_author_display_name Like '%" + videoCommenter + "%' "

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
                        # if videoName is not None:
                            # self.addVideoToInternalDB(videoName)
                            # retry the search
                            # searchRes = DB.fetchFromDB(False,videoName, videoUploader, videoCommenter, commentText)
                            # return video_Objs
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
                    video_ids.append("'" + row[5] + "'")

                # now we have video id's, and we can preform the query
                query = "SELECT	* From searcher_videos as videos, searcher_users as users " \
                        "Where videos.video_channel_id_id = users.user_channel_id " \
                        "and videos.video_id in ("+ ",".join(video_ids) + ") "
                exec_result = cursor.execute(query)
                video_rows = cursor.fetchall()

        """
        now we have videos+comments data, we can bulid the results for the web app

        if len(video_rows)<=0 we wiil return the empty searchRes and web will know there are no results
        """
        if len(video_rows) >0:
            for video in video_rows:
                comment_list = []
                for comment in comment_rows:
                    if video[0] == comment[5]:
                        cmt = CommentResult(comment[1].decode('utf-8'), comment[2].decode('utf-8'), str(comment[3]))
                        comment_list.append(cmt)
                if len(comment_list) != 0:
                    vid = SearchResult(video[1], video[8], comment_list, video[5], video[4])
                    searchRes.append(vid)

        return searchRes

    def addVideoToInternalDB(self, videoName):
        #For Tamar And Alon
        raise NotImplemented()

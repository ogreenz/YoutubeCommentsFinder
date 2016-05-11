from django.db import connection

class DB:

    def __init__(self):
        #For Tamar And Alon
        pass

    def getVideosAndComments(self, videoName, videoUploader, videoCommenter, commentText):
        cursor = connection.cursor()

        query = "SELECT searcher_comments.comment_text, searcher_comments.comment_author_display_name, " \
                "searcher_comments.comment_channel_id, searcher_videos.video_name, searcher_videos.video_channel_id" \
                "From searcher_comments, searcher_videos" \
                "Where searcher_comments.video_id = searcher_videos.video_id" \

        if videoName != null:
            query += " And searcher_videos.video_name Like '%" + videoName + "%'"

        #if videoUploader != null:
        #   query += "And searcher_videos.video_name Like '%" + videoName + "%'"

        if videoCommenter != null:
            query += " And searcher_comments.comment_author_display_name Like '%" + videoCommenter + "%'"

        if commentText != null:
            query += " And searcher_comments.comment_text Like '%" + commentText + "%'"

        query += " Order by searcher_videos.video_name"
        exec_result = cursor.execute(query)
        row = cursor.fetchall()


        if exec_result.rowcount == 0:
            #call the update query..
            self.addVideoToInternalDB(videoName)

        return row

    def addVideoToInternalDB(self, videoName):
        #For Tamar And Alon
        raise NotImplemented()
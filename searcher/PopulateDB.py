from apiclient.discovery import build
from apiclient.errors import HttpError
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

# TODO: Delete this
TEMP_SPECIFIC_VIDEO_ID = '-OWkLF2HLp0'
TEMP_SPECIFIC_VIDEO_URL = "//www.youtube.com/embed/-OWkLF2HLp0"

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
		
		
	def addVideo(self, videoId):
		'''
		Adds the video with the given id to the db, using the youtube service to get its details.
		'''
		# Retrieving the video's details
		video_response = self.youtube_service.videos().list(
			id = videoId, 
			part = 'snippet, statistics, status, player', 
			fields  = 'items(snippet(channelId, title, thumbnails), statistics(viewCount, commentCount), status(embeddable), player(embedHtml))'
		).execute()
		
		video_result = video_response.get("items", [])[0]
		video_name = video_result["snippet"]["title"]
		video_channel_id = video_result["snippet"]["channelId"]
		video_view_count = int(video_result["statistics"]["viewCount"])
		video_comment_count = int(video_result["statistics"]["commentCount"])
		video_embedable = video_result["status"]["embeddable"]
		video_url = TEMP_SPECIFIC_VIDEO_URL
		
		# First adding the uploader to the db
		if not self.addUser(video_channel_id):
			return False
		
		# Adding the video to the db
		try:
			self.db_cursor.execute(
				 "INSERT INTO searcher_videos (video_id, video_name, video_channel_id_id, video_view_count, video_comment_count, video_embedable, video_url)" \
				 "Values (%s, %s, %s, %s, %s, %s, %s)" \
				 "ON DUPLICATE KEY UPDATE video_id = video_id",
				(videoId, video_name, video_channel_id, video_view_count, video_comment_count, video_embedable, video_url)
			)
			self.db_connection.commit()
		except:
			# TODO: Handle
			self.db_connection.rollback()
			raise
		
		return True
		
	def addComments(self, videoId):
		'''
		Adds comments related to the given video id to the db, using the youtube service.
		'''
		requested_fields = "nextPageToken, pageInfo, items(snippet(topLevelComment(id, snippet(authorDisplayName, authorChannelId, textDisplay, likeCount))))"
		
		# Retrieving the first comments page
		comment_threads = self.youtube_service.commentThreads().list(
			part = "snippet", 
			fields = requested_fields, 
			videoId = videoId, 
			maxResults = 100
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
				pageToken = next_page_token
			).execute()
			
			comments_num += len(comment_threads.get('items', []))
			self.addCommentPage(comment_threads.get('items', []), videoId)
			
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
		
		
	def addVideoAndComments(self, videoId):
		'''
		Adds the video with the given id to the db, along with its related comments.
		'''
		self.addVideo(videoId)
		self.addComments(videoId)
		
if __name__ == "__main__":
	db = PopulateDB()
	db.addVideoAndComments(TEMP_SPECIFIC_VIDEO_ID)
	db.cleanup()
		

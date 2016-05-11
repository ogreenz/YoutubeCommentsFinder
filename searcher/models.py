from __future__ import unicode_literals

from django.db import models
from django.utils.encoding import python_2_unicode_compatible

# Create your models here.


@python_2_unicode_compatible
class Users(models.Model):
    user_channel_id = models.TextField(primary_key=True, help_text="user display name comes from comments: snippets.authorDisplayName")
    user_channel_title = models.TextField(help_text="comes from the channels api snippet.title")

    def __str__(self):
        return 'id: %s\n channel _title: %s' % (self.user_channel_id, self.user_channel_title)


@python_2_unicode_compatible
class Videos(models.Model):
    video_id = models.CharField(max_length=500, primary_key=True, help_text="Youtube unique video id")
    video_name = models.TextField(help_text="video name caption")
    video_channel_id = models.ForeignKey(Users, on_delete=models.CASCADE, help_text="comes from the snippets.ChannelId")
    video_view_count = models.BigIntegerField(help_text="number of video view count")
    video_comment_count = models.BigIntegerField(help_text="number of video comments")
    # there's no need for rating, the rating means what is the appropriate target audience.
    video_embedable = models.BooleanField(help_text="this value indicates whether the video can be embedded on another website (ours :)")
    video_url = models.TextField(help_text="embed url of the video it's the embedHtml field in player key, is the video isn't embeddable the video url is a regular video.")

    def __str__(self):
        return "video name: %s\nvideo url: %s" % (self.video_name, self.video_url)



@python_2_unicode_compatible
class Comments(models.Model):
    comment_id = models.BigIntegerField(help_text="The ID that YouTube uses to uniquely identify the comment. comes from id", primary_key=True)
    video_id = models.ForeignKey(Videos, on_delete=models.CASCADE)
    comment_channel_id = models.ForeignKey(Users, on_delete=models.CASCADE, help_text="Comes from snippet.authorChannelId.value")
    comment_author_display_name = models.TextField(help_text="can be fetched from snippet.authorDisplayName")
    comment_text = models.TextField(help_text="the comment text")
    like_count = models.BigIntegerField(help_text="comment like count")

    def __str__(self):
        return "comment id: %d\n comment_test:%s" % (self.comment_id, self.comment_text)





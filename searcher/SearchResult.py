

class SearchResult(object):

    def __init__(self, video_title, video_uploader, comments_list, video_url, is_embed):
        self.video_title = video_title
        self.video_uploader = video_uploader
        self.comments_list = comments_list # a list of CommentResult
        self.video_url = video_url
        self.is_embed = is_embed


class CommentResult(object):

    def __init__(self, user, content, likes):
        self.user = user
        self.content = content
        self.likes = likes

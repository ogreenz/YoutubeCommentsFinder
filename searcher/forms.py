from django import forms


class SearchForm(forms.Form):
    video_name = forms.CharField(required=False)
    video_uploader = forms.CharField(required=False)
    video_commenter = forms.CharField(required=False)
    video_comment_keyword = forms.CharField(required=True)

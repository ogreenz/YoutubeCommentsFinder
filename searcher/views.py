from django.shortcuts import render

# Create your views here.
from django.http import HttpResponse
from django.shortcuts import render
from .forms import SearchForm
from . import DB
from .PopulateDB import PopulateDB
from MySQLdb import escape_string
def index(request):
    return render(request, 'searcher/base.html')

def searcher(request):
    form_class = SearchForm
    results = []
    has_comment_keyword = False
    if request.method == 'POST':
        # form = form_class(data=request.POST)
        # if form.is_valid():
        video_name = request.POST.get("video_name", None)
        if video_name:
            video_name = escape_string(video_name)
        video_uploader = request.POST.get("video_uploader", None)
        if video_uploader:
            video_uploader = escape_string(video_uploader)
        commenter = request.POST.get("commenter", None)
        if commenter:
            commenter = escape_string(commenter)
        keyword = request.POST.get("comment_keyword", None)
        if keyword:
            keyword = escape_string(keyword)
         
        db = DB.DB()
        results = db.getVideosAndComments(video_name, video_uploader, commenter, keyword)
        db.cleanup()
        #results = DB.DB.getVideosAndComments(video_name, video_uploader, commenter, keyword)
        
        if keyword:
            has_comment_keyword = True
    context = {'results': results, 'has_comment_keyword': has_comment_keyword}
    return render(request, 'searcher/search_results.html', context)
    """
    rows = DB.getVideosAndComments(video_name, uplaoder_name)
    context = {'rows': rows}
    if rows:
        return render(request, 'searcher/index.html', context)
    else:
        # lets fetch some information from youtube
        addvideotointernalDB
        rows = DB.getVideosAndComments(...)
        if not rows:
            return HttpResponse("we got nothing")
        return render(request, 'searcher/index.html', context)
    """


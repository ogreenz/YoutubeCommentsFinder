from django.shortcuts import render

# Create your views here.
from django.http import HttpResponse
from django.shortcuts import render
from .forms import SearchForm
from . import DB
from .PopulateDB import PopulateDB

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
        video_uploader = request.POST.get("video_uploader", None)
        commenter = request.POST.get("commenter", None)
        keyword = request.POST.get("comment_keyword", None)
         
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

def populateDb(request):
	db = PopulateDB()
	db.addVideoAndComments('-OWkLF2HLp0')
	db.cleanup()
	return render(request, 'searcher/base.html')

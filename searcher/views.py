from django.shortcuts import render

# Create your views here.
from django.http import HttpResponse
from django.shortcuts import render
from forms import SearchForm
import DB
def index(request):
    return render(request, 'searcher/base.html')

def searcher(request):
    form_class = SearchForm
    results = []
    if request.method == 'POST':
        # form = form_class(data=request.POST)
        # if form.is_valid():
        video_name = request.POST.get("video_name", None)
        video_uploader = request.POST.get("video_uploader", None)
        commenter = request.POST.get("commenter", None)
        keyword = request.POST.get("keyword", None)
        results = DB.DB.getVideosAndComments(video_name, video_uploader, commenter, keyword)
    context = {'results': results}
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

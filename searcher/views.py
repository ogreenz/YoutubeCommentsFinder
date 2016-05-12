from django.shortcuts import render

# Create your views here.
from django.http import HttpResponse
from django.shortcuts import render

def index(request):
    return HttpResponse("Hello, world. You're at the polls index.")

def searcher(request, video_name, uploader_name):
    return HttpResponse("Hello, searcher")
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

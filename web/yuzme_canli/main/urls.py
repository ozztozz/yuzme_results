from django.urls import path
from . import views 
urlpatterns = [
    path('', views.home, name='home'),  
    path('events/', views.event_list, name='event_list'),
    path('events/<int:event_id>/', views.event_detail, name='event_detail'),
    path('events/<int:event_id>/selected/', views.event_selected_results, name='event_selected_results'),
    path('swimmers/', views.swimmer_results, name='swimmer_results'),
    path('clubs/', views.club_select, name='club_select'),
    path('api/ingest-results/', views.ingest_results, name='ingest_results'),
]
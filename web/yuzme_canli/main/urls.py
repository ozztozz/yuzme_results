from django.urls import path
from . import views 
urlpatterns = [
    path('', views.home, name='home'),  
    path('events/', views.event_list, name='event_list'),
    path('events/create/', views.create_event_page, name='create_event_page'),
    path('events/<int:event_id>/', views.event_detail, name='event_detail'),
    path('events/<int:event_id>/selected/', views.event_selected_results, name='event_selected_results'),
    path('swimmers/', views.swimmer_results, name='swimmer_results'),
    path('clubs/', views.club_select, name='club_select'),
    path('api/create-event/', views.create_event_from_url, name='create_event_from_url'),
    path('api/ingest-events/', views.ingest_events, name='ingest_events'),
    path('api/ingest-results/', views.ingest_results, name='ingest_results'),
]
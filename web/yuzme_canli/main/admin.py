from django.contrib import admin

# Register your models here.
from .models import Event, Result
admin.site.register(Event)
admin.site.register(Result)

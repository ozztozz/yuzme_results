from django.db import models

# Create your models here.
class Event(models.Model):
    unique_name = models.CharField(max_length=100, unique=True, default="")
    title = models.CharField(max_length=255)
    date = models.CharField(max_length=64)
    location = models.CharField(max_length=255)

    class Meta:
        unique_together = [("title", "date", "location")]

    def __str__(self):
        return self.title + " - " + self.date + " - " + self.location

class Result(models.Model):
    event = models.ForeignKey(Event, on_delete=models.CASCADE)
    event_order = models.IntegerField(default=0)
    swimmer_name = models.CharField(max_length=255)
    year_of_birth = models.IntegerField()
    gender = models.CharField(max_length=10)    
    club = models.CharField(max_length=255)
    swimming_style = models.CharField(max_length=50)
    distance = models.IntegerField()
    seri_no = models.IntegerField()
    lane = models.IntegerField()
    seed = models.CharField(max_length=50)
    result = models.CharField(max_length=50)
    rank = models.IntegerField()



    def __str__(self):
        return f"{self.swimmer_name} - {self.result} (Rank: {self.rank})"
    
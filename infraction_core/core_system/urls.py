from django.urls import path
from . import views

urlpatterns = [
    path('comparendos/', views.Fotomultas.as_view(), name='comparendos'),
    path('comparendosconsulta/', views.FotomultasConsulta.as_view(), name='comparendosconsulta'),
]
from django.urls import path
from . import consultamasiva, views

urlpatterns = [
    path('comparendos/', views.Fotomultas.as_view(), name='comparendos'), #consulta de comparendos a la bd
    path('comparendosconsulta/', views.FotomultasConsulta.as_view(), name='comparendosconsulta'), #consulta de comparendos al scraper
    path('consultamasiva/', consultamasiva.Multas.as_view(), name='consultamasiva'), #consulta masiva de comparendos
    path('ComparendosCrm/', views.ComparendosCrm.as_view(), name='ComparendosCrm'), #consulta de comparendos sin thread
]
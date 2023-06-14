from django.core.files.storage import FileSystemStorage
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework import status
from utils.tools import IUtility
from django.core.files.storage import default_storage
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from datetime import date
from core_system.models import *
import pandas as pd
import json
import requests
import multiprocessing
from multiprocessing.pool import ThreadPool
from django.core.files.storage import FileSystemStorage
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework import status
from .profiles import BasicProfile
from utils.tools import IUtility
from .controllers import InfractionController
from django.core.files.storage import default_storage
from core_system.serializers.profiles import BasicProfileSerializer
from core_system.serializers.personas import PersonasSerializer
from core_system.models import *
import threading


class Multas(APIView):
    
    permission_classes = [IsAuthenticated]
    
    def post(self, request):  
        csv_file = request.FILES.get('file')
        try:
            csv_file.content_type
        except Exception as err:
            return Response(status=status.HTTP_400_BAD_REQUEST, data={'error': 'Invalid parameters.'})  
        
        if 'text/csv' in csv_file.content_type:

            # df_multas = pd.DataFrame(columns=cols)
            #df_placas = pd.read_csv(csv_file, usecols=['Tipo_documento','Documento','Nombres','Apellidos','Email','Mobile','Origin'])
            df_placas = pd.read_csv(csv_file, usecols=['Tipo_documento','Documento','Origin'])
            list_placas = df_placas.values.tolist()  
            print(df_placas.items)
        
            with ThreadPool(30) as pool:
                pool.starmap(multihilos, list_placas)
            
        else:
            return  Response(status=status.HTTP_200_OK, data={'error': 'Invalid type file.'}) 

        return Response(status=status.HTTP_200_OK, data={'object': 'finalizado'}) 

    
def multihilos(Tipo_documento,Documento,Origin):

    customer = BasicProfile(origin=Origin, doc_number=Documento, doc_type=Tipo_documento)
    print('buscando a' + ' ' + Documento + ' ' + Tipo_documento )
    
    person = customer.save(customer.__dict__)

    infractions = InfractionController(customer)
     
    # Fetching to the external API 
    data_infractions, err = infractions._fetch_data_infractions()
    _, _, err = infractions._save_infractions(person)
    person.fecha_consulta_comp = IUtility().datetime_utc_now()
    person.save()
    

    
    return Response(status=status.HTTP_200_OK)
    
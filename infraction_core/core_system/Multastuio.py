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


def get_infractions(placa, cliente) -> dict:
    """
    Function to fetch infractions from Verifik to two endpoints,
    consultarComparendos and consultarResoluciones.
    Args:
        customer (Profile): A profile with custer data. The mandatory
                            fields are doc_number and doc_type.
    Returns:
        dict:   A specific dictionary with saparate responses.
                One for comparendos another for resoluciones.
    """
    num_placa = placa
    print(num_placa)
    cliente = cliente
    fecha_consulta = IUtility.datetime_utc_now()
    
    body = dict(documentType='CC', documentNumber=num_placa)
    url = 'https://api.verifik.co/v2/co/simit/consultar'
    token = Tokens.objects.get(id_token=1)
    hds = {'Authorization': token.token_key, 'Content-Type': 'application/json'}
    try:
        response = json.loads(requests.get(url=url, headers=hds, data=json.dumps(body).encode('utf8')).text)
        LogMultas.objects.create(placa=num_placa, fecha_consulta=fecha_consulta, resultado='exitoso')
        if response.get('data').get('multas'):
            multas = response.get('data').get('multas')
            for multa in multas:
                try:
                    data_multa = {
                        'fecha_consulta': fecha_consulta,
                        'id_comparendo': multa.get('numeroComparendo'),
                        'placa': num_placa,
                        'cliente': cliente,
                        'documento': multa.get('infractor').get('numeroDocumento'),
                        'estado_comparendo': multa.get('estadoComparendo'),
                        'comparendo_electronico': multa.get('comparendoElectronico'),
                        'fecha_comparendo': IUtility.format_date(multa.get('fechaComparendo')),
                        'tipo_infraccion':multa.get('infracciones')[0].get('codigoInfraccion'),
                        'ciudad_infraccion': multa.get('organismoTransito'),
                        'tiene_resolucion': multa.get('numeroResolucion'),
                        'fecha_resolucion': IUtility.format_date(multa.get('fechaResolucion')),
                        'tiene_cobro_coactivo': multa.get('nroCoactivo'),
                        'fecha_cobro_coactivo': IUtility.format_date(multa.get('fechaCoactivo')),
                        'valor_infraccion': multa.get('valorPagar'),
                        'fecha_notificacion': IUtility.format_date(multa.get('proyeccion')[0].get('fecha')) if len(multa.get('proyeccion')) > 0 else None,
                        'valor_notificacion': multa.get('proyeccion')[0].get('valor') if len(multa.get('proyeccion')) > 0 else None,
                        'dias_notificacion': multa.get('proyeccion')[0].get('dias') if len(multa.get('proyeccion')) > 0 else None,
                        'fecha_descuento_50': IUtility.format_date(multa.get('proyeccion')[1].get('fecha')) if len(multa.get('proyeccion')) > 1 else None,
                        'valor_descuento_50': multa.get('proyeccion')[1].get('valor') if len(multa.get('proyeccion')) > 1 else None,
                        'dias_descuento_50':  multa.get('proyeccion')[1].get('dias') if len(multa.get('proyeccion')) > 1 else None,
                        'fecha_descuento_25': IUtility.format_date(multa.get('proyeccion')[2].get('fecha')) if len(multa.get('proyeccion')) > 2 else None,
                        'valor_descuento_25': multa.get('proyeccion')[2].get('valor') if len(multa.get('proyeccion')) > 2 else None,
                        'dias_descuento_25': multa.get('proyeccion')[2].get('dias') if len(multa.get('proyeccion')) > 2 else None,
                        'fecha_sin_intereses': IUtility.format_date(multa.get('proyeccion')[3].get('fecha')) if len(multa.get('proyeccion')) > 3 else None,
                        'valor_sin_intereses': multa.get('proyeccion')[3].get('valor')if len(multa.get('proyeccion')) > 3 else None,
                        'dias_sin_intereses':multa.get('proyeccion')[3].get('dias') if len(multa.get('proyeccion')) > 3 else None,
                        
                    }
                    Multas.objects.update_or_create(id_comparendo=data_multa.get('id_comparendo'), defaults=data_multa)
                    print('creado')
                except Exception as e:
                    print(e)
                #df_multas = df_multas.append(data_multa, ignore_index=True)
        else:
            print('no encontro nada')
    except Exception as err:
        LogMultas.objects.create(placa=num_placa, fecha_consulta=fecha_consulta, resultado='fallido')


class MultasTuio(APIView):
    
    permission_classes = [IsAuthenticated]
    
    def post(self, request):
        get_data = False    
        csv_file = request.FILES.get('file')
        try:
            csv_file.content_type
        except Exception as err:
            return Response(status=status.HTTP_400_BAD_REQUEST, data={'error': 'Invalid parameters.'})  
        
        if 'text/csv' in csv_file.content_type:

            # df_multas = pd.DataFrame(columns=cols)
            df_placas = pd.read_csv(request.FILES.get('file'), usecols=['Placa', 'Cliente'])
            token = Tokens.objects.get(id_token=1)
            hds = {'Authorization': token.token_key, 'Content-Type': 'application/json'}

            list_placas = df_placas.values.tolist()
            
            with ThreadPool(30) as pool:
                results = pool.starmap(get_infractions, list_placas)
            
        else:
            return  Response(status=status.HTTP_200_OK, data={'error': 'Invalid type file.'}) 

        return Response(status=status.HTTP_200_OK, data={'object': 'finalizado'}) 


class CronMultasTuio(APIView):
    
    permission_classes = [IsAuthenticated]
    
    def post(self, request):
        try:
            today = date.today()
            logsmultas = LogMultas.objects.filter(fecha_consulta__year=today.year,fecha_consulta__month=today.month,fecha_consulta__day=today.day,resultado='fallido')
            print(logsmultas)
        except Exception as err:
            return Response(status=status.HTTP_400_BAD_REQUEST, data={'error': 'Invalid query.'})  

        token = Tokens.objects.get(id_token=1)
        hds = {'Authorization': token.token_key, 'Content-Type': 'application/json'}
        for rec in logsmultas:
            num_placa = rec.placa
            fecha_consulta = IUtility.datetime_utc_now()
            
            body = dict(documentType='CC', documentNumber=num_placa)
            url = 'https://api.verifik.co/v2/co/simit/consultar'
            try:
                response = json.loads(requests.get(url=url, headers=hds, data=json.dumps(body).encode('utf8')).text)
                print(response)
                LogMultas.objects.create(placa=num_placa, fecha_consulta=fecha_consulta, resultado='exitoso')
            except Exception as err:
                LogMultas.objects.create(placa=num_placa, fecha_consulta=fecha_consulta, resultado='fallido')
                continue
            if response.get('data').get('multas'):
                multas = response.get('data').get('multas')
                for multa in multas:
                    try:
                        data_multa = {
                            'fecha_consulta': fecha_consulta,
                            'id_comparendo': multa.get('numeroComparendo'),
                            'placa': num_placa,
                            'documento': multa.get('infractor').get('numeroDocumento'),
                            'estado_comparendo': multa.get('estadoComparendo'),
                            'comparendo_electronico': multa.get('comparendoElectronico'),
                            'fecha_comparendo': IUtility.format_date(multa.get('fechaComparendo')),
                            'tipo_infraccion':multa.get('infracciones')[0].get('codigoInfraccion'),
                            'ciudad_infraccion': multa.get('organismoTransito'),
                            'tiene_resolucion': multa.get('numeroResolucion'),
                            'fecha_resolucion': IUtility.format_date(multa.get('fechaResolucion')),
                            'tiene_cobro_coactivo': multa.get('nroCoactivo'),
                            'fecha_cobro_coactivo': IUtility.format_date(multa.get('fechaCoactivo')),
                            'valor_infraccion': multa.get('valorPagar'),
                            'fecha_notificacion': IUtility.format_date(multa.get('proyeccion')[0].get('fecha')) if len(multa.get('proyeccion')) > 0 else None,
                            'valor_notificacion': multa.get('proyeccion')[0].get('valor') if len(multa.get('proyeccion')) > 0 else None,
                            'dias_notificacion': multa.get('proyeccion')[0].get('dias') if len(multa.get('proyeccion')) > 0 else None,
                            'fecha_descuento_50': IUtility.format_date(multa.get('proyeccion')[1].get('fecha')) if len(multa.get('proyeccion')) > 1 else None,
                            'valor_descuento_50': multa.get('proyeccion')[1].get('valor') if len(multa.get('proyeccion')) > 1 else None,
                            'dias_descuento_50':  multa.get('proyeccion')[1].get('dias') if len(multa.get('proyeccion')) > 1 else None,
                            'fecha_descuento_25': IUtility.format_date(multa.get('proyeccion')[2].get('fecha')) if len(multa.get('proyeccion')) > 2 else None,
                            'valor_descuento_25': multa.get('proyeccion')[2].get('valor') if len(multa.get('proyeccion')) > 2 else None,
                            'dias_descuento_25': multa.get('proyeccion')[2].get('dias') if len(multa.get('proyeccion')) > 2 else None,
                            'fecha_sin_intereses': IUtility.format_date(multa.get('proyeccion')[3].get('fecha')) if len(multa.get('proyeccion')) > 3 else None,
                            'valor_sin_intereses': multa.get('proyeccion')[3].get('valor')if len(multa.get('proyeccion')) > 3 else None,
                            'dias_sin_intereses':multa.get('proyeccion')[3].get('dias') if len(multa.get('proyeccion')) > 3 else None,
                            
                        }
                        Multas.objects.update_or_create(id_comparendo=data_multa.get('id_comparendo'), defaults=data_multa)
                    except Exception as e:
                        print(e)
                    #df_multas = df_multas.append(data_multa, ignore_index=True)

        return Response(status=status.HTTP_200_OK, data={'object': 'finalizado'}) 
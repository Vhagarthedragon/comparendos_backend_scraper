from .schemas import *
from django.core.exceptions import ObjectDoesNotExist
from asgiref.sync import sync_to_async
from utils.tools import IUtility
from .ifc_verifik import IVerifik
from .profiles import Profile
from .models import Tokens, Logs, Personas, Comparendos, ComparendosHistory
import datetime
import aiohttp
import asyncio
import copy
import json

class ComparendoVerifik(IVerifik):
    """
    A concrete class that implements methods to fetch,
    transform and save the Verifik data.
    Args:
        IVerifik (Interface):   Class as interface with abstract methods
                                fetch infrations.
     Attributes:
        origin (str):           Request source system or client.
        endpoints (list):       Verifik endpoints.
        customer (Profile):     Profile type associated with the }
                                source of the query.
        comparendos_obj (dict): A data structure to map data from Verifik.

    """
    def __init__(self) -> None:
        self.origin = None
        self.__endpoints = [                           
                            'https://1h4ey6bqf1.execute-api.us-east-1.amazonaws.com/prod']
        
        #self.__endpoints = ['https://1h4ey6bqf1.execute-api.us-east-1.amazonaws.com/prod']
        self.__customer = None
        self.__comparendos_obj = {'comparendos': list(), 'resoluciones': list()}
        
    async def get_infractions(self, customer: Profile) -> dict:
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
        verifik_resp = list()
        actions = list()
        self.__customer = customer
        try:
            token = await self.__get_connection()
            if token is not None:
                
                hds = {'Authorization': token, 'Content-Type': 'application/json'} 
                _data = {'number': self.__customer._doc_number, 
                         'doc_type': self.__customer._doc_type}  
                json_data = json.dumps(_data)
                async with aiohttp.ClientSession(headers=hds) as session:
                    for endpoint in self.__endpoints:
                       actions.append(asyncio.ensure_future(
                           self.__get_data(session, endpoint, _data)))
                       
                    response_data = await asyncio.gather(*actions)
                    for data in response_data:
                        print(data)
                        verifik_resp.append(data)
                        
                    self.__transform_data(verifik_resp)
                

                
                return self.__comparendos_obj, None    
        except Exception as _e:
            print(_e)
            log_data =  {
                'origen': self.__customer._origin,
                'destino': 'scrapers',
                'resultado': 1,
                'fecha': IUtility.datetime_utc_now,
                'detalle': _e.args
            }
            Logs.objects.create(**log_data)
            return self.__comparendos_obj, str(_e)
                
    def _save_infractions(self, customer: Personas) -> bool:
        """
        Function to save all the infractions fetched from Verifik endpoints.
        This function collect the data and return if it was possible
        to save.

        Args:
            customer (Personas): A existing person in database.

        Raises:
            Exception: When a error has ocurred saving the infractions.

        Returns:
            Boolean: True when all infractions has saved.
        """
        
        saved = False
        if (isinstance(customer, Personas) and 
            isinstance(self.__comparendos_obj, dict)):
            
            _data_tmp = (self.__comparendos_obj.get('comparendos') +
                        self.__comparendos_obj.get('resoluciones'))
            
            data_api = copy.deepcopy(_data_tmp)
            ids_data_api = [id['id_comparendo'] for id in data_api]
            data_bd = Comparendos.objects.filter(id_persona=customer.pk) 
            
            if len(data_bd) > 0:
                data_bd.exclude(id_comparendo__in=ids_data_api).update(estado='Inactivo')
            
                          
            for cmp in data_api:
                infraccion = IUtility.get_infraction(cmp.get('infraccion'),
                                                     cmp.get('fecha_imposicion'))
                cmp.update({'id_persona': customer})
                cmp.update({'infraccion': infraccion})
                
                if cmp.get('fotodeteccion') is None: cmp.pop('fotodeteccion')
                print(cmp) 
                obj, created = Comparendos.objects.update_or_create(
                    id_comparendo=cmp.get('id_comparendo'),
                    defaults=cmp)
                ComparendosHistory.objects.create(
                    **cmp)
                print('creado')
            saved = True            

        return saved
    
    def __transform_data(self, infractions: list):
        """
        Funtion to map data structure from Verifik to Juzto structure.

        Args:
            infractions (list): A list with all infractions
                                previously obtained in the get violations method.
        """
        self.__comparendos_obj = {'comparendos': list(), 'resoluciones': list()}
        #comparendo_ids = set()
        #resolucion_ids = set()
        try:
            for element in infractions:
                try:
                    print('entro a transformar la data')
                    print(element['d']['data'][0]['resoluciones'])
                    if element['d']['data'][0]['comparendos']:  
                        print('entro')          
                        for cmp in element['d']['data'][0]['comparendos']:
                            print(cmp)
                            if cmp['scraper'] == 'Juzto-bogota':
                                try:
                                    date_obj = datetime.datetime.strptime(cmp['fecha_imposicion'], "%m/%d/%Y")
                                    formatted_date_str = date_obj.strftime("%Y-%m-%d")
                                except:
                                    formatted_date_str = None
                                try:
                                    date_obj2 = datetime.datetime.strptime(cmp['fecha_notificacion'], "%m/%d/%Y")
                                    formatted_date_str2 = date_obj2.strftime("%Y-%m-%d")
                                except:
                                    formatted_date_str2 = None
                                _map = {
                                    'id_comparendo': cmp['id_comparendo'],
                                    'infraccion': cmp['infraccion'],
                                    'id_persona': None,
                                    'fotodeteccion': cmp['fotodeteccion'],
                                    'estado': cmp['estado'],
                                    'fecha_imposicion': IUtility().format_date_verifik(formatted_date_str),
                                    'fecha_resolucion': None,
                                    'fecha_cobro_coactivo': None,
                                    'numero_resolucion': None,
                                    'numero_cobro_coactivo': None,
                                    'placa': cmp['placa'],
                                    'servicio_vehiculo': None,
                                    'tipo_vehiculo': None,
                                    'secretaria': cmp['secretaria'],
                                    'direccion': cmp['direccion'],
                                    'valor_neto': int(float(cmp['valor_neto'].replace(",", "").replace("$", ""))),
                                    'valor_pago': int(float(cmp['valor_pago'].replace(",", "").replace("$", ""))),
                                    'scraper': cmp['scraper'],
                                    'fecha_cobro_coactivo': cmp['nroCoactivo'],
                                    'numero_cobro_coactivo': cmp['fechaCoactivo'],
                                    'fecha_notificacion': IUtility().format_date_verifik(formatted_date_str2),
                                    'origen': self.__customer._origin
                                }
                            else:
                                _map = {
                                    'id_comparendo': cmp['numeroComparendo'],
                                    'infraccion': cmp['codigoInfraccion'],
                                    'id_persona': None,
                                    'fotodeteccion': True if cmp['fotodeteccion']== 'S' else False,
                                    'estado': 'Comparendo',
                                    'fecha_imposicion': IUtility().format_date_verifik(cmp['fechaComparendo']),
                                    'fecha_resolucion': None,
                                    'fecha_cobro_coactivo': None,
                                    'numero_resolucion': None,
                                    'numero_cobro_coactivo': None,
                                    'placa': cmp['placaVehiculo'],
                                    'servicio_vehiculo': None,
                                    'tipo_vehiculo': None,
                                    'secretaria': cmp['secretariaComparendo'],
                                    'direccion': cmp['direccion'],
                                    'valor_neto': None,
                                    'valor_pago': cmp['total'],
                                    'scraper': cmp['scraper'],
                                    'fecha_cobro_coactivo': cmp['nroCoactivo'],
                                    'numero_cobro_coactivo': cmp['fechaCoactivo'],
                                    'fecha_notificacion': IUtility().format_date_verifik(cmp['fechaNotificacion']),
                                    'origen': self.__customer._origin
                                }
                            
                            # verificar si el ID del comparendo ya está en el conjunto
                            #if _map['id_comparendo'] not in comparendo_ids:
                            #    # si no está, agregar el diccionario a la lista y agregar el ID al conjunto
                            #    self.__comparendos_obj['comparendos'].append(_map)
                            #    comparendo_ids.add(_map['id_comparendo'])
                            self.__comparendos_obj['comparendos'].append(_map)

                    if element['d']['data'][0]['resoluciones']:
                        print('entro a resolucion')
                        for res in element['d']['data'][0]['resoluciones']:
                            print('entro a resolucion 2')
                            print(res)
                            _map = {
                                'id_comparendo': res['numeroComparendo'],
                                'infraccion': res['codigoInfraccion'],
                                'id_persona': None,
                                'fotodeteccion': True if res['fotodeteccion']== 'S' else False,
                                'estado': 'Resolución',
                                'fecha_imposicion': IUtility().format_date_verifik(res['fechaComparendo']),
                                'fecha_resolucion': IUtility().format_date_verifik(res['fechaResolucion']),
                                'fecha_cobro_coactivo': None,
                                'numero_resolucion': res['numeroResolucion'],
                                'numero_cobro_coactivo': None,
                                'placa': res['placaVehiculo'],
                                'servicio_vehiculo': None,
                                'tipo_vehiculo': None,
                                'secretaria': res['secretariaComparendo'],
                                'direccion': res['direccion'],
                                'valor_neto': None,
                                'valor_pago': res['total'],
                                'scraper': res['scraper'],
                                'fecha_cobro_coactivo': IUtility().format_date_verifik(res['fechaCoactivo']),
                                'numero_cobro_coactivo': res['nroCoactivo'],
                                "fecha_notificacion": IUtility().format_date_verifik(res['fechaNotificacion']),
                                'origen': self.__customer._origin
                            }

                            # verificar si el ID de la resolución ya está en el conjunto
                            #if _map['id_comparendo'] not in resolucion_ids:
                            #    # si no está, agregar el diccionario a la lista y agregar el ID al conjunto
                            #    self.__comparendos_obj['resoluciones'].append(_map)
                            #    resolucion_ids.add(_map['id_comparendo'])
                            self.__comparendos_obj['resoluciones'].append(_map)

                          
                except Exception as _e:
                    print('hubo un error')
                    print(_e)
                    # report log de excepción en transform data
                    log_data =  {
                        'origen': self.__customer._origin,
                        'destino': 'Verifik',
                        'resultado': 7,
                        'fecha': IUtility.datetime_utc_now(),
                        'detalle': _e
                    }
                    Logs.objects.create(**log_data)

        
        except Exception as _e:
            print(_e)
            # report log de excepción loop response data from verifik
            log_data =  {
                'origen': self.__customer._origin,
                'destino': 'Verifik',
                'resultado': 6,
                'fecha': IUtility.datetime_utc_now(),
                'detalle': _e.args
            }
            return Logs.objects.create(**log_data)
      
    async def __get_data(self, session: aiohttp.ClientSession, url: str, params: dict) -> dict:
        """
        Async function to send the request to Verifik endpoints. This function asynchronously
        collects the data of the infractions.

        Args:
            session (aiohttp.ClientSession):    Object session to make a async request.
            url (str):                          Endpoint to make a async request.
            params (dict):                      Params with de doc_number and doc_type.
        Returns:
            dict:                               With the all data fetched from endpoints.
        """
        
        api = None
        async with session.post(url=url, data=json.dumps(params)) as resp:
            data = await resp.json()
            return {'api': api, 'd': data}
        
    async def __get_connection(self) -> str:
        
        try:
            rs_token = await Tokens.objects.aget(id_token=2)  
            print(rs_token.token_key)  
            return rs_token.token_key
            
        except ObjectDoesNotExist as e:
            print(e)
            # Registar log de que el token no existe o no se puede recuperar
            log_data =  {
                'origen': self.__customer._origin,
                'destino': 'Verifik',
                'resultado': 4,
                'fecha': IUtility.datetime_utc_now(),
                'detalle': e.args
            }
            return Logs.objects.create(**log_data)
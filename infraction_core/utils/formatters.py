from datetime import datetime
import pytz
class Formatter:

    @staticmethod
    def format_date_verifik(c_date: str) -> str:
        """
        Funtion to transform the differents formats of dates in
        Verifik responses.
        
        Args:
            c_date (str):   A string with a specific format from
                            Verifik.
                            
        Returns:
            str:            A string formatted.
        """    
        try:
            
            if '/' in c_date:
                new_date = c_date.replace('/', '-')
                date_object = datetime.strptime(str(new_date), "%d-%m-%Y %H:%M:%S")
                formatted_date = date_object.strftime("%Y-%m-%d")
                return formatted_date
            else:
                new_date = c_date[0:4] + '-'+ c_date[4:6] + '-' + c_date[6:]
                date_object = datetime.strptime(str(new_date), "%d-%m-%Y %H:%M:%S")
                formatted_date = date_object.strftime("%Y-%m-%d")
                return new_date
            
        except Exception as _e:
            return c_date
            
    @staticmethod
    def clean_null_keys(exp: dict) -> dict:
        """
        Funtion to clean null keys in a dictionary.
        
        Args:
            exp (dict):   A dictionary with multiples keys.
                            
        Returns:
            str:          A dictionary without null keys.
        """            
        try:
            return {k:v for k, v in exp.items() if v is not None}
        except:
            return None

    @staticmethod
    def datetime_utc_now():
        """
        Funtion to transform the differents formats of dates in
        Verifik responses.
        
        Args:
            c_date (str):   A string with a specific format from
                            Verifik.
                            
        Returns:
            str:            A string formatted.
        """           
        time_zone = pytz.timezone('America/Bogota')
        dt = datetime.now(time_zone)
        dt = dt.strftime('%Y-%m-%d %H:%M:%S')
        return str(dt)
    
    @staticmethod
    def format_date(c_date: str) -> str:
        try:
            if c_date is not None:
                
                if ' ' in c_date:
                    dt_date = datetime.strptime(c_date, '%d/%m/%Y %H:%M:%S')
                    dt_date = dt_date.strftime('%Y-%m-%d %H:%M:%S')
                    return str(dt_date)
                else:
                    dt_date = datetime.strptime(c_date, '%d/%m/%Y')
                    dt_date = dt_date.strftime('%Y-%m-%d')
                    return str(dt_date)
            return None
        except Exception as err:
            print(err)
            return None
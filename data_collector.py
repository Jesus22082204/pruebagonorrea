# data_collector.py
import requests
import time
from datetime import datetime, timezone
import json
from database_setup import insert_air_quality_data

class AirQualityCollector:
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = "https://api.openweathermap.org/data/2.5"
        
        # Puntos de interés de Aguachica
        self.locations = [
            {
                'id': 'aguachica_general',
                'name': 'Aguachica - Vista General',
                'lat': 8.312,
                'lon': -73.626
            },
            {
                'id': 'parque_central',
                'name': 'Parque Central',
                'lat': 8.310675833008426,
                'lon': -73.62363665855918
            },
            {
                'id': 'universidad',
                'name': 'Universidad Popular del Cesar',
                'lat': 8.314789098234467,
                'lon': -73.59638568793966
            },
            {
                'id': 'parque_morrocoy',
                'name': 'Parque Morrocoy',
                'lat': 8.310373774726447,
                'lon': -73.61670782048647
            },
            {
                'id': 'patinodromo',
                'name': 'Patinódromo',
                'lat': 8.297149888853758,
                'lon': -73.62335200184627
            },
            {
                'id': 'ciudadela_paz',
                'name': 'Ciudadela de la Paz',
                'lat': 8.312099985681844,
                'lon': -73.63467832511535
            },
            {
                'id': 'bosque',
                'name': 'Bosque',
                'lat': 8.312303609676293,
                'lon': -73.61448867800057
            },
            {
                'id': 'estadio',
                'name': 'Estadio',
                'lat': 8.30159931733102,
                'lon': -73.622763654179
            }
        ]
    
    def get_air_quality_data(self, lat, lon):
        """Obtener datos de calidad del aire de OpenWeather API"""
        try:
            # URL para calidad del aire
            air_url = f"{self.base_url}/air_pollution"
            air_params = {
                'lat': lat,
                'lon': lon,
                'appid': self.api_key
            }
            
            # URL para datos meteorológicos
            weather_url = f"{self.base_url}/weather"
            weather_params = {
                'lat': lat,
                'lon': lon,
                'appid': self.api_key,
                'units': 'metric'
            }
            
            # Hacer las peticiones
            air_response = requests.get(air_url, params=air_params, timeout=10)
            weather_response = requests.get(weather_url, params=weather_params, timeout=10)
            
            if air_response.status_code == 200 and weather_response.status_code == 200:
                air_data = air_response.json()
                weather_data = weather_response.json()
                
                return {
                    'air_quality': air_data,
                    'weather': weather_data,
                    'success': True
                }
            else:
                print(f"Error en API: Air={air_response.status_code}, Weather={weather_response.status_code}")
                return {'success': False, 'error': 'API request failed'}
                
        except requests.exceptions.Timeout:
            print("Timeout en la petición a la API")
            return {'success': False, 'error': 'Timeout'}
        except requests.exceptions.RequestException as e:
            print(f"Error en la petición: {e}")
            return {'success': False, 'error': str(e)}
    
    def process_and_save_data(self, location, api_data):
        """Procesar y guardar datos en la base de datos"""
        try:
            air_quality = api_data['air_quality']
            weather = api_data['weather']
            
            # Extraer datos de calidad del aire
            components = air_quality['list'][0]['components']
            aqi = air_quality['list'][0]['main']['aqi']
            
            # Extraer datos meteorológicos
            temp = weather['main']['temp']
            humidity = weather['main']['humidity']
            pressure = weather['main']['pressure']
            wind_speed = weather.get('wind', {}).get('speed', None)
            
            # Timestamp actual
            timestamp = datetime.now(timezone.utc).isoformat()
            
            # Guardar en base de datos
            success = insert_air_quality_data(
                location_id=location['id'],
                location_name=location['name'],
                lat=location['lat'],
                lon=location['lon'],
                timestamp=timestamp,
                pm2_5=components.get('pm2_5'),
                pm10=components.get('pm10'),
                o3=components.get('o3'),
                no2=components.get('no2'),
                aqi=aqi,
                temp=temp,
                humidity=humidity,
                pressure=pressure,
                wind_speed=wind_speed
            )
            
            if success:
                print(f"✅ Datos guardados para {location['name']}")
                return True
            else:
                print(f"❌ Error guardando datos para {location['name']}")
                return False
                
        except Exception as e:
            print(f"Error procesando datos para {location['name']}: {e}")
            return False
    
    def collect_all_locations(self):
        """Recolectar datos de todas las ubicaciones"""
        print(f"🔄 Iniciando recolección de datos - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        successful = 0
        failed = 0
        
        for location in self.locations:
            print(f"📡 Recolectando datos para {location['name']}...")
            
            # Obtener datos de la API
            api_data = self.get_air_quality_data(location['lat'], location['lon'])
            
            if api_data['success']:
                # Procesar y guardar
                if self.process_and_save_data(location, api_data):
                    successful += 1
                else:
                    failed += 1
            else:
                print(f"❌ Error en API para {location['name']}: {api_data.get('error', 'Unknown')}")
                failed += 1
            
            # Esperar entre peticiones para evitar límites de rate
            time.sleep(2)
        
        print(f"\n📊 Resumen de recolección:")
        print(f"   ✅ Exitosos: {successful}")
        print(f"   ❌ Fallidos: {failed}")
        print(f"   📅 Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        return successful, failed
    
    def collect_single_location(self, location_id):
        """Recolectar datos de una ubicación específica"""
        location = next((loc for loc in self.locations if loc['id'] == location_id), None)
        
        if not location:
            print(f"❌ Ubicación '{location_id}' no encontrada")
            return False
        
        print(f"📡 Recolectando datos para {location['name']}...")
        
        api_data = self.get_air_quality_data(location['lat'], location['lon'])
        
        if api_data['success']:
            return self.process_and_save_data(location, api_data)
        else:
            print(f"❌ Error en API: {api_data.get('error', 'Unknown')}")
            return False

def main():
    """Función principal para ejecutar la recolección"""
    import os
    
    # Obtener API key del archivo de configuración o variable de entorno
    api_key = os.getenv('OPENWEATHER_API_KEY')
    
    if not api_key:
        # Intentar leer de archivo de configuración
        try:
            with open('config.json', 'r') as f:
                config = json.load(f)
                api_key = config.get('openweather_api_key')
        except FileNotFoundError:
            print("❌ No se encontró config.json")
    
    if not api_key:
        print("❌ API key no configurada. Crea un archivo config.json con tu API key:")
        print('{"openweather_api_key": "tu_api_key_aqui"}')
        return
    
    # Crear instancia del recolector
    collector = AirQualityCollector(api_key)
    
    # Recolectar datos de todas las ubicaciones
    collector.collect_all_locations()

if __name__ == "__main__":
    main()
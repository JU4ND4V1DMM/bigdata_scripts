import os
from itertools import chain
from datetime import datetime
import pyspark
import unidecode
import re
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark_session import get_spark_session
from pyspark.sql.functions import (col, lower, regexp_replace, lit, concat, when, split, sum, expr, row_number, floor,
                                    upper, to_date, datediff, lpad, concat_ws, trim, length, create_map, sort_array, collect_set)
from pyspark.sql.types import IntegerType

def cruice_department_mapping():
    
    # Define a dictionary to map substrings to department names
    Norte_Santander = {
        
        "VILLA DEL ROSARIO": "N. DE SANTANDER",  # Incluye Norte de Santander y sus variaciones
        "SAN CALIXTO": "N. DE SANTANDER",
        "EL CARMEN": "N. DE SANTANDER",
        "VILLA CARO": "N. DE SANTANDER",
        "SAN CAYETANO": "N. DE SANTANDER",
        "LA ESPERANZA": "N. DE SANTANDER",
        "LOS PATIOS": "N. DE SANTANDER",
        "LA PLAYA": "N. DE SANTANDER",
        "PUERTO SANTANDER": "N. DE SANTANDER",
        "EL TARRA": "N. DE SANTANDER",
        "EL ZULIA": "N. DE SANTANDER",
        "ABREGO": "N. DE SANTANDER",
        "ARBOLEDAS": "N. DE SANTANDER",
        "BOCHALEMA": "N. DE SANTANDER",
        "BUCARASICA": "N. DE SANTANDER",
        "CACHIRA": "N. DE SANTANDER",
        "CACOTA": "N. DE SANTANDER",
        "CHINACOTA": "N. DE SANTANDER",
        "CHITAGA": "N. DE SANTANDER",
        "CONVENCION": "N. DE SANTANDER",
        "CUTA": "N. DE SANTANDER",
        "CUCUTILLA": "N. DE SANTANDER",
        "DURANIA": "N. DE SANTANDER",
        "GRAMALOTE": "N. DE SANTANDER",
        "HACARI": "N. DE SANTANDER",
        "HERRAN": "N. DE SANTANDER",
        "LABATECA": "N. DE SANTANDER",
        "LOURDES": "N. DE SANTANDER",
        "MUTISCUA": "N. DE SANTANDER",
        "OCANA": "N. DE SANTANDER",
        "PAMPLONA": "N. DE SANTANDER",
        "PAMPLONITA": "N. DE SANTANDER",
        "RAGONVALIA": "N. DE SANTANDER",
        "SALAZAR": "N. DE SANTANDER",
        "SANTIAGO": "N. DE SANTANDER",
        "SARDINATA": "N. DE SANTANDER",
        "SILOS": "N. DE SANTANDER",
        "TEORAMA": "N. DE SANTANDER",
        "TIBU": "N. DE SANTANDER",
        "TOLEDO": "N. DE SANTANDER"
    }
    
    # Define a dictionary to map substrings to department names
    Guaviare = {
        
        "EL RETORNO": "GUAVIARE",   # Incluye Guavirae y sus variaciones
        "CALAMAR": "GUAVIARE",
        "MIRAFLORES": "GUAVIARE"
    }
    
    # Define a dictionary to map substrings to department names
    Cordoba = {
        
        "CIENAGA DE ORO": "CORDOBA",    # Incluye Córdoba y sus variaciones
        "SAN ANDRES SOTAVENTO": "CORDOBA",
        "SAN ANTERO": "CORDOBA",
        "LA APARTADA": "CORDOBA",
        "SAN CARLOS": "CORDOBA",
        "LOS CORDOBAS": "CORDOBA",
        "PUERTO ESCONDIDO": "CORDOBA",
        "PUERTO LIBERTADOR": "CORDOBA",
        "PUEBLO NUEVO": "CORDOBA",
        "SAN PELAYO": "CORDOBA",
        "PLANETA RICA": "CORDOBA",
        "AYAPEL": "CORDOBA",
        "BUENAVISTA": "CORDOBA",
        "CANALETE": "CORDOBA",
        "CERETE": "CORDOBA",
        "CHIMA": "CORDOBA",
        "CHINU": "CORDOBA",
        "COTORRA": "CORDOBA",
        "LORICA": "CORDOBA",
        "MOMIL": "CORDOBA",
        "MONTELIBANO": "CORDOBA",
        "MONTERIA": "CORDOBA",
        "MONITOS": "CORDOBA",
        "PURISIMA": "CORDOBA",
        "SAHAGUN": "CORDOBA",
        "TIERRALTA": "CORDOBA",
        "VALENCIA": "CORDOBA"
    }
    
    # Define a dictionary to map substrings to department names
    Choco = {
        
        "CARMEN DEL DARIEN": "CHOCO",   # Incluye Chocó y sus variaciones
        "MEDIO SAN JUAN": "CHOCO",
        "MEDIO ATRATO": "CHOCO",
        "ALTO BAUDO": "CHOCO",
        "BAJO BAUDO": "CHOCO",
        "MEDIO BAUDO": "CHOCO",
        "RIO IRO": "CHOCO",
        "UNION PANAMERICANA": "CHOCO",
        "RIO QUITO": "CHOCO",
        "BAHIA SOLANO": "CHOCO",
        "ACANDI": "CHOCO",
        "ATRATO": "CHOCO",
        "BAGADO": "CHOCO",
        "BOJAYA": "CHOCO",
        "CERTEGUI": "CHOCO",
        "CONDOTO": "CHOCO",
        "ISTMINA": "CHOCO",
        "JURADO": "CHOCO",
        "LLORO": "CHOCO",
        "NOVITA": "CHOCO",
        "NUQUI": "CHOCO",
        "QUIBDO": "CHOCO",
        "RIOSUCIO": "CHOCO",
        "SIPI": "CHOCO",
        "TADO": "CHOCO",
        "UNGUIA": "CHOCO"
    }
    
    # Define a dictionary to map substrings to department names
    Cesar = {
        "RIO DE ORO": "CESAR",  # Incluye Cesar y sus variaciones
        "SAN ALBERTO": "CESAR",
        "PUEBLO BELLO": "CESAR",
        "AGUSTIN CODAZZI": "CESAR",
        "EL COPEY": "CESAR",
        "SAN DIEGO": "CESAR",
        "LA GLORIA": "CESAR",
        "SAN MARTIN": "CESAR",
        "EL PASO": "CESAR",
        "LA PAZ": "CESAR",
        "AGUACHICA": "CESAR",
        "ASTREA": "CESAR",
        "BECERRIL": "CESAR",
        "BOSCONIA": "CESAR",
        "CHIMICHAGUA": "CESAR",
        "CHIRIGUANA": "CESAR",
        "CURUMANI": "CESAR",
        "GAMARRA": "CESAR",
        "GONZALEZ": "CESAR",
        "MANAURE": "CESAR",
        "PAILITAS": "CESAR",
        "PELAYA": "CESAR",
        "TAMALAMEQUE": "CESAR",
        "VALLEDUPAR": "CESAR"
    }
    
    # Define a dictionary to map substrings to department names
    Cauca = {
        
        "SANTANDER DE QUILICHAO": "CAUCA",   # Incluye Cauca y sus variaciones
        "BUENOS AIRES": "CAUCA",
        "VILLA RICA": "CAUCA",
        "SANTA ROSA": "CAUCA",
        "SAN SEBASTIAN": "CAUCA",
        "LA SIERRA": "CAUCA",
        "EL TAMBO": "CAUCA",
        "PUERTO TEJADA": "CAUCA",
        "LA VEGA": "CAUCA",
        "ALMAGUER": "CAUCA",
        "ARGELIA": "CAUCA",
        "BALBOA": "CAUCA",
        "BOLIVAR": "CAUCA",
        "CAJIBIO": "CAUCA",
        "CALDONO": "CAUCA",
        "CALOTO": "CAUCA",
        "CORINTO": "CAUCA",
        "FLORENCIA": "CAUCA",
        "GUACHENE": "CAUCA",
        "GUAPI": "CAUCA",
        "INZA": "CAUCA",
        "JAMBALO": "CAUCA",
        "LOPEZ": "CAUCA",
        "MERCADERES": "CAUCA",
        "MIRANDA": "CAUCA",
        "MORALES": "CAUCA",
        "PADILLA": "CAUCA",
        "PAEZ": "CAUCA",
        "PATIA": "CAUCA",
        "PIAMONTE": "CAUCA",
        "PIENDAMO": "CAUCA",
        "POPAYAN": "CAUCA",
        "PURACE": "CAUCA",
        "ROSAS": "CAUCA",
        "SILVIA": "CAUCA",
        "SOTARA": "CAUCA",
        "SUAREZ": "CAUCA",
        "TIMBIO": "CAUCA",
        "TIMBIQUI": "CAUCA",
        "TORIBIO": "CAUCA",
        "TOTORO": "CAUCA"
    }
    
    # Define a dictionary to map substrings to department names
    Casanare = {
        
        "PAZ DE ARIPORO": "CASANARE", # Incluye Casanare y sus variaciones
        "HATO COROZAL": "CASANARE",
        "LA SALINA": "CASANARE",
        "AGUAZUL": "CASANARE",
        "CHAMEZA": "CASANARE",
        "MANI": "CASANARE",
        "MONTERREY": "CASANARE",
        "NUNCHIA": "CASANARE",
        "OROCUE": "CASANARE",
        "PORE": "CASANARE",
        "RECETOR": "CASANARE",
        "SABANALARGA": "CASANARE",
        "SACAMA": "CASANARE",
        "TAMARA": "CASANARE",
        "TAURAMENA": "CASANARE",
        "TRINIDAD": "CASANARE",
        "VILLANUEVA": "CASANARE",
        "YOPAL": "CASANARE",
    }
    
    # Define a dictionary to map substrings to department names
    Arauca = {
        "CRAVO NORTE": "ARAUCA",   # Incluye Arauca y sus variaciones
        "PUERTO RONDON": "ARAUCA",
        "ARAUCA": "ARAUCA",
        "ARAUQUITA": "ARAUCA",
        "FORTUL": "ARAUCA",
        "SARAVENA": "ARAUCA",
        "TAME": "ARAUCA",
    }
    
    # Define a dictionary to map substrings to department names
    Cundinamarca_1 = {
    
        "BOGO": "CUNDINAMARCA",  # Incluye Cundinamrca y sus variaciones
        "CUNDINAMARCA": "CUNDINAMARCA",
        "20 DE JUL": "CUNDINAMARCA",
        "SOACHA": "CUNDINAMARCA",
        "URIB": "CUNDINAMARCA",
        "ZIPA": "CUNDINAMARCA",
        "FACATATI": "CUNDINAMARCA",
        "COTA": "CUNDINAMARCA",
        "CHIA": "CUNDINAMARCA",
        "CHÍA": "CUNDINAMARCA",
        "CARMEN DE CARUPA": "CUNDINAMARCA",
        "AGUA DE DIOS": "CUNDINAMARCA",
        "GUAYABAL DE SIQUIMA": "CUNDINAMARCA",
        "SAN BERNARDO": "CUNDINAMARCA",
        "LA CALERA": "CUNDINAMARCA",
        "SAN CAYETANO": "CUNDINAMARCA",
        "EL COLEGIO": "CUNDINAMARCA",
        "SAN FRANCISCO": "CUNDINAMARCA",
        "LA MESA": "CUNDINAMARCA",
        "LA PALMA": "CUNDINAMARCA",
        "LA PENA": "CUNDINAMARCA",
        "EL PENON": "CUNDINAMARCA",
        "EL ROSAL": "CUNDINAMARCA",
        "PUERTO SALGAR": "CUNDINAMARCA",
        "LA VEGA": "CUNDINAMARCA",
        "ALBAN": "CUNDINAMARCA",
        "ANAPOIMA": "CUNDINAMARCA",
        "ANOLAIMA": "CUNDINAMARCA",
        "APULO": "CUNDINAMARCA",
        "ARBELAEZ": "CUNDINAMARCA",
        "BELTRAN": "CUNDINAMARCA",
        "BITUIMA": "CUNDINAMARCA",
        "BOJACA": "CUNDINAMARCA",
        "CABRERA": "CUNDINAMARCA",
        "CACHIPAY": "CUNDINAMARCA",
        "CAJICA": "CUNDINAMARCA",
        "CAPARRAPI": "CUNDINAMARCA",
        "CAQUEZA": "CUNDINAMARCA",
        "CHAGUANI": "CUNDINAMARCA",
    }
    
    Cundinamarca_2 = {
        "QUEBRADANEGRA": "CUNDINAMARCA",
        "QUETAME": "CUNDINAMARCA",
        "QUIPILE": "CUNDINAMARCA",
        "RICAURTE": "CUNDINAMARCA",
        "SASAIMA": "CUNDINAMARCA",
        "SESQUILE": "CUNDINAMARCA",
        "SIBATE": "CUNDINAMARCA",
        "SILVANIA": "CUNDINAMARCA",
        "SIMIJACA": "CUNDINAMARCA",
        "SOACHA": "CUNDINAMARCA",
        "SOPO": "CUNDINAMARCA",
        "SUBACHOQUE": "CUNDINAMARCA",
        "SUESCA": "CUNDINAMARCA",
        "SUPATA": "CUNDINAMARCA",
        "SUSA": "CUNDINAMARCA",
        "SUTATAUSA": "CUNDINAMARCA",
        "TABIO": "CUNDINAMARCA",
        "TAUSA": "CUNDINAMARCA",
        "TENA": "CUNDINAMARCA",
        "TENJO": "CUNDINAMARCA",
        "TIBACUY": "CUNDINAMARCA",
        "TIBIRITA": "CUNDINAMARCA",
        "TOCAIMA": "CUNDINAMARCA",
        "TOCANCIPA": "CUNDINAMARCA",
        "TOPAIPI": "CUNDINAMARCA",
        "UBALA": "CUNDINAMARCA",
        "UBAQUE": "CUNDINAMARCA",
        "UNE": "CUNDINAMARCA",
        "UTICA": "CUNDINAMARCA",
        "VENECIA": "CUNDINAMARCA",
        "VERGARA": "CUNDINAMARCA",
        "VIANI": "CUNDINAMARCA",
        "VILLAGOMEZ": "CUNDINAMARCA",
        "VILLAPINZON": "CUNDINAMARCA",
        "VILLETA": "CUNDINAMARCA",
        "VIOTA": "CUNDINAMARCA",
        "YACOPI": "CUNDINAMARCA",
        "ZIPACON": "CUNDINAMARCA",
    }
    
    Cundinamarca_3 = {
        
        "CHIA": "CUNDINAMARCA",
        "CHIPAQUE": "CUNDINAMARCA",
        "CHOACHI": "CUNDINAMARCA",
        "CHOCONTA": "CUNDINAMARCA",
        "COGUA": "CUNDINAMARCA",
        "COTA": "CUNDINAMARCA",
        "CUCUNUBA": "CUNDINAMARCA",
        "FACATATIVA": "CUNDINAMARCA",
        "FOMEQUE": "CUNDINAMARCA",
        "FOSCA": "CUNDINAMARCA",
        "FUNZA": "CUNDINAMARCA",
        "FUQUENE": "CUNDINAMARCA",
        "FUSAGASUGA": "CUNDINAMARCA",
        "GACHALA": "CUNDINAMARCA",
        "GACHANCIPA": "CUNDINAMARCA",
        "GACHETA": "CUNDINAMARCA",
        "GAMA": "CUNDINAMARCA",
        "GIRARDOT": "CUNDINAMARCA",
        "GRANADA": "CUNDINAMARCA",
        "GUACHETA": "CUNDINAMARCA",
        "GUADUAS": "CUNDINAMARCA",
        "GUASCA": "CUNDINAMARCA",
        "GUATAQUI": "CUNDINAMARCA",
        "GUATAVITA": "CUNDINAMARCA",
        "GUAYABETAL": "CUNDINAMARCA",
        "GUTIERREZ": "CUNDINAMARCA",
        "JERUSALEN": "CUNDINAMARCA",
        "JUNIN": "CUNDINAMARCA",
        "LENGUAZAQUE": "CUNDINAMARCA",
        "MACHETA": "CUNDINAMARCA",
        "MADRID": "CUNDINAMARCA",
        "MANTA": "CUNDINAMARCA",
        "MEDINA": "CUNDINAMARCA",
        "MOSQUERA": "CUNDINAMARCA",
        "NARINO": "CUNDINAMARCA",
        "NEMOCON": "CUNDINAMARCA",
        "NILO": "CUNDINAMARCA",
        "NIMAIMA": "CUNDINAMARCA",
        "NOCAIMA": "CUNDINAMARCA",
        "PACHO": "CUNDINAMARCA",
        "PAIME": "CUNDINAMARCA",
        "PANDI": "CUNDINAMARCA",
        "PARATEBUENO": "CUNDINAMARCA",
        "PASCA": "CUNDINAMARCA",
        "PULI": "CUNDINAMARCA",
    }
    
    # Define a dictionary to map substrings to department names
    Valle_del_Cauca = {
        "CAUC": "VALLE DEL CAUCA",  # Incluye Cauca y sus variaciones
        "VALLE": "VALLE DEL CAUCA",
        "CALI": "VALLE DEL CAUCA",
        "JAMUND": "VALLE DEL CAUCA",
        "FLORID": "VALLE DEL CAUCA",
        "GUADALAJARA DE BUGA": "VALLE DEL CAUCA",
        "EL AGUILA": "VALLE DEL CAUCA",
        "EL CAIRO": "VALLE DEL CAUCA",
        "EL CERRITO": "VALLE DEL CAUCA",
        "LA CUMBRE": "VALLE DEL CAUCA",
        "EL DOVIO": "VALLE DEL CAUCA",
        "SAN PEDRO": "VALLE DEL CAUCA",
        "LA UNION": "VALLE DEL CAUCA",
        "LA VICTORIA": "VALLE DEL CAUCA",
        "ALCALA": "VALLE DEL CAUCA",
        "ANDALUCIA": "VALLE DEL CAUCA",
        "ANSERMANUEVO": "VALLE DEL CAUCA",
        "ARGELIA": "VALLE DEL CAUCA",
        "BOLIVAR": "VALLE DEL CAUCA",
        "BUENAVENTURA": "VALLE DEL CAUCA",
        "BUGALAGRANDE": "VALLE DEL CAUCA",
        "CAICEDONIA": "VALLE DEL CAUCA",
        "CALI": "VALLE DEL CAUCA",
        "CALIMA": "VALLE DEL CAUCA",
        "CANDELARIA": "VALLE DEL CAUCA",
        "CARTAGO": "VALLE DEL CAUCA",
        "DAGUA": "VALLE DEL CAUCA",
        "FLORIDA": "VALLE DEL CAUCA",
        "GINEBRA": "VALLE DEL CAUCA",
        "GUACARI": "VALLE DEL CAUCA",
        "JAMUNDI": "VALLE DEL CAUCA",
        "OBANDO": "VALLE DEL CAUCA",
        "PALMIRA": "VALLE DEL CAUCA",
        "PRADERA": "VALLE DEL CAUCA",
        "RESTREPO": "VALLE DEL CAUCA",
        "RIOFRIO": "VALLE DEL CAUCA",
        "ROLDANILLO": "VALLE DEL CAUCA",
        "SEVILLA": "VALLE DEL CAUCA",
        "TORO": "VALLE DEL CAUCA",
        "TRUJILLO": "VALLE DEL CAUCA",
        "TULUA": "VALLE DEL CAUCA",
        "ULLOA": "VALLE DEL CAUCA",
        "VERSALLES": "VALLE DEL CAUCA",
        "VIJES": "VALLE DEL CAUCA",
        "YOTOCO": "VALLE DEL CAUCA",
        "YUMBO": "VALLE DEL CAUCA",
        "ZARZAL": "VALLE DEL CAUCA",
    }
    
    # Define a dictionary to map substrings to department names
    Antioquia_1 = {
        
        "ANTIOQUIA": "ANTIOQUIA",  # Incluye Antioquia y sus variaciones
        "SANTAFE DE ANTIOQUIA": "ANTIOQUIA",
        "VIGIA DEL FUERTE": "ANTIOQUIA",
        "EL BAGRE": "ANTIOQUIA",
        "SANTA BARBARA": "ANTIOQUIA",
        "PUERTO BERRIO": "ANTIOQUIA",
        "CIUDAD BOLIVAR": "ANTIOQUIA",
        "SAN CARLOS": "ANTIOQUIA",
        "LA CEJA": "ANTIOQUIA",
        "SANTO DOMINGO": "ANTIOQUIA",
        "LA ESTRELLA": "ANTIOQUIA",
        "SAN FRANCISCO": "ANTIOQUIA",
        "SAN JERONIMO": "ANTIOQUIA",
        "SAN LUIS": "ANTIOQUIA",
        "DON MATIAS": "ANTIOQUIA",
        "PUERTO NARE": "ANTIOQUIA",
        "SAN PEDRO": "ANTIOQUIA",
        "LA PINTADA": "ANTIOQUIA",
        "GOMEZ PLATA": "ANTIOQUIA",
        "SAN RAFAEL": "ANTIOQUIA",
        "SAN ROQUE": "ANTIOQUIA",
        "EL SANTUARIO": "ANTIOQUIA",
        "PUERTO TRIUNFO": "ANTIOQUIA",
        "LA UNION": "ANTIOQUIA",
        "SAN VICENTE": "ANTIOQUIA",
        "ABEJORRAL": "ANTIOQUIA",
        "ABRIAQUI": "ANTIOQUIA",
        "ALEJANDRIA": "ANTIOQUIA",
        "AMAGA": "ANTIOQUIA",
        "AMALFI": "ANTIOQUIA",
        "ANDES": "ANTIOQUIA",
        "ANGELOPOLIS": "ANTIOQUIA",
        "ANGOSTURA": "ANTIOQUIA",
        "ANORI": "ANTIOQUIA",
        "ANZA": "ANTIOQUIA",
        "APARTADO": "ANTIOQUIA",
        "ARBOLETES": "ANTIOQUIA",
        "ARGELIA": "ANTIOQUIA",
        "ARMENIA": "ANTIOQUIA",
        "BARBOSA": "ANTIOQUIA",
        "BELLO": "ANTIOQUIA",
        "BELMIRA": "ANTIOQUIA",
        "BETANIA": "ANTIOQUIA",
        "BETULIA": "ANTIOQUIA",
        "BRICENO": "ANTIOQUIA",
        "BURITICA": "ANTIOQUIA",
        "CACERES": "ANTIOQUIA",
        "CAICEDO": "ANTIOQUIA",
        "CALDAS": "ANTIOQUIA",
        "CAMPAMENTO": "ANTIOQUIA",
    }

    Antioquia_2 = {
        "MEDELLIN": "ANTIOQUIA",
        "MONTEBELLO": "ANTIOQUIA",
        "MURINDO": "ANTIOQUIA",
        "MUTATA": "ANTIOQUIA",
        "NECHI": "ANTIOQUIA",
        "NECOCLI": "ANTIOQUIA",
        "OLAYA": "ANTIOQUIA",
        "PEÐOL": "ANTIOQUIA",
        "PEQUE": "ANTIOQUIA",
        "PUEBLORRICO": "ANTIOQUIA",
        "REMEDIOS": "ANTIOQUIA",
        "RETIRO": "ANTIOQUIA",
        "RIONEGRO": "ANTIOQUIA",
        "SABANALARGA": "ANTIOQUIA",
        "SABANETA": "ANTIOQUIA",
        "SALGAR": "ANTIOQUIA",
        "SEGOVIA": "ANTIOQUIA",
        "SONSON": "ANTIOQUIA",
        "SOPETRAN": "ANTIOQUIA",
        "TAMESIS": "ANTIOQUIA",
        "TARAZA": "ANTIOQUIA",
        "TARSO": "ANTIOQUIA",
        "TITIRIBI": "ANTIOQUIA",
        "TOLEDO": "ANTIOQUIA",
        "TURBO": "ANTIOQUIA",
        "URAMITA": "ANTIOQUIA",
        "URRAO": "ANTIOQUIA",
        "VALDIVIA": "ANTIOQUIA",
        "VALPARAISO": "ANTIOQUIA",
        "VEGACHI": "ANTIOQUIA",
        "VENECIA": "ANTIOQUIA",
        "YALI": "ANTIOQUIA",
        "YARUMAL": "ANTIOQUIA",
        "YOLOMBO": "ANTIOQUIA",
        "YONDO": "ANTIOQUIA",
        "ZARAGOZA": "ANTIOQUIA",
        "BARRAN": "ANTIOQUIA",
        "RDOBA": "ANTIOQUIA",
        "MEDEL": "ANTIOQUIA",
        "EL PE": "ANTIOQUIA",
    }
    
    Antioquia_3 = {
                "CANASGORDAS": "ANTIOQUIA",
        "CARACOLI": "ANTIOQUIA",
        "CARAMANTA": "ANTIOQUIA",
        "CAREPA": "ANTIOQUIA",
        "CAROLINA": "ANTIOQUIA",
        "CAUCASIA": "ANTIOQUIA",
        "CHIGORODO": "ANTIOQUIA",
        "CISNEROS": "ANTIOQUIA",
        "COCORNA": "ANTIOQUIA",
        "CONCEPCION": "ANTIOQUIA",
        "CONCORDIA": "ANTIOQUIA",
        "COPACABANA": "ANTIOQUIA",
        "DABEIBA": "ANTIOQUIA",
        "EBEJICO": "ANTIOQUIA",
        "ENTRERRIOS": "ANTIOQUIA",
        "ENVIGADO": "ANTIOQUIA",
        "FREDONIA": "ANTIOQUIA",
        "FRONTINO": "ANTIOQUIA",
        "GIRALDO": "ANTIOQUIA",
        "GIRARDOTA": "ANTIOQUIA",
        "GRANADA": "ANTIOQUIA",
        "GUADALUPE": "ANTIOQUIA",
        "GUARNE": "ANTIOQUIA",
        "GUATAPE": "ANTIOQUIA",
        "HELICONIA": "ANTIOQUIA",
        "HISPANIA": "ANTIOQUIA",
        "ITAGUI": "ANTIOQUIA",
        "ITUANGO": "ANTIOQUIA",
        "JARDIN": "ANTIOQUIA",
        "JERICO": "ANTIOQUIA",
        "LIBORINA": "ANTIOQUIA",
        "MACEO": "ANTIOQUIA",
        "MARINILLA": "ANTIOQUIA",
    }
    
    # Define a dictionary to map substrings to department names
    Magdalena = {
        "CERRO SAN ANTONIO": "MAGDALENA",       # Incluye Magdalena y sus variaciones
        "PIJINO DEL CARMEN": "MAGDALENA",
        "SANTA ANA": "MAGDALENA",
        "ZONA BANANERA": "MAGDALENA",
        "EL BANCO": "MAGDALENA",
        "NUEVA GRANADA": "MAGDALENA",
        "SANTA MARTA": "MAGDALENA",
        "EL PINON": "MAGDALENA",
        "EL RETEN": "MAGDALENA",
        "SAN ZENON": "MAGDALENA",
        "ALGARROBO": "MAGDALENA",
        "ARACATACA": "MAGDALENA",
        "ARIGUANI": "MAGDALENA",
        "CHIBOLO": "MAGDALENA",
        "CIENAGA": "MAGDALENA",
        "CONCORDIA": "MAGDALENA",
        "FUNDACION": "MAGDALENA",
        "GUAMAL": "MAGDALENA",
        "PEDRAZA": "MAGDALENA",
        "PIVIJAY": "MAGDALENA",
        "PLATO": "MAGDALENA",
        "PUEBLOVIEJO": "MAGDALENA",
        "REMOLINO": "MAGDALENA",
        "SALAMINA": "MAGDALENA",
        "SITIONUEVO": "MAGDALENA",
        "TENERIFE": "MAGDALENA",
        "ZAPAYAN": "MAGDALENA",
    }
    
    # Define a dictionary to map substrings to department names
    Guajira = {
        "EL MOLINO": "LA GUAJIRA",      # Incluye Guajira y sus variaciones
        "ALBANIA": "LA GUAJIRA",
        "BARRANCAS": "LA GUAJIRA",
        "DIBULLA": "LA GUAJIRA",
        "DISTRACCION": "LA GUAJIRA",
        "FONSECA": "LA GUAJIRA",
        "HATONUEVO": "LA GUAJIRA",
        "MAICAO": "LA GUAJIRA",
        "MANAURE": "LA GUAJIRA",
        "RIOHACHA": "LA GUAJIRA",
        "URIBIA": "LA GUAJIRA",
        "URUMITA": "LA GUAJIRA",
        "VILLANUEVA": "LA GUAJIRA",
    }
    
    # Define a dictionary to map substrings to department names
    Atlantico = {
        "ATLANTICO": "ATLANTICO",  # Incluye Atlántico y sus variaciones
        "BOLIVAR": "ATLANTICO",
        "CESAR": "ATLANTICO",
    }
    
    # Define a dictionary to map substrings to department names
    Quindio = {
        "LA TEBAIDA": "QUINDIO",    # Incluye Quindío y sus variaciones
        "ARMENIA": "QUINDIO",
        "BUENAVISTA": "QUINDIO",
        "CALARCA": "QUINDIO",
        "CIRCASIA": "QUINDIO",
        "CORDOBA": "QUINDIO",
        "FILANDIA": "QUINDIO",
        "GENOVA": "QUINDIO",
        "MONTENEGRO": "QUINDIO",
        "PIJAO": "QUINDIO",
        "QUIMBAYA": "QUINDIO",
        "SALENTO": "QUINDIO",
    }
    
    # Define a dictionary to map substrings to department names
    Risaralda = {
        "BELEN DE UMBRIA": "RISARALDA", # Incluye Risaralda y sus variaciones
        "LA CELIA": "RISARALDA",
        "PUEBLO RICO": "RISARALDA",
        "LA VIRGINIA": "RISARALDA",
        "APIA": "RISARALDA",
        "BALBOA": "RISARALDA",
        "DOSQUEBRADAS": "RISARALDA",
        "GUATICA": "RISARALDA",
        "MARSELLA": "RISARALDA",
        "MISTRATO": "RISARALDA",
        "PEREIRA": "RISARALDA",
        "QUINCHIA": "RISARALDA",
        "SANTUARIO": "RISARALDA",
    }
    
    # Define a dictionary to map substrings to department names
    Caldas = {
        "CALDAS": "CALDAS",  # Incluye Caldas y sus variaciones
        "QUINDIO": "CALDAS",
        "RISARALDA": "CALDAS",
    }
    
    # Define a dictionary to map substrings to department names
    Sucre = {
        "SAN BENITO ABAD": "SUCRE",  # Incluye Sucre y sus variaciones
        "SANTIAGO DE TOLU": "SUCRE",
        "SAN MARCOS": "SUCRE",
        "SAN ONOFRE": "SUCRE",
        "LOS PALMITOS": "SUCRE",
        "SAN PEDRO": "SUCRE",
        "EL ROBLE": "SUCRE",
        "LA UNION": "SUCRE",
        "TOLU VIEJO": "SUCRE",
        "BUENAVISTA": "SUCRE",
        "CAIMITO": "SUCRE",
        "CHALAN": "SUCRE",
        "COLOSO": "SUCRE",
        "COROZAL": "SUCRE",
        "COVENAS": "SUCRE",
        "GALERAS": "SUCRE",
        "GUARANDA": "SUCRE",
        "MAJAGUAL": "SUCRE",
        "MORROA": "SUCRE",
        "OVEJAS": "SUCRE",
        "PALMITO": "SUCRE",
        "SAMPUES": "SUCRE",
        "SINCELEJO": "SUCRE",
        "SUCRE": "SUCRE",
    }
    
    # Define a dictionary to map substrings to department names
    Santander = {
        "ARAUCA": "SANTANDER",  # Incluye Arauca y sus variaciones
        "NORTE DE SANTANDER": "SANTANDER",
        "SANTANDER": "SANTANDER",
        "CUCUTA": "SANTANDER",
        "PALMAS DEL SOCORRO": "SANTANDER",
        "SABANA DE TORRES": "SANTANDER",
        "SANTA BARBARA": "SANTANDER",
        "LA BELLEZA": "SANTANDER",
        "SAN BENITO": "SANTANDER",
        "SAN GIL": "SANTANDER",
        "EL GUACAMAYO": "SANTANDER",
        "SAN JOAQUIN": "SANTANDER",
        "JESUS MARIA": "SANTANDER",
        "SAN MIGUEL": "SANTANDER",
        "PUENTE NACIONAL": "SANTANDER",
        "PUERTO PARRA": "SANTANDER",
        "LA PAZ": "SANTANDER",
        "EL PENON": "SANTANDER",
        "EL PLAYON": "SANTANDER",
        "LOS SANTOS": "SANTANDER",
        "PUERTO WILCHES": "SANTANDER",
        "AGUADA": "SANTANDER",
        "ALBANIA": "SANTANDER",
        "ARATOCA": "SANTANDER",
        "BARBOSA": "SANTANDER",
        "BARICHARA": "SANTANDER",
        "BARRANCABERMEJA": "SANTANDER",
        "BETULIA": "SANTANDER",
        "BUCARAMANGA": "SANTANDER",
        "CABRERA": "SANTANDER",
        "CALIFORNIA": "SANTANDER",
        "CAPITANEJO": "SANTANDER",
        "CARCASI": "SANTANDER",
        "CEPITA": "SANTANDER",
        "CERRITO": "SANTANDER",
        "CHARALA": "SANTANDER",
        "CHARTA": "SANTANDER",
        "CHIMA": "SANTANDER",
        "CHIPATA": "SANTANDER",
        "CIMITARRA": "SANTANDER",
        "CONCEPCION": "SANTANDER",
        "CONFINES": "SANTANDER",
        "CONTRATACION": "SANTANDER",
        "COROMORO": "SANTANDER",
        "CURITI": "SANTANDER",
        "ENCINO": "SANTANDER",
        "ENCISO": "SANTANDER",
        "FLORIAN": "SANTANDER",
        "FLORIDABLANCA": "SANTANDER",
        "GALAN": "SANTANDER",
        "GAMBITA": "SANTANDER",
        "GIRON": "SANTANDER",
        "GsEPSA": "SANTANDER",
        "GUACA": "SANTANDER",
        "GUADALUPE": "SANTANDER",
        "GUAPOTA": "SANTANDER",
        "GUAVATA": "SANTANDER",
        "HATO": "SANTANDER",
        "JORDAN": "SANTANDER",
        "LANDAZURI": "SANTANDER",
        "LEBRIJA": "SANTANDER",
        "MACARAVITA": "SANTANDER",
        "MALAGA": "SANTANDER",
        "MATANZA": "SANTANDER",
        "MOGOTES": "SANTANDER",
        "MOLAGAVITA": "SANTANDER",
        "OCAMONTE": "SANTANDER",
        "OIBA": "SANTANDER",
        "ONZAGA": "SANTANDER",
        "PALMAR": "SANTANDER",
        "PARAMO": "SANTANDER",
        "PIEDECUESTA": "SANTANDER",
        "PINCHOTE": "SANTANDER",
        "RIONEGRO": "SANTANDER",
        "SIMACOTA": "SANTANDER",
        "SOCORRO": "SANTANDER",
        "SUAITA": "SANTANDER",
        "SURATA": "SANTANDER",
        "TONA": "SANTANDER",
        "VELEZ": "SANTANDER",
        "VETAS": "SANTANDER",
        "VILLANUEVA": "SANTANDER",
        "ZAPATOCA": "SANTANDER",
    }
    
    # Define a dictionary to map substrings to department names
    Caqueta = {
        "CARTAGENA DEL CHAIRA": "CAQUETA", # Incluye Caquetá y sus variaciones
        "EL DONCELLO": "CAQUETA",
        "LA MONTANITA": "CAQUETA",
        "EL PAUJIL": "CAQUETA",
        "PUERTO RICO": "CAQUETA",
        "ALBANIA": "CAQUETA",
        "CURILLO": "CAQUETA",
        "FLORENCIA": "CAQUETA",
        "MILAN": "CAQUETA",
        "MORELIA": "CAQUETA",
        "SOLANO": "CAQUETA",
        "SOLITA": "CAQUETA",
        "VALPARAISO": "CAQUETA",
    }
    
    # Define a dictionary to map substrings to department names
    Caldas = {
        "LA DORADA": "CALDAS", # Incluye Caldas y sus variaciones
        "SAN JOSE": "CALDAS",
        "LA MERCED": "CALDAS",
        "AGUADAS": "CALDAS",
        "ANSERMA": "CALDAS",
        "ARANZAZU": "CALDAS",
        "BELALCAZAR": "CALDAS",
        "CHINCHINA": "CALDAS",
        "FILADELFIA": "CALDAS",
        "MANIZALES": "CALDAS",
        "MANZANARES": "CALDAS",
        "MARMATO": "CALDAS",
        "MARQUETALIA": "CALDAS",
        "MARULANDA": "CALDAS",
        "NEIRA": "CALDAS",
        "NORCASIA": "CALDAS",
        "PACORA": "CALDAS",
        "PALESTINA": "CALDAS",
        "PENSILVANIA": "CALDAS",
        "RIOSUCIO": "CALDAS",
        "RISARALDA": "CALDAS",
        "SALAMINA": "CALDAS",
        "SAMANA": "CALDAS",
        "SUPIA": "CALDAS",
        "VICTORIA": "CALDAS",
        "VILLAMARIA": "CALDAS",
        "VITERBO": "CALDAS",
    }
    
    # Define a dictionary to map substrings to department names
    San_Andres = {
        "SAN ANDRES": "SAN ANDRES", # Incluye San Andrés y sus variaciones
        "PROVIDENCIA": "SAN ANDRES",
    }
    
    # Define a dictionary to map substrings to department names
    Boyaca_1 = {
        "VILLA DE LEYVA": "BOYACA", # Incluye Boyacá y sus variaciones
        "PAZ DE RIO": "BOYACA",
        "PUERTO BOYACA": "BOYACA",
        "LA CAPILLA": "BOYACA",
        "EL COCUY": "BOYACA",
        "NUEVO COLON": "BOYACA",
        "SAN EDUARDO": "BOYACA",
        "EL ESPINO": "BOYACA",
        "SANTA MARIA": "BOYACA",
        "SAN MATEO": "BOYACA",
        "SANTA SOFIA": "BOYACA",
        "LA UVITA": "BOYACA",
        "LA VICTORIA": "BOYACA",
        "ALMEIDA": "BOYACA",
        "AQUITANIA": "BOYACA",
        "ARCABUCO": "BOYACA",
        "BELEN": "BOYACA",
        "BERBEO": "BOYACA",
        "BETEITIVA": "BOYACA",
        "BOAVITA": "BOYACA",
        "BOYACA": "BOYACA",
        "BRICENO": "BOYACA",
        "BUENAVISTA": "BOYACA",
        "BUSBANZA": "BOYACA",
        "CALDAS": "BOYACA",
        "CAMPOHERMOSO": "BOYACA",
        "CERINZA": "BOYACA",
        "CHINAVITA": "BOYACA",
        "CHIQUINQUIRA": "BOYACA",
        "CHIQUIZA": "BOYACA",
        "CHISCAS": "BOYACA",
        "CHITA": "BOYACA",
        "CHITARAQUE": "BOYACA",
        "CHIVATA": "BOYACA",
        "CHIVOR": "BOYACA",
        "CIENEGA": "BOYACA",
        "COMBITA": "BOYACA",
        "COPER": "BOYACA",
        "CORRALES": "BOYACA",
        "COVARACHIA": "BOYACA",
        "CUBARA": "BOYACA",
        "CUCAITA": "BOYACA",
        "CUITIVA": "BOYACA",
        "DUITAMA": "BOYACA",
        "FIRAVITOBA": "BOYACA",
        "FLORESTA": "BOYACA",
        "GACHANTIVA": "BOYACA",
        "GAMEZA": "BOYACA",
        "GARAGOA": "BOYACA",
        "GsICAN": "BOYACA",
        "GUACAMAYAS": "BOYACA",
        "GUATEQUE": "BOYACA",
        "GUAYATA": "BOYACA",
        "IZA": "BOYACA",
        "JENESANO": "BOYACA",
        "JERICO": "BOYACA",
        "LABRANZAGRANDE": "BOYACA"
    }
    
    Boyaca_2 = {
        "MACANAL": "BOYACA",
        "MARIPI": "BOYACA",
        "MIRAFLORES": "BOYACA",
        "MONGUA": "BOYACA",
        "MONGUI": "BOYACA",
        "MONIQUIRA": "BOYACA",
        "MOTAVITA": "BOYACA",
        "MUZO": "BOYACA",
        "NOBSA": "BOYACA",
        "OICATA": "BOYACA",
        "OTANCHE": "BOYACA",
        "PACHAVITA": "BOYACA",
        "PAEZ": "BOYACA",
        "PAIPA": "BOYACA",
        "PAJARITO": "BOYACA",
        "PANQUEBA": "BOYACA",
        "PAUNA": "BOYACA",
        "PAYA": "BOYACA",
        "PESCA": "BOYACA",
        "PISBA": "BOYACA",
        "QUIPAMA": "BOYACA",
        "RAMIRIQUI": "BOYACA",
        "RAQUIRA": "BOYACA",
        "RONDON": "BOYACA",
        "SABOYA": "BOYACA",
        "SACHICA": "BOYACA",
        "SAMACA": "BOYACA",
        "SANTANA": "BOYACA",
        "SATIVANORTE": "BOYACA",
        "SATIVASUR": "BOYACA",
        "SIACHOQUE": "BOYACA",
        "SOATA": "BOYACA",
        "SOCHA": "BOYACA",
        "SOCOTA": "BOYACA",
        "SOGAMOSO": "BOYACA",
        "SOMONDOCO": "BOYACA",
        "SORA": "BOYACA",
        "SORACA": "BOYACA",
        "SOTAQUIRA": "BOYACA",
        "SUSACON": "BOYACA",
        "SUTAMARCHAN": "BOYACA",
        "SUTATENZA": "BOYACA",
        "TASCO": "BOYACA",
        "TENZA": "BOYACA",
        "TIBANA": "BOYACA",
        "TIBASOSA": "BOYACA",
        "TINJACA": "BOYACA",
        "TIPACOQUE": "BOYACA",
        "TOCA": "BOYACA",
        "TOGsI": "BOYACA",
        "TOPAGA": "BOYACA",
        "TOTA": "BOYACA",
        "TUNJA": "BOYACA",
        "TUNUNGUA": "BOYACA",
        "TURMEQUE": "BOYACA",
        "TUTA": "BOYACA",
        "TUTAZA": "BOYACA",
        "UMBITA": "BOYACA",
        "VENTAQUEMADA": "BOYACA",
        "VIRACACHA": "BOYACA",
        "ZETAQUIRA": "BOYACA"
    }
    
    # Define a dictionary to map substrings to department names
    Narino = {
        "LOS ANDES": "NARINO",  # Incluye NariNo y sus variaciones
        "SANTA BARBARA": "NARINO",
        "SAN BERNARDO": "NARINO",
        "EL CHARCO": "NARINO",
        "LA CRUZ": "NARINO",
        "LA FLORIDA": "NARINO",
        "OLAYA HERRERA": "NARINO",
        "LA LLANADA": "NARINO",
        "SAN LORENZO": "NARINO",
        "SAN PABLO": "NARINO",
        "ROBERTO PAYAN": "NARINO",
        "EL PENOL": "NARINO",
        "FRANCISCO PIZARRO": "NARINO",
        "EL ROSARIO": "NARINO",
        "EL TAMBO": "NARINO",
        "LA TOLA": "NARINO",
        "LA UNION": "NARINO",
        "ALBAN": "NARINO",
        "ALDANA": "NARINO",
        "ANCUYA": "NARINO",
        "ARBOLEDA": "NARINO",
        "BARBACOAS": "NARINO",
        "BELEN": "NARINO",
        "BUESACO": "NARINO",
        "CHACHAGsI": "NARINO",
        "COLON": "NARINO",
        "CONSACA": "NARINO",
        "CONTADERO": "NARINO",
        "CORDOBA": "NARINO",
        "CUASPUD": "NARINO",
        "CUMBAL": "NARINO",
        "CUMBITARA": "NARINO",
        "FUNES": "NARINO",
        "GUACHUCAL": "NARINO",
        "GUAITARILLA": "NARINO",
        "GUALMATAN": "NARINO",
        "ILES": "NARINO",
        "IMUES": "NARINO",
        "IPIALES": "NARINO",
        "LEIVA": "NARINO",
        "LINARES": "NARINO",
        "MAGsI": "NARINO",
        "MALLAMA": "NARINO",
        "MOSQUERA": "NARINO",
        "NARINO": "NARINO",
        "OSPINA": "NARINO",
        "PASTO": "NARINO",
        "POLICARPA": "NARINO",
        "POTOSI": "NARINO",
        "PUERRES": "NARINO",
        "PUPIALES": "NARINO",
        "RICAURTE": "NARINO",
        "SAMANIEGO": "NARINO",
        "SANDONA": "NARINO",
        "SANTACRUZ": "NARINO",
        "SAPUYES": "NARINO",
        "TAMINANGO": "NARINO",
        "TANGUA": "NARINO",
        "TUQUERRES": "NARINO",
        "YACUANQUER": "NARINO",
    }
    
    # Define a dictionary to map substrings to department names
    Huila = {
        "SAN AGUSTIN": "HUILA",     # Incluye Huila y sus variaciones
        "LA ARGENTINA": "HUILA",
        "SANTA MARIA": "HUILA",
        "LA PLATA": "HUILA",
        "ACEVEDO": "HUILA",
        "AGRADO": "HUILA",
        "AIPE": "HUILA",
        "ALGECIRAS": "HUILA",
        "ALTAMIRA": "HUILA",
        "BARAYA": "HUILA",
        "CAMPOALEGRE": "HUILA",
        "COLOMBIA": "HUILA",
        "ELIAS": "HUILA",
        "GARZON": "HUILA",
        "GIGANTE": "HUILA",
        "GUADALUPE": "HUILA",
        "HOBO": "HUILA",
        "IQUIRA": "HUILA",
        "ISNOS": "HUILA",
        "NATAGA": "HUILA",
        "NEIVA": "HUILA",
        "OPORAPA": "HUILA",
        "PAICOL": "HUILA",
        "PALERMO": "HUILA",
        "PALESTINA": "HUILA",
        "PITAL": "HUILA",
        "PITALITO": "HUILA",
        "RIVERA": "HUILA",
        "SALADOBLANCO": "HUILA",
        "SUAZA": "HUILA",
        "TARQUI": "HUILA",
        "TELLO": "HUILA",
        "TERUEL": "HUILA",
        "TESALIA": "HUILA",
        "TIMANA": "HUILA",
        "VILLAVIEJA": "HUILA",
        "YAGUARA": "HUILA",
    }
    
    # Define a dictionary to map substrings to department names
    Guainia = {
        "PUERTO COLOMBIA": "GUAINIA",   # Incluye Guainia y sus variaciones
        "SAN FELIPE": "GUAINIA",
        "LA GUADALUPE": "GUAINIA",
        "BARRANCO MINAS": "GUAINIA",
        "PANA PANA": "GUAINIA",
        "CACAHUAL": "GUAINIA",
        "INIRIDA": "GUAINIA",
        "MAPIRIPANA": "GUAINIA",
        "MORICHAL": "GUAINIA",
    }
    
    # Define a dictionary to map substrings to department names
    Putumayo = {
        "VALLE DEL GUAMUEZ": "PUTUMAYO", # Incluye Putumayo y sus variaciones
        "PUERTO ASIS": "PUTUMAYO",
        "PUERTO CAICEDO": "PUTUMAYO",
        "SAN FRANCISCO": "PUTUMAYO",
        "PUERTO GUZMAN": "PUTUMAYO",
        "SAN MIGUEL": "PUTUMAYO",
        "COLON": "PUTUMAYO",
        "LEGUIZAMO": "PUTUMAYO",
        "MOCOA": "PUTUMAYO",
        "ORITO": "PUTUMAYO",
        "SANTIAGO": "PUTUMAYO",
        "SIBUNDOY": "PUTUMAYO",
        "VILLAGARZON": "PUTUMAYO",
    }
    
    # Define a dictionary to map substrings to department names
    Tolima = {
        "CARMEN DE APICALA": "TOLIMA",  # Incluye Tolima y sus variaciones
        "SAN ANTONIO": "TOLIMA",
        "SANTA ISABEL": "TOLIMA",
        "SAN LUIS": "TOLIMA",
        "ALPUJARRA": "TOLIMA",
        "ALVARADO": "TOLIMA",
        "AMBALEMA": "TOLIMA",
        "ANZOATEGUI": "TOLIMA",
        "ARMERO": "TOLIMA",
        "ATACO": "TOLIMA",
        "CAJAMARCA": "TOLIMA",
        "CASABIANCA": "TOLIMA",
        "CHAPARRAL": "TOLIMA",
        "COELLO": "TOLIMA",
        "COYAIMA": "TOLIMA",
        "CUNDAY": "TOLIMA",
        "DOLORES": "TOLIMA",
        "ESPINAL": "TOLIMA",
        "FALAN": "TOLIMA",
        "FLANDES": "TOLIMA",
        "FRESNO": "TOLIMA",
        "GUAMO": "TOLIMA",
        "HERVEO": "TOLIMA",
        "HONDA": "TOLIMA",
        "IBAGUE": "TOLIMA",
        "ICONONZO": "TOLIMA",
        "LERIDA": "TOLIMA",
        "LIBANO": "TOLIMA",
        "MARIQUITA": "TOLIMA",
        "MELGAR": "TOLIMA",
        "MURILLO": "TOLIMA",
        "NATAGAIMA": "TOLIMA",
        "ORTEGA": "TOLIMA",
        "PALOCABILDO": "TOLIMA",
        "PIEDRAS": "TOLIMA",
        "PLANADAS": "TOLIMA",
        "PRADO": "TOLIMA",
        "PURIFICACION": "TOLIMA",
        "RIOBLANCO": "TOLIMA",
        "RONCESVALLES": "TOLIMA",
        "ROVIRA": "TOLIMA",
        "SALDANA": "TOLIMA",
        "SUAREZ": "TOLIMA",
        "VENADILLO": "TOLIMA",
        "VILLAHERMOSA": "TOLIMA",
        "VILLARRICA": "TOLIMA",
    }
    
    # Define a dictionary to map substrings to department names
    Vaupes = {
        "CARURU": "VAUPES",   # Incluye Vaupés y sus variaciones
        "MITU": "VAUPES",
        "PACOA": "VAUPES",
        "PAPUNAUA": "VAUPES",
        "TARAIRA": "VAUPES",
        "YAVARATE": "VAUPES",
    }
    
    # Define a dictionary to map substrings to department names
    Amazonas = {
        "AMAZONAS": "AMAZONAS",  # Incluye Amazonas y sus variaciones
        "MIRITI - PARANA": "AMAZONAS",
        "PUERTO ALEGRIA": "AMAZONAS",
        "PUERTO ARICA": "AMAZONAS",
        "LA CHORRERA": "AMAZONAS",
        "EL ENCANTO": "AMAZONAS",
        "PUERTO NARI": "AMAZONAS",
        "LA PEDRERA": "AMAZONAS",
        "PUERTO SANTANDER": "AMAZONAS",
        "LA VICTORIA": "AMAZONAS",
        "LETICIA": "AMAZONAS",
        "TARAPACA": "AMAZONAS",
    }
    
    # Define a dictionary to map substrings to department names
    Vichada = {
        "PUERTO CARRENO": "VICHADA",     # Incluye Vichada y sus variaciones
        "LA PRIMAVERA": "VICHADA",
        "SANTA ROSALIA": "VICHADA",
        "CUMARIBO": "VICHADA",
    }
    
    # Define a dictionary to map substrings to department names
    Atlantico = {
        "JUAN DE ACOSTA": "ATLANTICO",     # Incluye Atlantico y sus variaciones
        "PALMAR DE VARELA": "ATLANTICO",
        "PUERTO COLOMBIA": "ATLANTICO",
        "SANTA LUCIA": "ATLANTICO",
        "SANTO TOMAS": "ATLANTICO",
        "BARANOA": "ATLANTICO",
        "BARRANQUILLA": "ATLANTICO",
        "CANDELARIA": "ATLANTICO",
        "GALAPA": "ATLANTICO",
        "LURUACO": "ATLANTICO",
        "MALAMBO": "ATLANTICO",
        "MANATI": "ATLANTICO",
        "PIOJO": "ATLANTICO",
        "POLONUEVO": "ATLANTICO",
        "PONEDERA": "ATLANTICO",
        "REPELON": "ATLANTICO",
        "SABANAGRANDE": "ATLANTICO",
        "SABANALARGA": "ATLANTICO",
        "SOLEDAD": "ATLANTICO",
        "SUAN": "ATLANTICO",
        "TUBARA": "ATLANTICO",
        "USIACURI": "ATLANTICO",
    }
    
    # Define a dictionary to map substrings to department names
    Meta = {
        "META": "META",    # Incluye Meta y sus variaciones
        "VILLAV": "META",  
        "CASTILLA LA NUEVA": "META",
        "FUENTE DE ORO": "META",
        "BARRANCA DE UPIA": "META",
        "EL CALVARIO": "META",
        "EL CASTILLO": "META",
        "PUERTO CONCORDIA": "META",
        "EL DORADO": "META",
        "PUERTO GAITAN": "META",
        "SAN JUANITO": "META",
        "PUERTO LLERAS": "META",
        "PUERTO LOPEZ": "META",
        "LA MACARENA": "META",
        "SAN MARTIN": "META",
        "PUERTO RICO": "META",
        "ACACIAS": "META",
        "CABUYARO": "META",
        "CUBARRAL": "META",
        "CUMARAL": "META",
        "GRANADA": "META",
        "GUAMAL": "META",
        "LEJANIAS": "META",
        "MAPIRIPAN": "META",
        "MESETAS": "META",
        "RESTREPO": "META",
        "URIBE": "META",
        "VILLAVICENCIO": "META",
        "VISTAHERMOSA": "META",
    }
    
    # Define a dictionary to map substrings to department names
    Bolivar = {
        "MARIA LA BAJA": "BOLIVAR", # Incluye Bolivar y sus variaciones
        "BARRANCO DE LOBA": "BOLIVAR",
        "HATILLO DE LOBA": "BOLIVAR",
        "SAN JUAN NEPOMUCENO": "BOLIVAR",
        "ALTOS DEL ROSARIO": "BOLIVAR",
        "SANTA CATALINA": "BOLIVAR",
        "SAN CRISTOBAL": "BOLIVAR",
        "SAN ESTANISLAO": "BOLIVAR",
        "SAN FERNANDO": "BOLIVAR",
        "EL GUAMO": "BOLIVAR",
        "SAN JACINTO": "BOLIVAR",
        "TALAIGUA NUEVO": "BOLIVAR",
        "SAN PABLO": "BOLIVAR",
        "EL PENON": "BOLIVAR",
        "SANTA ROSA": "BOLIVAR",
        "RIO VIEJO": "BOLIVAR",
        "ACHI": "BOLIVAR",
        "ARENAL": "BOLIVAR",
        "ARJONA": "BOLIVAR",
        "ARROYOHONDO": "BOLIVAR",
        "CALAMAR": "BOLIVAR",
        "CANTAGALLO": "BOLIVAR",
        "CARTAGENA": "BOLIVAR",
        "CICUCO": "BOLIVAR",
        "CLEMENCIA": "BOLIVAR",
        "MAGANGUE": "BOLIVAR",
        "MAHATES": "BOLIVAR",
        "MARGARITA": "BOLIVAR",
        "MOMPOS": "BOLIVAR",
        "MONTECRISTO": "BOLIVAR",
        "MORALES": "BOLIVAR",
        "NOROSI": "BOLIVAR",
        "PINILLOS": "BOLIVAR",
        "REGIDOR": "BOLIVAR",
        "SIMITI": "BOLIVAR",
        "SOPLAVIENTO": "BOLIVAR",
        "TIQUISIO": "BOLIVAR",
        "TURBACO": "BOLIVAR",
        "TURBANA": "BOLIVAR",
        "VILLANUEVA": "BOLIVAR",
        "ZAMBRANO": "BOLIVAR"

    }
    
    # Combine all department mappings into a single list
    department_mapping = [Arauca, Casanare, Cauca, Choco, Norte_Santander, Guaviare, Cesar, Cordoba, Santander, Caqueta, 
                          Caldas, San_Andres, Magdalena, Guajira, Risaralda, Quindio, Sucre, Guainia, Putumayo, Vaupes,
                          Antioquia_1, Antioquia_2, Antioquia_3]
    
    # for i in department_mapping:
    #     unique_values = set(i.values())
    #     print(f"{unique_values}: {len(i)}")
    
    department_mapping_2 = [Huila, Vichada, Atlantico, Meta, Amazonas, Bolivar, Cundinamarca_1, Cundinamarca_2, 
                            Cundinamarca_3, Boyaca_1, Boyaca_2, Narino, Valle_del_Cauca, Tolima]
    
    # for i in department_mapping_2:
    #     unique_values = set(i.values())
    #     print(f"{unique_values}: {len(i)}")
    
    return department_mapping, department_mapping_2

def rename_columns(df, column_map):
    """Renames columns based on a provided mapping."""
    for old_name, new_name in column_map.items():
        df = df.withColumnRenamed(old_name, new_name)
    return df

def Save_Data_Frame (Data_Frame, Directory_to_Save, Partitions, year_data, month_data):

    now = datetime.now()
    Time_File = now.strftime("%Y%m%d")
    Type_File = f"Lectura Data Claro {year_data} - {month_data}"

    output_path = f'{Directory_to_Save}{Type_File}{Time_File}'
    Data_Frame.repartition(Partitions).write.mode("overwrite").option("header", "true").option("delimiter",";").csv(output_path)
    
    for root, dirs, files in os.walk(output_path):
        for file in files:
            if file.startswith("._") or file == "_SUCCESS" or file.endswith(".crc"):
                os.remove(os.path.join(root, file))
    
    for i, file in enumerate(os.listdir(output_path), start=1):
        if file.endswith(".csv"):
            old_file_path = os.path.join(output_path, file)
            new_file_path = os.path.join(output_path, f'Compilado Data Claro {year_data} - {month_data}.csv')
            os.rename(old_file_path, new_file_path)

def filters_report(df):
    
    df = df.select("CUENTA NEXT", "FECHA_GESTION_RG", "GRUPO_TIPIFICACION_RG")

    df = df.withColumn("CANTIDAD_NOCONTACTO", when(col("GRUPO_TIPIFICACION_RG") == "NOC", 1).otherwise(0)) \
           .withColumn("CANTIDAD_CONTACTO_INDIRECTO", when(col("GRUPO_TIPIFICACION_RG") == "CIND", 1).otherwise(0)) \
           .withColumn("CANTIDAD_CONTACTO_DIRECTO", when(col("GRUPO_TIPIFICACION_RG") == "CDIR", 1).otherwise(0))

    df_grouped = df.groupBy("CUENTA NEXT") \
        .agg(sum("CANTIDAD_NOCONTACTO").alias("CANTIDAD_NOCONTACTO"),
             sum("CANTIDAD_CONTACTO_INDIRECTO").alias("CANTIDAD_CONTACTO_INDIRECTO"),
             sum("CANTIDAD_CONTACTO_DIRECTO").alias("CANTIDAD_CONTACTO_DIRECTO"))

    df_grouped = df_grouped.dropDuplicates(["CUENTA NEXT"])
    
    df_grouped = df_grouped.withColumn(
        "FILTRO_REPORTE_GESTION",
        when(
            (col("CANTIDAD_CONTACTO_DIRECTO")  > 0) &
            (col("CANTIDAD_CONTACTO_INDIRECTO")> 0) &
            (col("CANTIDAD_NOCONTACTO")        > 0),
            lit("CDIR CIND NOC")
        ).when(
            (col("CANTIDAD_CONTACTO_DIRECTO")  > 0) &
            (col("CANTIDAD_CONTACTO_INDIRECTO")> 0) &
            (col("CANTIDAD_NOCONTACTO")        == 0),
            lit("CDIR CIND")
        ).when(
            (col("CANTIDAD_CONTACTO_DIRECTO")  > 0) &
            (col("CANTIDAD_CONTACTO_INDIRECTO")== 0) &
            (col("CANTIDAD_NOCONTACTO")        > 0),
            lit("CDIR NOC")
        ).when(
            (col("CANTIDAD_CONTACTO_DIRECTO")  == 0) &
            (col("CANTIDAD_CONTACTO_INDIRECTO")> 0) &
            (col("CANTIDAD_NOCONTACTO")        > 0),
            lit("CIND NOC")
        ).when(
            (col("CANTIDAD_CONTACTO_DIRECTO")  > 0) &
            (col("CANTIDAD_CONTACTO_INDIRECTO")== 0) &
            (col("CANTIDAD_NOCONTACTO")        == 0),
            lit("CDIR")
        ).when(
            (col("CANTIDAD_CONTACTO_DIRECTO")  == 0) &
            (col("CANTIDAD_CONTACTO_INDIRECTO")> 0) &
            (col("CANTIDAD_NOCONTACTO")        == 0),
            lit("CIND")
        ).when(
            (col("CANTIDAD_CONTACTO_DIRECTO")  == 0) &
            (col("CANTIDAD_CONTACTO_INDIRECTO")== 0) &
            (col("CANTIDAD_NOCONTACTO")        > 0),
            lit("NOC")
        ).otherwise(lit(""))
    )
    
    return df_grouped

def read_compilation_datasets(input_folder, output_path, num_partitions, month_data, year_data):
    
    spark = SparkSession.builder \
    .appName("BD_MONTH") \
        .config("spark.driver.memory", "20g") \
        .config("spark.executor.memory", "20g") \
        .config("spark.driver.extraJavaOptions", "-Xss4m -XX:MaxDirectMemorySize=24g") \
        .config("spark.executor.extraJavaOptions", "-Xss4m -XX:MaxDirectMemorySize=24g") \
    .getOrCreate()

    spark.conf.set("mapreduce.fileoutputcomitter.marksuccessfuljobs", "false")
    
    # File paths
    Root_Data = os.path.join(input_folder, f"Asignacion Estructurada/Asignacion Multimarca {year_data}-{month_data}.csv")
    Root_Base = os.path.join(input_folder, f"Base General Mensual/BaseGeneral {year_data}-{month_data}.csv")
    Root_Touch = os.path.join(input_folder, f"Toques por Telemática/Toques {year_data}-{month_data}.csv")
    Root_Demo = os.path.join(input_folder, f"Demograficos Estructurados/Demograficos {year_data}-{month_data}.csv")
    Root_Colas = os.path.join(input_folder, f"Archivos Complementarios/Colas/Colas {year_data}-{month_data}.csv")
    Root_Ranking = os.path.join(input_folder, f"Archivos Complementarios/Ranking/Rankings Claro {year_data}-{month_data}.csv")
    Root_Exc_Doc = os.path.join(input_folder, f"Archivos Complementarios/Exclusiones/Exclusion Documentos {year_data}-{month_data}.csv")
    Root_Exc_Cta = os.path.join(input_folder, f"Archivos Complementarios/Exclusiones/Exclusion Cuentas {year_data}-{month_data}.csv")
    Root_PSA = os.path.join(input_folder, f"Archivos Complementarios/Pagos sin Aplicar/Pagos sin Aplicar {year_data}-{month_data}.csv")
    Root_NG = os.path.join(input_folder, f"Archivos Complementarios/No Gestión/No Gestion {year_data}-{month_data}.csv")
    Root_Managment = os.path.join(input_folder, f"Archivos Complementarios/Reporte Gestión/ReporteGestion-{year_data}-{month_data}.csv")
    
    try:
        # Read DataFrames
        File_Data = spark.read.csv(Root_Data, header=True, sep=";")
        File_Base = spark.read.csv(Root_Base, header=True, sep=";")
        File_Touch = spark.read.csv(Root_Touch, header=True, sep=";")
        File_Demo = spark.read.csv(Root_Demo, header=True, sep=";")
        File_Colas = spark.read.csv(Root_Colas, header=True, sep=";")
        File_Ranking = spark.read.csv(Root_Ranking, header=True, sep=";")
        File_Exc_Doc = spark.read.csv(Root_Exc_Doc, header=True, sep=";")
        File_Exc_Cta = spark.read.csv(Root_Exc_Cta, header=True, sep=";")
        File_PSA = spark.read.csv(Root_PSA, header=True, sep=";")
        File_NG = spark.read.csv(Root_NG, header=True, sep=";")
        File_Managment = spark.read.csv(Root_Managment, header=True, sep=";")
        
        # Rename columns
        column_renames = {
            "CUENTA_NEXT": "CUENTA NEXT",
            "Cuenta_Sin_Punto": "CUENTA NEXT",
            "Cuenta_Real": "CUENTA NEXT",
            "marca": None  # We will handle this in the loop below
        }
        
        # Create a dictionary for DataFrames
        dataframes = {
            'Data': File_Data,
            'Touch': File_Touch,
            'Demo': File_Demo,
            'Base': File_Base,
            'Colas': File_Colas,
            'Ranking': File_Ranking,
            'Exclusion_Documents': File_Exc_Doc,
            'Exclusion_Accounts': File_Exc_Cta,
            'Payments_Not_Applied': File_PSA,
            'No_Managment': File_NG,
            'Report_Managment': File_Managment
        }
        
        # List of columns to exclude
        days_columns = []
        for i in range(1, 32):
            days_columns.append(f"Dia_{i}")

        phone_columns = []
        for i in range(1, 21):
            phone_columns.append(f"phone{i}")
        
        email_columns = []
        for i in range(1, 6):
            email_columns.append(f"email{i}")
            
        columns_custom = ['MES DE ASIGNACION', 'PERIODO DE ASIGNACION']
        column_touch = ['Cuenta_Sin_Punto', 'marca']
        
        columns_to_exclude_data = columns_custom #+ days_columns
        columns_to_exclude_demo = columns_custom #+ phone_columns + email_columns
        columns_to_exclude_touch = columns_custom + column_touch
        columns_to_exclude_base = ['COLUMNA 2', ' COLUMNA 3', 'COLUMNA 4', 'SEGMENTO55', 'SEGMENTO', 'MULTIPRODUCTO']
        
        # Assuming columns_to_exclude_data, columns_to_exclude_demo, and columns_to_exclude_touch are lists of column names
        dataframes['Data'] = dataframes['Data'].drop(*columns_to_exclude_data)  # Use * to unpack the list
        dataframes['Demo'] = dataframes['Demo'].drop(*columns_to_exclude_demo)  # Use * to unpack the list
        dataframes['Touch'] = dataframes['Touch'].drop(*columns_to_exclude_touch)  # Use * to unpack the list
        dataframes['Base'] = dataframes['Base'].drop(*columns_to_exclude_base)  # Use * to unpack the list
        
        # Cleaning column names in Report_Managment
        cols = [c.strip() for c in dataframes['Report_Managment'].columns]
        dataframes['Report_Managment'] = dataframes['Report_Managment'].toDF(*cols)
        dataframes['Report_Managment'] = dataframes['Report_Managment'] \
            .select(col("Cuenta").alias("CUENTA NEXT"),
                    col("fecha_gestion").alias("FECHA_GESTION_RG"),
                    col("tipificacion").alias("TIPIFICACION_RG"))
            
        map_group = {
            "aldia": "CDIR",
            "colgo": "CIND",
            "dificultaddepago": "CDIR",
            "fallecido": "NOC",
            "falleció": "NOC",
            "interesadoenpagar": "CDIR",
            "contactocontitular": "CDIR",
            "mensajecontercero": "CIND",
            "conocealtitular": "CIND",
            "noconocealtitular": "CIND",
            "noasumedeuda": "CDIR",
            "nocontestan": "NOC",
            "nocontesta": "NOC",
            "numeroerrado": "NOC",
            "posiblefraude": "CDIR",
            "promesa": "CDIR",
            "promesadepago": "CDIR",
            "promesaincumplida": "CDIR",
            "promesaparcial": "CDIR",
            "reclamacion": "CDIR",
            "reclamaciónvigente": "CDIR",
            "recordatorio": "CDIR",
            "renuente": "CDIR",
            "insolvente": "CDIR",
            "terceronotomamensaje": "CIND",
            "volverallamar": "CDIR",
            "reprogramarcontacto": "CDIR",
            "llamarluego": "CDIR",
            "yapago": "CDIR",
            "yapagó": "CDIR",
            "singestion": "NOC",
            "gestionivr": "CIND"
        }

        # Convert the dictionary to a list of tuples for create_map
        map_expr = create_map([lit(x) for x in chain(*map_group.items())])

        # Create a new column 'GRUPO_TIPIFICACION_RG' based on the mapping
        dataframes['Report_Managment'] = dataframes['Report_Managment'] \
            .withColumn("TIPIFICACION_RG_CLEAN", regexp_replace(lower(col("TIPIFICACION_RG")), " ", "")) \
            .withColumn("GRUPO_TIPIFICACION_RG", map_expr.getItem(col("TIPIFICACION_RG_CLEAN")))
        
        dataframes['Report_Managment'] = dataframes['Report_Managment'].filter(
            col("TIPIFICACION_RG_CLEAN") != "smsenviado"
        )
        
        dataframes['Report_Managment'] = dataframes['Report_Managment'] \
            .withColumn("CUENTA NEXT", concat(regexp_replace(col("CUENTA NEXT"), "\\.", ""), lit("-")))
            
        dataframes['Report_Managment'] \
            .select("TIPIFICACION_RG_CLEAN") \
            .distinct() \
            .show(truncate=False)
        
        dataframes['Report_Managment'] = filters_report(dataframes['Report_Managment'])
        
        dataframes['Touch'] = dataframes['Touch'].withColumn("Cuenta_Real", regexp_replace(col("Cuenta_Real"), "\\.", ""))
        dataframes['Demo'] = dataframes['Demo'].withColumnRenamed("cuenta", "CUENTA NEXT")
        
        dataframes['Data'] = dataframes['Data'].withColumnRenamed("MARCA", "MARCA DATA")
        dataframes['Data'] = dataframes['Data'].withColumnRenamed("FECHA INGRESO", "FECHA INGRESO DATA")
        dataframes['Data'] = dataframes['Data'].withColumnRenamed("FECHA RETIRO", "FECHA RETIRO DATA")
        
        
        ### Filters in aditional dataframes
        dataframes['Colas'] = dataframes['Colas'].withColumnRenamed("Cuenta", "CUENTA NEXT")
        dataframes['Colas'] = dataframes['Colas'].withColumn("CUENTA NEXT", concat(col("CUENTA NEXT"), lit("-")))
        dataframes['Colas'] = dataframes['Colas'].withColumnRenamed("Fecha", "FECHA PAGO DE COLA")
        dataframes['Colas'] = dataframes['Colas'].withColumnRenamed("Valor", "VALOR PAGO DE COLAS")
        dataframes['Colas'] = dataframes['Colas'].withColumnRenamed("Referencia", "REFERENCIA DE COLAS")
        dataframes['Colas'] = dataframes['Colas'].withColumn("FILTRO COLAS", lit("APLICA"))
        
        dataframes['Ranking'] = dataframes['Ranking'].withColumnRenamed("CUENTA", "CUENTA NEXT")
        dataframes['Ranking'] = dataframes['Ranking'].withColumn("CUENTA NEXT", concat(col("CUENTA NEXT"), lit("-")))
        dataframes['Ranking'] = dataframes['Ranking'].withColumnRenamed("SERVICIOS", "SERVICIOS RANKING")
        dataframes['Ranking'] = dataframes['Ranking'].withColumnRenamed("ESTADO", "ESTADO RANKING")
        
        #dataframes['Exclusion_Documents'] = dataframes['Exclusion_Documents'].withColumnRenamed("DOCUMENTO", "DOCUMENTO EXCLUSION")
        dataframes['Exclusion_Documents'] = dataframes['Exclusion_Documents'].withColumn("EXCLUSION DOCUMENTO", lit("APLICA"))
        dataframes['Exclusion_Accounts'] = dataframes['Exclusion_Accounts'].withColumnRenamed("CUENTA", "CUENTA NEXT")
        dataframes['Exclusion_Accounts'] = dataframes['Exclusion_Accounts'].withColumn("CUENTA NEXT", concat(col("CUENTA NEXT"), lit("-")))
        dataframes['Exclusion_Accounts'] = dataframes['Exclusion_Accounts'].withColumn("EXCLUSION CUENTA", lit("APLICA"))
        
        dataframes['Payments_Not_Applied'] = dataframes['Payments_Not_Applied'].withColumnRenamed("CUENTA", "CUENTA NEXT")
        dataframes['Payments_Not_Applied'] = dataframes['Payments_Not_Applied'].withColumn("CUENTA NEXT", concat(col("CUENTA NEXT"), lit("-")))
        dataframes['Payments_Not_Applied'] = dataframes['Payments_Not_Applied'].withColumnRenamed("RECUENTO", "RECUENTO PAGOS SIN APLICAR")
        
        dataframes['No_Managment'] = dataframes['No_Managment'].withColumnRenamed("CUENTA", "CUENTA NEXT")
        dataframes['No_Managment'] = dataframes['No_Managment'].withColumn("CUENTA NEXT", concat(col("CUENTA NEXT"), lit("-")))
        dataframes['No_Managment'] = dataframes['No_Managment'].withColumnRenamed("RECUENTO", "RECUENTO NO GESTION")
        
        
        # Rename columns in each DataFrame and rename 'CUENTA NEXT' to avoid ambiguity
        for base in dataframes.keys():
            dataframes[base] = rename_columns(dataframes[base], {k: v for k, v in column_renames.items() if v is not None})
            #dataframes[base] = dataframes[base].withColumnRenamed("CUENTA NEXT", f"CUENTA_NEXT_{base}")
        
        # Initialize Data_Frame with File_Base
        dataframes['Touch'] = dataframes['Touch'].withColumn("CUENTA NEXT", concat(col("CUENTA NEXT"), lit("-")))
        
        Data_Frame = dataframes['Base']
        Data_Frame = Data_Frame.withColumn("CUENTA NEXT", concat(col("CUENTA NEXT"), lit("-")))
        
        Data_Frame = Data_Frame.dropDuplicates(["CUENTA NEXT"])
        print("1 Cantidad de registros:", Data_Frame.count())
        
        joins = [
            ('Demo',             'CUENTA NEXT'),
            ('Touch',            'CUENTA NEXT'),
            ('Data',             'CUENTA NEXT'),
            ('Colas',            'CUENTA NEXT'),
            ('Ranking',          'CUENTA NEXT'),
            ('Exclusion_Accounts','CUENTA NEXT'),
            ('Payments_Not_Applied','CUENTA NEXT'),
            ('No_Managment',     'CUENTA NEXT'),
            ('Report_Managment', 'CUENTA NEXT'),
        ]
        for tbl_name, key in joins:
            sec = dataframes[tbl_name].dropDuplicates([key])
            Data_Frame = Data_Frame.join(sec, on=key, how='left')
            
        print("2 Cantidad de registros:", Data_Frame.count())
        
        sec = dataframes['Exclusion_Documents'].dropDuplicates(['DOCUMENTO'])
        Data_Frame = Data_Frame.join(sec, on='DOCUMENTO', how='left')
        print("3 Cantidad de registros:", Data_Frame.count())

        Data_Frame = Data_Frame.withColumn(
            "FILTRO DESCUENTO",
            when(col("PORCENTAJE") == 0, lit("No Aplica")).otherwise(lit("Aplica"))
        )
        
        department_mapping, department_mapping_2 = cruice_department_mapping()
        
        Data_Frame = depto(Data_Frame, department_mapping, department_mapping_2)

        list_columns_base = dataframes['Base'].columns
        
        Data_Frame = RenameColumns(Data_Frame)
        
        # Filter and write the DataFrame
        if Data_Frame.count() > 0:
            
            list_columns_base =  [
                'Nmero de Cliente', '[AccountAccountCode?]', 'CRM_ORIGEN', 'Edad de Deuda',
                '[PotencialMark?]', '[PrePotencialMark?]', '[WriteOffMark?]', 'Monto inicial',
                '[ModInitCta?]', '[DeudaRealCuenta?]', '[BillCycleName?]', 'Nombre Campaa',
                '[DebtAgeInicial?]', 'Nombre Casa de Cobro', 'Fecha de asignacin', 'Deuda Gestionable',
                'Direccin Completa', '[Documento?]', '[AccStsName?]',
                'Ciudad', '[InboxName?]', 'Nombre del Cliente', 'Id de Ejecucion',
                'Fecha de Vencimiento', 'Numero Referencia de Pago', 'MIN', 'Plan',
                'Cuotas Aceleradas', 'Fecha de Aceleracion', 'Valor Acelerado', 'Intereses Contingentes',
                'Intereses Corrientes Facturados', 'Intereses por mora facturados', 'Cuotas Facturadas', 
                'Iva Intereses Contigentes Facturado', 'Iva Intereses Corrientes Facturados',
                'Iva Intereses por Mora Facturado', 'Precio Subscripcion', 'Cdigo de proceso',
                '[CustomerTypeId?]', '[RefinanciedMark?]', '[Discount?]', '[Permanencia?]',
                '[DeudaSinPermanencia?]', 'Telefono 1', 'Telefono 2', 'Telefono 3',
                'Telefono 4', 'Email', '[ActivesLines?]', 'MARCA_ASIGNACION',
                'CUENTA_NEXT', 'SALDO', 'SEGMENTO_ASIGNACION', 'RANGO', 'FECHA INGRESO',
                'FECHA SALIDA', 'VALOR PAGO', 'VALOR PAGO REAL', 'FECHA_ULT_PAGO',
                'DESCUENTO', 'EXCLUSION DCTO', 'LIQUIDACION', 'TIPO DE PAGO', 'PAGO'
            ]
            
            Data_Frame = Data_Frame.withColumnRenamed("MARCA_DATA", "NOMBRE_CAMPANA")
            Data_Frame = Data_Frame.withColumnRenamed("RANGO DEUDA", "RANGO_DEUDA")
            Data_Frame = Data_Frame.withColumnRenamed("GRUPO RANGO DE DIAS", "RANGO_DE_DIAS_ASIGNADA")
            Data_Frame = Data_Frame.withColumnRenamed("FILTRO REFERENCIA", "FILTRO_REFERENCIA")
            Data_Frame = Data_Frame.withColumnRenamed("Filtro Demografico", "FILTRO_DEMOGRAFICOS")
            Data_Frame = Data_Frame.withColumnRenamed("CUSTOMER TYPE", "CUSTOMER_TYPE")
            Data_Frame = Data_Frame.withColumnRenamed("DIAS ASIGNADA", "DIAS_EN_BASE")
            Data_Frame = Data_Frame.withColumnRenamed("RANGO DE DIAS", "DIAS_ASIGNADA")
            Data_Frame = Data_Frame.withColumnRenamed("DIAS RETIRADA", "DIAS_RETIRADA")
                        
            Data_Frame = Data_Frame.withColumn("CANTIDAD_DEMOGRAFICOS", 
                                    (col("NumMovil") + col("NumFijo") + col("NumEmail")).cast(IntegerType()))
            
            Data_Frame = Data_Frame.withColumnRenamed("NumMovil", "CANTIDAD_MOVILES")
            Data_Frame = Data_Frame.withColumnRenamed("NumFijo", "CANTIDAD_FIJOS")
            Data_Frame = Data_Frame.withColumnRenamed("NumEmail", "CANTIDAD_EMAILS")
            
            Data_Frame = Data_Frame.withColumn("RANGO_DE_DIAS_RETIRADA", 
                when((col("DIAS_RETIRADA") >= 1) & (col("DIAS_RETIRADA") <= 4), "1 Entre 1 a 4 dias")
                .when((col("DIAS_RETIRADA") >= 5) & (col("DIAS_RETIRADA") <= 8), "2 Entre 5 a 8 dias")
                .when((col("DIAS_RETIRADA") >= 9) & (col("DIAS_RETIRADA") <= 12), "3 Entre 9 a 12 dias")
                .when((col("DIAS_RETIRADA") >= 13) & (col("DIAS_RETIRADA") <= 16), "4 Entre 13 a 16 dias")
                .when((col("DIAS_RETIRADA") >= 17) & (col("DIAS_RETIRADA") <= 20), "5 Entre 17 a 20 dias")
                .when((col("DIAS_RETIRADA") >= 21) & (col("DIAS_RETIRADA") <= 24), "6 Entre 21 a 24 dias")
                .when((col("DIAS_RETIRADA") >= 25) & (col("DIAS_RETIRADA") <= 28), "7 Entre 25 a 28 dias")
                .when((col("DIAS_RETIRADA") >= 29) & (col("DIAS_RETIRADA") <= 31), "8 Entre 29 a 31 dias")
                .otherwise("Fuera de rango"))  # Optional: handle cases outside
            
            Data_Frame = Data_Frame.withColumn("RANGO_DE_DIAS_EN_BASE", 
                when((col("DIAS_EN_BASE") >= 1) & (col("DIAS_EN_BASE") <= 4), "1 Entre 1 a 4 dias")
                .when((col("DIAS_EN_BASE") >= 5) & (col("DIAS_EN_BASE") <= 8), "2 Entre 5 a 8 dias")
                .when((col("DIAS_EN_BASE") >= 9) & (col("DIAS_EN_BASE") <= 12), "3 Entre 9 a 12 dias")
                .when((col("DIAS_EN_BASE") >= 13) & (col("DIAS_EN_BASE") <= 16), "4 Entre 13 a 16 dias")
                .when((col("DIAS_EN_BASE") >= 17) & (col("DIAS_EN_BASE") <= 20), "5 Entre 17 a 20 dias")
                .when((col("DIAS_EN_BASE") >= 21) & (col("DIAS_EN_BASE") <= 24), "6 Entre 21 a 24 dias")
                .when((col("DIAS_EN_BASE") >= 25) & (col("DIAS_EN_BASE") <= 28), "7 Entre 25 a 28 dias")
                .when((col("DIAS_EN_BASE") >= 29) & (col("DIAS_EN_BASE") <= 31), "8 Entre 29 a 31 dias")
                .otherwise("Fuera de rango"))  # Optional: handle cases outside
            
            # Separete the date columns into day, month, and year
            Data_Frame = calculate_mora(Data_Frame)
            
            Data_Frame = Data_Frame.withColumn("RANGO_DE_DIAS_MORA", 
                when((col("DIAS_MORA") <= 0) & (col("DIAS_MORA") >= -30), "0 Entre 0 y menos de 30 dias")
                .when((col("DIAS_MORA") >= 1) & (col("DIAS_MORA") <= 4), "1 Entre 1 a 4 dias")
                .when((col("DIAS_MORA") >= 5) & (col("DIAS_MORA") <= 8), "2 Entre 5 a 8 dias")
                .when((col("DIAS_MORA") >= 9) & (col("DIAS_MORA") <= 12), "3 Entre 9 a 12 dias")
                .when((col("DIAS_MORA") >= 13) & (col("DIAS_MORA") <= 16), "4 Entre 13 a 16 dias")
                .when((col("DIAS_MORA") >= 17) & (col("DIAS_MORA") <= 20), "5 Entre 17 a 20 dias")
                .when((col("DIAS_MORA") >= 21) & (col("DIAS_MORA") <= 24), "6 Entre 21 a 24 dias")
                .when((col("DIAS_MORA") >= 25) & (col("DIAS_MORA") <= 28), "7 Entre 25 a 28 dias")
                .when((col("DIAS_MORA") >= 29) & (col("DIAS_MORA") <= 31), "8 Entre 29 a 31 dias")
                .when((col("DIAS_MORA") >= 32) & (col("DIAS_MORA") <= 100), "9 Entre 32 a 100 dias")
                .when(col("DIAS_MORA").isNull(), "Nulo")
                .otherwise("Fuera de rango"))  # Optional: handle cases outside
       
            list_columns_science = [
                'NOMBRE_CAMPANA', 'CRM', 'MULTIPRODUCTO', 'GRUPO_MULTIPRODUCTO', 'MULTIMORA', 'GRUPO_MULTIMORA', 'MULTICUENTA', 'RANGO_DEUDA', 'RANGO CON DESCUENTO', 
                'TIPO_DE_DOCUMENTO', 'DEPARTAMENTO', 'RANGO_DE_DIAS_ASIGNADA',
                'RANGO_DE_DIAS_EN_BASE', 'RANGO_DE_DIAS_RETIRADA', 'RANGO_DE_DIAS_MORA', 'RANGO_DE_DIAS_PAGO',
                'FILTRO_REFERENCIA', 'FILTRO_DEMOGRAFICOS', 'CANTIDAD_DEMOGRAFICOS', 'CANTIDAD_MOVILES', 
                'CANTIDAD_FIJOS', 'CANTIDAD_EMAILS', 'FILTRO_REPORTE_GESTION', 'CANTIDAD_CONTACTO_DIRECTO', 'CANTIDAD_CONTACTO_INDIRECTO', 
                'CANTIDAD_NOCONTACTO', 'CUSTOMER_TYPE', 'DIAS_ASIGNADA', 'DIAS_EN_BASE', 'DIAS_RETIRADA', 'DIAS_MORA', 'DIAS_PAGO'
            ]
            
            Data_Frame = Data_Frame.withColumn("FILTRO_PAGOS_SIN_APLICAR", 
                when((col("RECUENTO PAGOS SIN APLICAR") >= 1), "Aplica")
                .otherwise(""))  # Optional: handle cases outside
            
            Data_Frame = Data_Frame.withColumn("FILTRO_PAGOS_NO_GESTION", 
                when((col("RECUENTO NO GESTION") >= 1), "Aplica")
                .otherwise(""))  # Optional: handle cases outside
            
            list_columns_add = [
                'ESTADO CUENTA', 'FILTRO DESCUENTO', 'FILTRO_PAGOS_SIN_APLICAR', 'FILTRO_PAGOS_NO_GESTION', 'EXCLUSION CUENTA', 'ESTADO RANKING', 'SERVICIOS RANKING',
                'RECUENTO PAGOS SIN APLICAR', 'RECUENTO NO GESTION', 'Toques por SMS', 'Toques por EMAIL', 'Toques por BOT',
                'Toques por IVR', 'GRUPO_TOQUES_TELEMATICA'
            ]
            
            columns_final = ['FECHA INGRESO DATA', 'FECHA RETIRO DATA', 'CUENTA', 'DOCUMENTO', 'NOMBRE']
            
            list_columns_delete = [
                'Dia_1', 'Dia_2', 'Dia_3', 'Dia_4', 'Dia_5', 'Dia_6', 'Dia_7', 'Dia_8', 'Dia_9', 'Dia_10',
                'Dia_11', 'Dia_12', 'Dia_13', 'Dia_14', 'Dia_15', 'Dia_16', 'Dia_17', 'Dia_18', 'Dia_19', 'Dia_20',
                'Dia_21', 'Dia_22', 'Dia_23', 'Dia_24', 'Dia_25', 'Dia_26', 'Dia_27', 'Dia_28', 'Dia_29', 'Dia_30', 'Dia_31',
                'phone1', 'phone2', 'phone3', 'phone4', 'phone5', 'phone6', 'phone7', 'phone8', 'phone9', 'phone10',
                'phone11', 'phone12', 'phone13', 'phone14', 'phone15', 'phone16', 'phone17', 'phone18', 'phone19', 'phone20',
                'email1', 'email2', 'email3', 'email4', 'email5'
            ]
            
            list_columns_exclude = [
                'VALOR DEUDA', 
                'FECHA VENCIMIENTO',
                'REFERENCIA', 'FECHA PAGO DE COLA', 'VALOR PAGO DE COLAS', 
                'PORCENTAJE', 'VALOR DESCUENTO', 
                'REFERENCIA DE COLAS',
                'department_column',
                'department_column_2'
            ]
            
            list_columns = list_columns_base + list_columns_science + list_columns_add  + columns_final + list_columns_delete
            
            # Group by MULTIMORA
            df_multimora = (
                Data_Frame.filter(col("MULTIMORA") == "Aplica")
                .groupBy("DOCUMENTO")
                .agg(
                    concat_ws(", ",
                        sort_array(collect_set("MARCA_ASIGNACION"))
                    ).alias("GRUPO_MULTIMORA")
                )
            )

            # Group by MULTIPRODUCTO
            df_multiproducto = (
                Data_Frame.filter(col("MULTIPRODUCTO") == "Aplica")
                .groupBy("DOCUMENTO")
                .agg(
                    concat_ws(", ",
                        sort_array(collect_set("CRM"))
                    ).alias("GRUPO_MULTIPRODUCTO")
                )
            )

            # Union with the original DataFrame
            Data_Frame = (
                Data_Frame.join(df_multimora, on="DOCUMENTO", how="left")
                .join(df_multiproducto, on="DOCUMENTO", how="left")
            )
            
            df_toques = (
                Data_Frame
                .withColumn("CANAL_SMS",   when(col("Toques por SMS")   > 0, "SMS"))
                .withColumn("CANAL_EMAIL", when(col("Toques por EMAIL") > 0, "EMAIL"))
                .withColumn("CANAL_BOT",   when(col("Toques por BOT")   > 0, "BOT"))
                .withColumn("CANAL_IVR",   when(col("Toques por IVR")   > 0, "IVR"))
            )
            
            df_canales = df_toques.select(
                "DOCUMENTO",
                "CANAL_SMS", "CANAL_EMAIL", "CANAL_BOT", "CANAL_IVR"
            ).selectExpr(
                "DOCUMENTO",
                "stack(4, CANAL_SMS, CANAL_EMAIL, CANAL_BOT, CANAL_IVR) as CANAL"
            ).filter(col("CANAL").isNotNull())
            
            df_grupo_toques = (
                df_canales.groupBy("DOCUMENTO")
                .agg(
                    concat_ws(", ", sort_array(collect_set("CANAL"))).alias("GRUPO_TOQUES_TELEMATICA")
                )
            )

            Data_Frame = Data_Frame.join(df_grupo_toques, on="DOCUMENTO", how="left")
            
            Data_Frame = Data_Frame.withColumnRenamed("FECHA ULT PAGO", "FECHA_ULT_PAGO")

            Data_Frame = Data_Frame.withColumn(
                "FECHA_ULT_PAGO_INT",
                when(
                    (col("FECHA_ULT_PAGO").isNull()) | (col("FECHA_ULT_PAGO") == 0),
                    lit(None).cast("int")
                ).otherwise(
                    floor(col("FECHA_ULT_PAGO").cast("double")).cast("int")
                )
            )
            
            Data_Frame = Data_Frame.withColumn(
                "FECHA_ULT_PAGO_DATE",
                when(
                    col("FECHA_ULT_PAGO_INT").isNotNull(),
                    expr("date_add(to_date('1899-12-30'), FECHA_ULT_PAGO_INT)")
                )
            )
 
            Data_Frame = Data_Frame.withColumn(
                "FECHA_INGRESO_NORM",
                regexp_replace(col("FECHA INGRESO DATA"), " ", "") 
            )

            Data_Frame = Data_Frame.withColumn("DIA", lpad(split(col("FECHA_INGRESO_NORM"), "/").getItem(0), 2, "0"))
            Data_Frame = Data_Frame.withColumn("MES", lpad(split(col("FECHA_INGRESO_NORM"), "/").getItem(1), 2, "0"))
            Data_Frame = Data_Frame.withColumn("ANIO", split(col("FECHA_INGRESO_NORM"), "/").getItem(2))

            Data_Frame = Data_Frame.withColumn(
                "FECHA_INGRESO_DATE",
                to_date(expr("concat(DIA, '/', MES, '/', ANIO)"), "dd/MM/yyyy")
            )

            Data_Frame = Data_Frame.withColumn(
                "DIAS_PAGO",
                datediff(col("FECHA_ULT_PAGO_DATE"), col("FECHA_INGRESO_DATE"))
            )
            
            Data_Frame.select("DIAS_PAGO").distinct().show(30, truncate=False)
            
            Data_Frame = Data_Frame.withColumn(
                "RANGO_DE_DIAS_PAGO",
                when((col("DIAS_PAGO") <= 0) & (col("DIAS_PAGO") >= -30), "0 Entre 0 y menos de 30 días")
                .when((col("DIAS_PAGO") >= 1) & (col("DIAS_PAGO") <= 4), "1 Entre 1 a 4 días")
                .when((col("DIAS_PAGO") >= 5) & (col("DIAS_PAGO") <= 8), "2 Entre 5 a 8 días")
                .when((col("DIAS_PAGO") >= 9) & (col("DIAS_PAGO") <= 12), "3 Entre 9 a 12 días")
                .when((col("DIAS_PAGO") >= 13) & (col("DIAS_PAGO") <= 16), "4 Entre 13 a 16 días")
                .when((col("DIAS_PAGO") >= 17) & (col("DIAS_PAGO") <= 20), "5 Entre 17 a 20 días")
                .when((col("DIAS_PAGO") >= 21) & (col("DIAS_PAGO") <= 24), "6 Entre 21 a 24 días")
                .when((col("DIAS_PAGO") >= 25) & (col("DIAS_PAGO") <= 28), "7 Entre 25 a 28 días")
                .when((col("DIAS_PAGO") >= 29) & (col("DIAS_PAGO") <= 31), "8 Entre 29 a 31 días")
                .when(col("DIAS_PAGO").isNull(), "Nulo")
                .otherwise("Fuera de rango")
            )
            
            # Step 1: Clean the column names
            cleaned_columns = [clean_column_name(col) for col in Data_Frame.columns]
            Data_Frame = Data_Frame.toDF(*cleaned_columns)
            valid_columns = [col for col in list_columns if col in Data_Frame.columns]
            Data_Frame = Data_Frame.select(valid_columns)
        
            print(f'First Columns: {Data_Frame.columns}')
        
            # Step 2: Create a dictionary mapping original names to normalized names for the first list
            Data_Frame = rename_columns_with_prefix(Data_Frame, list_columns_base, list_columns_science, list_columns_add, list_columns_delete)
            
            Data_Frame = Data_Frame.filter(~col("BG_fecha_ingreso").contains("Manual"))
            
            brands_list = ["0", "30"]
            #Data_Frame = Data_Frame.filter(col("BG_marca_asignacion").isin(brands_list))
            Data_Frame = Data_Frame.filter(col("BG_marca_asignacion") != "Castigo")
            
            Save_Data_Frame(Data_Frame, output_path, num_partitions, year_data, month_data)
            
        else:
            print("No data was merged.")

        return Data_Frame
    
    except Exception as e:
        
        print(f"Error in read_compilation_datasets: {e}")
        return None

def rename_columns_with_prefix(data_frame, bg_columns, sd_columns, add_columns, delete_columns):

    # Rename columns with the 'BG_' prefix
    for col in bg_columns:
        if col in data_frame.columns:
            data_frame = data_frame.withColumnRenamed(col, f"BG_{col.lower().replace(' ', '_')}")

    # Rename columns with a different naming convention
    for col in sd_columns:
        if col in data_frame.columns:
            data_frame = data_frame.withColumnRenamed(col, f"DS_VAR_{col.lower().replace(' ', '_')}")
            
    for col in add_columns:
        if col in data_frame.columns:
            data_frame = data_frame.withColumnRenamed(col, f"AD_{col.lower().replace(' ', '_')}")
    
    for col in delete_columns:
        if col in data_frame.columns:
            data_frame = data_frame.withColumnRenamed(col, f"FLT_{col.lower().replace(' ', '_')}")
            
    return data_frame

def calculate_mora(df):
    try:
        df = df.withColumn("fecha_ing_raw", trim(col("FECHA INGRESO DATA"))) \
               .withColumn("fecha_ven_raw", trim(col("Fecha de Vencimiento")))

        df = df.withColumn("split_fecha_ing", split(col("fecha_ing_raw"), "/")) \
               .withColumn("split_fecha_ven", split(col("fecha_ven_raw"), "/"))

        df = df.withColumn("fecha_ing_formateada", 
                           concat_ws("/",
                                     lpad(col("split_fecha_ing")[0], 2, "0"),
                                     lpad(col("split_fecha_ing")[1], 2, "0"),
                                     col("split_fecha_ing")[2])) \
               .withColumn("fecha_ven_formateada", 
                           concat_ws("/",
                                     lpad(col("split_fecha_ven")[0], 2, "0"),
                                     lpad(col("split_fecha_ven")[1], 2, "0"),
                                     col("split_fecha_ven")[2]))

        df = df.withColumn("fecha_ing", to_date(col("fecha_ing_formateada"), "dd/MM/yyyy")) \
               .withColumn("fecha_ven", to_date(col("fecha_ven_formateada"), "dd/MM/yyyy"))
        
        df = df.withColumn(
            "DIAS_MORA",
            when(
                (length(trim(col("fecha_ing"))) > 3) & 
                (length(trim(col("fecha_ven"))) > 3),
                datediff(col("fecha_ing"), col("fecha_ven"))
            ).otherwise(None)
        )

        return df

    except Exception as e:
        print("Error al calcular los días de mora:", str(e))
        return df
    
def RenameColumns(Data_Frame):
    
    Data_Frame = Data_Frame.withColumnRenamed("CUENTA NEXT", "CUENTA_NEXT")
    Data_Frame = Data_Frame.withColumnRenamed("CRM Origen", "CRM_ORIGEN")
    Data_Frame = Data_Frame.withColumnRenamed("TIPO DE DOCUMENTO", "TIPO_DE_DOCUMENTO")
    Data_Frame = Data_Frame.withColumnRenamed("Marca", "MARCA_ASIGNACION")
    Data_Frame = Data_Frame.withColumnRenamed("MARCA DATA", "MARCA_DATA")
    Data_Frame = Data_Frame.withColumnRenamed("SEGMENTO", "SEGMENTO_ASIGNACION")
    Data_Frame = Data_Frame.withColumnRenamed("'RANGO DEUDA'", "'RANGO_DEUDA'")
    Data_Frame = Data_Frame.withColumnRenamed("'GRUPO RANGO DE DIAS'", "'GRUPO_RANGO_DIAS_ASIGNADA'")
    Data_Frame = Data_Frame.withColumnRenamed("'RANGO DE DIAS'", "'RANGO_DE_DIAS_ASIGNADA'")
    Data_Frame = Data_Frame.withColumnRenamed("'RANGO CON DESCUENTO'", "'GRUPO_RANGO_CON_DESCUENTO'")
    Data_Frame = Data_Frame.withColumnRenamed("'NumMovil'", "'CANTIDAD_MOVILES'")
    Data_Frame = Data_Frame.withColumnRenamed("'NumFijo'", "'CANTIDAD_FIJOS'")
    Data_Frame = Data_Frame.withColumnRenamed("'NumEmail'", "'CANTIDAD_EMAILS'")
    Data_Frame = Data_Frame.withColumnRenamed("'VALOR DEUDA'", "'VALOR_DEUDA'")
    Data_Frame = Data_Frame.withColumnRenamed("'FECHA INGRESO DATA'", "'FECHA_INGRESO_DATA'")
    Data_Frame = Data_Frame.withColumnRenamed("'FECHA RETIRO DATA'", "'FECHA_RETIRO_DATA'")
    Data_Frame = Data_Frame.withColumnRenamed("'DIAS ASIGNADA'", "'DIAS_ASIGNADA'")
    Data_Frame = Data_Frame.withColumnRenamed("'DIAS RETIRADA'", "'DIAS_RETIRADA'")
    Data_Frame = Data_Frame.withColumnRenamed("'Filtro Demografico'", "'FILTRO_DEMOGRAFICO'")
    Data_Frame = Data_Frame.withColumnRenamed("'FILTRO REFERENCIA'", "'FILTRO_REFERENCIA'")    
    Data_Frame = Data_Frame.withColumnRenamed("'ESTADO CUENTA'", "'ESTADO_CUENTA'")    
    Data_Frame = Data_Frame.withColumnRenamed("'FILTRO DESCUENTO'", "'FILTRO_DESCUENTO'")
    
    return Data_Frame
            
def clean_column_name(col_name):
    
    # Normalize and remove unwanted characters
    col_name = unidecode.unidecode(col_name)  # Remove accents
    col_name = re.sub(r'[{}.,]', '', col_name)  # Remove specific symbols
    
    return col_name

def depto(data_frame, department_mapping, department_mapping_2):
    
    # Handle empty or null values in "Ciudad"
    data_frame = data_frame.withColumn("Ciudad", when((col("Ciudad") == "") | (col("Ciudad").isNull()), "VACIO")
                                        .otherwise(col("Ciudad")))
    # Convert "Ciudad" to uppercase and split
    data_frame = data_frame.withColumn("Ciudad", upper(split(col("Ciudad"), "/").getItem(0)))

    # Initialize a column for department names
    data_frame = data_frame.withColumn("department_column", col("Ciudad"))  # Default case
    data_frame = data_frame.withColumn("department_column_2", col("Ciudad"))  # Default case
    
    # Loop through the dictionary to check for substring matches
    for department_mapping_selected in department_mapping:
        # Create a mapping for each department
        for key, department in department_mapping_selected.items():
            data_frame = data_frame.withColumn("department_column", 
                                               when(col("Ciudad").contains(key), lit(department)).otherwise(col("department_column")))

    for department_mapping_selected in department_mapping_2:
        # Create a mapping for each department
        for key, department in department_mapping_selected.items():
            data_frame = data_frame.withColumn("department_column_2", 
                                               when(col("Ciudad").contains(key), lit(department))
                                               .otherwise(col("department_column_2")))
    
        # List of departments
        deptos = [
            "ARAUCA", "CASANARE", "CAUCA", "CHOCO", "N. DE SANTANDER",
            "GUAVIARE", "CESAR", "CORDOBA", "SANTANDER", "CAQUETA",
            "CALDAS", "SAN ANDRES", "MAGDALENA", "LA GUAJIRA", "RISARALDA",
            "QUINDIO", "SUCRE", "GUAINIA", "PUTUMAYO", "VAUPES",
            "ANTIOQUIA", "HUILA", "VICHADA", "ATLANTICO", "META",
            "AMAZONAS", "BOLIVAR", "CUNDINAMARCA", "BOYACA", "NARINO",
            "VALLE DEL CAUCA", "TOLIMA"
        ]

        # Update the DataFrame to assign department names
        data_frame = data_frame.withColumn(
            "DEPARTAMENTO", when(col("Ciudad") == "VACIO", lit("VACIO"))
            .when(col("department_column").isin(deptos), col("department_column"))
            .when(col("department_column_2").isin(deptos), col("department_column_2"))
            .otherwise(lit("NO IDENTIFICADO"))
        )
        
    return data_frame
    
# # Input parameters
# input_folder = r"D:\Cloud\OneDrive - Recupera SAS\Data Claro\Claro_Data_Lake"
# output_folder = r"C:/Users/juan_/Downloads/"
# num_partitions = 1
# year = "2025"
# month = "03"
# read_compilation_datasets(input_folder, output_folder, num_partitions, month, year)

# Input parameters
input_folder = r"D:\Cloud\OneDrive - Recupera SAS\Data Claro\Claro_Data_Lake"
output_folder = r"C:/Users/juan_/Downloads/"
num_partitions = 1
start_year, start_month = 2024, 7
end_year, end_month     = 2025, 5
year, month = start_year, start_month

while (year < end_year) or (year == end_year and month <= end_month):
    month_str = str(month).zfill(2)
    year_str  = str(year)
    print(f"Procesando {month_str}/{year_str}...")
    read_compilation_datasets(input_folder, output_folder, num_partitions, month_str, year_str)
    if month == 12:
        month = 1
        year += 1
    else:
        month += 1
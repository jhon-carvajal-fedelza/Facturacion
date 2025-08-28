import json
import qrcode
import textwrap
from num2words import num2words
from datetime import datetime
from zoneinfo import ZoneInfo
from reportlab.lib.utils import ImageReader
from qrcode.image.pure import PymagingImage 
from reportlab.pdfbase.pdfmetrics import stringWidth
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, PageTemplate, Frame, BaseDocTemplate, Paragraph
from reportlab.lib import colors
from reportlab.pdfgen import canvas
from reportlab.platypus.flowables import Spacer
from reportlab.lib.colors import HexColor
from reportlab.lib.units import mm
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import PageBreak
import boto3
import io
import time
from reportlab.platypus import SimpleDocTemplate
from decimal import Decimal, ROUND_HALF_UP

start_time = time.perf_counter()
TEMPLATE_PATH = "D:\\ProyectoFacturacionElectronica\\Python\\PDF-V1\\PDF-V1\\"
#TEMPLATE_PATH = "D:\\DIAN\\PDF-V1\\"

REGION = 'us-east-1'
URL_SQS_SENDEMAIL = 'https://sqs.us-east-1.amazonaws.com/381491847136/'

S3_BUCKET_IMAGES = "images.fedelza"
s3 = boto3.client("s3")

# Configurar el cliente de SQS
sqs = boto3.client('sqs', region_name=REGION)

# Inicializar el cliente de DynamoDB
dynamodb = boto3.resource('dynamodb', region_name=REGION)

page_width, page_height = 612, 792
fill_color = HexColor("#404040")
gris_claro = HexColor("#EDEDED") #Gris claro
gris_oscuro = HexColor("#BDBCBC") #gris oscuro
radius = 6
pruebas = False
ruta_salida = "D:\DIAN\PruebaNotas.pdf"
#ruta_salida = "D:\\ProyectoFacturacionElectronica\\Images\\PruebaNotas.pdf" 

data_ant_fv = {
  "Records": [
    {
      "body": "{\"numFac\": \"SETP990000001\", \"attrUuid\": \"CUFE\", \"trm\": \"4200.10\", \"currency\": \"USD\", \"documentTypeAbbreviation\": \"fv\", \"orderReference\": \"referencia\", \"QR\": \"NumFac:SETP990000001 FecFac:2025-02-06 HorFac:06:40:00-05:00 NitFac:901188189 DocAdq:901366540 ValFac:60817.00 ValIva:01 ValOtroIm:0.00 ValTolFac:62350.00 CUFE:58a59d324567960fc79d3867e2183d421c1796604a29a4800e629cd596b8585868502e5b18497e9d665850c183f78d9e QRCode:https:\/\/catalogo-vpfe.dian.gov.co\/User\/SearchDocument?DocumentKey=58a59d324567960fc79d3867e2183d421c1796604a29a4800e629cd596b8585868502e5b18497e9d665850c183f78d9e\", \"CUFE-CUDE\": \"58a59d324567960fc79d3867e2183d421c1796604a29a4800e629cd596b8585868502e5b18497e9d665850c183f78d9e\", \"valTotFac\": \"62350.00\", \"fileName\": \"090118818912325SETP990000001\", \"filePath\": \"2025\/02\/10\/Invoice\/SETP990000001\", \"issueDate\": \"2025-02-06\", \"issueTime\": \"06:40:00-05:00\", \"subject\": \"901188189;FEDELZA SAS;SETP990000001;01;FEDELZA SAS;\", \"dueDate\": \"2025-02-16\", \"invoiceAuthorization\": \"18760000001\", \"authorizationStartPeriod\": \"2019-01-19\", \"authorizationEndPeriod\": \"2030-01-19\", \"authorizationInvoicesPrefix\": \"SETP\", \"authorizationInvoicesFrom\": \"990000000\", \"authorizationInvoicesTo\": \"995000000\", \"seller\": \"\", \"paymentId\": \"ZZZ\", \"totalInWords\": \"SESENTA Y DOS MIL TRECIENTOS CINCUENTA PESOS SESENTA Y DOS MIL TRECIENTOS CINCUENTA PESOS\", \"concept\": \"Prueba de texto para el campo de concepto, Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Risus nullam eget felis eget nunc lobortis mattis.\", \"supplierRegistrationName\": \"FEDELZA SAS\", \"supplierId\": \"901188189\", \"supplierIndustryClassificationCode\": \"FEDELZA SAS\", \"supplierAddressLine\": \"CL 84 A 47 50 GL 10 BG 16 17 DIRECCION MAS LARGA\", \"supplierCityName\": \"CARTAGENA\", \"supplierCountrySubentity\": \"BOLIVAR\", \"supplierTelephone\": \"4446925\", \"supplierElectronicMail\": \"contabilidad@lhm.com.co\", \"customerRegistrationName\": \"5619\", \"customerName\": \"CHRISNAIS INVESTMENTS SAS\", \"customerId\": \"901366540\", \"customerAddressLine\": \"CENTRO CL SEGUNDA DE BADILLO 36 50\", \"customerCityName\": \"CARTAGENA\", \"customerCountrySubentity\": \"BOLIVAR\", \"customerTelephone\": \"6781538\", \"customerElectronicMail\": \"CANCHACEVICHERIA@GMAIL.COM\", \"customerNeighborhood\": \"\", \"paymentMeansCode\": \"Efectivo - Tarjeta débito - Tarjeta crédito - Consignación bancaria\", \"expires\": \"2025-03-23\", \"term\": \"Term\", \"purchaceOrder\": \"Orden compra\", \"lineExtensionAmount\": \"60817.00\", \"allowanceTotalAmount\": \"0.00\", \"advanceAmount\": \"\", \"giftAmount\": \"\", \"taxExclusiveAmount\": \"8067.00\", \"payableAmount\": \"62350.34\", \"taxAmount\": \"\", \"impoconsumoAmount\": \"\", \"bagTaxAmount\": \"\", \"taxIva\": \"\", \"taxImpoconsumo\": \"1234678\", \"taxImpuestoBolsa\": \"1458754\", \"taxObsequio\": \"748549\", \"taxAnticipo\": \"45755\", \"taxTotal\": \"748596\", \"taxScheme\": \"78541\", \"taxableAmount\": \"53563\", \"impTaxAmount\": \"\", \"items\": 6, \"lines\": [{\"id\": \"01\", \"description\": \"CILANTRO\", \"unitOfMeasure\": \"\", \"quantity\": \"0.400\", \"unitPrice\": \"11500.00\", \"discountPercentage\": \"\", \"vatPercentage\": \"\", \"consumptionTax\": \"\", \"amount\": \"4600.00\"}, {\"id\": \"023456\", \"description\": \"NARANJA VALENCIA\", \"unitOfMeasure\": \"\", \"quantity\": \"1.400\", \"unitPrice\": \"3100.00\", \"discountPercentage\": \"\", \"vatPercentage\": \"\", \"consumptionTax\": \"\", \"amount\": \"4340.00\"}, {\"id\": \"03\", \"description\": \"yerbabuena fresca x kilo para probar el sato de linea en la columna de descripcion\", \"unitOfMeasure\": \"\", \"quantity\": \"2.000\", \"unitPrice\": \"4033.50\", \"discountPercentage\": \"\", \"vatPercentage\": \"\", \"consumptionTax\": \"\", \"amount\": \"8067.00\"}, {\"id\": \"04\", \"description\": \"TOMATE CHONTO\", \"unitOfMeasure\": \"\", \"quantity\": \"3.000\", \"unitPrice\": \"3600.00\", \"discountPercentage\": \"\", \"vatPercentage\": \"\", \"consumptionTax\": \"\", \"amount\": \"10800.00\"}, {\"id\": \"05\", \"description\": \"KIWI\", \"unitOfMeasure\": \"\", \"quantity\": \"0.600\", \"unitPrice\": \"20600.00\", \"discountPercentage\": \"\", \"vatPercentage\": \"\", \"consumptionTax\": \"\", \"amount\": \"12360.00\"}, {\"id\": \"06\", \"description\": \"LIMON TAHITI\", \"unitOfMeasure\": \"\", \"quantity\": \"7.000\", \"unitPrice\": \"2950.00\", \"discountPercentage\": \"\", \"vatPercentage\": \"\", \"consumptionTax\": \"\", \"amount\": \"20650.00\"}]}"
    }
  ]
}

data_new_fv = {
    "Records": [
        {
            "body": "{\"numFac\": \"SETP990000026\", \"QR\": \"NumFac:SETP990000026 FecFac:2025-02-25 HorFac:14:31:00-05:00 NitFac:901188189 DocAdq:222222222222 ValFac:143353.00 ValIva:01 ValOtroIm:25618.00 ValTolFac:173004.00 CUFE:7f4f41aa98cd0de323f43142db80feae8a8ec93f2474d292e76a06bf1e7df44ac7a91db92be67de125a9960d3bd253ad QRCode:https:\/\/catalogo-vpfe.dian.gov.co\/User\/SearchDocument?DocumentKey=7f4f41aa98cd0de323f43142db80feae8a8ec93f2474d292e76a06bf1e7df44ac7a91db92be67de125a9960d3bd253ad\", \"documentTypeAbbreviation\": \"fv\", \"attrUuid\": \"CUFE\", \"CUFE-CUDE\": \"7f4f41aa98cd0de323f43142db80feae8a8ec93f2474d292e76a06bf1e7df44ac7a91db92be67de125a9960d3bd253ad\", \"valTotFac\": \"173004.00\", \"fileName\": \"090118818912325SETP990000026\", \"filePath\": \"2025\/02\/26\/Invoice\/SETP990000026\", \"issueDate\": \"2025-02-25\", \"issueTime\": \"14:31:00-05:00\", \"subject\": \"901188189;FEDELZA SAS;SETP990000026;01;FEDELZA SAS;\", \"dueDate\": \"2025-02-25\", \"invoiceAuthorization\": \"18760000001\", \"authorizationStartPeriod\": \"2019-01-19\", \"authorizationEndPeriod\": \"2030-01-19\", \"authorizationInvoicesPrefix\": \"SETP\", \"authorizationInvoicesFrom\": \"990000000\", \"authorizationInvoicesTo\": \"995000000\", \"seller\": \"\", \"paymentId\": \"Contado\", \"totalInWords\": \"CIENTO SETENTA Y TRES MIL CUATRO PESOS MCTE.\", \"concept\": \"\", \"supplierRegistrationName\": \"FEDELZA SAS\", \"supplierId\": \"901188189\", \"supplierIndustryClassificationCode\": \"4711\", \"supplierAddressLine\": \"MEDELLIN\", \"supplierCityName\": \"MEDELLIN\", \"supplierCountrySubentity\": \"ANTIOQUIA\", \"supplierTelephone\": \"\", \"supplierElectronicMail\": \"jhoncarvajal88@gmail.com\", \"customerRegistrationName\": \"VENTAS DE CONTADO\", \"customerName\": \"VENTAS DE CONTADO\", \"customerId\": \"222222222222\", \"customerAddressLine\": \"CALLE 9 B SUR 79 A 221\", \"customerCityName\": \"MEDELLIN\", \"customerCountrySubentity\": \"ANTIOQUIA\", \"customerTelephone\": \"\", \"customerElectronicMail\": \"jhoncarvajal88@gmail.com\", \"customerNeighborhood\": \"\", \"paymentMeansCode\": \"10\", \"expires\": \"Vence\", \"term\": \"CONTADO\", \"purchaceOrder\": \"\", \"orderReference\": \"Orden de compra\", \"lineExtensionAmount\": \"143353.00\", \"allowanceTotalAmount\": \"0.00\", \"advanceAmount\": \"\", \"giftAmount\": \"giftAmount\", \"taxExclusiveAmount\": \"68581.00\", \"payableAmount\": \"173004.00\", \"taxAmount\": \"taxxAmount\", \"impoconsumoAmount\": \"impoconsumo\", \"bagTaxAmount\": \"bagtax\", \"taxScheme\": \"taxScheme\", \"taxableAmount\": \"taxableAmount\", \"impTaxAmount\": \"impTaxAmount\", \"taxIva\": \"texIva\", \"taxImpoconsumo\": \"taxImpoconsumo\", \"taxImpuestoBolsa\": \"taxImpuestoBolsa\", \"taxObsequio\": \"taxObsequio\", \"taxAnticipo\": \"taxAnticipo\", \"taxTotal\": \"taxTotal\", \"items\": 5, \"lines\": [{\"id\": \"015567\", \"description\": \"MARGARINA BLUE BAND X 110GR CON SAL\", \"unitOfMeasure\": \"UND\", \"quantity\": \"3.000\", \"unitPrice\": \"1596.66\", \"discountPercentage\": \"10.00%\", \"vatPercentage\": \"19.00%\", \"consumptionTax\": \"\", \"amount\": \"4310.98\"}, {\"id\": \"000094\", \"description\": \"CAFE AGUILA ROJA X 250GR\", \"unitOfMeasure\": \"UND\", \"quantity\": \"4.000\", \"unitPrice\": \"8285.50\", \"discountPercentage\": \"10.00%\", \"vatPercentage\": \"5.00%\", \"consumptionTax\": \"\", \"amount\": \"29828.00\"}, {\"id\": \"004926\", \"description\": \"AGUARDIENTE BOTELLA\", \"unitOfMeasure\": \"UND\", \"quantity\": \"2.000\", \"unitPrice\": \"19134.50\", \"discountPercentage\": \"10.00%\", \"vatPercentage\": \"5.00%\", \"consumptionTax\": \"25618.00\", \"amount\": \"60060.00\"}, {\"id\": \"008650\", \"description\": \"PLATANO X KG\", \"unitOfMeasure\": \"Kg\", \"quantity\": \"10.000\", \"unitPrice\": \"3400.00\", \"discountPercentage\": \"10.00%\", \"vatPercentage\": \"\", \"consumptionTax\": \"\", \"amount\": \"30600.00\"}, {\"id\": \"003501\", \"description\": \"ARROZ CARIBE X 1000GR\", \"unitOfMeasure\": \"UND\", \"quantity\": \"12.000\", \"unitPrice\": \"4090.00\", \"discountPercentage\": \"10.00%\", \"vatPercentage\": \"\", \"consumptionTax\": \"\", \"amount\": \"44172.00\"}], \"tax\": [{\"tax_category\": \"01\", \"tax_percentage\": \"5.00\", \"tax_amount\": 3214.0, \"taxable_amount\": \"64270.00\"}, {\"tax_category\": \"01\", \"tax_percentage\": \"19.00\", \"tax_amount\": 819.0, \"taxable_amount\": \"4311.00\"}]}"
        }
    ]
}

data_ant_notas = {
  "Records": [
    {
      "body": "{\"noteType\":\"Tipo Nota\",\"invoiceReferenceId\": \"SETP990000000\",\"invoiceReferenceDate\": \"2025-03-11\",\"invoiceUuid\": \"7f4f41aa98cd0de323f43142db80feae8a8ec93f2474d292e76a06bf1e7df44ac7a91db92be67de125a9960d3bd253ad\",\"numFac\": \"SETP990000001\", \"attrUuid\": \"CUDE\", \"documentTypeAbbreviation\": \"nc\", \"orderReference\": \"referencia\", \"QR\": \"NumFac:SETP990000001 FecFac:2025-02-06 HorFac:06:40:00-05:00 NitFac:901188189 DocAdq:901366540 ValFac:60817.00 ValIva:01 ValOtroIm:0.00 ValTolFac:62350.00 CUFE:58a59d324567960fc79d3867e2183d421c1796604a29a4800e629cd596b8585868502e5b18497e9d665850c183f78d9e QRCode:https:\/\/catalogo-vpfe.dian.gov.co\/User\/SearchDocument?DocumentKey=58a59d324567960fc79d3867e2183d421c1796604a29a4800e629cd596b8585868502e5b18497e9d665850c183f78d9e\", \"CUFE-CUDE\": \"58a59d324567960fc79d3867e2183d421c1796604a29a4800e629cd596b8585868502e5b18497e9d665850c183f78d9e\", \"valTotFac\": \"62350.00\", \"fileName\": \"090118818912325SETP990000001\", \"filePath\": \"2025\/02\/10\/Invoice\/SETP990000001\", \"issueDate\": \"2025-02-06\", \"issueTime\": \"06:40:00-05:00\", \"subject\": \"901188189;FEDELZA SAS;SETP990000001;01;FEDELZA SAS;\", \"dueDate\": \"2025-02-16\", \"invoiceAuthorization\": \"18760000001\", \"authorizationStartPeriod\": \"2019-01-19\", \"authorizationEndPeriod\": \"2030-01-19\", \"authorizationInvoicesPrefix\": \"SETP\", \"authorizationInvoicesFrom\": \"990000000\", \"authorizationInvoicesTo\": \"995000000\", \"seller\": \"Vendedor\", \"authorized\":\"Autorizado\", \"paymentId\": \"ZZZ\", \"totalInWords\": \"\", \"concept\": \"\", \"supplierRegistrationName\": \"FEDELZA SAS\", \"supplierId\": \"901188189\", \"supplierIndustryClassificationCode\": \"FEDELZA SAS\", \"supplierAddressLine\": \"CL 84 A 47 50 GL 10 BG 16 17\", \"supplierCityName\": \"CARTAGENA\", \"supplierCountrySubentity\": \"BOLIVAR\", \"supplierTelephone\": \"4446925\", \"supplierElectronicMail\": \"contabilidad@lhm.com.co\", \"customerRegistrationName\": \"5619\", \"customerName\": \"CHRISNAIS INVESTMENTS SAS\", \"customerId\": \"901366540\", \"customerAddressLine\": \"CENTRO CL SEGUNDA DE BADILLO 36 50\", \"customerCityName\": \"CARTAGENA\", \"customerCountrySubentity\": \"BOLIVAR\", \"customerTelephone\": \"6781538\", \"customerElectronicMail\": \"CANCHACEVICHERIA@GMAIL.COM\", \"customerNeighborhood\": \"\", \"paymentMeansCode\": \"ZZZ\", \"expires\": \"\", \"term\": \"term\", \"purchaceOrder\": \"Orden compra\", \"lineExtensionAmount\": \"60817.00\", \"allowanceTotalAmount\": \"0.00\", \"advanceAmount\": \"\", \"giftAmount\": \"\", \"taxExclusiveAmount\": \"8067.00\", \"payableAmount\": \"62350.00\", \"taxAmount\": \"\", \"impoconsumoAmount\": \"\", \"bagTaxAmount\": \"\", \"taxIva\": \"\", \"taxImpoconsumo\": \"\", \"taxImpuestoBolsa\": \"\", \"taxObsequio\": \"\", \"taxAnticipo\": \"\", \"taxTotal\": \"\", \"taxScheme\": \"\", \"taxableAmount\": \"\", \"impTaxAmount\": \"\", \"items\": 6, \"lines\": [{\"id\": \"01\", \"description\": \"CILANTRO\", \"mark\": \"marca\", \"unitOfMeasure\": \"\", \"quantity\": \"0.400\", \"unitPrice\": \"11500.00\", \"discountPercentage\": \"\", \"vatPercentage\": \"\", \"consumptionTax\": \"\", \"amount\": \"4600.00\"}], \"tax\": [{\"tax_category\": \"01\", \"tax_percentage\": \"5.00\", \"tax_amount\": 3214.0, \"taxable_amount\": \"64270.00\"}, {\"tax_category\": \"01\", \"tax_percentage\": \"19.00\", \"tax_amount\": 819.0, \"taxable_amount\": \"4311.00\"}]}"
    }
  ]
}

def convert_to_dollars(trm,value):
    """ Convierte un valor de pesos a dólares usando TRM sin perder precisión """
    if value == "" or value == None or value == 0:
        return ""
    trm = Decimal(trm)
    value = Decimal(value)
    dollar_value = (value / trm).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return str(dollar_value)

def send_sqs(url_sqs:str,message:list)->None:
    """
    Esta función realiza el envio a las colas de SQS.

    Args:
        url_sqs (str): Url de la cola SQS en AWS
        messages (list): Lista con los mensajes (Maximo 10)

    Returns:
        dict: Nombre de la campaña
    """

    try:
        response = sqs.send_message(
            QueueUrl=url_sqs,
            MessageBody=message
        )
        print(response)

    except Exception as e:
        print(e)

def numero_a_letras_con_centavos(numero, sufijo):
    parte_entera = int(numero)
    centavos = int(round((numero - parte_entera) * 100))  # Obtiene los centavos
    
    texto = num2words(parte_entera, lang='es') + f" {sufijo}"
    
    if centavos > 0:
        texto += f" con {num2words(centavos, lang='es')} centavos"
    
    return texto

def save_document(bucket_name,path,file_type,input_file,file_name,prefix):
    """
    Guarda archivos en S3 segun un path, prefijo y nombre entregado.

    Args:
        *args: Una cantidad variable de argumentos que serán concatenados.

    Returns:
        str: Url del objeto en S3.
    """
    try:
        #Revisar: ContentType='application/xml'
        name = f"{prefix}{file_name}.{file_type}"
        key = f'{path}/{file_type}/{name}'             
        s3.put_object(Bucket=bucket_name, Key=key, Body=input_file)
        return str('https://'+ bucket_name +'.s3.amazonaws.com/' + name)
    except Exception as ex:
        raise Exception(f'Problemas al guardar el documento {name}, error: {str(ex)}')

def create_rounded_rect(c, x, y, width, height, fill=0, stroke=1):
    y = page_height - y - height
    c.roundRect(x, y, width, height, radius, stroke=1, fill=fill)

def create_top_rounded_rectangle(c, x, y, width, height, radius=10):
    radius = min(radius, height/2, width/2)
    
    c.setFillColor(HexColor("#ABA9A9"))
    c.setStrokeColor(fill_color)
    c.setLineWidth(1)
    
    p = c.beginPath()
    p.moveTo(x, y)
    p.lineTo(x, y + height - radius)
    p.arcTo(x, y + height - 2*radius, x + 2*radius, y + height, 180, -90)
    p.lineTo(x + width - radius, y + height)
    p.arcTo(x + width - 2*radius, y + height - 2*radius, x + width, y + height, 90, -90)
    p.lineTo(x + width, y)
    p.close()
    
    # Dibujar la forma
    c.drawPath(p, fill=1, stroke=1)
    c.setFillColor(colors.black)
    c.setStrokeColor(colors.black)

class PageNumCanvas(canvas.Canvas):
    def __init__(self, *args, **kwargs):
        """Constructor"""
        canvas.Canvas.__init__(self, *args, **kwargs)
        self.pages = []
        
    #----------------------------------------------------------------------
    def showPage(self):
        """
        On a page break, add information to the list
        """
        self.pages.append(dict(self.__dict__))
        self._startPage()
        
    #----------------------------------------------------------------------
    def save(self):
        """
        Add the page number to each page (page x of y)
        """
        page_count = len(self.pages)
        for page in self.pages:
            self.__dict__.update(page)
            self.draw_page_number(page_count)
            canvas.Canvas.showPage(self)
            
        canvas.Canvas.save(self)
        
    #----------------------------------------------------------------------
    def draw_page_number(self, page_count):
        """
        Add the page number
        """
        text = "Página %s de %s" % (self._pageNumber, page_count)
        self.setFont("Helvetica", 10)
        self.drawRightString(590, 20, text)

def draw_resized_text(c, text, x, y, max_width, font_name="Helvetica", max_font_size=12, min_font_size=5):
    c.saveState()
    font_size = max_font_size

    while font_size >= min_font_size:
        text_width = stringWidth(text, font_name, font_size)
        if text_width <= max_width:
            break  # Si el texto cabe, usamos este tamaño
        font_size -= 1  # Reducimos el tamaño de la fuente

    c.setFont(font_name, font_size)
    c.drawCentredString(x, y, text)  # Dibujamos el texto con el tamaño ajustado
    c.restoreState()

def make_header_invoice(data,dollars_invoice=False):
    def header(c, doc):
        """
        Función para dibujar el encabezado en cada página.
        """
        currency = data.get("currency") or ""
        trm = data.get("trm") or ""
        #Frame de texto "Son"
        frame2 = Frame(30, 176, 308, 38, showBoundary=1)
        qr_string = data['QR'].replace(" ",'\n')
        #print(f"QR: {qr_string}")
        customer_tin = data["supplierId"]


        # 1️⃣ Generar el código QR en memoria
        qr1 = qrcode.make(qr_string, image_factory=PymagingImage)

        # 2️⃣ Guardar el QR en un buffer en lugar de un archivo
        qr_buffer1 = io.BytesIO()
        qr1.save(qr_buffer1)  # Guardar en memoria como PNG
        qr_buffer1.seek(0)

        # 3️⃣ Convertir el buffer a un objeto compatible con ReportLab
        qr_img1 = ImageReader(qr_buffer1)
        #90 x 90 es un tamaño adecuado
        c.drawImage(qr_img1, 300, 596, width=80, height=80)
        #QR en la parte de abajo
        #c.drawImage(qr_img1, 19, 4, width=80, height=80) 


        #############IMAGEN LOGO EMPRESA################
        if pruebas:
            #image_logo_path = f"D:/ProyectoFacturacionElectronica/Images/{customer_tin}.png"
            image_logo_path = f"D:/DIAN/{customer_tin}.png"

            # Abrir la imagen desde el disco
            with open(image_logo_path, "rb") as image_file:
                image_logo_stream = io.BytesIO(image_file.read())
        else:
            image_logo_key = f"invoices/{customer_tin}.png"
            image_logo_stream = io.BytesIO()
            s3.download_fileobj(S3_BUCKET_IMAGES, image_logo_key, image_logo_stream)
        image_reader = ImageReader(image_logo_stream)
        
        align = "center"  # Cambia a "center" para centrar
        max_height = 90
        max_width = 130
        y_base = 750  # Altura máxima donde empieza el área
        
        # Obtener dimensiones originales de la imagen
        img_width, img_height = image_reader.getSize()
        scale = min(max_width / img_width, max_height / img_height)
        
        # Calcular nuevo tamaño ajustado
        new_width = img_width * scale
        new_height = img_height * scale
        
        # Calcular el espacio sobrante
        extra_space = max_height - new_height                

        if align == "top":
            y = y_base  # Alineada arriba
        elif align == "center":
            y = y_base - (extra_space / 2)  # Centrada
        else:
            y = y_base - extra_space  # Alineada abajo (por defecto)
        
        c.drawImage(image_reader, 22.68, y, width=new_width, height=new_height, mask='auto')
        #c.rect(22.68, (680), 141.73, 107.87)
        #############IMAGEN LOGO EMPRESA################
        
        supplier_address_line = data.get('supplierAddressLine') or ""
        supplier_industry_classificatio_code = data.get('supplierIndustryClassificationCode') or ""
        supplier_country_subentity = data.get('supplierCountrySubentity') or ""
        supplier_telephone = data.get('supplierTelephone') or ""
        supplier_city_name = data.get('supplierCityName') or ""
        supplier_electronic_mail = data.get('supplierElectronicMail') or ""
        
        customer_address_line = data.get('customerAddressLine') or ""
        customer_city_name = data.get('customerCityName') or ""
        customer_neighborhood = data.get('customerNeighborhood') or ""
        customer_telephone = data.get('customerTelephone') or ""
        payable_amount = data.get('payableAmount') or ""
        
        term = data.get('term')
        orden_reference = data.get('orderReference') or ""
        currency = data.get('currency') or ""
        trm = data.get('trm') or ""
        
        if dollars_invoice:
            sufijo = "dólares"
            payable_amount = convert_to_dollars(trm,payable_amount)
        else:
            sufijo = "pesos"
        total_in_words = numero_a_letras_con_centavos(float(payable_amount), sufijo)
        
        c.setStrokeColor(fill_color)  # Color del borde
        #c.setLineWidth(1)  # Grosor del borde  
        
        #Informacion de la factura
        c.setFont("Helvetica-Bold", 13)
        c.drawString(380, 765, "FACTURA ELECTRÓNICA")
        c.drawString(380, 750, "DE VENTA N°")
        #Numero factura
        c.setFont("Helvetica-Bold", 12)
        c.drawString(475, 750, data['numFac'])

        #Fecha y hora generación y expedición
        c.setFont("Helvetica", 9)
        c.drawString(380, 735, "Fecha y Hora de Generación:")
        c.drawString(380, 725, "Fecha y Hora de Expedición:")
        c.setFont("Helvetica", 8)
        c.drawString(500, 735, f"{data['issueDate']} {data['issueTime']}")
        c.drawString(500, 725, data['dueDate'])

        
        #Resolucion
        c.drawString(380, 710, f"Número de Autorización: {data['invoiceAuthorization']}")
        c.drawString(380, 700, f"Fecha Expedición {data['authorizationStartPeriod']}")
        c.drawString(380, 690, f"Fecha Expiración {data['authorizationEndPeriod']}")
        c.drawString(380, 680, f"Rango Numeración: {data['authorizationInvoicesPrefix']}{data['authorizationInvoicesTo']} - {data['authorizationInvoicesPrefix']}{data['authorizationInvoicesTo']}")

        #Datos del cliente
        c.setFillColor(gris_claro)
        create_rounded_rect(c, 8*mm, 42*mm, 96*mm, 44*mm, 1)
        c.setFillColor(colors.black)
        c.setFont("Helvetica-Bold", 9)
        c.drawString(30, page_height - 135, "Cliente:")
        c.drawString(30, page_height - 146, "Establecimiento:")
        c.drawString(30, page_height - 157, "Identificación:")
        c.drawString(30, page_height - 168, "Teléfono:")
        
        c.drawString(30, page_height - 191, "Dirección:")
        c.drawString(30, page_height - 202, "Ciudad:")
        c.drawString(30, page_height - 213, "Vendedor:")
        c.drawString(30, page_height - 224, "Forma de pago:")
        c.drawString(30, page_height - 235, "Total ítems:")

        #Informacion del cliente
        c.setFont("Helvetica", 9)
        c.drawString(65, 657, data['customerRegistrationName'])
        c.drawString(104, 646, data['customerName'])
        c.drawString(93, 635, data['customerId'])
        c.drawString(73, 624, customer_telephone)
        
        c.drawString(74, 600, customer_address_line)
        c.drawString(65, 590, customer_city_name)
        c.drawString(77, 579, data['seller'])
        c.drawString(100, 568, data['paymentId'])
        c.drawString(84, 557, str(data['items']))

        c.setFillColor(gris_claro)
        #Medios de pago
        create_rounded_rect(c, 106*mm, 73*mm, 101*mm, 13*mm, 1)
        #vence plazo y orden de compra
        create_rounded_rect(c, 386, 120, 201, 80, 1)
        c.setFillColor(colors.black)
        c.drawString(305, 573, "Medios de Pago")
        

        c.setFillColor(colors.black)
        data3 = [["Vence",""],["Plazo",""],["Orden de Compra",""],["Moneda",""],["TRM",""]]
        col_widths3 = [85, 114]
        table3 = Table(data3, colWidths=col_widths3, rowHeights=15.3, splitByRow=1)
        style3 = TableStyle([
            # Bordes internos (solo dentro de la tabla)
            ('LINEBELOW', (0, 0), (-1, -2), 1, fill_color),
            ('LINEAFTER', (0, 0), (-2, -1), 1, fill_color),
            # Centrar el texto horizontalmente
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),  # Alineación vertical al centro
            # Texto en negrita para la fila del encabezado
            ('FONTNAME', (0, 0), (-1, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 8.5),
        ])
        table3.setStyle(style3)
        x_position3 = 387
        y_position3 = 594
        table3.wrapOn(c, 0, 0)
        table3.drawOn(c, x_position3, y_position3)

        #Medio de pago    
        c.setFont("Helvetica", 8)
        c.drawString(305, 558, data['paymentMeansCode'])   #Medio de pago
        c.drawString(475, 658, data['expires'])            #Vence
        c.drawString(475, 644, term)                       #Plazo
        c.drawString(475, 630, orden_reference)            #Orden de compra
        c.drawString(475, 614, currency)                   #Moneda
        c.drawString(475, 598, trm)                        #TRM

        '''
        c.setFillColor(colors.black)
        data3 = [["Medio de Pago","Vence","Plazo","Orden de Compra"],["","","",""]]
        col_widths3 = [94, 63, 40, 88]
        table3 = Table(data3, colWidths=col_widths3, rowHeights=16.5, splitByRow=1)
        style3 = TableStyle([
            # Bordes internos (solo dentro de la tabla)
            ('LINEBELOW', (0, 0), (-1, -2), 1, fill_color),
            ('LINEAFTER', (0, 0), (-2, -1), 1, fill_color),
            # Centrar el texto horizontalmente
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),  # Alineación vertical al centro
            # Texto en negrita para la fila del encabezado
            ('FONTNAME', (0, 0), (-1, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
        ])
        table3.setStyle(style3)
        x_position3 = 301
        y_position3 = 550
        table3.wrapOn(c, 0, 0)
        table3.drawOn(c, x_position3, y_position3)
        '''

        #Tabla de items
        c.setFillColor(colors.black)
        create_rounded_rect(c, 8*mm, 88*mm, 199*mm, 108*mm)
        #Linea horizontal
        c.line(23, 522, 587, 522)
        data4 = [["Código", "Descripción", "U.M", "Cantidad", "V.Unitario", "Dcto", "IVA", "Imp.\nConsumo", "Valor Total"]]
        col_widths4 = [35,222,30,45,59,32,30,48,60]
        table4 = Table(data4, colWidths=col_widths4, rowHeights=16.5, splitByRow=1)
        style4 = TableStyle([
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('FONTNAME', (0, 0), (-1, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 8.6),
        ])
        table4.setStyle(style4)
        x_position4 = 25
        y_position4 = 524
        table4.wrapOn(c, 0, 0)
        table4.drawOn(c, x_position4, y_position4)
        
        #Tabla vacia
        c.setStrokeColor(fill_color)
        data5 = [["","","","","","","","",""]]
        table5 = Table(data5, colWidths=col_widths4, rowHeights=305, splitByRow=1)
        style5= TableStyle([
            ('LINEBELOW', (0, 0), (-1, -2), 1, fill_color),
            ('LINEAFTER', (0, 0), (-2, -1), 1, fill_color),
            ('FONTNAME', (0, 0), (-1, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 8),
        ])
        table5.setStyle(style5)
        x_position5 = 25
        y_position5 = 237
        table5.wrapOn(c, 0, 0)
        table5.drawOn(c, x_position5, y_position5)

        c.setFont("Helvetica-Bold", 10)        
              

        #Totales
        create_rounded_rect(c, 129*mm, 198*mm, 78*mm, 30*mm)
        data_t = [["Total Bruto:",""],["Descuento:",""],["I.V.A:",""],["Impoconsumo:",""],["Imp. Bolsa:",""],["Obsequio:",""],["Anticipo:",""],["Total:",""]]
        col_widths = [80, 140]
        table = Table(data_t,colWidths=col_widths,rowHeights=10.5)    
        style = TableStyle([
            ('LINEBELOW', (0, 0), (-1, -2), 1, fill_color),  
            ('LINEAFTER', (0, 0), (-2, -1), 1, fill_color),
            ('ALIGN', (0, 0), (0, -1), 'RIGHT'),
            ('ALIGN', (1, 0), (1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ])
        table.setStyle(style)
        x_position = 366
        y_position = 146
        table.wrapOn(c, 0, 0)
        table.drawOn(c, x_position, y_position)
        
        #Impuestos
        create_rounded_rect(c, 366, 229*mm, 78*mm, 20*mm)
        #Creo que este no se estaba pintando
        #create_top_rounded_rectangle(c, x=366, y=129, width=221, height=14, radius=6)
        data2 = [
            ["Impuestos", "", ""],
            ["Tipo de Impuestos", "Monto Base", "Total"],
            ["IVA:", "", ""],
            ["IVA:", "", ""],
        ]
        col_widths2 = [88, 64, 68]
        table2 = Table(data2, colWidths=col_widths2, rowHeights=14.5, splitByRow=1)
        style2 = TableStyle([
            ('LINEBELOW', (0, 0), (-1, -2), 1, fill_color),
            ('LINEAFTER', (0, 0), (-2, -1), 1, fill_color),
            ('BACKGROUND', (0, 1), (-1, 1), gris_claro),
            ('ALIGN', (0, 0), (0, 0), 'CENTER'),
            ('ALIGN', (1, 0), (1, -1), 'LEFT'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('FONTNAME', (0, 0), (-1, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('LEFTPADDING', (0, 0), (-1, -1), 5),
            ('RIGHTPADDING', (0, 0), (-1, -1), 5),
            ('SPAN', (0, 0), (2, 0)),
        ])
        table2.setStyle(style2)
        x_position2 = 366.5
        y_position2 = 87
        table2.wrapOn(c, 0, 0)
        table2.drawOn(c, x_position2, y_position2)
        
        c.saveState()
        
        
        
        

        '''
        seller = data.get('seller') or ""
        paymentId = data.get('paymentId') or ""
        items = data.get('items') or ""
        issueDate = data.get('issueDate') or ""
        '''

        #recuadro de imagen
        #c.rect(22.68, (680), 141.73, 107.87)
        
        #Informacion de la compañia
        c.setFont("Helvetica-Bold", 12)
        c.drawCentredString(260, 765, data['supplierRegistrationName'])
        c.setFont("Helvetica", 10)
        c.drawCentredString(260, 755, f"Nit {data['supplierId']}")
        c.drawCentredString(260, 745, f"Actividad Económica: {supplier_industry_classificatio_code}")
        max_width_supplier_data = 240 
        draw_resized_text(c, f"Dirección: {supplier_address_line}", 260, 735, max_width_supplier_data)
        #c.drawCentredString(260, 735, f"Dirección: {supplier_address_line}")
        c.drawCentredString(260, 725, f"{supplier_city_name}, {supplier_country_subentity}")
        c.drawCentredString(260, 715, f"Teléfono: {supplier_telephone}")
        c.drawCentredString(260, 705, f"E-mail: {supplier_electronic_mail}")
        c.drawCentredString(260, 695, f"Responsables de IVA")
        
        

        c.setFont("Helvetica", 10)
        if dollars_invoice:
            #convertir a dolares los valores totales
            tax_exclusive_amount = convert_to_dollars(trm, data['taxExclusiveAmount'])
            allowance_total_amount = convert_to_dollars(trm, data['allowanceTotalAmount'])
            tax_amount = convert_to_dollars(trm, data['taxAmount'])
            impoconsumo_amount = convert_to_dollars(trm, data['impoconsumoAmount'])
            bag_tax_amount = convert_to_dollars(trm, data['bagTaxAmount'])
            gift_amount = convert_to_dollars(trm, data['giftAmount'])
            tax_anticipo = convert_to_dollars(trm, data['taxAnticipo'])
            payable_amount = convert_to_dollars(trm, data['payableAmount'])
        else:
            tax_exclusive_amount = data['taxExclusiveAmount']
            allowance_total_amount = data['allowanceTotalAmount']
            tax_amount = data['taxAmount']
            impoconsumo_amount = data['impoconsumoAmount']
            bag_tax_amount = data['bagTaxAmount']
            gift_amount = data['giftAmount']
            tax_anticipo = data['taxAnticipo']
            payable_amount = data['payableAmount']
        
        #Totales
        c.drawRightString(582, 222, tax_exclusive_amount)      #Total bruto
        c.drawRightString(582, 211, allowance_total_amount)    #Sumatoria descuento
        c.drawRightString(582, 200, tax_amount)               #Sumatoria IVA
        c.drawRightString(582, 190, impoconsumo_amount)       #Sumatoria Impoconsumo
        c.drawRightString(582, 179, bag_tax_amount)
        c.drawRightString(582, 169, gift_amount)
        c.drawRightString(582, 158, tax_anticipo)
        c.setFont("Helvetica-Bold", 10)
        c.drawRightString(582, 148, payable_amount)
        c.setFont("Helvetica", 9.7)
        c.drawString(65, 60, data["CUFE-CUDE"])
        
        c.setFont("Helvetica-Bold", 10)        
        #Son
        para1 = []
        styles = getSampleStyleSheet()
        style = styles["Normal"]
        frame1 = Frame(
            x1=30,
            y1=18,
            width=310,
            height=150,
            leftPadding=0,
            rightPadding=0,
            topPadding=0,
            bottomPadding=0
        )
        data_concept = "<b>Concepto:</b> " + data['concept']
        para1.append(Paragraph(data_concept, style))
        frame1.addFromList(para1,c)
        
        para2 = []
        frame2 = Frame(
            x1=30,
            y1=128,
            width=310,
            height=100,
            leftPadding=0,
            rightPadding=0,
            topPadding=0,
            bottomPadding=0
        )
        data_concept = "<b>Son:</b> " + total_in_words
        para2.append(Paragraph(data_concept, style))
        frame2.addFromList(para2,c)
        create_rounded_rect(c, 8*mm, 198*mm, 114*mm, 20*mm)
        #c.drawString(30, 218, f"Son: {data['totalInWords']}")

        #Concepto
        create_rounded_rect(c, 8*mm, 219*mm, 114*mm, 30*mm)
        #c.drawString(30, 155, f"Concepto: {data['concept']}")        
        
        try:
            tax_percent_1 = data['tax'][0]['tax_percentage']
            tax_amount_1 = data['tax'][0]['tax_amount']
            taxable_amount_1 = data['tax'][0]['taxable_amount']
        except:
            tax_percent_1 = ""
            tax_amount_1 = ""
            taxable_amount_1 = ""
        try:
            tax_percent_2 = data['tax'][1]['tax_percentage']
            tax_amount_2 = data['tax'][1]['tax_amount']
            taxable_amount_2 = data['tax'][1]['taxable_amount']
        except:
            tax_percent_2 = ""
            tax_amount_2 = ""
            taxable_amount_2 = ""
               
        c.drawString(400, 104, f"{tax_percent_1}")
        c.drawString(400, 90, f"{tax_percent_2}")
        c.drawRightString(516, 104, taxable_amount_1)
        c.drawRightString(516, 90, taxable_amount_2)
        c.drawRightString(585, 104, str(tax_amount_1))
        c.drawRightString(585, 90, str(tax_amount_2))
        
        cufe_cude = data["attrUuid"]
        c.drawString(30, 60, f"{cufe_cude}:")  

        c.restoreState()
    return header

def make_header_notes(data,dollars_invoice=False):
    def header(c, doc):
        """
        Función para dibujar el encabezado en cada página.
        """
        currency = data.get("currency") or ""
        trm = data.get("trm") or ""
        #Frame de texto "Son"
        frame2 = Frame(30, 176, 308, 38, showBoundary=1)
        qr_string = data['QR'].replace(" ",'\\n')
        qr_string = data['QR'].replace(":",': ')
        customer_tin = data["supplierId"]


        # 1️⃣ Generar el código QR en memoria
        qr1 = qrcode.make(qr_string, image_factory=PymagingImage)

        # 2️⃣ Guardar el QR en un buffer en lugar de un archivo
        qr_buffer1 = io.BytesIO()
        qr1.save(qr_buffer1)  # Guardar en memoria como PNG
        qr_buffer1.seek(0)

        # 3️⃣ Convertir el buffer a un objeto compatible con ReportLab
        qr_img1 = ImageReader(qr_buffer1)
        #90 x 90 es un tamaño adecuado
        c.drawImage(qr_img1, 510, 690, width=80, height=80) 
        
        
        #############IMAGEN LOGO EMPRESA################
        if pruebas:
            #image_logo_path = f"D:/ProyectoFacturacionElectronica/Images/{customer_tin}.png"
            image_logo_path = f"D:/DIAN/{customer_tin}.png"

            # Abrir la imagen desde el disco
            with open(image_logo_path, "rb") as image_file:
                image_logo_stream = io.BytesIO(image_file.read())
        else:
            image_logo_key = f"invoices/{customer_tin}.png"
            image_logo_stream = io.BytesIO()
            s3.download_fileobj(S3_BUCKET_IMAGES, image_logo_key, image_logo_stream)
        image_reader = ImageReader(image_logo_stream)
        
        align = "center"  # Cambia a "center" para centrar
        max_height = 90
        max_width = 130
        y_base = 750  # Altura máxima donde empieza el área
        
        # Obtener dimensiones originales de la imagen
        img_width, img_height = image_reader.getSize()
        scale = min(max_width / img_width, max_height / img_height)
        
        # Calcular nuevo tamaño ajustado
        new_width = img_width * scale
        new_height = img_height * scale
        
        # Calcular el espacio sobrante
        extra_space = max_height - new_height                

        if align == "top":
            y = y_base  # Alineada arriba
        elif align == "center":
            y = y_base - (extra_space / 2)  # Centrada
        else:
            y = y_base - extra_space  # Alineada abajo (por defecto)
            
        c.drawImage(image_reader, 22.68, y, width=new_width, height=new_height, mask='auto')
        
        
        c.setStrokeColor(fill_color)  # Color del borde
        c.setLineWidth(1)  # Grosor del borde  
        
        #Informacion de la factura
        c.setFont("Helvetica-Bold", 12)
        c.drawString(350, 680, "No.")
        c.setFont("Helvetica", 10)
        c.drawString(350, 665, "Fecha y Hora de Expedición:")


        #Datos cliente
        create_rounded_rect(c, 8*mm, 152, 199*mm, 72)

        #Rectangulo Cufe factura
        create_rounded_rect(c, 8*mm, 80*mm, 199*mm, 7*mm)

        c.setFont("Helvetica-Bold", 8)
        c.drawString(30, 628, "CLIENTE")
        c.drawString(170, 628, "DOCUMENTO CLIENTE")
        c.drawString(376, 628, "DIRECCIÓN")
        c.drawString(30, 590, "TELÉFONO")
        c.drawString(170, 590, "FACTURA")
        c.drawString(344, 590, "TIPO DE NOTA:")
        c.drawString(30, 553, "CUFE FACTURA:")
        #Linea vertical antes de (TOTAL ÏTEMS)
        c.line(448, 545, 448, 565)
        c.drawString(450, 553, "TOTAL ÍTEMS:")
        c.line(23, 623, 587, 623)
        c.line(23, 602, 587, 602)
        c.line(23, 584, 587, 584)

        #Horizontal
        c.line(270, 593, 339, 593)    
        #Verticales
        c.line(270, 602, 270, 568)
        c.line(339, 602, 339, 568)
        c.line(293, 593, 293, 568)
        c.line(316, 593, 316, 568)

        c.setFont("Helvetica-Bold", 7)
        c.drawString(275, 595, "FECHA FACTURA")
        c.drawString(275, 586, "DÍA")
        c.drawString(297, 586, "MES")
        c.drawString(320, 586, "AÑO")

        #Linea vertical antes de (DOCUMENTO CLIENTE, FACTURA)
        c.line(165, 640, 165, 568)

        #Linea vertical antes de (DIRECCION)
        c.line(370, 640, 370, 602)
        
        #Tabla de items
        create_rounded_rect(c, 8*mm, 88*mm, 199*mm, 178.7*mm)
        #Linea horizontal
        c.line(23, 522, 587, 522)
        data4 = [["Código", "Detalle", "Marca", "Und.Medida", "Cantidad", "Valor Unitario", "% Dcto", "% IVA", "Imp.\nConsumo", "Valor Total"]]
        col_widths4 = [36,186,40,48,38,55,28,26,40,66]
        table4 = Table(data4, colWidths=col_widths4, rowHeights=16.5, splitByRow=1)
        style4 = TableStyle([
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('FONTNAME', (0, 0), (-1, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 8),
        ])
        table4.setStyle(style4)
        x_position4 = 25
        y_position4 = 524
        table4.wrapOn(c, 0, 0)
        table4.drawOn(c, x_position4, y_position4)
        
        #Tabla vacia
        c.setStrokeColor(fill_color)
        data5 = [["","","","","","","","",""]]
        table5 = Table(data5, colWidths=col_widths4, rowHeights=356, splitByRow=1)
        style5= TableStyle([
            ('LINEBELOW', (0, 0), (-1, -2), 1, fill_color),
            ('LINEAFTER', (0, 0), (-1, -1), 1, fill_color),
            ('FONTNAME', (0, 0), (-1, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 8),
        ])
        table5.setStyle(style5)
        x_position5 = 25
        y_position5 = 186
        table5.wrapOn(c, 0, 0)
        table5.drawOn(c, x_position5, y_position5)
        #Linea horizontal
        c.line(23, 186, 587, 186)
        #Linea horizontal
        c.line(23, 66, 587, 66)
        c.setFont("Helvetica-Bold", 9)        
        #Observaciones
        c.drawString(30, 176, "OBSERVACIONES:")

 
        #Impuestos

        data2 = [
            ["IMPUESTOS", "", ""],
            ["TIPO DE IMPUESTOS", "MONTO BASE", "TOTAL"],
            ["IVA:", "", ""],
            ["IVA:", "", ""],
        ]
        col_widths2 = [105, 65, 65]
        table2 = Table(data2, colWidths=col_widths2, rowHeights=15, splitByRow=1)
        style2 = TableStyle([
            ('GRID', (0, 0), (-1, -1), 1, fill_color), 
            # Fondo gris claro para el encabezado
            ('BACKGROUND', (0, 0), (-1, 0), colors.lightgrey),
            #('BACKGROUND', (0, 1), (-1, 1), gris_claro),
            ('ALIGN', (0, 0), (0, 0), 'CENTER'),
            ('ALIGN', (1, 0), (1, -1), 'LEFT'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('FONTNAME', (0, 0), (-1, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 8),
            ('SPAN', (0, 0), (2, 0)),
        ])
        table2.setStyle(style2)
        x_position2 = 352
        y_position2 = 36
        table2.wrapOn(c, 0, 0)
        table2.drawOn(c, x_position2, y_position2)


        #tabla totales
        data_totales = [
            ["SUBTOTAL:", ""],
            ["IVA:", ""],
            ["IMPOCONSUMO", ""],
            ["IMP. BOLSA", ""],
            ["DESCUENTO", ""],
            ["TOTAL", ""],
        ]
        col_widths_totales = [170,65]
        style_totales = TableStyle([
            # Bordes internos
            ('GRID', (0, 0), (-1, -1), 1, fill_color),        
            # Texto centrado horizontal y verticalmente
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('LEFTPADDING', (0, 0), (1, -1), 50),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            # Fuente en negrita para el encabezado
            ('FONTNAME', (0, 0), (-1, -1), 'Helvetica-Bold'),
            # Tamaño de fuente
            ('FONTSIZE', (0, 0), (-1, -1), 9),
        ])
        table_totales = Table(data_totales, colWidths=col_widths_totales, rowHeights=15, splitByRow=1)
        table_totales.setStyle(style_totales)
        x_position_totales = 352
        y_position_totales = 96
        table_totales.wrapOn(c, 0, 0)
        table_totales.drawOn(c, x_position_totales, y_position_totales)
        
        c.line(190, 66, 190, 36)
        c.drawString(30, 54, "Vendedor")
        c.drawString(196, 54, "Autorizado")
        
        
        c.saveState()
        supplier_address_line = data.get('supplierAddressLine') or ""
        supplier_industry_classificatio_code = data.get('supplierIndustryClassificationCode') or ""
        supplier_country_subentity = data.get('supplierCountrySubentity') or ""
        supplier_telephone = data.get('supplierTelephone') or ""
        supplier_city_name = data.get('supplierCityName') or ""
        supplier_electronic_mail = data.get('supplierElectronicMail') or ""
        
        customer_address_line = data.get('customerAddressLine') or ""
        customer_city_name = data.get('customerCityName') or ""
        customer_neighborhood = data.get('customerNeighborhood') or ""
        customer_telephone = data.get('customerTelephone') or ""
        payable_amount = data.get('payableAmount') or ""
        '''
        seller = data.get('seller') or ""
        paymentId = data.get('paymentId') or ""
        items = data.get('items') or ""
        issueDate = data.get('issueDate') or ""
        '''

        #VARIABLE
        #Numero factura
        c.setFont("Helvetica-Bold", 11)
        c.drawString(372, 680, data['numFac'])
        #Informacion de la compañia
        c.setFont("Helvetica-Bold", 10)
        c.drawString(25, 682, data['supplierRegistrationName'])
        c.setFont("Helvetica", 8)
        c.drawString(25, 672, f"Nit {data['supplierId']} - Actividad Económica: {supplier_industry_classificatio_code}")
        c.drawString(25, 663, f"Dirección: {supplier_address_line}, {supplier_country_subentity}.")
        c.drawString(25, 654, f"Teléfono: {supplier_telephone} - E-mail: {supplier_electronic_mail}")
        c.drawString(25, 645, "Responsables de IVA")

        #CUDE
        #c.rect(220, (page_height - 32*mm), 220, 16*mm)

        #Informacion del cliente
        c.setFont("Helvetica", 9)
        c.drawString(30, 609, data['customerName'])
        c.drawString(170, 609, data['customerId'])
        c.drawString(30, 572, customer_telephone)
        c.drawString(375, 609, customer_address_line)
        
        c.drawString(170, 572, data['invoiceReferenceId'])
        
        invoice_day = data['invoiceReferenceDate'].split("-")[2]
        invoice_month = data['invoiceReferenceDate'].split("-")[1]
        invoice_year = data['invoiceReferenceDate'].split("-")[0]
        c.drawString(276, 572, invoice_day)
        c.drawString(300, 572, invoice_month)
        c.drawString(317, 572, invoice_year)
        
        c.drawString(344, 572, data['noteType'])

        #c.drawString(66, 605, customer_city_name)
        #c.drawString(62, 590, customer_neighborhood)
        c.drawString(30, 40, data['seller'])
        c.drawString(196, 40, data['authorized'])
        #c.drawString(100, 568, data['paymentId'])
        c.drawString(508, 553, str(data['items']))
        c.setFont("Helvetica", 8)
        c.drawString(480, 665, f"{data['issueDate']} {data['issueTime']}")
        #c.drawString(500, 700, data['dueDate'])
        #Resolucion
        #c.drawString(320, 570, data['invoiceAuthorization'])
        #c.drawString(400, 570, data['authorizationStartPeriod'])
        #c.drawString(459, 570, data['authorizationInvoicesPrefix'])

        #Medio de pago
        c.setFont("Helvetica", 10)
        #c.drawString(320, 554, data['dueDate'])
        #c.drawString(400, 554, data['issueDate'])
        #c.drawString(459, 554, data['term'])
        #c.drawString(500, 554, data['purchaceOrder'])

        #Totales
        c.drawRightString(580, 175, str(data["taxExclusiveAmount"]))
        #c.drawRightString(580, 150, str(data["allowanceTotalAmount"]))
        c.drawRightString(580, 160, str(data["taxTotal"]))
        c.drawRightString(580, 145, str(data["taxImpoconsumo"]))
        c.drawRightString(580, 130, str(data["bagTaxAmount"]))
        c.drawRightString(580, 115, str(data["giftAmount"]))
        #c.drawRightString(580, 100, str(data["taxAnticipo"]))
        c.setFont("Helvetica-Bold", 10)
        c.drawRightString(580, 100, str(data["payableAmount"]))
        c.setFont("Helvetica", 9.7)
        
        try:
            tax_percent_1 = data['tax'][0]['tax_percentage']
            tax_amount_1 = data['tax'][0]['tax_amount']
            taxable_amount_1 = data['tax'][0]['taxable_amount']
        except:
            tax_percent_1 = ""
            tax_amount_1 = ""
            taxable_amount_1 = ""
        try:
            tax_percent_2 = data['tax'][1]['tax_percentage']
            tax_amount_2 = data['tax'][1]['tax_amount']
            taxable_amount_2 = data['tax'][1]['taxable_amount']
        except:
            tax_percent_2 = ""
            tax_amount_2 = ""
            taxable_amount_2 = ""
               
        c.drawString(380, 55, f"{tax_percent_1}")
        c.drawString(380, 40, f"{tax_percent_2}")
        c.drawRightString(516, 55, taxable_amount_1)
        c.drawRightString(516, 40, taxable_amount_2)
        c.drawRightString(583, 55, str(tax_amount_1))
        c.drawRightString(583, 40, str(tax_amount_2))
        
        #Cufe lineal
        #c.drawString(65, 760, data["CUFE-CUDE"])
        lineas = textwrap.wrap(data["CUFE-CUDE"], width=40)  # Divide el texto en líneas
        y = 740
        for linea in lineas:
            c.drawString(220, y, linea)
            y -= 10  # Espaciado entre líneas


        c.restoreState()
    return header

def footer(c, doc):
    """
    Función para agregar numeración de página: 'Página X de Y'.
    """
    page_num = c.getPageNumber()
    text = f"Página {page_num} de "  # La parte final se llenará al final del PDF

    # Definir fuente y posición
    c.setFont("Helvetica", 10)
    c.drawRightString(550, 30, text)  # Posición en la parte inferior derecha

def create_table(document_type_abbreviation,data,data_table,doc,dollars_invoice=False):    
    # Definir un marco (Frame) para TODAS las páginas con altura reducida
    trm = data.get("trm") or ""
    frame = Frame(21, 235, 568, 286)
    styles = getSampleStyleSheet()
    styleN = styles["Normal"]
    if document_type_abbreviation == "fv":
        col_widths5 = [35,222,30,45,59,32,30,48,60]
        table_data = []  # Comienza con los encabezados
        if dollars_invoice:
            template = PageTemplate(id='page_template', frames=[frame], onPage=make_header_invoice(data))
            doc.addPageTemplates([template])
            trm = data.get('trm') or ""
            for item in data_table:
                #item['description'] = Paragraph(item['description'], styleN) 
                
                #Convertir los campos de valores que vienen en pesos a dolares
                item['unitPrice'] = convert_to_dollars(trm, item['unitPrice'])
                item['amount'] = convert_to_dollars(trm, item['amount'])                                                
                row = list(item.values())
                table_data.append(row)
        else:
            template = PageTemplate(id='page_template', frames=[frame], onPage=make_header_invoice(data))
            doc.addPageTemplates([template])
            for item in data_table:                
                #item['description'] = Paragraph(item['description'], styleN)  
                row = list(item.values())  # Convertir los valores de cada objeto en una fila
                table_data.append(row)
    
        # Definir estilo de tabla
        table_style = TableStyle([   
            ('VALIGN', (0, 0), (-1, -1), 'TOP'), 
            ('LEFTPADDING', (0, 0), (-1, -1), 3),   # Padding izquierdo en 3 
            ('RIGHTPADDING', (0, 0), (-1, -1), 3),  # Padding derecho en 3
            ('TOPPADDING', (0, 0), (-1, -1), 2),    # Padding superior en 2
            ('BOTTOMPADDING', (0, 0), (-1, -1), 2), # Padding inferior en 2  
            ('ALIGN', (0, 0), (1, -1), 'LEFT'),    # Columnas 1 y 2
            ('ALIGN', (2, 0), (3, -1), 'CENTER'),  # Columnas 3 y 4
            ('ALIGN', (4, 0), (4, -1), 'RIGHT'),   # Columna 5
            ('ALIGN', (5, 0), (7, -1), 'CENTER'),  # Columnas 6, 7 y 8
            ('ALIGN', (8, 0), (8, -1), 'RIGHT'),   # Columna 9
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica'),
            ('FONTSIZE', (0, 0), (-1, -1), 8),
            ('WORDWRAP', (0, 0), (-1, -1))
        ])

        # Crear la tabla como Flowable
        #table = Table(table_data, colWidths=col_widths5, rowHeights=10.5, splitByRow=1) #caben 25 lineas en la tabla
        #table = Table(table_data, colWidths=col_widths5, rowHeights=12.4, splitByRow=1) #caben 22 lineas en la tabla
        #table = Table(table_data, colWidths=col_widths5, rowHeights=13.7, splitByRow=1) #caben 20 lineas en la tabla
        table = Table(table_data, colWidths=col_widths5, rowHeights=None, splitByRow=1) #caben 20 lineas en la tabla
        #table = Table(table_data, colWidths=col_widths5, rowHeights=None, splitByRow=1) #prueba de alto variable
        table.setStyle(table_style)
        return table
    elif document_type_abbreviation == "nd" or document_type_abbreviation == "nc":
        template = PageTemplate(id='page_template', frames=[frame], onPage=make_header_notes(data))
        doc.addPageTemplates([template])

        col_widths5 = [36,186,40,48,38,55,28,26,40,66]
        table_data = []  # Comienza con los encabezados
        if dollars_invoice:
            trm = data.get('trm') or ""
            for item in data_table:                
                #item['description'] = Paragraph(item['description'], styleN) 
                
                #Convertir los campos de valores que vienen en pesos a dolares
                item['unitPrice'] = convert_to_dollars(trm, item['unitPrice'])
                item['amount'] = convert_to_dollars(trm, item['amount'])                                                
                row = list(item.values())
                table_data.append(row)
        else:
            for item in data_table:
                #item['description'] = Paragraph(item['description'], styleN)  
                row = list(item.values())  # Convertir los valores de cada objeto en una fila
                table_data.append(row)
                
        # Definir estilo de tabla
        table_style = TableStyle([        
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('ALIGN', (1, 0), (1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica'),
            ('FONTSIZE', (0, 0), (-1, -1), 8),
            ('WORDWRAP', (0, 0), (-1, -1))
        ])

        # Crear la tabla como Flowable
        table = Table(table_data, colWidths=col_widths5, rowHeights=10.5, splitByRow=1) #caben 25 lineas en la tabla
        #table = Table(table_data, colWidths=col_widths5, rowHeights=12.4, splitByRow=1) #caben 22 lineas en la tabla
        #table = Table(table_data, colWidths=col_widths5, rowHeights=13.7, splitByRow=1) #caben 20 lineas en la tabla
        table.setStyle(table_style)
        return table
    else:
        print("El tipo de factura no corresponde a FV, NC o ND")

def generate_pdf(data,customer_bucket):    
    elements = []
    document_type_abbreviation = data["documentTypeAbbreviation"]
    customer_tin = data["supplierId"]
    data_table = data['lines']
    file_path = data["filePath"]
    file_name = data["fileName"]
    currency = data.get('currency') or ""
    documentType = "Invoice"
    template = f"{TEMPLATE_PATH}{documentType}TemplateV1.pdf" 
    
    buffer = io.BytesIO()
    # Crear documento con BaseDocTemplate para control más preciso
    doc = BaseDocTemplate(buffer, pagesize=letter)
    #doc = SimpleDocTemplate(buffer, pagesize=letter)
    
    ####PRUEBAS
    frame = Frame(21, 235, 568, 286)
    if document_type_abbreviation == "fv":
        template_cop = PageTemplate(id='page_template_cop', frames=[frame], onPage=make_header_invoice(data, dollars_invoice=False))
        template_usd = PageTemplate(id='page_template_usd', frames=[frame], onPage=make_header_invoice(data, dollars_invoice=True))
    elif document_type_abbreviation == "nd" or document_type_abbreviation == "nc":
        template_cop = PageTemplate(id='page_template_cop', frames=[frame], onPage=make_header_notes(data, dollars_invoice=False))
        template_usd = PageTemplate(id='page_template_usd', frames=[frame], onPage=make_header_notes(data, dollars_invoice=True))
        
    doc.addPageTemplates([template_cop, template_usd])

    elements = []

    # Agregar la tabla en COP
    doc.handle_nextPageTemplate('page_template_cop')  # Asegurar que la primera página es COP
    
    ####PRUEBAS

    # Definir una plantilla de página usando el marco y con encabezxado en todas las paginas
    #onPage (Agrega la info que quiera a cada pagina)
    #template = PageTemplate(id='page_template', frames=[frame], onPage=header, onPageEnd=footer)

    table_cop = create_table(document_type_abbreviation,data,data_table,doc)
    elements.append(table_cop)
    # Construir el PDF con la tabla
    #start_time3 = time.perf_counter()
    print("Inicio build")
    #elements.append(table)


    # Si hay una hoja de dólares, cambiar de plantilla
    if data.get("currency") == "USD":
        elements.append(PageBreak())  # Salto de página
        table_usd = create_table(document_type_abbreviation,data,data_table,doc,True)
        doc.handle_nextPageTemplate('page_template_usd')  # Cambiar a plantilla USD
        elements.append(table_usd)
    '''
    if currency == "USD":
        elements.append(PageBreak())
        table = create_table(document_type_abbreviation,data,data_table,doc,True)
        elements.append(table)
    '''
    ###Cambio
    
    
    doc.build(elements, canvasmaker=PageNumCanvas)
    buffer.seek(0)

    if pruebas:
        with open(ruta_salida, "wb") as f:
            f.write(buffer.getvalue())

        # Cerrar el buffer
        buffer.close()
    else:
        # Subir a S3
        save_document(customer_bucket,file_path,"pdf",buffer.getvalue(),file_name,document_type_abbreviation) #pdf factura
        print("PDF subido a S3")

    print("Fin build")

def format_date(date):
    formatted_date = date.strftime("%Y-%m-%dT%H:%M:%S%z")
    formatted_date = formatted_date[:-2] + ":" + formatted_date[-2:]
    return formatted_date

def update_traceability_process(table_name:str,num_fac:str,step:int,pdf_creation_start_date:str,pdf_creation_duration:str)->None:
    table_traceability_process = dynamodb.Table(table_name)
    try:
        response = table_traceability_process.update_item(
            Key={
                'numFac': num_fac
            },
            UpdateExpression="SET step = :step, pdfCreationStartDate = :v1, pdfCreationDuration = :v2",
            ExpressionAttributeValues={
                ':step': step,
                ':v1': pdf_creation_start_date,
                ':v2': pdf_creation_duration
            },
            ReturnValues="UPDATED_NEW"
        )
    except Exception as e:
        print(e)

def lambda_handler(event, context):
    #Prueba local
    #body = data_ant["Records"][0]["body"]
    environment = context.invoked_function_arn.split(":")[-1]
    body = event["Records"][0]["body"]
    #print(body)
    #print(event)
    num_fac = json.loads(body)["numFac"]
    pdf_creation_start_date = format_date(datetime.now(ZoneInfo("America/Bogota")))
    pdf_creation_start_time = time.time()
    print(f"##########{num_fac}##########")
    data = json.loads(body)
    customer_tin = data["supplierId"]
    customer_bucket = f"fe.{environment.lower()}.{customer_tin}"

    traceability_process_table_name = f"{environment}_{customer_tin}_traceabilityProcess"

    file_path = data["filePath"]
    file_name = data["fileName"]

    generate_pdf(data,customer_bucket)

    pdf_creation_end_time = time.time()
    pdf_creation_execution_time_ms = int((pdf_creation_end_time - pdf_creation_start_time) * 1000)
    if not pruebas:
        update_traceability_process(traceability_process_table_name,num_fac,3,pdf_creation_start_date,pdf_creation_execution_time_ms)

    send_data = {}
    send_data["supplierId"] = customer_tin
    send_data["customerId"] = data["customerId"]
    send_data["customerBucket"] = customer_bucket
    send_data["valTotFac"] = data["valTotFac"]
    send_data["numFac"] = data["numFac"]
    #send_data["attrUuid"] = data["attrUuid"]
    send_data["documentTypeAbbreviation"] = data["documentTypeAbbreviation"]
    send_data["customerRegistrationName"] = data["customerRegistrationName"]
    send_data["customerName"] = data["customerName"]
    send_data["supplierRegistrationName"] = data["supplierRegistrationName"]    
    send_data["customerElectronicMail"] = data["customerElectronicMail"]
    send_data["filePath"] = file_path
    send_data["fileName"] = file_name
    send_data["subject"] = data["subject"]
    
    message = json.dumps(send_data)
    if not pruebas:
        send_sqs(f"{URL_SQS_SENDEMAIL}{environment}_Email_Send-ondemand-raw-EAP",message)

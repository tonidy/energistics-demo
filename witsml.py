from zeep import Client
from requests.auth import HTTPBasicAuth
from requests import Session
from zeep.transports import Transport
import pandas as pd
import untangle
from io import StringIO

session = Session()

user = "ilab.user"
password = "n@6C5rN!"

logQuery = """<logs xmlns:gml="http://www.opengis.net/gml/3.2" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:dc="http://purl.org/dc/terms/" version="1.4.1.1" xmlns="http://www.witsml.org/schemas/1series">
  <log uidWell="Energistics-well-0001" uidWellbore="Energistics-w1-wellbore-0001" uid="Energistics-w1-wb1-log-0002">
    <nameWell>Energistics Certification Well 1</nameWell>
    <nameWellbore>Energistics Certification Well 1 Wellbore 1</nameWellbore>
    <name>Energistics Certification Well 1 Wellbore 1 Log 2</name>
  </log>
</logs>"""

session.auth = HTTPBasicAuth(user, password)
client = Client('https://witsmlstudio.pds.software/staging/WitsmlStore.svc?wsdl', transport=Transport(session=session))
result = client.service.WMLS_GetFromStore("log", logQuery, "returnElements=all", "")

# print(result.XMLout)

logs = untangle.parse(result.XMLout)

output = StringIO()
val = ""
for data in logs.logs.log.logData.data:
    output.write(data.cdata + "\n")

# output.flush()
output.seek(0)

print(pd.read_csv(output, index_col=0, header=None).convert_objects(convert_numeric=True))


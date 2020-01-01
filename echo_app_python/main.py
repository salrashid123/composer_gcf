
import requests
import json
from flask import jsonify

def echo_app_python(request):
    
    target_audience = '491562778408-sj8hb4035bp7ui918ra0i9qbhbqnejk1.apps.googleusercontent.com'
    metadata_url = "http://metadata/computeMetadata/v1/instance/service-accounts/default/identity?audience=" + target_audience
    r = requests.get(metadata_url, headers={"Metadata-Flavor":"Google"})
    idt = r.text

    url = 'https://r1d366b885bb81b73-tp.appspot.com'

    endpoint_url = url + '/api/experimental/dags/fromgcf/dag_runs'

    headers = {
        'Authorization': 'Bearer ' + idt,
        'Content-Type': 'application/json'
    }
    data = { 'conf': '' }
    r = requests.post(endpoint_url,data=json.dumps(data), headers=headers )

    return jsonify({'status_code' : r.status_code })

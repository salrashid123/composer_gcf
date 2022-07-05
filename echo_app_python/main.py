
import json
from flask import Flask
from flask import jsonify
from google.oauth2 import id_token
from google.auth import impersonated_credentials
import google.auth
import google.auth.transport.requests
from google.auth.transport.requests import AuthorizedSession
from google.auth import compute_engine 

import os 

app = Flask(__name__)

@app.route("/")
def default(request):
  return 'ok'

@app.route("/echo")
def echoapp(request):
    
    creds, _ = google.auth.default(scopes=['https://www.googleapis.com/auth/cloud-platform'])
    authed_session = AuthorizedSession(creds)
    url = os.getenv('AIRFLOW_URI') 
    endpoint_url = url + '/api/v1/dags/{dag_id}/dagRuns'.format(dag_id="fromgcf")
    headers = {
        'Content-Type': 'application/json'
    }
    data = { "conf":  {"key": "value"} }
    r = authed_session.post(endpoint_url,data=json.dumps(data), headers=headers )
    # do error handling.  if GCF isn't allowed to invoke the DAG via IAM permissions
    ## composer still returns a '200 ok' response for some reason...
    ## meaning the following woudn't be json at all.  i'm just expecting everything to go smoothly here
    print(r.json())
    return jsonify({'status_code' : r.status_code })

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
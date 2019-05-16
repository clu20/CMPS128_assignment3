

from flask import Flask, jsonify, request, make_response, g
from flask_restful import Api, Resource

import os, requests

app = Flask(__name__)
api = Api(app)
forwarding = os.environ.get('FORWARDING_ADDRESS') or 0 ## forwarding ip
newdict = {}
replica_ips = ["10.10.0.2:8080", "10.10.0.3:8080", "10.10.0.4.8080"]

class key_value(Resource):
    def get(self, key):
        if 'FORWARDING_ADDRESS' in os.environ:
            #nonempty forwarding address forward to main instance
            try:
                r = requests.get('http://10.10.0.2:8080/key-value-store/' + key)
                return r.json(),r.status_code
            except:
                return make_response(jsonify(error= 'Main instance is down', message = 'Error in GET'), 503)
        else:
            if key in newdict:
                #on key value found return found value
                value = newdict[key]
                return make_response(jsonify(doesExist=True, message="Retrieved successfully", value=value), 200)
            else:
                #on key value not found error
                return make_response(jsonify(doesExist=False, error="Key does not exist", message="Error in GET"), 404)

    def put(self, key):
        if 'FORWARDING_ADDRESS' in os.environ:
            try:
                json = request.get_json()
                r = requests.put('http://10.10.0.2:8080/key-value-store/'+key, json=json)
                return r.json(),r.status_code
            except:
                return make_response(jsonify(error = 'Main instance is down', message="Error in PUT"), 503)
        else:
            if len(key) < 50:
                message = request.get_json()
                v = message.get('value')
                if v:
                    if key in newdict:
                        #edit value @ key, key
                        newdict[key] = v
                        return make_response(jsonify(message='Updated successfully', replaced=True),200)
                    else:
                    #add new value @ key, key
                        newdict[key] = v
                        return make_response(jsonify(message='Added successfully', replaced=False), 201)
                else:
                    return make_response(jsonify(error="Value is missing",message="Error in PUT"), 400)
            else:
                return make_response(jsonify(error="Key is too long", message="Error in PUT"), 400)


    def delete(self, key):
        if 'FORWARDING_ADDRESS' in os.environ:
            try:
                r = requests.delete('http://10.10.0.2:8080/key-value-store/'+key)
                return r.json(),r.status_code
            except:
                return make_response(jsonify(error='Main instance is down', message='Error in DELETE'),503)
        else:
            if newdict.pop(key,None) == None:
                return make_response(jsonify(doesExist=False, error="Key does not exist", message="Error in DELETE"), 404)
            else:
                return make_response(jsonify(doesExist=True, message="Deleted successfully"), 200)



class Views(Resource):
    def get(self):
    	if request.host == '127.0.0.1:8082':
    		return make_response(jsonify(message="View retrieved successfully", view = replica_ips[0]))
    	elif request.host == '127.0.0.1:8083':
    		return make_response(jsonify(message="View retrieved successfully", view = replica_ips[1]))
    	else:
    		return make_response(jsonify(message="View retrieved successfully", view = replica_ips[2]))


        




api.add_resource(key_value, '/key-value-store/', '/key-value-store/<key>')
api.add_resource(Views, '/key-value-store-view')


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8080)

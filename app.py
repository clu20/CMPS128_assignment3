from flask import Flask, jsonify, request, make_response, g
from flask_restful import Api, Resource

import os, requests

app = Flask(__name__)
api = Api(app)
forwarding = os.environ.get('FORWARDING_ADDRESS') or 0 ## forwarding ip
newdict = {}
versionList = []
counter = 0

class view(Resource):
    # contains view operations (functions)----GET, DELETE, PUT
    # operations are from replica to replica (ie. socket in view)
    #GET: return all ip's in view currently
    #DELETE: if socket is in view, remove from current process, send DELETE message to all ips in VIEW
    #       if not, send error message
    #PUT: if socket is already in view, send message: already in view
    #       if not, add socket ip to view, send PUT message to all ips in view except socket's
    def get(self):
        #find out ip's in view (running on port-ip)
        view_addrs = os.environ['VIEW']
        return make_response(jsonify(message='View retrieved successfully', view = view_addrs), 200)

    def put(self):
        view_list = os.environ['VIEW'].split(',')
        msg = request.get_json()
        socket_add = msg.get('socket-address')
        if socket_add:
            if socket_add in view_list:
                return make_response(jsonify(error='Socket address already exists in the view', message= 'Error in PUT'), 404)
            else:
                new_view = os.environ['VIEW'] + ',' + socket_add
                os.environ['VIEW'] = new_view
                #broadcast the new replica to be in other replica views
                for view in view_list:
                    if view != os.environ['SOCKET_ADDRESS']:
                        beginning = 'http://'
                        end_point = '/key-value-store-view'
                        replica = beginning+view+end_point
                        requests.put(replica, json = {'socket-address': socket_add})
                return make_response(jsonify(message= 'Replica added successfully to the view'), 200)
        else:
            return make_response(jsonify(error='socket address is missing', message= 'Error in PUT'), 400)

    def delete(self):
        view_list = os.environ['VIEW'].split(',')
        new_view = ''
        msg = request.get_json()
        socket_add = msg.get('socket-address')
        if socket_add in view_list:
            #Remove socket-address if in VIEW
            view_list.remove(socket_add)
            list_length = len(view_list)
            x = 0
            #Create new VIEW string for environment var 'VIEW'
            while x < list_length:
                if(x == list_length - 1):
                    new_view+=view_list[x]
                else:
                    new_view += view_list[x]+','
                x+=1
            os.environ['VIEW'] = new_view
            #Broadcast the VIEW delete to all other socket-addresses
            for view in view_list:
                if view != os.environ['SOCKET_ADDRESS']:
                    beginning = 'http://'
                    end_point = '/key-value-store-view'
                    replica = beginning+view+end_point
                    requests.delete(replica, json = {'socket-address': socket_add})
            return make_response(jsonify(message= 'Replica successfully deleted from the view'))
        else:
            return make_response(jsonify(error='Socket address does not exist in the view', message= 'Error in DELETE'), 404)

class key_value(Resource):

    def __init__(self):
        self.counter = 0

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
                beginning = 'http://'
                end_point = '/key-value-store/'
                json = request.get_json()
                view_list = os.environ['VIEW'].split(',')
                message = request.get_json()
                v = message.get('value')
                meta = message.get('causal-metadata')
                # for some reason splitting "" breaks the code
                if len(meta) > 1:
                    meta = meta.split(', ')
                # if there is no meta data list or the meta is already in the list
                if meta == "" or meta == versionList:
                    #create version
                    global counter
                    counter += 1
                    version = "V" + str(counter)
                    versionList.append(version)
                    if v:
                        if key in newdict:
                            #edit value @ key, key
                            newdict[key] = v
                            if request.remote_addr not in os.environ['VIEW']:
                                self.broadcast_request(view_list, "PUT", key, v, version, meta)
                            return make_response(jsonify(message='Updated successfully', version = version, meta = versionList),200)
                        else:
                            #add new value @ key, key
                            newdict[key] = v
                            if request.remote_addr not in os.environ['VIEW']:
                                self.broadcast_request(view_list, "PUT", key, v, version, meta)
                            return make_response(jsonify(message='Added successfully', version = version, meta = versionList), 201)
                    else:
                        return make_response(jsonify(error="Value is missing",message="Error in PUT"), 400)
                else:
                    return make_response(jsonify(error="did not do all the operations that are depended on"), 400)

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
            beginning = 'http://'
            end_point = '/key-value-store/'
            json = request.get_json()
            view_list = os.environ['VIEW'].split(',')
            message = request.get_json()
            meta = message.get('causal-metadata')
            #test for key membership
            if newdict.get(key,None) == None:
                return make_response(jsonify(doesExist=False, error="Key does not exist", message="Error in DELETE"), 404)
            else:
                if len(meta) > 1:
                    meta = meta.split(', ')
                if meta == "" or meta == versionList:
                    #create version
                    global counter
                    counter += 1
                    version = "V" + str(counter)
                    versionList.append(version)
                    newdict[key] = None
                    if request.remote_addr not in os.environ['VIEW']:
                        self.broadcast_request(view_list, "DELETE", key, versionList, version)
                        json = {}
                        return make_response(jsonify(message='Deleted successfully', version = version),200)
                else:
                    return make_response(jsonify(error="did not do all the operations that are depended on"), 400)

    #TODO: need to add optional parameter for key
    def broadcast_request(self, viewlist, method , key, value, version, meta):
        current_address = os.environ['SOCKET_ADDRESS']
        beginning = 'http://'
        end_point = '/key-value-store/'
        for reps in viewlist:
            rep_url = beginning + reps + end_point + key
            # json = request.get_json()
            if current_address != reps:
                if method == "PUT":
                    try:
                        requests.put(rep_url, json={'value' : value, 'version': version, 'causal-metadata': meta})
                    except:
                        requests.delete(beginning+current_address+'/key-value-store-view', json = {'socket-address': reps})
                elif method == 'DEL':
                    try:
                        requests.delete(rep_url, json={'value' : value, 'version': version, 'causal-metadata': meta})
                    except:
                        requests.delete(beginning+current_address+'/key-value-store-view', json = {'socket-address': reps})

api.add_resource(view, '/key-value-store-view')
api.add_resource(key_value, '/key-value-store/', '/key-value-store/<key>')
# we should have a GET_ALL(self) that returns all keys

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8080)

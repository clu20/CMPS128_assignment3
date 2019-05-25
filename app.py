from flask import Flask, jsonify, request, make_response, g
from flask_restful import Api, Resource

import os, requests

app = Flask(__name__)
api = Api(app)
forwarding = os.environ.get('FORWARDING_ADDRESS') or 0 ## forwarding ip
newdict = {}
versionList = []
versionDict = {}
counter = 0

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
                string_versionList = ','.join(versionList)
                json = jsonify({'message': 'Retrieved successfully', 'version': versionDict[key], 'causal-metadata': string_versionList, 'value':value})
                return make_response(json, 200)
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
                view_list = os.environ['VIEW'].split(',')
                message = request.get_json()
                try:
                    v = message.get('value')
                except:
                    current_address = os.environ['SOCKET_ADDRESS']
                    return make_response(jsonify(error="error in try catch put()", current_address=current_address),400)
                sentFromClient = True if request.remote_addr not in os.environ['VIEW'] else False
                global counter
                meta = message.get('causal-metadata')
                
                # for some reason splitting "" breaks the code
                if len(meta) > 1:
                    meta = meta.split(', ')
                # if there is no meta data list or the meta is the same as the list (received all messages)
                if meta == "" or meta == versionList:
                    response = self.doPut(key, sentFromClient, view_list, meta, message)
                    return make_response(response)
                else:
                    for i in meta:
                        if i not in versionList:
                            while i not in versionList:
                                r = request.get_json()
                                new_meta = r.get('causal-metadata')
                                if new_meta == i:
                                    self.doPut(key, sentFromClient, view_list, new_meta, r)
                                    break
                                else:
                                    pass
                    response = self.doPut(key, sentFromClient, view_list, meta, message)
                    return make_response(response)
                    
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
                view_list = os.environ['VIEW'].split(',')
                self.broadcast_request(view_list, 'DEL', key)
                return make_response(jsonify(doesExist=True, message="Deleted successfully"), 200)

    #TODO: need to add optional parameter for key
    def broadcast_request(self, viewlist, method , key, value, version, meta, counter):
        current_address = os.environ['SOCKET_ADDRESS']
        beginning = 'http://'
        end_point = '/key-value-store/'
        for reps in viewlist:
            rep_url = beginning + reps + end_point + key
            # json = request.get_json()
            if current_address != reps:
                if method == "PUT":
                    try:
                        requests.put(rep_url, json={'value' : value, 'version': version, 'causal-metadata': meta, 'counter': counter})
                    except: 
                        requests.delete(beginning+current_address+'/key-value-store-view', json = {'socket-address': reps})
                elif method == 'DEL':
                    try:
                        requests.delete(rep_url, json={'value' : value, 'version': version, 'causal-metadata': meta, 'counter': counter})
                    except:
                        requests.delete(beginning+current_address+'/key-value-store-view', json = {'socket-address': reps})


    def doPut(self, key, fromClient, view_list, meta, message):
        try:
            v = message.get('value')
        except:
            current_address = os.environ['SOCKET_ADDRESS']
            return make_response(jsonify(error='error in try catch on doPUT', replica = current_address), 400)
        json = None
        global counter
        if not fromClient:
            broadcasted_counter = message.get('counter')
            counter = broadcasted_counter
            version = message.get('version')
            versionDict[key] = version
            versionList.append(version)
        else: 
            counter += 1
            version = "V" + str(counter)
            versionDict[key] = version
            versionList.append(version)
        if v:
            #need to convert it to this because of testing
            string_versionList = ','.join(versionList)
            if key in newdict:
                       #edit value @ key, key
                newdict[key] = v
                if fromClient:
                    self.broadcast_request(view_list, "PUT", key, v, version, meta, counter)
                    json = jsonify({'message': 'Updated successfully', 'version': version, 'causal-metadata': string_versionList})
                else:
                    json = jsonify({'message': 'Replicated successfully', 'version': version})
                    
                return json, 200
            else:
                            #add new value @ key, key
                newdict[key] = v
                if fromClient:
                    self.broadcast_request(view_list, "PUT", key, v, version, meta, counter)
                    json = jsonify({'message': 'Added successfully', 'version': version, 'causal-metadata': string_versionList})
                else:
                    json = jsonify({'message': 'Replicated successfully', 'version': version})
                return json, 201
        else:
            json = jsonify({'error':'Value is missing', 'message':'Error in PUT' })
            return json, 400




class Views(Resource):


    def get(self):
        return make_response(jsonify(message='View retrieved successfully', view = os.environ['VIEW']))

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
                beginning = 'http://'
                end_point = '/key-value-store-view'
                #new replica now contains everything from another key-value-store replica
                replica_url = beginning+socket_add+end_point
                for key in newdict:
                    json = request.get_json()
                    requests.put(replica_url+'/'+key, json=json)
                #broadcast the new replica to be in other replica views
                for view in view_list:
                    if view != os.environ['SOCKET_ADDRESS']:
                        replica = beginning+view+end_point
                        try:
                            requests.put(replica, json = {'socket-address': socket_add})
                        except:
                            requests.delete(beginning+os.environ['SOCKET_ADDRESS']+end_point, json = {'socket-address': socket_add})
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
                    try:
                        requests.delete(replica, json = {'socket-address': socket_add})
                    except:
                        requests.delete(beginning+os.environ['SOCKET_ADDRESS']+end_point, json = {'socket-address': view})
            return make_response(jsonify(message= 'Replica successfully deleted from the view'), 200)
        else:
            return make_response(jsonify(error='Socket address does not exist in the view', message= 'Error in DELETE'), 404)


api.add_resource(key_value, '/key-value-store/', '/key-value-store/<key>')
api.add_resource(Views, '/key-value-store-view')


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8080)
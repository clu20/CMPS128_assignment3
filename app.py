from flask import Flask, jsonify, request, make_response, g
from flask_restful import Api, Resource

import os, requests

app = Flask(__name__)
api = Api(app)
forwarding = os.environ.get('FORWARDING_ADDRESS') or 0 ## forwarding ip
newdict = {}

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
                view_list = os.environ['VIEW'].split(',')
                message = request.get_json()
                v = message.get('value')
                #must check that all the other replicas are alive
                responses = self.replicas_statuses()
                #delete the dead replicas from the VIEW
                for rep in responses:
                    if rep[1] != 200:
                    	responses.remove(rep)

                beginning = 'http://'
                end_point = '/key-value-store/'
                json = request.get_json()
                #check if the request is being forwarded by another replica
                if request.remote_addr not in os.environ["VIEW"]:
                    self.broadcast_request(responses, "PUT", key)
                if v:
                    if key in newdict:
                        #edit value @ key, key
                        newdict[key] = v
                        return make_response(jsonify(message='Updated successfully', replaced=True, responses=responses),200)
                    else:
                    #add new value @ key, key
                        newdict[key] = v
                        return make_response(jsonify(message='Added successfully', replaced=False, responses=responses), 201)
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

    #to check all the statuses of every replica, used before every put key-value-store method
    def replicas_statuses(self):
        view_list = os.environ['VIEW'].split(',')
        beginning = 'http://'
        end_point = '/key-value-store-view'
        status_list = []
        for rep in view_list:
            try:
                rep_url = beginning + rep + end_point
                r = requests.get(rep_url)
                status_list.append((rep, r.status_code))
            except:
                status_list.append((rep, 500))

        return status_list


    #TODO: need to add optional parameter for key
    def broadcast_request(self, statuses, method , key):
        current_address = os.environ['SOCKET_ADDRESS']
        beginning = 'http://'
        end_point = '/key-value-store/'
        for reps in statuses:
            rep_url = beginning + reps[0] + end_point + key
            json = request.get_json()
            if current_address != reps[0]:
                if method == "PUT":
                    requests.put(rep_url, json=json)
                elif method == "DELETE":
                    requests.delete(rep_url, json=json)
                elif method == "GET":
                    requests.get(rep_url, json=json)
 
    def printRemote(self, addr):
        return make_response(jsonify(address=addr))


class Views(Resource):


    def get(self):
        #find out which view you are in 
        view_addr = os.environ['SOCKET_ADDRESS']
        remote_addr =  request.remote_addr
        access_route = request.access_route
        host = request.host
        return make_response(jsonify(message='View retrieved successfully', view = view_addr , remote_addr = remote_addr, access_route = access_route, host=host), 200)

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

    def buildView(view):
        res = [','] * (len(view) * 2 - 1)
        res[0::2] = view
        return ''.join(res)


api.add_resource(key_value, '/key-value-store/', '/key-value-store/<key>')
api.add_resource(Views, '/key-value-store-view')


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8080)
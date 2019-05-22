from flask import Flask, jsonify, request, make_response, g
from flask_restful import Api, Resource

import os, requests

app = Flask(__name__)
api = Api(app)
newdict = {}

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

    def buildView(view):
        res = [','] * (len(view) * 2 - 1)
        res[0::2] = view
        return ''.join(res)


class key_value(Resource):
    #operations are open to clients and replicas
    def get(self, key):
        if key in newdict:
            #on key value found return found value
            value = newdict[key]
            return make_response(jsonify(doesExist=True, message="Retrieved successfully", value=value), 200)
        else:
            #on key value not found error
            return make_response(jsonify(doesExist=False, error="Key does not exist", message="Error in GET"), 404)

    def put(self, key):
        # for ip in view ....
        # Then add response to a list hashed to the key? -- we can have "value ie.(key_value)" and "version, (incremented per write)"
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
        # for ip in view ....
        # Then add response to a list hashed to the key? -- we can have "value(key_value)" and "version(incremented per write)"
        if newdict.pop(key,None) == None:
            return make_response(jsonify(doesExist=False, error="Key does not exist", message="Error in DELETE"), 404)
        else:
            return make_response(jsonify(doesExist=True, message="Deleted successfully"), 200)

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


api.add_resource(view, '/key-value-store-view')
api.add_resource(key_value, '/key-value-store/', '/key-value-store/<key>')
# we should have a GET_ALL(self) that returns all keys

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8080)

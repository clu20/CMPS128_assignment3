import unittest
import requests
import time
import os

######################## initialize variables ################################################
subnetName = "assignment3-net"
subnetAddress = "10.10.0.0/16"

replica1Ip = "10.10.0.2"
replica1HostPort = "8082"
replica1SocketAddress = replica1Ip + ":8080"

replica2Ip = "10.10.0.3"
replica2HostPort = "8083"
replica2SocketAddress = replica2Ip + ":8080"

replica3Ip = "10.10.0.4"
replica3HostPort = "8084"
replica3SocketAddress = replica3Ip + ":8080"

view = replica1SocketAddress + "," + replica2SocketAddress + "," + replica3SocketAddress

############################### Docker Linux Commands ###########################################################
def removeSubnet(subnetName):
    command = "docker network rm " + subnetName
    os.system(command)
    time.sleep(2)

def createSubnet(subnetAddress, subnetName):
    command  = "docker network create --subnet=" + subnetAddress + " " + subnetName
    os.system(command)
    time.sleep(2)

def buildDockerImage():
    command = "docker build -t assignment3-img ."
    os.system(command)

def runReplica(hostPort, ipAddress, subnetName, instanceName):
    command = "docker run -d -p " + hostPort + ":8080 --net=" + subnetName + " --ip=" + ipAddress + " --name=" + instanceName + " -e SOCKET_ADDRESS=" + ipAddress + ":8080" + " -e VIEW=" + view + " assignment3-img"
    os.system(command)
    time.sleep(20)

def stopAndRemoveInstance(instanceName):
    stopCommand = "docker stop " + instanceName
    removeCommand = "docker rm " + instanceName
    os.system(stopCommand)
    time.sleep(2)
    os.system(removeCommand)

################################# Unit Test Class ############################################################

class TestHW3(unittest.TestCase):

    ######################## Build docker image and create subnet ################################
    print("###################### Building Dokcer image ######################\n")
    # build docker image
    buildDockerImage()




    ########################## Run tests #######################################################
    def test_a_view_operations(self):

        # stop and remove containers from possible previous runs
        print("\n###################### Stopping and removing containers from previous run ######################\n")
        stopAndRemoveInstance("replica1")
        stopAndRemoveInstance("replica2")
        stopAndRemoveInstance("replica3")

        print("\n###################### Creating the subnet ######################\n")
        # remove the subnet possibly created from the previous run
        removeSubnet(subnetName)

        # create subnet
        createSubnet(subnetAddress, subnetName)

        #run instances
        print("\n###################### Running replicas ######################\n")
        runReplica(replica1HostPort, replica1Ip, subnetName, "replica1")
        runReplica(replica2HostPort, replica2Ip, subnetName, "replica2")
        runReplica(replica3HostPort, replica3Ip, subnetName, "replica3")

        print("\n###################### Getting the view from replicas ######################\n")
        # get the view from replica1
        response = requests.get( 'http://localhost:8082/key-value-store-view')
        responseInJson = response.json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(responseInJson['view'], view)

        # get the view from replica2
        response = requests.get( 'http://localhost:8083/key-value-store-view')
        responseInJson = response.json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(responseInJson['view'], view)

        # get the view from replica3
        response = requests.get( 'http://localhost:8084/key-value-store-view')
        responseInJson = response.json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(responseInJson['view'], view)

        print("\n###################### Putting key1/value1 to the store ######################\n")
        # put a new key in the store
        response = requests.put('http://localhost:8082/key-value-store/key1', json={'value': "value1"})
        self.assertEqual(response.status_code, 201)

        print("\n###################### Waiting for 10 seconds ######################\n")
        time.sleep(10)

        print("\n###################### Getting key1 from the replicas ######################\n")

        # get the value of the new key from replica1 after putting the new key
        response = requests.get('http://localhost:8082/key-value-store/key1')
        responseInJson = response.json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(responseInJson['value'], 'value1')

        # get the value of the new key from replica2 after putting the new key
        response = requests.get('http://localhost:8083/key-value-store/key1')
        responseInJson = response.json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(responseInJson['value'], 'value1')

        # get the value of the new key from replica3 after putting the new key
        response = requests.get('http://localhost:8084/key-value-store/key1')
        responseInJson = response.json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(responseInJson['value'], 'value1')

        print("\n###################### Stopping and removing replica3 ######################\n")
        stopAndRemoveInstance("replica3")

        print("\n###################### Waiting for 10 seconds ######################\n")
        time.sleep(10)

        print("\n###################### Putting key2/value2 to the store ######################\n")
        # put a new key in the store
        response = requests.put('http://localhost:8082/key-value-store/key2', json={'value': "value2"})
        self.assertEqual(response.status_code, 201)

        print("\n###################### Waiting for 50 seconds ######################\n")
        time.sleep(50)

        print("\n###################### Getting key2 from the replica1 and replica2 ######################\n")

        # get the value of the new key from replica1 after putting the new key
        response = requests.get('http://localhost:8082/key-value-store/key2')
        responseInJson = response.json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(responseInJson['value'], 'value2')

        # get the value of the new key from replica2 after putting the new key
        response = requests.get('http://localhost:8083/key-value-store/key2')
        responseInJson = response.json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(responseInJson['value'], 'value2')


        print("\n###################### Getting the view from replica1 and replica2 ######################\n")
        # get the view from replica1
        response = requests.get( 'http://localhost:8082/key-value-store-view')
        responseInJson = response.json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(responseInJson['view'], replica1SocketAddress + "," + replica2SocketAddress)

        # get the view from replica2
        response = requests.get( 'http://localhost:8083/key-value-store-view')
        responseInJson = response.json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(responseInJson['view'], replica1SocketAddress + "," + replica2SocketAddress)

        print("\n###################### Starting replica3 ######################\n")
        runReplica(replica3HostPort, replica3Ip, subnetName, "replica3")

        print("\n###################### Waiting for 50 seconds ######################\n")
        time.sleep(50)

        print("\n###################### Getting the view from replicas ######################\n")
        # get the view from replica1
        response = requests.get( 'http://localhost:8082/key-value-store-view')
        responseInJson = response.json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(responseInJson['view'], view)

        # get the view from replica2
        response = requests.get( 'http://localhost:8083/key-value-store-view')
        responseInJson = response.json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(responseInJson['view'], view)

        # get the view from replica3
        response = requests.get( 'http://localhost:8084/key-value-store-view')
        responseInJson = response.json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(responseInJson['view'], view)

        print("\n###################### Getting key1 and key2 from replica3 ######################\n")

        # get the value of the new key from replica1 after putting a new key
        response = requests.get('http://localhost:8084/key-value-store/key1')
        responseInJson = response.json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(responseInJson['value'], 'value1')

        # get the value of the new key from replica2 after putting a new key
        response = requests.get('http://localhost:8084/key-value-store/key2')
        responseInJson = response.json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(responseInJson['value'], 'value2')

if __name__ == '__main__':
    unittest.main()

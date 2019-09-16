import socket
import pickle
import uuid
import json
from pathlib import Path
from threading import Thread
from TRM.topology_manager import TopologyManager
from SEX.service_execution import ServiceExecution
from RT.runtime import RunTime
from API.api import API


class Agent:

    def __init__(self, node_info):
        self.agents_alive = {}
        self.node_info = node_info
        self.services = [] # list of services ids {'id': service_id}
        self.generated_services_id = []
        self.services_results = []
        self.my_services_results = []
        self.service_execution = ServiceExecution(self)
        self.runtime = RunTime(self)
        self.topology_manager = None
        self.API = API(self, host=self.node_info["myIP"])
        self.get_attributes()
        # if self.node_info["role"] != "cloud_agent":
        #     self.API.register_to_leader()
        # else:
        #     self.API.register_cloud_agent()
        self.topology_manager = TopologyManager(self, self.node_info["ipDB"], self.node_info["portDB"])
        self.API.start(silent_access=True)


    def get_attributes(self):
        config_file = Path("./config/agent.conf")
        if config_file.is_file():
            with open("./config/agent.conf") as fp:
                content = json.load(fp)
                for key, value in content.items():
                    self.node_info[key] = value

######################### TRM OPERATIONS ##########################

    def register_to_DB(self):
        status, id = self.topology_manager.register(self.node_info)
        if status == 200:
            #print("Registrado en la base de datos con id {}".format(id))
            self.node_info['nodeID'] = id.zfill(10)
        else:
            pass
            #print("El agent no se ha registrado en la base de datos")


    def update_DB_info(self):
        updated = self.topology_manager.update(self.node_info)
        if updated != 200:
            pass
            #print("El nodo {} no se ha podido actualizar".format(self.node_info['myIP']))
        else:
            pass
            #print("El nodo {} se ha actualizado correctamente".format(self.node_info['myIP']))



    def remove_from_DB(self):
        deleted = self.topology_manager.delete(self.node_info['myIP'])
        if deleted != 200:
            pass
            #print("El nodo {} no se ha podido borrar de la base de datos".format(self.node_info['myIP']))
        else:
            pass
            #print("El nodo {} se ha borrado correctamente de la base de datos".format(self.node_info['myIP']))


######################### TRM OPERATIONS ##########################


######################## SOCKET OPERATIONS ########################

    def register_to_leader(self, leaderIP, port):
        self.node_info['leaderIP'] = leaderIP
        self.node_info['port'] = port
        self.socket_leader.close()
        self.socket_alive.close()
        try:
            self.socket_leader =  socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket_alive =  socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket_leader.connect((leaderIP, port))
            self.socket_alive.connect((leaderIP, port+1))
            self.send_register_message()
            #print("Me he conectado con el agent")
        except Exception as e:
            pass
            # print(e)
            #print("El agente no se ha podido conectar al leader")

    def send_register_message(self):
        attributes = {
            'type' : "register",
            'id': self.node_info['nodeID'],
            'port': self.node_info['port'],
            'CPU': self.node_info['cpu'],
            'RAM': self.node_info['ram'],
            'id_group': ''
        }
        self.socket_leader.send(pickle.dumps(attributes))
        self.socket_alive.send(pickle.dumps(attributes))

    def send_message(self, message):
        self.socket_leader.send(message.encode())

    def send_dict(self, dict):
        # #print("Envio al leader {}".format(dict.items()))
        self.socket_leader.send(pickle.dumps(dict))

    def close_leader(self):
        self.socket_leader.close()

    def receive_messages(self):
        th_receive_dicts = Thread(target=self.receive_dicts)
        th_receive_dicts.start()

    def receive_dicts(self):
        while True:
            dict = pickle.loads(self.socket_leader.recv(4096))
            self.process_received_dict(dict)

    def process_received_dict(self, dict):
        if dict["type"] == "service":
            self.services.append(dict)
            # #print("He recibido service {}".format(dict.items()))
        elif dict["type"] == "service_result":
            # TODO IF PARA LOS DIFERENTES ESTADOS (UNATTENDEDED...)
            #print("He recibido resultado {} del servicio".format(dict.get("output")))
            pass

######################## SOCKET OPERATIONS ########################

    def add_service(self, service_id, params=None):
        service_info = {
            "type": "service",
            "id": self.generate_service_id(),
            'service_id': service_id,
            'agent_id': self.node_info['nodeID']
        }
        if params:
            service_info['params'] = params
        self.generated_services_id.append(service_info["id"])
        self.services.append(service_info)
        return service_info["id"]


############################## UTILS ##############################

    def generate_service_id(self):
        random_id = uuid.uuid4()
        if random_id in self.generated_services_id:
            return self.generate_service_id()
        return random_id

############################## UTILS ##############################

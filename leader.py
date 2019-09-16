import socket
import random
import pickle
import time
import errno
from threading import Thread
from agent import Agent



class Leader(Agent):

    MAX_NUM_AGENTS = 100
    CLOUD_IP = '10.1.136.179'
    CLOUD_PORT = 5000

    def __init__(self, node_info):
        super().__init__(node_info)
        self.agents = {}
        self.agents_alive = {}
        self.socket_agents = self.bind_connection(self.node_info['myIP'], self.node_info['port'])
        self.socket_alive_agents = self.bind_connection(self.node_info['myIP'], self.node_info['port']+1)
        self.accept_connections()
        self.check_alive_agents()
        self.receive_messages()


######################## SOCKET OPERATIONS ########################

    def bind_connection(self, ip, port):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind((ip, port))
        s.listen(self.MAX_NUM_AGENTS)
        return s

    def accept_connections(self):
        th_accept = Thread(target=self.accept_agent_connections)
        th_accept_alive = Thread(target=self.accept_alive_connections)
        th_accept.start()
        th_accept_alive.start()

    def accept_agent_connections(self):
        while True:
            agent_connection, addr = self.socket_agents.accept()
            agent_id = str(self.generate_id())
            self.agents[agent_id] = agent_connection
            self.agents[agent_id].setblocking(0)
            #print("Nuevo agent con id " + agent_id)

    def accept_alive_connections(self):
        while True:
            agent_connection, addr = self.socket_alive_agents.accept()
            agent_id = str(self.generate_id())
            self.agents_alive[agent_id] = agent_connection
            self.agents_alive[agent_id].setblocking(0)

    def check_alive_agents(self):
        th_alive = Thread(target=self.th_check_alive_agents)
        th_alive.start()

    def th_check_alive_agents(self):
        while True:
            for agent_id in list(self.agents_alive):
                try:
                    self.agents_alive[agent_id].send("check-alive".encode())
                except IOError as e:
                    if e.errno == errno.EPIPE:
                        if agent_id in self.agents.keys() and agent_id in self.agents_alive.keys():
                            #print("He detectado que el agent {} esta desconectado".format(agent_id))
                            self.agents.pop(agent_id)
                            self.agents_alive.pop(agent_id)
                            self.topology_manager.update({'nodeID': agent_id, 'status': 0})
                            #print("He hecho update de {} a status 0".format(agent_id))
                except:
                    pass

    def receive_messages(self):
        th_messages = Thread(target=self.th_receive_messages)
        th_messages_alive = Thread(target=self.th_receive_messages_alive)
        th_messages.start()
        th_messages_alive.start()

    def th_receive_messages(self):
        while True:
            for agent_id in list(self.agents):
                dict = None
                try:
                    dict = pickle.loads(self.agents[agent_id].recv(4096))
                    self.process_dict(dict, agent_id)
                except Exception as e:
                    pass

    def th_receive_messages_alive(self):
        while True:
            for agent_id in list(self.agents_alive):
                dict = None
                try:
                    dict = pickle.loads(self.agents_alive[agent_id].recv(4096))
                    if dict["type"] == "register":
                        new_id = dict["id"]
                        self.agents_alive[new_id] = self.agents_alive[agent_id]
                        del self.agents_alive[agent_id]
                except Exception as e:
                    pass

    def process_dict(self, dict, agent_id):
        if dict["type"] == "register":
            # #print("Entro en register")
            new_id = dict["id"]
            self.agents[new_id] = self.agents[agent_id]
            del self.agents[agent_id]
            self.topology_manager.update({'nodeID': new_id, 'zone': self.node_info['zone']})
            # #print("He hecho update")
        elif dict["type"] == "service":
            # #print("He recibido el servicio {}".format(dict.items()))
            self.services.append(dict)
        elif dict["type"] == "service_result":
            #print("Resultado: ", dict.get("output"))
            self.services_results.append(dict)

    def send_dict_to(self, dict, agent_id):
        self.agents[agent_id].send(pickle.dumps(dict))

######################## SOCKET OPERATIONS ########################

############################## UTILS ##############################

    def generate_id(self):
        seed = random.getrandbits(32)
        if seed not in self.agents:
            return seed
        else:
            return self.generate_id()

############################## UTILS ##############################

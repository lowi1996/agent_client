import json
import os.path
from agent import Agent
from leader import Leader


if os.path.exists("./config/device.config"):
	config = open("./config/device.config", "r")
	content = json.load(config)
	node_info = content["node_info"]

	if node_info["role"] == "agent":
		agent = Agent(node_info)
	elif node_info["role"] == "leader" or node_info["role"] == "cloud_agent":
		leader = Leader(node_info)
	
	while True:
		pass
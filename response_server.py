import threading
import socket
import select
import argparse
import json
import sys
import logging
import struct
import os
import functools
import time
import random
from subprocess import check_output
from ctypes import create_string_buffer

MAX_UDP_PACKET_SIZE = 1472
MAX_WORKERS = 70

def get_host_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
    finally:
        s.close()

    return ip

class SimpleUDPServer:
  def __init__(self, address, command_handler):
    self.addres = address 
    self.command_handler = command_handler
    self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    self.server_socket.bind(address)
    self.descriptors = [self.server_socket,]
    self.response_client_list = set()
  
  def server_address(self):
    return self.server_socket.getsockname()
  
  def run(self):
    while True:
      (sread, swrite, sexc) = select.select(self.descriptors, [], [])
      for client in sread:
        try:
          data = client.recv(MAX_UDP_PACKET_SIZE)
          if len(data):
            # push into client_buffer
            self.request_handler(client, data)
        except OSError as err:
          logging.error(err)
          self.descriptors.remove(client)

  def request_handler(self, client, data):
    request = self.command_handler.parse_client_request(data)
    # failed to decode, fill to buffer
    if not request:
      return

    logging.debug(request)
    if request['type'] == CommandHandler.ClientActive:
      if client not in self.descriptors:
        self.descriptors.add(client)
        logging.info("%s:%s connect." % client.getpeername())
    else if request['type'] == CommandHandler.ClientResponse:
      response = self.command_handler.build_server_response()
      client.sendall(response)
    else
      logging.error("Unknow type %d for udp server!!"%request['type'])
  
  def exec_request(self, client):
    request = self.command_handler.build_exec_request()
    while True:
      client.sendall(request)
      try:
        self.socket.settimeout(2.0)
        response = self.socket.recv(MAX_UDP_PACKET_SIZE)
        response = self.command_handler.parse_client_request(response)
        if response['type'] == CommandHandler.ClientResponse
          logging.info("Get connection response from server")
          response = self.command_handler.build_server_response()
          client.sendall(response)
          break;
      except socket.timeout:
        logging.info("Get server response timeout!")
    return client

  def broadcast(self, data, ignore_clients=[]):
    try:
      for client in self.descriptors:
        if client != self.server_socket and (client not in ignore_clients):
          client.sendall(data)
    except OSError as err:
      logging.error(err)

  def start_request(self):
    executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
    all_task = []
    for client in self.descriptors:
      all_task.append(executor.submit(exec_request, client))
    logging.debug(len(all_task))
    for future in as_completed(all_task):
      client = future.result()

    self.transfer_end_time = time.time()
    logging.info("Notify all finished, spend %f!"%(self.transfer_end_time-self.transfer_start_time))
    self.response_client_list = []
    self.total_time = 0.0

clas SimpleUDPClien :
  def __init__(self, command_handler):
    self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    self.command_handler = command_handler

  def __del__(self):
    self.socket.close()

  def connect(self, server_address, server_port):
    self.server_address = server_address
    self.server_port = server_port
    try:
      self.socket.connect((self.server_address, self.server_port))
    except OSError as msg:
      logging.error("connect failed, error: %s" % msg)
      return False
    self.send_connect_request():
    return True

  def send_connect_request(self):
    request = self.command_handler.build_connection_request()
    while True:
      self.socket.sendall(request)
      self.socket.settimeout(2.0)
      try:
        response = self.socket.recv(MAX_UDP_PACKET_SIZE)
        response = self.command_handler.parse_server_request(response)
        if response['type'] == CommandHandler.ServerResponse
          logging.info("Get connection response from server")
          break;
        client_response = self.command_handler.build_client_response()
        self.socket.sendall(client_response)
      except socket.timeout:
        logging.info("Get server response timeout!")

  def wait_transfer_start(self):
    while True:
      data = self.socket.recv(MAX_UDP_PACKET_SIZE)
      if len(data):
        result = self.command_handler.parse_server_request(data)
        if result["type"] == CommandHandler.Exec:
          self.exec_response()
          break;
        elif if result["type"] == CommandHandler.ServerResponse:
          logging.error("Get server response!")
          client_response = self.command_handler.build_client_response()
          self.socket.sendall(client_response)
      else:
        logging.error("Can't receive data!")
        break;
    
  def exec_response(self):
    client_response = self.command_handler.build_client_response()
    while True:
      self.socket.sendall(request)
      self.socket.settimeout(2.0)
      try:
        response = self.socket.recv(MAX_UDP_PACKET_SIZE)
        response = self.command_handler.parse_server_request(response)
        if response['type'] == CommandHandler.ServerResponse
          logging.info("Get Exec response from server")
          break;
      except socket.timeout:
        logging.info("Get server response timeout!")

# Singletone class
class CommandHandler(object):
  _instance_lock = threading.Lock()
  ClientActive = 0
  Exec = 1
  ClientResponse = 2
  ServerResponse = 3

  def __init__(self):
    self.address_group = []

  def __new__(cls, *args, **kwargs):
    if not hasattr(CommandHandler, "_instance"):
      with CommandHandler._instance_lock:
        if not hasattr(CommandHandler, "_instance"):
          CommandHandler._instance = object.__new__(cls)
    return CommandHandler._instance

  def parse_server_request(self, data):
    resp = json.load(data) 
    logging.debug(resp)
    return resp

  def parse_client_request(self, data):
    resp = json.load(data) 
    logging.debug(resp)
    return resp

  def build_connection_request(self)
    req = {"type": CommandHandler.ClientActive}
    return bytes(json.dumps(req).encode('utf-8'))

  def build_server_response(self)
    req = {"type": CommandHandler.ServerResponse}
    return bytes(json.dumps(req).encode('utf-8'))

  def build_client_response(self)
    req = {"type": CommandHandler.ClientResponse}
    return bytes(json.dumps(req).encode('utf-8'))

  def build_exec_request(self):
    req = {"type": CommandHandler.Exec}
    return bytes(json.dumps(req).encode('utf-8'))

  
class MulticastBroker:
  def __init__(self, multicast_address):
    self.multicast_host, self.multicast_port = multicast_address
    self.multicast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    self.data_handler = self.default_data_handler
    self.interfaces = [get_host_ip(),]
#    self.interfaces = check_output(['hostname','--all-ip-addresses'])[:-1]
#    self.interfaces = str(self.interfaces, encoding="utf-8").split(' ')[:-1]
    self.interfaces.append('0.0.0.0')
    self.stop = False
    logging.debug(self.interfaces)

  def default_data_handler(self, data):
    pass
  
  def set_data_handler(self, handler):
    self.data_handler = handler

  def stop_receive(self):
    self.stop = True

  def receive_loop(self):
    self.stop = False
    receive_sockets = []
    for interface in self.interfaces:
      sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
      sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
      sock.bind((interface, self.multicast_port))
      mreq = struct.pack('4s4s', socket.inet_aton(self.multicast_host), socket.inet_aton(interface))
      sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
      receive_sockets.append(sock) 

    loop = True
    while loop and not self.stop:
      (sread, swrite, sexc) = select.select(receive_sockets, [], [])
      for sock in sread:
        try:
          data = sock.recv(MAX_UDP_PACKET_SIZE)
#          logging.debug("Multicast recv %d bytes", len(data))
          if len(data):
            if(self.data_handler(data) == False):
              logging.debug("Exit recve_loop")
              loop = False
              break;
        except OSError as err:
          logging.error(err)
          loop = False

    for sock in receive_sockets:
      sock.close()

  
  def send(self, data):
    # for test, random throw packet
    #if random.randint(0,99) < 20:
    #  return

    if len(data) > MAX_UDP_PACKET_SIZE:
      logging.warning("Data send by multicast should less than %d, now is %d" %(MAX_UDP_PACKET_SIZE, len(data)))
#    logging.debug("Multicast send %d bytes", len(data))
    self.multicast_socket.sendto(data, (self.multicast_host, self.multicast_port))

def input_handler(input, server, transfer):
  command = input
  if command == "start":
    server.start_request()

def main(args):
  LOG_FORMAT = "[%(asctime)s:%(levelname)s:%(funcName)s]  %(message)s"
  log_level = logging.INFO
  if args.debug:
    log_level = logging.DEBUG

  logging.basicConfig(level=log_level, format=LOG_FORMAT)

  if not args.address:
    logging.critical("Need server address as input, like \"127.0.0.1:60001\"")
    arg_parser.print_help()
    exit()

  server_ip, server_port = args.address.split(':')

  if args.client:
    command_client = SimpleUDPClient(CommandHandler())
    if command_client.connect(server_ip, int(server_port)):
      while True:
        file_info = command_client.wait_transfer_start()
        logging.debug("Get start signal")
      
  elif args.server:
    server = SimpleUDPServer((server_ip, int(server_port)), CommandHandler())
    server_ip, server_port = server.server_address()

    server_thread = threading.Thread(target=server.run)
    server_thread.daemon = True
    server_thread.start()
    logging.info("Server %s:%d loop running in thread: %s"%(server_ip, server_port, server_thread.name))

    for line in sys.stdin:
      is_exit = input_handler(line.strip('\n'), server, transfer)
      if is_exit:
        break;

    server_thread.join()

if __name__ == "__main__":

  arg_parser = argparse.ArgumentParser(description="manual to this script")
  arg_parser.add_argument('-c', "--client", help="run in client mode",
                          action="store_true")
  arg_parser.add_argument('-s', "--server", help="run in server mode",
                          action="store_true")
  arg_parser.add_argument('-d', "--debug", help="enable debug mode",
                          action="store_true")
  arg_parser.add_argument('-a', "--address", help="server address", 
                          type=str)
  args = arg_parser.parse_args()

  if not args.client and not args.server:
      logging.warning("Run in qpython as client mode")
      args.client=True
      args.address="172.18.93.85:60000"
      args.debug=True
  main(args)



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
    self.response_client_list = []
  
  def server_address(self):
    return self.server_socket.getsockname()
  
  def run(self):
    while True:
      (sread, swrite, sexc) = select.select(self.descriptors, [], [])
      for client in sread:
        try:
          data, sender = client.recvfrom(MAX_UDP_PACKET_SIZE)
          logging.debug("Receive from %s:%d"%sender)
          if len(data):
            # push into client_buffer
            self.request_handler(sender, data)
        except OSError as err:
          logging.error(err)
          self.descriptors.remove(client)

  def request_handler(self, client, data):
    request = self.command_handler.parse_client_request(data)
    # failed to decode, fill to buffer
    if not request:
      return

    if request['type'] == CommandHandler.ClientActive:
      if client not in self.response_client_list:
        logging.info("%s:%s connect." % client)
        self.response_client_list.append(client)
        response = self.command_handler.build_server_response()
        self.server_socket.sendto(response, client)

    elif request['type'] == CommandHandler.ClientResponse:
      response = self.command_handler.build_server_response()
      if client in self.response_client_list:
        self.server_socket.sendto(response, client)

    else:
      logging.error("Unknow type %d for udp server!!"%request['type'])
  
  def exec_request(self, client):
    request = self.command_handler.build_exec_request()
    while True:
      self.server_socket.sendto(request, client)
      try:
        self.server_socket.settimeout(2.0)
        response = self.server_socket.recv(MAX_UDP_PACKET_SIZE)
        response = self.command_handler.parse_client_request(response)
        if response['type'] == CommandHandler.ClientResponse:
          logging.info("Get exec response from server")
          response = self.command_handler.build_server_response()
          self.server_socket.sendto(response, client)
          break;
      except socket.timeout:
        logging.info("Get server response timeout!")
    return client

  def start_request(self):
#    executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
#    all_task = []
    for client in self.response_client_list:
      self.exec_request(client)
#      logging.debug("%s:%d Exec request"%client)
#      all_task.append(executor.submit(exec_request, client))
#    logging.debug(len(all_task))
#    for future in as_completed(all_task):
#      client = future.result()
    logging.info("Notify all finished!")

class SimpleUDPClient:
  def __init__(self, command_handler):
    self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    self.command_handler = command_handler

  def __del__(self):
    self.socket.close()

  def connect(self, server_address, server_port, client_port):
    self.server_address = server_address
    self.server_port = server_port
    try:
      self.socket.bind((get_host_ip(), client_port))
      self.socket.connect((self.server_address, self.server_port))
    except OSError as msg:
      logging.error("connect failed, error: %s" % msg)
      return False
    self.send_connect_request()
    return True

  def send_connect_request(self):
    request = self.command_handler.build_connection_request()
    while True:
      self.socket.sendall(request)
      self.socket.settimeout(2.0)
      try:
        response = self.socket.recv(MAX_UDP_PACKET_SIZE)
        response = self.command_handler.parse_server_request(response)
        logging.debug("Response %s"%response)
        if response['type'] == CommandHandler.ServerResponse:
          logging.info("Get connection response from server")
          break;
        client_response = self.command_handler.build_client_response()
        self.socket.sendall(client_response)
      except socket.timeout:
        logging.info("Get server response timeout!")

  def wait_transfer_start(self):
    self.socket.settimeout(None)
    while True:
      data = self.socket.recv(MAX_UDP_PACKET_SIZE)
      if len(data):
        result = self.command_handler.parse_server_request(data)
        if result["type"] == CommandHandler.Exec:
          self.exec_response()
          break;
        elif result["type"] == CommandHandler.ServerResponse:
          logging.error("Get server response!")
          client_response = self.command_handler.build_client_response()
          self.socket.sendall(client_response)
      else:
        logging.error("Can't receive data!")
        break;
    
  def exec_response(self):
    client_response = self.command_handler.build_client_response()
    while True:
      self.socket.sendall(client_response)
      self.socket.settimeout(2.0)
      try:
        response = self.socket.recv(MAX_UDP_PACKET_SIZE)
        response = self.command_handler.parse_server_request(response)
        if response['type'] == CommandHandler.ServerResponse:
          logging.info("Get Exec response from server")
          break;
        elif response['type'] == CommandHandler.Exec:
            self.exec_response()
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
    resp = json.loads(data.decode('utf-8')) 
    logging.debug(resp)
    return resp

  def parse_client_request(self, data):
    resp = json.loads(data.decode('utf-8')) 
    logging.debug(resp)
    return resp

  def build_connection_request(self):
    req = {"type": CommandHandler.ClientActive}
    return bytes(json.dumps(req).encode('utf-8'))

  def build_server_response(self):
    req = {"type": CommandHandler.ServerResponse}
    return bytes(json.dumps(req).encode('utf-8'))

  def build_client_response(self):
    req = {"type": CommandHandler.ClientResponse}
    return bytes(json.dumps(req).encode('utf-8'))

  def build_exec_request(self):
    req = {"type": CommandHandler.Exec}
    return bytes(json.dumps(req).encode('utf-8'))

def input_handler(input, server):
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
    if not args.port:
      logging.critical("Need server address as input, like \"127.0.0.1:60001\"")
      arg_parser.print_help()
      exit()

    command_client = SimpleUDPClient(CommandHandler())
    if command_client.connect(server_ip, int(server_port), args.port):
      while True:
        file_info = command_client.wait_transfer_start()
        logging.debug("Get start signal")
        cmd = "iperf -c 172.18.146.19 -b 5m -t 1 -i 1 -p 6666"
        os.system(cmd)
      
  elif args.server:
    server = SimpleUDPServer((server_ip, int(server_port)), CommandHandler())
    server_ip, server_port = server.server_address()

    server_thread = threading.Thread(target=server.run)
    server_thread.daemon = True
    server_thread.start()
    logging.info("Server %s:%d loop running in thread: %s"%(server_ip, server_port, server_thread.name))

    for line in sys.stdin:
      is_exit = input_handler(line.strip('\n'), server)
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
  arg_parser.add_argument('-p', "--port", help="client port", 
                          type=int)
  args = arg_parser.parse_args()

  if not args.client and not args.server:
      logging.warning("Run in qpython as client mode")
      args.client=True
      args.address="172.18.93.85:60000"
      args.debug=True
  main(args)



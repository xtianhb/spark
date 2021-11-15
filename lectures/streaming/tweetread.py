import tweepy
import socket
import json


import configparser
config = configparser.ConfigParser()
config.read('keys/twitter.txt')


# Set up your credentials
keys={}


class cTweetsListener(tweepy.Stream):

    def SetSocket(self, Socket):
        self.client_socket = Socket
    
    def on_status(self, status):
        print(status.text)

    def on_data(self, data):
        try:
            msg = json.loads( data )
            print( msg['text'].encode('utf-8') )
            self.client_socket.send( msg['text'].encode('utf-8') )
            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True

    def on_error(self, status):
        print(status)
        return True


def sendData(c_socket):

    MyStream = cTweetsListener( 
            keys["api_key"], keys["api_key_secret"],
            keys["token"], keys["token_secret"])

    MyStream.SetSocket(c_socket)

    MyStream.filter(track=['elecciones', 'argentina'])

    return

    
def read_keys():

    global keys

    keys["api_key"] = config['keys']['api_key']
    keys["api_key_secret"] = config['keys']['api_key_secret']
    keys["token"] = config['keys']['access_token']
    keys["token_secret"] = config['keys']['access_token_secret']

    
if __name__ == "__main__":
  
    read_keys()

    s = socket.socket()         # Create a socket object
    host = "127.0.0.1"          # Get local machine name
    port = 5555                 # Reserve a port for your service.
    s.bind((host, port))        # Bind to the port

    print("Listening on port: %s" % str(port))

    s.listen(5)                 # Now wait for client connection.
    c, addr = s.accept()        # Establish connection with client.

    print( "Received request from: " + str( addr ) )

    sendData( c )

